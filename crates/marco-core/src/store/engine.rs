//! The page store every reader shares: the file registry, the page cache and
//! the I/O behind it, and the read loop over them ([`Engine::pass`],
//! [`Engine::read_blocking`]).
//!
//! An engine made with [`Engine::new`] keeps whole files in memory
//! ([`Engine::add`]: an in-memory graph's, and tests'), and never misses a
//! page they have. One made with [`Engine::with_config`] registers snapshot files
//! ([`Engine::register`]): their pinned sections are read at open, checked
//! and kept, and their paged sections are read a page at a time into a cache
//! that never holds more than `--memory`. A pass reads only what's in memory;
//! [`Engine::fetch_blocking`] reads what a pass missed, once per page however
//! many readers want it, checking every page against its sum.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::*};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use rustc_hash::FxHashMap;

use crate::read::{
    BadPage, Found, Guard, Hold, LookupCounts, Misses, PageSet, ReadCx, ReadStats, Source, StorageError, UsedBytes, R,
};
use crate::snapshot::{sums_section, PagedFooter, SectionKind, TRAILER};
use crate::store::cache::{Cache, CacheBytes, Entry, FileTable, GROUP, NO_FRAME};
use crate::store::io::{
    default_device, AlignedBuf, DevFile, Device, IoPool, Lane, PageRead, Rate, Run, TokenBucket, MAX_RUN,
};
use crate::store::page::{page_sum, FileId, Page, PageId, PAGE};

/// How an engine with a page cache is set up.
#[derive(Clone)]
pub struct Config {
    /// The store's budget in bytes (`--memory`): pinned sections, frames,
    /// frames waiting on the epoch and the engine's tables.
    pub memory: u64,
    pub device: Arc<dyn Device>,
    /// Threads doing reads.
    pub io_threads: usize,
    /// Reads each of the two queues holds before a submit waits.
    pub queue: usize,
    /// The background lane's pace (`--background-io`).
    pub background: Rate,
    /// How long making room for a borrow may take.
    pub borrow_timeout: Duration,
    /// Caps the frames below what the budget allows: for tests that want
    /// eviction with small files.
    #[doc(hidden)]
    pub max_frames: Option<u32>,
}

impl Config {
    pub fn new(memory: u64) -> Config {
        Config {
            memory,
            device: default_device(),
            io_threads: 64,
            queue: 4096,
            background: Rate::default(),
            borrow_timeout: Duration::from_secs(30),
            max_frames: None,
        }
    }

    pub fn device(mut self, d: Arc<dyn Device>) -> Config {
        self.device = d;
        self
    }
}

/// What an engine's entry for a file holds.
pub(crate) enum EntryData {
    /// The whole file in memory.
    Resident { bytes: Vec<u8> },
    /// A FORMAT 6 file read through the cache.
    Paged(Arc<FileState>),
}

struct PinnedAt {
    name: String,
    buf: usize,
    off: usize,
    len: usize,
}

/// A paged section's pages and where their sums are.
struct PagedRange {
    start: u32,
    end: u32,
    tag: u64,
    section: usize,
    /// Its sums: the buffer and byte offset in [`FileState::sums`].
    sums: (usize, usize),
}

pub(crate) struct FileState {
    name: String,
    dev: Arc<dyn DevFile>,
    footer: PagedFooter,
    pages: u32,
    paged: Vec<PagedRange>,
    /// The `.sums` sections, the only pinned ones a file keeps for as long
    /// as it's registered; the rest are [`PinnedSections`].
    sums: Vec<AlignedBuf>,
}

impl FileState {
    fn range(&self, page: u32) -> Option<&PagedRange> {
        let i = self.paged.partition_point(|r| r.start <= page).checked_sub(1)?;
        let r = &self.paged[i];
        (page < r.end).then_some(r)
    }

    /// Page `page`'s seed and expected sum, if it's in a paged section.
    fn sum(&self, page: u32) -> Option<(u64, u32)> {
        let r = self.range(page)?;
        let at = r.sums.1 + 4 * (page - r.start) as usize;
        let sum = u32::from_le_bytes(self.sums[r.sums.0][at..at + 4].try_into().expect("4 bytes"));
        Some((self.footer.salt ^ r.tag ^ page as u64, sum))
    }

    fn bad(&self, page: u32) -> BadPage {
        BadPage {
            file: self.name.clone(),
            section: self.range(page).map(|r| self.footer.sections[r.section].name.clone()),
            page,
        }
    }
}

/// A FORMAT 6 file registered with an engine. Its page sums and slots stay
/// in memory, counted in the budget, until the last handle drops; then its
/// cached pages are freed (a pinned one at its last unpin) and its id is
/// never used again. Its other pinned sections are read separately
/// ([`Engine::pinned`]), only for as long as something reads them.
pub struct PagedFile {
    inner: Arc<Inner>,
    id: FileId,
    key: PathBuf,
    state: Arc<FileState>,
    borrowed: u64,
    pinned: Mutex<Weak<PinnedSections>>,
}

impl PagedFile {
    pub fn id(&self) -> FileId {
        self.id
    }

    /// Its name in the snapshot directory.
    pub fn name(&self) -> &str {
        &self.state.name
    }

    pub fn footer(&self) -> &PagedFooter {
        &self.state.footer
    }

    /// Paged sections' pages: what loading the file whole takes.
    pub fn paged_pages(&self) -> u64 {
        self.state.paged.iter().map(|r| (r.end - r.start) as u64).sum()
    }

    /// Page `i` of the paged section `name`.
    pub fn page(&self, name: &str, i: u32) -> Option<PageId> {
        let s = self.state.footer.section(name)?;
        match s.kind {
            SectionKind::Paged { pages, .. } if i < pages => Some(PageId::new(self.id, s.first_page() + i)),
            _ => None,
        }
    }

    /// Pages in the file.
    pub fn pages(&self) -> u32 {
        self.state.pages
    }

    /// Bytes this file holds against the budget: its page sums and its
    /// slots.
    pub fn borrowed(&self) -> u64 {
        self.borrowed
    }

    /// Whether its pinned sections are in memory now.
    pub fn has_pinned(&self) -> bool {
        self.pinned.lock().unwrap().strong_count() > 0
    }
}

/// A file's pinned sections other than its page sums: what a shard's newest
/// file and the directory are read through. Counted in the budget until the
/// last handle drops, so an older patch's index and heads go once no store
/// reads it as its shard's newest.
pub struct PinnedSections {
    at: Vec<PinnedAt>,
    bufs: Vec<AlignedBuf>,
    cache: Option<Arc<Cache>>,
    borrowed: u64,
}

impl PinnedSections {
    /// A section's bytes.
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        let p = self.at.iter().find(|p| p.name == name)?;
        Some(&self.bufs[p.buf][p.off..p.off + p.len])
    }

    /// Where section `name` is: the buffer it was read into
    /// ([`PinnedSections::buf`]) and its bytes there. Sections with nothing
    /// else between them share a buffer, in file order.
    pub fn at(&self, name: &str) -> Option<(usize, std::ops::Range<usize>)> {
        let p = self.at.iter().find(|p| p.name == name)?;
        Some((p.buf, p.off..p.off + p.len))
    }

    pub fn buf(&self, i: usize) -> &[u8] {
        &self.bufs[i]
    }

    /// Each section's name and length.
    pub fn sections(&self) -> impl Iterator<Item = (&str, usize)> {
        self.at.iter().map(|p| (p.name.as_str(), p.len))
    }

    /// Bytes they hold against the budget.
    pub fn borrowed(&self) -> u64 {
        self.borrowed
    }
}

impl Drop for PinnedSections {
    fn drop(&mut self) {
        if let Some(c) = &self.cache {
            c.unborrow(self.borrowed);
        }
    }
}

impl std::fmt::Debug for PinnedSections {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedSections").field("sections", &self.at.len()).field("bytes", &self.borrowed).finish()
    }
}

impl std::fmt::Debug for PagedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PagedFile").field("id", &self.id).field("name", &self.state.name).finish()
    }
}

impl Drop for PagedFile {
    fn drop(&mut self) {
        let inner = &self.inner;
        {
            let mut m = inner.by_path.lock().unwrap();
            if m.get(&self.key).is_some_and(|w| w.strong_count() == 0) {
                m.remove(&self.key);
            }
        }
        if let Some(cache) = &inner.cache {
            let guard = crossbeam_epoch::pin();
            if let Some(e) = inner.table.remove(self.id, &guard) {
                e.dropped.store(true, SeqCst);
                let mut retire = Vec::new();
                for s in e.slots.iter() {
                    let f = s.swap(NO_FRAME, SeqCst);
                    if f != NO_FRAME && cache.drop_frame(f) {
                        retire.push(f);
                    }
                }
                cache.retire_dropped(&guard, retire);
            }
            inner.bad.lock().unwrap().retain(|p, _| p.file != self.id);
            cache.unborrow(self.borrowed);
        }
    }
}

/// Memory taken from the budget for something outside the cache; returned
/// when dropped.
pub struct Borrow {
    cache: Arc<Cache>,
    bytes: u64,
}

impl Borrow {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for Borrow {
    fn drop(&mut self) {
        self.cache.unborrow(self.bytes);
    }
}

/// How long a read past the cache waits for frames of its own before it
/// reads through the cache instead.
const PAST_WAIT: Duration = Duration::from_millis(50);

/// Frames staged in a [`Stage`] at a time before it allocates more.
const STAGE_CHUNK: usize = 64;

/// How long a [`Stage`] waits for frames before it lets a page go.
const STAGE_WAIT: Duration = Duration::from_millis(2);

/// Pages a writer puts into the cache as it writes them (write-through): a
/// checkpoint's patches and new bases, a compaction's base. Each page is
/// copied into a frame of the budget that nobody can read until
/// [`Engine::publish`] makes it its file's page, once the file is written
/// and registered. A stage holds at most a quarter of the cache's frames,
/// and gives up a page rather than wait more than a moment for a frame, so
/// it never holds readers up; a page it lets go is read from disk on its
/// first miss like any other. Dropping it unpublished frees its frames.
pub struct Stage {
    inner: Arc<Inner>,
    cap: usize,
    /// (page of the file, frame)
    pages: Vec<(u32, u32)>,
    spare: Vec<u32>,
    skipped: u64,
    /// No frame came: the rest of the file isn't staged.
    gave_up: bool,
    /// Budget borrowed when the stage began for its file's registration
    /// (sums and slots), since a borrow can't take back the groups its own
    /// frames sit in ([`Engine::register_staged`]).
    prepaid: u64,
}

impl Stage {
    /// Copies page `page` of the file being written into a frame, if one
    /// comes within a moment.
    pub fn put(&mut self, page: u32, bytes: &Page) {
        let cache = self.inner.cache.as_ref().expect("a stage has a cache");
        if self.gave_up || self.pages.len() >= self.cap {
            self.skipped += 1;
            return;
        }
        if self.spare.is_empty() {
            let want = STAGE_CHUNK.min(self.cap - self.pages.len());
            self.spare = cache.alloc_staged(&self.inner.table, want, std::time::Instant::now() + STAGE_WAIT);
        }
        let Some(f) = self.spare.pop() else {
            // frames are short: waiting for each page would slow the write down
            self.gave_up = true;
            self.skipped += 1;
            return;
        };
        // SAFETY: an allocated, unpublished frame is this stage's alone.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), cache.frame_ptr(f), PAGE) };
        self.pages.push((page, f));
    }

    /// Pages staged so far.
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }
}

impl Drop for Stage {
    fn drop(&mut self) {
        let Some(cache) = &self.inner.cache else { return };
        // pages never published (the file wasn't registered) were let go too
        let skipped = self.skipped + self.pages.len() as u64;
        let mut frames: Vec<u32> = self.pages.drain(..).map(|x| x.1).collect();
        frames.append(&mut self.spare);
        cache.unstage(&frames);
        cache.free_unpublished(&frames);
        cache.unborrow(self.prepaid);
        if skipped > 0 {
            self.inner.counters.write_through_skipped.fetch_add(skipped, Relaxed);
        }
    }
}

impl std::fmt::Debug for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stage").field("pages", &self.pages.len()).field("skipped", &self.skipped).finish()
    }
}

/// One read in progress: every other reader that misses the page waits on it
/// instead of reading it again.
struct Flight {
    done: Mutex<Option<Result<(), StorageError>>>,
    cv: Condvar,
}

impl Flight {
    fn wait(&self) -> Result<(), StorageError> {
        let mut d = self.done.lock().unwrap();
        loop {
            if let Some(r) = &*d {
                return r.clone();
            }
            d = self.cv.wait(d).unwrap();
        }
    }

    fn finish(&self, r: Result<(), StorageError>) {
        *self.done.lock().unwrap() = Some(r);
        self.cv.notify_all();
    }
}

const FLIGHT_SHARDS: usize = 64;

type FlightShard = Mutex<FxHashMap<PageId, Arc<Flight>>>;
const STRIPES: usize = 16;

#[repr(align(128))]
#[derive(Default)]
struct Stripe {
    passes: AtomicU64,
    pages: AtomicU64,
    misses: AtomicU64,
    by: [[[AtomicU64; 4]; 7]; 3],
    used: [AtomicU64; 7],
}

#[derive(Default)]
struct Counters {
    stripes: [Stripe; STRIPES],
    fetched: AtomicU64,
    joins: AtomicU64,
    checksum: AtomicU64,
    eio: AtomicU64,
    short: AtomicU64,
    stalled: AtomicU64,
    written_through: AtomicU64,
    write_through_skipped: AtomicU64,
    past_pages: AtomicU64,
}

/// One clean pass in this many, per thread, counts the bytes it decoded,
/// scaled back up: measuring what a decode used costs about as much as the
/// decode, which on every pass was 15% of a hit-only load's CPU.
pub const USED_SAMPLE: u64 = 32;

fn sample_used() -> bool {
    use std::cell::Cell;
    thread_local!(static N: Cell<u32> = const { Cell::new(0) });
    N.with(|n| {
        let k = n.get().wrapping_add(1);
        n.set(k);
        k % USED_SAMPLE as u32 == 0
    })
}

fn stripe() -> usize {
    use std::cell::Cell;
    thread_local!(static ME: Cell<usize> = const { Cell::new(usize::MAX) });
    static NEXT: AtomicU64 = AtomicU64::new(0);
    ME.with(|m| {
        if m.get() == usize::MAX {
            m.set(NEXT.fetch_add(1, Relaxed) as usize % STRIPES);
        }
        m.get()
    })
}

/// The engine's counters and where its budget is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EngineStats {
    pub bytes: CacheBytes,
    /// Passes run, page lookups in them, and lookups that missed.
    pub passes: u64,
    pub pages: u64,
    pub misses: u64,
    /// Pages read into frames.
    pub fetched: u64,
    /// Read calls made on the device, request and background.
    pub reads: u64,
    /// Pages read past the cache on the background lane.
    pub background_pages: u64,
    /// Misses that waited on another reader's read of the same page.
    pub joins: u64,
    pub evict_small: u64,
    pub evict_main: u64,
    pub ghost_hits: u64,
    /// Fetches that found no free frame and waited.
    pub frame_waits: u64,
    pub flushes: u64,
    /// Frame waits long enough that the reader gave up its own pins.
    pub stalls: u64,
    /// Groups given back to the kernel.
    pub released: u64,
    pub checksum: u64,
    pub eio: u64,
    pub short: u64,
    pub eio_retried: u64,
    pub bad_pages: u64,
    /// Requests whose missing set stopped shrinking for 16 rounds.
    pub stalled_requests: u64,
    /// Nanoseconds the I/O threads spent in reads.
    pub io_nanos: u64,
    /// Pages a writer put into the cache as it wrote them ([`Stage`]), and
    /// pages it couldn't (no frame within its wait, or past its cap).
    pub written_through: u64,
    pub write_through_skipped: u64,
    /// Pages read past the cache into a job's own memory ([`Hold::past`]).
    pub past_pages: u64,
    /// Lookups by who read, structure and how they came out,
    /// `[source][structure][found]` ([`Source`], [`Structure`], [`Found`]).
    pub lookups: [[[u64; 4]; 7]; 3],
    /// Bytes requests' clean passes decoded, by structure
    /// ([`ReadCx::bytes_used`]), estimated from one pass in [`USED_SAMPLE`].
    pub bytes_used: UsedBytes,
}

struct Inner {
    table: FileTable,
    next_id: AtomicU32,
    cache: Option<Arc<Cache>>,
    io: Option<IoPool>,
    /// Paces background I/O: the I/O pool's background lane, and writes
    /// that ask ([`Engine::background`]).
    bucket: Arc<TokenBucket>,
    device: Option<Arc<dyn Device>>,
    borrow_timeout: Duration,
    by_path: Mutex<HashMap<PathBuf, Weak<PagedFile>>>,
    flights: Box<[FlightShard]>,
    bad: Mutex<FxHashMap<PageId, StorageError>>,
    /// Pages whose reads failed on the device (EIO past its retries, or
    /// short), with when: what the storm rule counts.
    failing: Mutex<std::collections::VecDeque<(std::time::Instant, PageId)>>,
    counters: Counters,
    /// Whether writes should read ahead ([`Engine::plan_writes`]).
    plan_writes: AtomicBool,
}

/// How far back [`Engine::failing_pages`] looks: the `/readyz` storm rule.
pub const STORM_WINDOW: Duration = Duration::from_secs(60);

/// Failures [`Inner::failing`] keeps at most: past the storm rule's 64.
const FAILING_KEPT: usize = 4096;

impl Inner {
    /// Counts a page that failed its read, and keeps device failures for
    /// the storm rule.
    fn failed(&self, p: PageId, e: &StorageError) {
        let c = &self.counters;
        match e {
            StorageError::Checksum(_) => &c.checksum,
            StorageError::Eio(_) => &c.eio,
            _ => &c.short,
        }
        .fetch_add(1, Relaxed);
        if matches!(e, StorageError::Eio(_) | StorageError::Short(_)) {
            let mut f = self.failing.lock().unwrap();
            if f.len() == FAILING_KEPT {
                f.pop_front();
            }
            f.push_back((std::time::Instant::now(), p));
        }
    }
}

/// The page store, one per process, shared by every reader.
pub struct Engine {
    inner: Arc<Inner>,
}

impl Default for Engine {
    fn default() -> Engine {
        Engine::new()
    }
}

enum Class {
    /// In memory: pinned into the hold if it keeps pins.
    Here,
    Missing,
    Bad(StorageError),
}

impl Engine {
    /// An engine that keeps whole files in memory and has no page cache.
    pub fn new() -> Engine {
        Engine::build(None, None, None, Duration::ZERO, Arc::new(TokenBucket::new(Rate::default())))
    }

    /// An engine with a page cache of `cfg.memory` bytes. Fails if that
    /// can't hold one group of frames and the engine's tables.
    pub fn with_config(cfg: Config) -> Result<Engine> {
        let cache = Cache::new(cfg.memory, cfg.max_frames).context("map the frame arena")?;
        if cache.capacity() == 0 {
            bail!(
                "a store budget of {} bytes holds no frames: it needs at least {} bytes",
                cfg.memory,
                GROUP + GROUP / PAGE * crate::store::cache::FRAME_META
            );
        }
        let bucket = Arc::new(TokenBucket::new(cfg.background));
        let io = IoPool::new(cfg.io_threads, cfg.queue, bucket.clone());
        Ok(Engine::build(Some(Arc::new(cache)), Some(io), Some(cfg.device), cfg.borrow_timeout, bucket))
    }

    fn build(
        cache: Option<Arc<Cache>>,
        io: Option<IoPool>,
        device: Option<Arc<dyn Device>>,
        t: Duration,
        bucket: Arc<TokenBucket>,
    ) -> Engine {
        Engine {
            inner: Arc::new(Inner {
                table: FileTable::new(),
                next_id: AtomicU32::new(0),
                cache,
                io,
                bucket,
                device,
                borrow_timeout: t,
                by_path: Mutex::new(HashMap::new()),
                flights: (0..FLIGHT_SHARDS).map(|_| Mutex::new(FxHashMap::default())).collect(),
                bad: Mutex::new(FxHashMap::default()),
                failing: Mutex::new(Default::default()),
                counters: Counters::default(),
                plan_writes: AtomicBool::new(true),
            }),
        }
    }

    fn next_id(&self) -> FileId {
        FileId(self.inner.next_id.fetch_add(1, Relaxed))
    }

    /// Registers a file's bytes under `name`. Its pages are `bytes` in 4 KiB
    /// steps; a partial last page is past the end.
    pub fn add(&self, name: impl Into<String>, bytes: Vec<u8>) -> FileId {
        let id = self.next_id();
        let e = Entry { dropped: Default::default(), slots: Box::new([]), data: EntryData::Resident { bytes } };
        self.inner.table.insert(id, &name.into(), e);
        id
    }

    pub(crate) fn cache(&self) -> Option<&Arc<Cache>> {
        self.inner.cache.as_ref()
    }

    /// Whether files are read a page at a time through a cache
    /// ([`Engine::register`]) rather than kept whole.
    pub fn paged(&self) -> bool {
        self.inner.cache.is_some()
    }

    /// Whether the writer's next batch should read ahead what it will read
    /// ([`crate::Graph::plan`]): yes until a plan finds everything in
    /// memory on its first pass, and again once a write's own read misses.
    /// When everything is resident, a plan is a third pass over the same
    /// reads for nothing.
    pub fn plan_writes(&self) -> bool {
        self.inner.plan_writes.load(Relaxed)
    }

    pub fn set_plan_writes(&self, on: bool) {
        if self.inner.plan_writes.load(Relaxed) != on {
            self.inner.plan_writes.store(on, Relaxed);
        }
    }

    /// A registered file's name.
    pub fn name(&self, f: FileId) -> Option<&str> {
        self.inner.table.name(f)
    }

    /// A guard for a pass. Only guards from here may build a [`ReadCx`] on
    /// this engine.
    pub fn pin(&self) -> Guard {
        crossbeam_epoch::pin()
    }

    /// Opens the FORMAT 6 file at `path` through the engine's device, or
    /// returns the handle already open for it: a snapshot and the checkpoint
    /// replaying it share files, pinned sections and cached pages. Reads the
    /// footer and the page sums, checks them, and takes their bytes and the
    /// file's slots from the budget. The other pinned sections are
    /// [`Engine::pinned`]'s.
    pub fn register(&self, path: &Path) -> Result<Arc<PagedFile>> {
        self.register_with(path, &mut 0)
    }

    /// [`Engine::register`], taking what it borrows from `prepaid` first.
    fn register_with(&self, path: &Path, prepaid: &mut u64) -> Result<Arc<PagedFile>> {
        let (Some(cache), Some(io), Some(dev)) = (&self.inner.cache, &self.inner.io, &self.inner.device) else {
            bail!("this engine keeps whole files in memory: it has no page cache");
        };
        let key = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        if let Some(f) = self.inner.by_path.lock().unwrap().get(&key).and_then(Weak::upgrade) {
            return Ok(f);
        }
        let name = file_name(path);
        let at = |e: String| format!("{}: {e}", path.display());
        let file = dev.open(path).with_context(|| format!("open {}", path.display()))?;
        let len = file.len();
        let last =
            read_pages(io, &file, len / PAGE as u64 - u64::from(len >= PAGE as u64), 1, Lane::Request).map_err(at);
        let last = last.map_err(anyhow::Error::msg)?;
        let r = PagedFooter::footer_range(len, &last[PAGE - TRAILER..]).with_context(|| path.display().to_string())?;
        let first = r.start / PAGE as u64;
        let fp = read_pages(io, &file, first, (r.end.div_ceil(PAGE as u64) - first) as usize, Lane::Request)
            .map_err(at)
            .map_err(anyhow::Error::msg)?;
        let lo = (r.start - first * PAGE as u64) as usize;
        let footer = PagedFooter::parse(&fp[lo..lo + (r.end - r.start) as usize], r.start)
            .with_context(|| path.display().to_string())?;
        drop(fp);

        let pages = (len / PAGE as u64) as u32;
        let sums = Pinned::plan(&footer, true);
        let borrowed = sums.bytes() + 4 * pages as u64;
        let paid = borrowed.min(*prepaid);
        *prepaid -= paid;
        if let Err(e) = cache.borrow(&self.inner.table, borrowed - paid, self.inner.borrow_timeout) {
            cache.unborrow(paid);
            bail!(at(e));
        }
        let undo = scopeguard(|| cache.unborrow(borrowed));
        let (bufs, at_) = sums.read(io, &file, &footer, Lane::Request).with_context(|| path.display().to_string())?;
        let mut paged = Vec::new();
        for (i, s) in footer.sections.iter().enumerate() {
            if let SectionKind::Paged { pages, .. } = s.kind {
                let p = at_.iter().find(|p| p.name == sums_section(&s.name)).context("no sums")?;
                let start = s.first_page();
                paged.push(PagedRange { start, end: start + pages, tag: s.tag(), section: i, sums: (p.buf, p.off) });
            }
        }
        undo.disarm();
        let state = Arc::new(FileState { name: name.clone(), dev: file, footer, pages, paged, sums: bufs });
        let id = self.next_id();
        let slots = (0..pages).map(|_| AtomicU32::new(NO_FRAME)).collect();
        self.inner.table.insert(
            id,
            &name,
            Entry { dropped: Default::default(), slots, data: EntryData::Paged(state.clone()) },
        );
        let f = Arc::new(PagedFile {
            inner: self.inner.clone(),
            id,
            key: key.clone(),
            state,
            borrowed,
            pinned: Mutex::new(Weak::new()),
        });
        // Opened without the lock held, so two registrations can race: the
        // first one in wins, and the loser's handle drops, freeing its id.
        let mut by_path = self.inner.by_path.lock().unwrap();
        if let Some(won) = by_path.get(&f.key).and_then(Weak::upgrade) {
            drop(by_path);
            return Ok(won);
        }
        by_path.insert(key, Arc::downgrade(&f));
        Ok(f)
    }

    /// `f`'s pinned sections other than its page sums, read and checked
    /// against the footer the first time they're asked for, and kept until
    /// the last handle drops. Their bytes come from the budget, giving back
    /// groups of frames first if it's full. Never call it holding pins.
    pub fn pinned(&self, f: &PagedFile) -> Result<Arc<PinnedSections>> {
        let (Some(cache), Some(io)) = (&self.inner.cache, &self.inner.io) else {
            bail!("this engine keeps whole files in memory: it has no page cache");
        };
        let mut slot = f.pinned.lock().unwrap();
        if let Some(p) = slot.upgrade() {
            return Ok(p);
        }
        let st = &f.state;
        let plan = Pinned::plan(&st.footer, false);
        let bytes = plan.bytes();
        cache
            .borrow(&self.inner.table, bytes, self.inner.borrow_timeout)
            .map_err(|e| anyhow::anyhow!("{}: {e}", st.name))?;
        let undo = scopeguard(|| cache.unborrow(bytes));
        let (bufs, at) = plan.read(io, &st.dev, &st.footer, Lane::Request).with_context(|| st.name.clone())?;
        undo.disarm();
        let p = Arc::new(PinnedSections { at, bufs, cache: Some(cache.clone()), borrowed: bytes });
        *slot = Arc::downgrade(&p);
        Ok(p)
    }

    /// A stage for writing a file through into the cache ([`Stage`]), or
    /// None for an engine that keeps whole files.
    /// `share`: stages that run at once, which split a quarter of the cache.
    pub fn stage(&self, share: usize) -> Option<Stage> {
        let cache = self.inner.cache.as_ref()?;
        let cap = cache.room() / 4 / share.max(1);
        if cap == 0 {
            return None;
        }
        // sums and slots, 8 B a page, plus a page of rounding for each sums run
        let prepaid = 8 * cap as u64 + 32 * PAGE as u64;
        cache.borrow(&self.inner.table, prepaid, self.inner.borrow_timeout).ok()?;
        Some(Stage {
            inner: self.inner.clone(),
            cap,
            pages: Vec::new(),
            spare: Vec::new(),
            skipped: 0,
            gave_up: false,
            prepaid,
        })
    }

    /// [`Engine::register`] for the file `stage` was written into, paying
    /// for the registration from what the stage borrowed when it began, then
    /// [`Engine::publish`]. Returns the pages published.
    pub fn register_staged(&self, path: &Path, mut stage: Stage) -> Result<(Arc<PagedFile>, u64)> {
        let f = self.register_with(path, &mut stage.prepaid)?;
        let n = self.publish(stage, &f);
        Ok((f, n))
    }

    /// Makes the pages `stage` holds the cached pages of `file`, the file
    /// they were written into, in the small queue as a fetched page would
    /// be. A page that doesn't match its sum (it can't, unless the writer
    /// staged the wrong page) or that `file` already has is dropped. Returns
    /// the pages published.
    pub fn publish(&self, mut stage: Stage, file: &PagedFile) -> u64 {
        let Some(cache) = &self.inner.cache else { return 0 };
        let pages = std::mem::take(&mut stage.pages);
        let st = &file.state;
        let guard = self.pin();
        let Some(e) = self.inner.table.get(file.id, &guard) else {
            stage.pages = pages;
            return 0;
        };
        let mut ok = Vec::with_capacity(pages.len());
        let mut drop_ = Vec::new();
        for &(page, f) in &pages {
            let good = st.sum(page).is_some_and(|(seed, want)| {
                // SAFETY: the frame is still the stage's alone.
                page_sum(unsafe { cache.page(f) }, seed) == want
            });
            debug_assert!(good || st.sum(page).is_none(), "staged page {page} of {} doesn't match its sum", st.name);
            if good && e.slots[page as usize].load(Acquire) == NO_FRAME {
                cache.own(f, PageId::new(file.id, page));
                ok.push((page, f));
            } else {
                drop_.push(f);
            }
        }
        let frames: Vec<u32> = ok.iter().map(|x| x.1).collect();
        cache.unstage(&frames);
        cache.unstage(&drop_);
        cache.free_unpublished(&drop_);
        cache.admit(&frames, 0, false);
        for &(page, f) in &ok {
            let slot = &e.slots[page as usize];
            if slot.compare_exchange(NO_FRAME, f, SeqCst, Relaxed).is_err() {
                cache.drop_frame(f);
            } else if e.dropped.load(SeqCst) {
                let _ = slot.compare_exchange(f, NO_FRAME, SeqCst, Relaxed);
                cache.drop_frame(f);
            }
            cache.unpin(f, &guard);
        }
        self.inner.counters.written_through.fetch_add(ok.len() as u64, Relaxed);
        ok.len() as u64
    }

    /// Takes `bytes` of the budget for memory outside the cache, giving back
    /// whole groups of frames first if the budget is full.
    pub fn borrow(&self, bytes: u64) -> Result<Borrow> {
        let cache = self.inner.cache.as_ref().context("this engine has no page cache")?;
        cache.borrow(&self.inner.table, bytes, self.inner.borrow_timeout).map_err(anyhow::Error::msg)?;
        Ok(Borrow { cache: cache.clone(), bytes })
    }

    /// Page `p`, if it's in memory, valid while `guard` is. `bump` counts it
    /// as a use for the eviction policy; `queue` says which queue a hit was
    /// in (else [`Found::Other`]).
    #[inline]
    pub(crate) fn lookup<'g>(
        &'g self,
        p: PageId,
        guard: &'g Guard,
        bump: bool,
        queue: bool,
    ) -> Option<(&'g Page, Found)> {
        let e = self.inner.table.get(p.file, guard)?;
        match &e.data {
            EntryData::Paged(_) => {
                let f = e.slots.get(p.page as usize)?.load(Acquire);
                if f == NO_FRAME {
                    return None;
                }
                let c = self.inner.cache.as_deref()?;
                if bump {
                    c.touch(f);
                }
                // SAFETY: f came from a slot loaded under `guard`, so its frame
                // isn't reused before the guard is gone.
                Some((unsafe { c.page(f) }, if queue { c.found(f) } else { Found::Other }))
            }
            EntryData::Resident { bytes } => {
                let off = p.offset() as usize;
                Some((bytes.get(off..off + PAGE)?.try_into().ok()?, Found::Other))
            }
        }
    }

    fn count(&self, source: Source, st: ReadStats, by: &LookupCounts) {
        let s = &self.inner.counters.stripes[stripe()];
        s.passes.fetch_add(1, Relaxed);
        s.pages.fetch_add(st.pages as u64, Relaxed);
        s.misses.fetch_add(st.misses as u64, Relaxed);
        for (a, b) in s.by[source as usize].iter().zip(by) {
            for (x, &n) in a.iter().zip(b) {
                if n > 0 {
                    x.fetch_add(n as u64, Relaxed);
                }
            }
        }
    }

    fn count_used(&self, used: &UsedBytes) {
        let s = &self.inner.counters.stripes[stripe()];
        for (x, &n) in s.used.iter().zip(used) {
            if n > 0 {
                x.fetch_add(n * USED_SAMPLE, Relaxed);
            }
        }
    }

    /// One pass of `f`. Never waits. Ok if `f` succeeded and nothing was
    /// missing; otherwise the pages that were, after pinning the pages the
    /// pass touched into `hold` (before the guard drops).
    pub fn pass<T>(&self, hold: &mut Hold, f: impl FnOnce(&ReadCx<'_>) -> R<T>) -> Result<T, Misses> {
        let guard = self.pin();
        let (pages, touched) = {
            let sampled = hold.count_used && sample_used();
            let cx = ReadCx::with(self, &guard, hold.keep, hold.quiet, &hold.pins, hold.private.as_deref(), sampled);
            let r = f(&cx);
            self.count(hold.source, cx.stats(), &cx.lookups());
            if cx.clean() {
                if let Ok(t) = r {
                    if sampled {
                        self.count_used(&cx.bytes_used());
                    }
                    return Ok(t);
                }
            }
            cx.into_sets()
        };
        let touched_n = touched.len();
        self.hold_touched(&guard, hold, touched);
        Err(Misses { pages, touched: touched_n })
    }

    /// Pins pages a missing pass read into `hold`, under the pass's guard. A
    /// pin that loses to an eviction just leaves the page to miss next pass.
    fn hold_touched(&self, guard: &Guard, hold: &mut Hold, touched: PageSet) {
        if !hold.keep || touched.is_empty() {
            return;
        }
        let mut add = Vec::new();
        for p in touched.iter() {
            if hold.full() {
                break;
            }
            if hold.pins.contains(p) {
                continue;
            }
            if let Some(f) = self.try_pin(p, guard) {
                add.push((p, f));
            }
        }
        hold.add(add, self.inner.cache.as_ref());
    }

    /// Pins `p` if it's in memory: Some(frame), or Some(NO_FRAME) for a page
    /// of a file kept whole, which needs no pin.
    fn try_pin(&self, p: PageId, guard: &Guard) -> Option<u32> {
        let e = self.inner.table.get(p.file, guard)?;
        match &e.data {
            EntryData::Resident { bytes } => (p.offset() as usize + PAGE <= bytes.len()).then_some(NO_FRAME),
            EntryData::Paged(_) => {
                let f = e.slots.get(p.page as usize)?.load(Acquire);
                (f != NO_FRAME && self.inner.cache.as_ref()?.pin(f)).then_some(f)
            }
        }
    }

    fn classify(&self, p: PageId, guard: &Guard, hold: &mut Hold, add: &mut Vec<(PageId, u32)>) -> Class {
        let Some(e) = self.inner.table.get(p.file, guard) else {
            // A file dropped since the pass that named it (a checkpoint switched
            // the copies over): nothing to fetch, and the next pass, on the
            // published copy, names what it needs.
            if p.file.0 < self.inner.next_id.load(Relaxed) && self.name(p.file).is_some() {
                return Class::Here;
            }
            let file = self.name(p.file).map_or_else(|| format!("file {}", p.file.0), str::to_string);
            return Class::Bad(StorageError::Short(BadPage { file, section: None, page: p.page }));
        };
        match &e.data {
            EntryData::Resident { bytes } => {
                if p.offset() as usize + PAGE <= bytes.len() {
                    if hold.keep && !hold.pins.contains(p) {
                        add.push((p, NO_FRAME));
                    }
                    return Class::Here;
                }
                let file = self.name(p.file).unwrap_or_default().to_string();
                Class::Bad(StorageError::Short(BadPage { file, section: None, page: p.page }))
            }
            EntryData::Paged(st) => {
                if st.range(p.page).is_none() {
                    return Class::Bad(StorageError::Short(st.bad(p.page)));
                }
                if hold.keep && hold.pins.contains(p) {
                    return Class::Here;
                }
                let f = e.slots[p.page as usize].load(Acquire);
                if f != NO_FRAME {
                    if !hold.keep {
                        return Class::Here;
                    }
                    if self.inner.cache.as_ref().is_some_and(|c| c.pin(f)) {
                        add.push((p, f));
                        return Class::Here;
                    }
                    return Class::Missing;
                }
                match self.inner.bad.lock().unwrap().get(&p) {
                    Some(err) => Class::Bad(err.clone()),
                    None => Class::Missing,
                }
            }
        }
    }

    /// Reads what a pass missed into memory, pinned into `hold`, waiting on
    /// disk. Never call it on a tokio worker. A page another reader is
    /// already reading is waited for, not read again. Every page read is
    /// checked against its sum (once more on a mismatch); one that fails, or
    /// that EIO keeps failing, is marked bad and its error returned until a
    /// [`Engine::verify`] reads it clean. The first error in page order is
    /// returned, after every read this call started is done.
    pub fn fetch_blocking(&self, m: Misses, hold: &mut Hold) -> Result<(), StorageError> {
        debug_assert!(!crate::read::on_async_thread(), "a blocking fetch on a tokio runtime thread");
        hold.rounds += 1;
        hold.fetched += m.pages().len() as u64;
        if hold.private.is_some() {
            return self.fetch_past(m.pages().iter().collect(), hold);
        }
        self.fetch(m.pages().iter().collect(), hold, false)
    }

    /// Reads `todo` past the cache into `hold`'s own memory on the
    /// background lane ([`Hold::past`]), checking each page as a fetch does.
    fn fetch_past(&self, todo: Vec<PageId>, hold: &mut Hold) -> Result<(), StorageError> {
        if todo.is_empty() {
            return Err(StorageError::NoPage);
        }
        let (Some(cache), Some(io)) = (&self.inner.cache, &self.inner.io) else {
            unreachable!("only paged files miss");
        };
        let mut need: Vec<(PageId, Arc<FileState>)> = Vec::new();
        let mut err: Option<(PageId, StorageError)> = None;
        {
            let guard = self.pin();
            let private = hold.private.as_deref().expect("a hold that reads past the cache");
            for p in todo {
                if private.has(p) {
                    continue;
                }
                let Some(e) = self.inner.table.get(p.file, &guard) else { continue };
                let EntryData::Paged(st) = &e.data else { continue };
                if st.range(p.page).is_none() {
                    err.get_or_insert((p, StorageError::Short(st.bad(p.page))));
                    continue;
                }
                if let Some(e) = self.inner.bad.lock().unwrap().get(&p) {
                    err.get_or_insert((p, e.clone()));
                    continue;
                }
                need.push((p, st.clone()));
            }
        }
        if let Some((_, e)) = err {
            return Err(e);
        }
        if need.is_empty() {
            return Ok(());
        }
        let held = hold.private.as_deref().map_or(0, |p| p.len());
        if held + need.len() > hold.page_limit {
            // past what a job may hold: through the cache, as any reader would
            return self.fetch(need.into_iter().map(|x| x.0).collect(), hold, false);
        }
        let mut frames = Vec::with_capacity(need.len());
        let until = std::time::Instant::now() + PAST_WAIT;
        while frames.len() < need.len() {
            let got = cache.alloc_staged(&self.inner.table, need.len() - frames.len(), until);
            if got.is_empty() {
                break;
            }
            frames.extend(got);
        }
        if frames.len() < need.len() {
            // the cache is short of frames that aren't someone's alone: read
            // these through it instead, as any reader would
            cache.unstage(&frames);
            cache.free_unpublished(&frames);
            return self.fetch(need.into_iter().map(|x| x.0).collect(), hold, false);
        }
        let mut runs: Vec<Run> = Vec::new();
        let mut at: Vec<(usize, usize)> = Vec::with_capacity(need.len());
        for (i, (p, st)) in need.iter().enumerate() {
            let extend = i > 0
                && need[i - 1].0.file == p.file
                && need[i - 1].0.page + 1 == p.page
                && runs.last().is_some_and(|r| r.frames.len() < MAX_RUN);
            if !extend {
                runs.push(Run { file: st.dev.clone(), page: p.page, frames: Vec::new() });
            }
            let n = runs.len() - 1;
            at.push((n, runs[n].frames.len()));
            runs[n].frames.push(crate::store::io::FramePtr(cache.frame_ptr(frames[i])));
        }
        let out = io.run(runs, Lane::Background);
        let private = hold.private.as_deref_mut().expect("a hold that reads past the cache");
        let mut bad = Vec::new();
        for (i, ((p, st), (r, k))) in need.iter().zip(at).enumerate() {
            let (seed, want) = st.sum(p.page).expect("a paged page");
            // SAFETY: the frame is this read's alone.
            let page = unsafe { cache.page(frames[i]) };
            let mut e = match out[r][k] {
                PageRead::Eio => Some(StorageError::Eio(st.bad(p.page))),
                PageRead::Short => Some(StorageError::Short(st.bad(p.page))),
                _ if page_sum(page, seed) != want => Some(StorageError::Checksum(st.bad(p.page))),
                _ => None,
            };
            if let Some(StorageError::Checksum(_)) = e {
                // once more before it's called bad, as a fetch does
                let again = Run {
                    file: st.dev.clone(),
                    page: p.page,
                    frames: vec![crate::store::io::FramePtr(cache.frame_ptr(frames[i]))],
                };
                let ok = matches!(io.run(vec![again], Lane::Background)[0][0], PageRead::Ok | PageRead::Retried);
                // SAFETY: as above.
                if ok && page_sum(unsafe { cache.page(frames[i]) }, seed) == want {
                    e = None;
                }
            }
            match e {
                None => private.add(*p, frames[i]),
                Some(e) => {
                    bad.push(frames[i]);
                    self.inner.failed(*p, &e);
                    if self.inner.table.get(p.file, &self.pin()).is_some() {
                        self.inner.bad.lock().unwrap().insert(*p, e.clone());
                    }
                    err.get_or_insert((*p, e));
                }
            }
        }
        cache.unstage(&bad);
        cache.free_unpublished(&bad);
        self.inner.counters.past_pages.fetch_add((need.len() - bad.len()) as u64, Relaxed);
        match err {
            Some((_, e)) => Err(e),
            None => Ok(()),
        }
    }

    fn fetch(&self, mut todo: Vec<PageId>, hold: &mut Hold, main: bool) -> Result<(), StorageError> {
        if todo.is_empty() {
            return Err(StorageError::NoPage);
        }
        loop {
            let mut need = Vec::new();
            let mut err: Option<(PageId, StorageError)> = None;
            {
                let guard = self.pin();
                let mut add = Vec::new();
                for &p in &todo {
                    match self.classify(p, &guard, hold, &mut add) {
                        Class::Here => {}
                        Class::Missing => need.push(p),
                        Class::Bad(e) => {
                            if err.as_ref().is_none_or(|(q, _)| p < *q) {
                                err = Some((p, e));
                            }
                        }
                    }
                }
                hold.add(add, self.inner.cache.as_ref());
            }
            if let Some((_, e)) = err {
                return Err(e);
            }
            if need.is_empty() {
                return Ok(());
            }
            let (Some(cache), Some(io)) = (&self.inner.cache, &self.inner.io) else {
                unreachable!("only paged files miss");
            };
            todo = self.fetch_round(cache, io, &need, hold, main)?;
        }
    }

    /// One round: claims what it can of `need`, reads it, publishes it and
    /// waits for the pages others were reading. Returns the pages to look at
    /// again.
    fn fetch_round(
        &self,
        cache: &Arc<Cache>,
        io: &IoPool,
        need: &[PageId],
        hold: &mut Hold,
        main: bool,
    ) -> Result<Vec<PageId>, StorageError> {
        // Pages another reader is already fetching need no frame here: without
        // this a herd missing one page would each evict for it.
        let free = need.iter().filter(|p| !self.inner.flights[flight_shard(**p)].lock().unwrap().contains_key(p));
        let want = free.count().clamp(1, 4096);
        let frames = cache.alloc(&self.inner.table, want, &mut || hold.release());
        let mut again = Vec::new();
        let mut claims: Vec<(PageId, u32, Arc<FileState>, Arc<Flight>)> = Vec::new();
        let mut waits: Vec<(PageId, Arc<Flight>)> = Vec::new();
        {
            let guard = self.pin();
            let mut fi = 0;
            for &p in need {
                let Some(e) = self.inner.table.get(p.file, &guard) else {
                    again.push(p);
                    continue;
                };
                let EntryData::Paged(st) = &e.data else { unreachable!("resident pages never miss") };
                let mut shard = self.inner.flights[flight_shard(p)].lock().unwrap();
                if e.slots[p.page as usize].load(Acquire) != NO_FRAME {
                    again.push(p);
                } else if let Some(fl) = shard.get(&p) {
                    waits.push((p, fl.clone()));
                } else if fi < frames.len() {
                    let fl = Arc::new(Flight { done: Mutex::new(None), cv: Condvar::new() });
                    shard.insert(p, fl.clone());
                    cache.own(frames[fi], p);
                    claims.push((p, frames[fi], st.clone(), fl));
                    fi += 1;
                } else {
                    again.push(p);
                }
            }
            cache.free_unpublished(&frames[fi..]);
        }
        let mut first_err: Option<(PageId, StorageError)> = None;
        if !claims.is_empty() {
            let results = self.read_claims(cache, io, &claims);
            let mut ok = Vec::new();
            let mut failed = Vec::new();
            for ((p, f, _, _), r) in claims.iter().zip(&results) {
                match r {
                    Ok(()) => ok.push(*f),
                    Err(e) => {
                        failed.push(*f);
                        self.inner.failed(*p, e);
                        // a file dropped meanwhile has already cleared its marks
                        if self.inner.table.get(p.file, &self.pin()).is_some() {
                            self.inner.bad.lock().unwrap().insert(*p, e.clone());
                        }
                        if first_err.as_ref().is_none_or(|(q, _)| p < q) {
                            first_err = Some((*p, e.clone()));
                        }
                    }
                }
            }
            cache.free_unpublished(&failed);
            self.inner.counters.fetched.fetch_add(ok.len() as u64, Relaxed);
            cache.admit(&ok, hold.keep as u32, main);
            let guard = self.pin();
            let mut add = Vec::new();
            for ((p, f, _, fl), r) in claims.iter().zip(results) {
                if r.is_ok() {
                    match self.inner.table.get(p.file, &guard) {
                        Some(e) => {
                            let slot = &e.slots[p.page as usize];
                            slot.store(*f, SeqCst);
                            if e.dropped.load(SeqCst) {
                                let _ = slot.compare_exchange(*f, NO_FRAME, SeqCst, Relaxed);
                                cache.drop_frame(*f);
                            }
                        }
                        None => {
                            cache.drop_frame(*f);
                        }
                    }
                    if hold.keep {
                        add.push((*p, *f));
                    }
                    cache.unpin(*f, &guard);
                }
                self.inner.flights[flight_shard(*p)].lock().unwrap().remove(p);
                fl.finish(r);
            }
            hold.add(add, Some(cache));
        }
        if !waits.is_empty() {
            self.inner.counters.joins.fetch_add(waits.len() as u64, Relaxed);
        }
        for (p, fl) in waits {
            match fl.wait() {
                Ok(()) => again.push(p),
                Err(e) => {
                    if first_err.as_ref().is_none_or(|(q, _)| p < *q) {
                        first_err = Some((p, e));
                    }
                }
            }
        }
        if let Some((_, e)) = first_err {
            return Err(e);
        }
        again.sort_unstable();
        Ok(again)
    }

    /// Reads claimed pages into their frames, coalescing adjacent pages of a
    /// file, and checks each against its sum, re-reading a mismatch once.
    fn read_claims(
        &self,
        cache: &Cache,
        io: &IoPool,
        claims: &[(PageId, u32, Arc<FileState>, Arc<Flight>)],
    ) -> Vec<Result<(), StorageError>> {
        let mut runs: Vec<Run> = Vec::new();
        let mut at: Vec<(usize, usize)> = Vec::with_capacity(claims.len());
        for (i, (p, f, st, _)) in claims.iter().enumerate() {
            let prev = i.checked_sub(1).map(|j| claims[j].0);
            let extend = prev.is_some_and(|q| q.file == p.file && q.page + 1 == p.page)
                && runs.last().is_some_and(|r| r.frames.len() < MAX_RUN);
            if !extend {
                runs.push(Run { file: st.dev.clone(), page: p.page, frames: Vec::new() });
            }
            let n = runs.len() - 1;
            let r = &mut runs[n];
            at.push((n, r.frames.len()));
            r.frames.push(crate::store::io::FramePtr(cache.frame_ptr(*f)));
        }
        let results = io.run(runs, Lane::Request);
        claims
            .iter()
            .zip(at)
            .map(|((p, f, st, _), (r, k))| {
                match results[r][k] {
                    PageRead::Ok | PageRead::Retried => {}
                    PageRead::Eio => return Err(StorageError::Eio(st.bad(p.page))),
                    PageRead::Short => return Err(StorageError::Short(st.bad(p.page))),
                }
                let (seed, want) = st.sum(p.page).expect("claims are paged pages");
                // SAFETY: the frame is this claim's alone until it's published.
                let page = || unsafe { cache.page(*f) };
                if page_sum(page(), seed) == want {
                    return Ok(());
                }
                let again = Run {
                    file: st.dev.clone(),
                    page: p.page,
                    frames: vec![crate::store::io::FramePtr(cache.frame_ptr(*f))],
                };
                match io.run(vec![again], Lane::Request)[0][0] {
                    PageRead::Ok | PageRead::Retried if page_sum(page(), seed) == want => Ok(()),
                    PageRead::Eio => Err(StorageError::Eio(st.bad(p.page))),
                    PageRead::Short => Err(StorageError::Short(st.bad(p.page))),
                    _ => Err(StorageError::Checksum(st.bad(p.page))),
                }
            })
            .collect()
    }

    /// `f` in one pass that pins nothing, then, if it missed, in passes
    /// that fetch between them ([`Engine::read_blocking`]): for readers that
    /// may wait on disk and mostly don't have to. Never call it on a tokio
    /// worker over an engine that may miss.
    pub fn read<T>(&self, f: impl Fn(&ReadCx<'_>) -> R<T>) -> Result<T, StorageError> {
        match self.pass(&mut Hold::none(), &f) {
            Ok(t) => Ok(t),
            Err(m) => self.resume_blocking(&mut Hold::job(self), Some(m), f),
        }
    }

    /// [`Engine::read`] past the cache ([`Hold::past`]): what's cached is
    /// read without counting a use, and what isn't is read on the background
    /// lane into memory that goes when the read ends. For scans (list stats,
    /// compaction) that would otherwise push the served pages out.
    pub fn read_past<T>(&self, f: impl Fn(&ReadCx<'_>) -> R<T>) -> Result<T, StorageError> {
        let mut hold = Hold::past(self);
        match self.pass(&mut hold, &f) {
            Ok(t) => Ok(t),
            Err(m) => self.resume_blocking(&mut hold, Some(m), f),
        }
    }

    /// The token bucket background I/O goes through (`--background-io`):
    /// writes that should yield to requests take from it as they go.
    pub fn background(&self) -> &Arc<TokenBucket> {
        &self.inner.bucket
    }

    /// Runs passes of `f` until one is clean, fetching what each missed in
    /// between: the loop every reader that may wait on disk runs. Never call
    /// it on a tokio worker. A request runs as many rounds as it needs; one
    /// whose missing set stops shrinking for 16 rounds is logged and counted
    /// as a sign of a bug. A pass that needs more pages than the hold's
    /// limit ([`Hold::request`]) fails with [`StorageError::TooBig`].
    pub fn read_blocking<T>(&self, hold: &mut Hold, f: impl Fn(&ReadCx<'_>) -> R<T>) -> Result<T, StorageError> {
        self.resume_blocking(hold, None, f)
    }

    /// [`Engine::read_blocking`] for a read whose first pass ran elsewhere
    /// and missed `first` (a fast pass on a tokio worker): fetches those,
    /// then runs passes as `read_blocking` does.
    pub fn resume_blocking<T>(
        &self,
        hold: &mut Hold,
        mut first: Option<Misses>,
        f: impl Fn(&ReadCx<'_>) -> R<T>,
    ) -> Result<T, StorageError> {
        debug_assert!(!crate::read::on_async_thread(), "read_blocking on a tokio runtime thread");
        let mut best = usize::MAX;
        let mut flat = 0;
        loop {
            let m = match first.take() {
                Some(m) => m,
                None => match self.pass(hold, &f) {
                    Ok(t) => return Ok(t),
                    Err(m) => m,
                },
            };
            if m.needed() > hold.page_limit {
                return Err(StorageError::TooBig { pages: m.needed(), limit: hold.page_limit });
            }
            let n = m.pages().len();
            if n < best {
                best = n;
                flat = 0;
            } else {
                flat += 1;
                if flat == 16 {
                    self.inner.counters.stalled.fetch_add(1, Relaxed);
                    tracing::warn!(missing = n, "a read's missing pages stopped shrinking for 16 rounds");
                }
            }
            self.fetch_blocking(m, hold)?
        }
    }

    /// Most pages one pass may need at once: half the small queue the
    /// budget's frames make, or no limit without a cache.
    pub fn page_limit(&self) -> usize {
        match &self.inner.cache {
            Some(c) => (c.room() / 20).max(1),
            None => usize::MAX,
        }
    }

    /// Most pages a blocking job's pass may need at once: half the cache.
    pub fn job_limit(&self) -> usize {
        match &self.inner.cache {
            Some(c) => (c.room() / 2).max(1),
            None => usize::MAX,
        }
    }

    /// Frames the budget holds once the borrowed bytes (pinned sections,
    /// slots) are taken out.
    pub fn room(&self) -> usize {
        self.inner.cache.as_ref().map_or(0, |c| c.room())
    }

    /// Reads every paged page of `files` into the cache's main queue, if
    /// those not in memory yet fit in the free frames with the reserve to
    /// spare: the full-residency load, after which reads of these files
    /// never miss. Returns whether it loaded them; false (reading nothing)
    /// when they don't fit or the engine keeps whole files. Best effort: a
    /// page that fails its read is marked bad, as a fetch would, and the
    /// rest still load.
    pub fn load(&self, files: &[&PagedFile]) -> bool {
        let Some(cache) = &self.inner.cache else { return false };
        let mut chunks: Vec<Vec<PageId>> = Vec::new();
        {
            let guard = self.pin();
            for f in files {
                let Some(e) = self.inner.table.get(f.id, &guard) else { continue };
                for r in &f.state.paged {
                    let mut chunk = Vec::new();
                    for q in r.start..r.end {
                        if e.slots[q as usize].load(Acquire) == NO_FRAME {
                            chunk.push(PageId::new(f.id, q));
                            if chunk.len() == 4096 {
                                chunks.push(std::mem::take(&mut chunk));
                            }
                        }
                    }
                    if !chunk.is_empty() {
                        chunks.push(chunk);
                    }
                }
            }
        }
        let want: u64 = chunks.iter().map(|c| c.len() as u64).sum();
        let b = cache.bytes();
        let used = b.small_frames + b.main_frames + b.retired_frames;
        let free = (cache.room() as u64).saturating_sub(used);
        if want + cache.reserve_frames() as u64 > free {
            return false;
        }
        let next = std::sync::atomic::AtomicUsize::new(0);
        let bad = AtomicU64::new(0);
        let threads = 8.min(chunks.len()).max(1);
        std::thread::scope(|s| {
            for _ in 0..threads {
                s.spawn(|| loop {
                    let i = next.fetch_add(1, Relaxed);
                    let Some(c) = chunks.get(i) else { return };
                    if self.fetch(c.clone(), &mut Hold::none(), true).is_err() {
                        // one page at a time, so a bad page doesn't stop its chunk's others
                        for &p in c {
                            if self.fetch(vec![p], &mut Hold::none(), true).is_err() {
                                bad.fetch_add(1, Relaxed);
                            }
                        }
                    }
                });
            }
        });
        let bad = bad.load(Relaxed);
        if bad > 0 {
            tracing::warn!(pages = bad, "loading the snapshot: pages failed their reads and are marked bad");
        }
        true
    }

    /// Reads every page of `file` past the cache on the background lane and
    /// checks it: marks the pages that fail bad and clears the mark of pages
    /// that read clean (a transient fault). Returns the bad ones.
    pub fn verify(&self, file: &PagedFile) -> Vec<(PageId, StorageError)> {
        let Some(io) = &self.inner.io else { return Vec::new() };
        let st = &file.state;
        let mut bad = Vec::new();
        let mut buf = AlignedBuf::new(MAX_RUN * PAGE);
        for r in &st.paged {
            let mut p = r.start;
            while p < r.end {
                let n = (r.end - p).min(MAX_RUN as u32);
                let frames = buf.frames().into_iter().take(n as usize).collect();
                let out = io.run(vec![Run { file: st.dev.clone(), page: p, frames }], Lane::Background);
                for (i, how) in out[0].iter().enumerate() {
                    let page = p + i as u32;
                    let id = PageId::new(file.id, page);
                    let (seed, want) = st.sum(page).expect("a paged page");
                    let bytes: &Page = buf[i * PAGE..(i + 1) * PAGE].try_into().expect("a page");
                    let e = match how {
                        PageRead::Eio => Some(StorageError::Eio(st.bad(page))),
                        PageRead::Short => Some(StorageError::Short(st.bad(page))),
                        _ if page_sum(bytes, seed) != want => Some(StorageError::Checksum(st.bad(page))),
                        _ => None,
                    };
                    if let Some(e) = &e {
                        self.inner.failed(id, e);
                    }
                    let mut b = self.inner.bad.lock().unwrap();
                    match e {
                        Some(e) => {
                            b.insert(id, e.clone());
                            bad.push((id, e));
                        }
                        None => {
                            b.remove(&id);
                        }
                    }
                }
                p += n;
            }
        }
        bad
    }

    /// Reads `file`'s pinned sections from disk again and checks them
    /// against the footer: what [`Engine::verify`] leaves out.
    pub fn verify_pinned(&self, file: &PagedFile) -> Result<()> {
        let io = self.inner.io.as_ref().context("this engine has no page cache")?;
        let st = &file.state;
        for s in st.footer.sections.iter() {
            let SectionKind::Pinned { xxh3, .. } = s.kind else { continue };
            let first = s.off / PAGE as u64;
            let last = (s.off + s.len()).div_ceil(PAGE as u64).max(first + 1);
            let buf = read_pages(io, &st.dev, first, (last - first) as usize, Lane::Background)
                .map_err(|e| anyhow::anyhow!("{}: section {}: {e}", st.name, s.name))?;
            let lo = (s.off - first * PAGE as u64) as usize;
            let h = xxhash_rust::xxh3::xxh3_64(&buf[lo..lo + s.len() as usize]);
            if h != xxh3 {
                bail!("section {} checksum {h:016x}, footer says {xxh3:016x}", s.name);
            }
        }
        Ok(())
    }

    /// Distinct pages whose reads failed on the device (EIO past its
    /// retries, or short) in the last `window`: 64 in [`STORM_WINDOW`] is the
    /// storm that takes `/readyz` down, since it means the disk is going.
    pub fn failing_pages(&self, window: Duration) -> usize {
        let now = std::time::Instant::now();
        let mut f = self.inner.failing.lock().unwrap();
        while f.front().is_some_and(|(t, _)| now.duration_since(*t) > STORM_WINDOW.max(window)) {
            f.pop_front();
        }
        let mut pages: Vec<PageId> = f.iter().filter(|(t, _)| now.duration_since(*t) <= window).map(|x| x.1).collect();
        pages.sort_unstable();
        pages.dedup();
        pages.len()
    }

    /// Reads waiting on the I/O pool's lanes now: request, background.
    pub fn io_depth(&self) -> (usize, usize) {
        self.inner.io.as_ref().map_or((0, 0), |io| io.depth())
    }

    /// Each lane's reads so far, request then background.
    pub fn io_latency(&self) -> [crate::store::io::IoLatency; 2] {
        match &self.inner.io {
            Some(io) => [io.stats().lanes[0].snapshot(), io.stats().lanes[1].snapshot()],
            None => Default::default(),
        }
    }

    /// Per lane, how long reads waited for an I/O thread (`bytes` unused).
    pub fn io_queue_wait(&self) -> [crate::store::io::IoLatency; 2] {
        match &self.inner.io {
            Some(io) => [io.stats().waits[0].snapshot(), io.stats().waits[1].snapshot()],
            None => Default::default(),
        }
    }

    /// Pages marked bad, with why.
    pub fn bad_pages(&self) -> Vec<(PageId, StorageError)> {
        let mut v: Vec<_> = self.inner.bad.lock().unwrap().iter().map(|(p, e)| (*p, e.clone())).collect();
        v.sort_by_key(|x| x.0);
        v
    }

    /// Where the budget is: zero for an engine without a cache.
    pub fn bytes(&self) -> CacheBytes {
        self.inner.cache.as_ref().map(|c| c.bytes()).unwrap_or_default()
    }

    /// Frames the budget can hold at once (0 without a cache).
    pub fn capacity(&self) -> usize {
        self.inner.cache.as_ref().map_or(0, |c| c.capacity())
    }

    pub fn stats(&self) -> EngineStats {
        let c = &self.inner.counters;
        let mut s = EngineStats { bytes: self.bytes(), ..EngineStats::default() };
        for x in &c.stripes {
            s.passes += x.passes.load(Relaxed);
            s.pages += x.pages.load(Relaxed);
            s.misses += x.misses.load(Relaxed);
        }
        s.fetched = c.fetched.load(Relaxed);
        s.joins = c.joins.load(Relaxed);
        s.checksum = c.checksum.load(Relaxed);
        s.eio = c.eio.load(Relaxed);
        s.short = c.short.load(Relaxed);
        s.stalled_requests = c.stalled.load(Relaxed);
        s.written_through = c.written_through.load(Relaxed);
        s.write_through_skipped = c.write_through_skipped.load(Relaxed);
        s.past_pages = c.past_pages.load(Relaxed);
        for x in &c.stripes {
            for (a, b) in s.lookups.iter_mut().flatten().zip(x.by.iter().flatten()) {
                for (y, z) in a.iter_mut().zip(b) {
                    *y += z.load(Relaxed);
                }
            }
            for (y, z) in s.bytes_used.iter_mut().zip(&x.used) {
                *y += z.load(Relaxed);
            }
        }
        s.bad_pages = self.inner.bad.lock().unwrap().len() as u64;
        if let Some(io) = &self.inner.io {
            let x = io.stats();
            s.reads = x.reads.load(Relaxed);
            s.background_pages = x.background_pages.load(Relaxed);
            s.eio_retried = x.eio_retried.load(Relaxed);
            s.io_nanos = x.io_nanos.load(Relaxed);
        }
        if let Some(cache) = &self.inner.cache {
            let x = &cache.counters;
            s.evict_small = x.evict_small.load(Relaxed);
            s.evict_main = x.evict_main.load(Relaxed);
            s.ghost_hits = x.ghost_hits.load(Relaxed);
            s.frame_waits = x.frame_waits.load(Relaxed);
            s.flushes = x.flushes.load(Relaxed);
            s.stalls = x.stalls.load(Relaxed);
            s.released = x.released.load(Relaxed);
        }
        s
    }
}

/// Which pinned sections of a file to read, as runs of them with nothing
/// else between: one read and one buffer per run.
struct Pinned {
    runs: Vec<(u64, u64, Vec<usize>)>,
}

impl Pinned {
    /// The page sums with `sums`, else every other pinned section.
    fn plan(footer: &PagedFooter, sums: bool) -> Pinned {
        let mut runs: Vec<(u64, u64, Vec<usize>)> = Vec::new();
        let mut gap = true;
        for (i, s) in footer.sections.iter().enumerate() {
            if s.is_paged() || s.name.ends_with(".sums") != sums {
                gap = true;
                continue;
            }
            match runs.last_mut() {
                Some(r) if !gap => {
                    r.1 = s.off + s.len();
                    r.2.push(i);
                }
                _ => runs.push((s.off, s.off + s.len(), vec![i])),
            }
            gap = false;
        }
        Pinned { runs }
    }

    fn span((a, b): (u64, u64)) -> (u64, u64) {
        (a / PAGE as u64, b.div_ceil(PAGE as u64).max(a / PAGE as u64 + 1))
    }

    /// The bytes the runs' buffers take: whole pages.
    fn bytes(&self) -> u64 {
        self.runs
            .iter()
            .map(|r| {
                let (a, b) = Pinned::span((r.0, r.1));
                (b - a) * PAGE as u64
            })
            .sum()
    }

    /// Reads the runs and checks each section against the footer's xxh3.
    fn read(
        &self,
        io: &IoPool,
        file: &Arc<dyn DevFile>,
        footer: &PagedFooter,
        lane: Lane,
    ) -> Result<(Vec<AlignedBuf>, Vec<PinnedAt>)> {
        let mut bufs = Vec::with_capacity(self.runs.len());
        let mut at = Vec::new();
        for (a, b, secs) in &self.runs {
            let (p0, p1) = Pinned::span((*a, *b));
            let buf = read_pages(io, file, p0, (p1 - p0) as usize, lane).map_err(anyhow::Error::msg)?;
            for &i in secs {
                let s = &footer.sections[i];
                let off = (s.off - p0 * PAGE as u64) as usize;
                let len = s.len() as usize;
                if let SectionKind::Pinned { xxh3, .. } = s.kind {
                    let h = xxhash_rust::xxh3::xxh3_64(&buf[off..off + len]);
                    if h != xxh3 {
                        bail!("section {} checksum {h:016x}, footer says {xxh3:016x}", s.name);
                    }
                }
                at.push(PinnedAt { name: s.name.clone(), buf: bufs.len(), off, len });
            }
            bufs.push(buf);
        }
        Ok((bufs, at))
    }
}

fn flight_shard(p: PageId) -> usize {
    (((p.file.0 as u64) << 32 | p.page as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 58) as usize % FLIGHT_SHARDS
}

fn file_name(path: &Path) -> String {
    path.file_name().map_or_else(String::new, |n| n.to_string_lossy().into_owned())
}

/// Reads `n` pages from page `first` of `file` on `lane`, retrying EIO as
/// fetches do.
fn read_pages(io: &IoPool, file: &Arc<dyn DevFile>, first: u64, n: usize, lane: Lane) -> Result<AlignedBuf, String> {
    let mut buf = AlignedBuf::new(n * PAGE);
    let frames = buf.frames();
    let mut runs = Vec::new();
    for (i, c) in frames.chunks(MAX_RUN).enumerate() {
        runs.push(Run { file: file.clone(), page: (first + (i * MAX_RUN) as u64) as u32, frames: c.to_vec() });
    }
    for (i, r) in io.run(runs, lane).iter().enumerate() {
        for (k, how) in r.iter().enumerate() {
            let page = first + (i * MAX_RUN + k) as u64;
            match how {
                PageRead::Ok | PageRead::Retried => {}
                PageRead::Eio => return Err(format!("page {page}: eio")),
                PageRead::Short => return Err(format!("page {page}: short")),
            }
        }
    }
    Ok(buf)
}

struct ScopeGuard<F: FnMut()> {
    f: F,
    armed: bool,
}

impl<F: FnMut()> ScopeGuard<F> {
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl<F: FnMut()> Drop for ScopeGuard<F> {
    fn drop(&mut self) {
        if self.armed {
            (self.f)();
        }
    }
}

fn scopeguard<F: FnMut()>(f: F) -> ScopeGuard<F> {
    ScopeGuard { f, armed: true }
}
