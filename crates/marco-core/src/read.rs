//! The read contract between marco's structures and its page store.
//!
//! Structures stay synchronous and slice-based. A slice of a paged section
//! comes from [`ReadCx::page`], which either has the page in memory or
//! records it as missing and returns [`Pending`]. One run of a reader's
//! closure is a pass: [`Engine::pass`] pins an epoch guard, runs the closure
//! with a fresh [`ReadCx`] and succeeds only if nothing was missing. A caller
//! that may wait on disk (the miss pool, the writer, checkpoints, admin jobs)
//! calls [`Engine::read_blocking`], which fetches what a pass missed and runs
//! the closure again until a pass comes back clean. A closure may therefore
//! run many times: it must only read, and build its answer from scratch on
//! each run.
//!
//! Every slice is borrowed for `'g`, the life of the guard, so none outlives
//! the pass that read it; the engine reuses a page's frame only once every
//! guard that could have seen it is gone. Passes never wait on I/O while
//! holding a guard.
//!
//! The [`Engine`] lives in [`crate::store::engine`]: it keeps whole files in
//! memory, or reads FORMAT 6 files a page at a time through its cache.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

pub use crossbeam_epoch::Guard;

use rustc_hash::FxHashMap;

use crate::store::cache::{Cache, NO_FRAME};
pub use crate::store::engine::Engine;
use crate::store::page::{Page, PageId, PAGE};

/// A pass can't finish: a page it needs isn't in memory. The [`ReadCx`] that
/// returned it has already recorded the page, so callers propagate it with
/// `?`, or, in a batch, catch it per item and carry on with the next so one
/// pass names every page the batch misses. Swallowing it can't produce a
/// wrong answer: the pass fails anyway ([`ReadCx::clean`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pending;

/// The result of a read inside a pass.
pub type R<T> = Result<T, Pending>;

thread_local!(static ASYNC_THREAD: Cell<bool> = const { Cell::new(false) });

/// Marks this thread as one that must never wait on disk (a tokio runtime
/// worker): in debug builds, [`Engine::read_blocking`] and
/// [`Engine::fetch_blocking`] panic on it.
pub fn mark_async_thread() {
    ASYNC_THREAD.with(|c| c.set(true));
}

/// Whether [`mark_async_thread`] marked this thread.
pub fn on_async_thread() -> bool {
    ASYNC_THREAD.with(|c| c.get())
}

/// Pages, as a pass records them: [`PageSet::insert`] is a push that skips a
/// repeat of the last page, and the sets a pass hands out
/// ([`Misses::pages`], [`Hold`]) are sorted and unique.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PageSet(Vec<PageId>);

impl PageSet {
    pub fn insert(&mut self, p: PageId) {
        if self.0.last() != Some(&p) {
            self.0.push(p);
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = PageId> + '_ {
        self.0.iter().copied()
    }

    /// Whether `p` is in the set; it must be sorted ([`PageSet::normalize`]).
    pub fn contains(&self, p: PageId) -> bool {
        !self.0.is_empty() && self.0.binary_search(&p).is_ok()
    }

    /// Sorted, repeats removed.
    pub fn normalize(&mut self) {
        self.0.sort_unstable();
        self.0.dedup();
    }
}

static NO_PAGES: PageSet = PageSet(Vec::new());

/// What one pass read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadStats {
    /// Calls to [`ReadCx::page`].
    pub pages: u32,
    /// Those that found the page missing.
    pub misses: u32,
}

/// The paged structure a lookup reads, for the hit ratios by structure.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Structure {
    OutSmall,
    OutSubj,
    OutRkey,
    OutBysubj,
    InSmall,
    InBlocks,
    /// Pages read by anything else (tests, tools).
    #[default]
    Other,
}

impl Structure {
    pub const ALL: [Structure; 7] = [
        Structure::OutSmall,
        Structure::OutSubj,
        Structure::OutRkey,
        Structure::OutBysubj,
        Structure::InSmall,
        Structure::InBlocks,
        Structure::Other,
    ];

    /// Its metric label.
    pub fn name(self) -> &'static str {
        ["out_small", "out_subj", "out_rkey", "out_bysubj", "in_small", "in_blocks", "other"][self as usize]
    }

    /// The structure of a paged section, from its name (`r0.out.subj`).
    pub fn of_section(name: &str) -> Structure {
        let part = name.split_once('.').map_or(name, |(_, p)| p);
        match part {
            "out.small" => Structure::OutSmall,
            "out.subj" => Structure::OutSubj,
            "out.rkey" => Structure::OutRkey,
            "out.bysubj" => Structure::OutBysubj,
            "in.small" => Structure::InSmall,
            "in.blocks" => Structure::InBlocks,
            _ => Structure::Other,
        }
    }
}

/// How a lookup came out: found in the small queue, in main, in memory
/// otherwise (a file kept whole, or a page read past the cache), or missing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Found {
    Small,
    Main,
    Other,
    Miss,
}

impl Found {
    pub const ALL: [Found; 4] = [Found::Small, Found::Main, Found::Other, Found::Miss];
}

/// Who a read is for: the `source` label of marco_store_lookups_total. A
/// job's scan (a compaction reads every list of a shard past the cache)
/// misses on pages nobody serves, so the hit ratio that says whether the
/// budget holds the hot set counts requests and writes alone.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum Source {
    /// An RPC's reads.
    Request,
    /// A write's reads, a replay's included.
    Write,
    /// Checkpoints, compactions, list stats, verifies, the console and tools.
    #[default]
    Job,
}

impl Source {
    pub const ALL: [Source; 3] = [Source::Request, Source::Write, Source::Job];

    /// Its metric label.
    pub fn name(self) -> &'static str {
        ["request", "write", "job"][self as usize]
    }
}

/// Lookups by structure and how they came out, `[structure][found]`.
pub type LookupCounts = [[u32; 4]; 7];

/// What a decode used of the bytes a [`ReadCx`] served, for bytes used by
/// structure ([`ReadCx::bytes_used`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Use {
    /// All of the slice.
    Whole,
    /// The first n varints.
    Varints(u32),
    /// An in-set block of k ids ([`crate::inset::block_bytes`]).
    Set(u32),
    /// A by-subject block of k entries: k positions, then a set block.
    BySubj(u32),
    /// A small out list, given subj_index.
    OutList(usize),
    /// A small in set.
    InList,
}

impl Use {
    fn len(self, b: &[u8]) -> usize {
        match self {
            Use::Whole => b.len(),
            Use::Varints(n) => crate::varint::len_of(b, n as usize),
            Use::Set(k) => crate::inset::block_bytes(b, k as usize),
            Use::BySubj(k) => {
                let p = 4 * k as usize;
                p.min(b.len()) + b.get(p..).map_or(0, |r| crate::inset::block_bytes(r, k as usize))
            }
            Use::OutList(si) => crate::outlog::encoded_len(b, si),
            Use::InList => crate::inset::encoded_len(b),
        }
    }
}

/// Bytes decoded by structure, [`Structure`] order.
pub type UsedBytes = [u64; 7];

/// Pages a hold read past the cache ([`Hold::past`]): each in a frame of
/// the budget that's never published, so nobody else reads it, freed when
/// the hold drops.
pub(crate) struct Private {
    at: FxHashMap<PageId, u32>,
    pub(crate) cache: Arc<Cache>,
}

impl Private {
    pub(crate) fn new(cache: Arc<Cache>) -> Private {
        Private { at: FxHashMap::default(), cache }
    }

    #[inline]
    fn get(&self, p: PageId) -> Option<&Page> {
        let &f = self.at.get(&p)?;
        // SAFETY: the frame is this hold's alone until it drops, and a slice
        // can't outlive the ReadCx that borrows the hold.
        Some(unsafe { self.cache.page(f) })
    }

    pub(crate) fn has(&self, p: PageId) -> bool {
        self.at.contains_key(&p)
    }

    pub(crate) fn len(&self) -> usize {
        self.at.len()
    }

    /// Takes frame `f`, which holds page `p`.
    pub(crate) fn add(&mut self, p: PageId, f: u32) {
        self.at.insert(p, f);
    }
}

impl Drop for Private {
    fn drop(&mut self) {
        let frames: Vec<u32> = self.at.values().copied().collect();
        self.cache.unstage(&frames);
        self.cache.free_unpublished(&frames);
    }
}

/// One pass's view of the store. Built by [`Engine::pass`] (or directly, by
/// tests and tools that hold a guard), borrowed by every structure method
/// that reads a paged section, and dropped before its guard.
///
/// Its state is interior-mutable, so every call takes `&ReadCx`. It isn't
/// `Send` or `Sync`: a pass runs on one thread.
///
/// A slice it hands out can outlive the `ReadCx` but not the guard:
///
/// ```
/// # use marco_core::read::{BlockAt, Blocks, Engine, PageBlocks, ReadCx};
/// # use marco_core::store::page::{PageId, PAGE};
/// # let engine = Engine::new();
/// # let file = engine.add("f", vec![7u8; PAGE]);
/// # let at = [BlockAt::new(PageId::new(file, 0), 0, 16)];
/// # let blocks = PageBlocks::new(&at);
/// let guard = engine.pin();
/// let b = {
///     let cx = ReadCx::new(&engine, &guard);
///     blocks.block(&cx, 0).unwrap()
/// };
/// assert_eq!(b, &[7u8; 16]);
/// ```
///
/// ```compile_fail,E0597
/// # use marco_core::read::{BlockAt, Blocks, Engine, PageBlocks, ReadCx};
/// # use marco_core::store::page::{PageId, PAGE};
/// # let engine = Engine::new();
/// # let file = engine.add("f", vec![7u8; PAGE]);
/// # let at = [BlockAt::new(PageId::new(file, 0), 0, 16)];
/// # let blocks = PageBlocks::new(&at);
/// let b = {
///     let guard = engine.pin();
///     let cx = ReadCx::new(&engine, &guard);
///     blocks.block(&cx, 0).unwrap()
/// };
/// assert_eq!(b, &[7u8; 16]);
/// ```
///
/// Nor can a pass return one: its result can't name the guard's lifetime.
///
/// ```
/// # use marco_core::read::{BlockAt, Blocks, Engine, Hold, PageBlocks};
/// # use marco_core::store::page::{PageId, PAGE};
/// # let engine = Engine::new();
/// # let file = engine.add("f", vec![7u8; PAGE]);
/// # let at = [BlockAt::new(PageId::new(file, 0), 0, 16)];
/// # let blocks = PageBlocks::new(&at);
/// let n = engine.pass(&mut Hold::none(), |cx| blocks.block(cx, 0).map(|b| b.len())).unwrap();
/// assert_eq!(n, 16);
/// ```
///
/// ```compile_fail
/// # use marco_core::read::{BlockAt, Blocks, Engine, Hold, PageBlocks};
/// # use marco_core::store::page::{PageId, PAGE};
/// # let engine = Engine::new();
/// # let file = engine.add("f", vec![7u8; PAGE]);
/// # let at = [BlockAt::new(PageId::new(file, 0), 0, 16)];
/// # let blocks = PageBlocks::new(&at);
/// let b = engine.pass(&mut Hold::none(), |cx| blocks.block(cx, 0)).unwrap();
/// assert_eq!(b, &[7u8; 16]);
/// ```
pub struct ReadCx<'g> {
    guard: &'g Guard,
    engine: &'g Engine,
    /// Record pages served, for the pass to pin if it misses.
    keep_touched: bool,
    /// A write's read ([`Hold::write`]): not a use for the eviction policy,
    /// and its hits aren't counted by queue, so a lookup loads the slot and
    /// the page and never the frame's state.
    quiet: bool,
    /// Pages the request already holds: reading them again isn't a new use
    /// for the eviction policy, so a page a request fetched and then reads
    /// doesn't look reused, and one big scan stays in the small queue.
    held: &'g PageSet,
    /// The last page served, so a run of blocks in one page counts once.
    last: Cell<Option<PageId>>,
    /// Pages its hold read past the cache, looked in after the cache.
    private: Option<&'g Private>,
    missing: RefCell<PageSet>,
    touched: RefCell<PageSet>,
    stats: Cell<ReadStats>,
    by: [[Cell<u32>; 4]; 7],
    /// Record what decodes use ([`ReadCx::bytes_used`]).
    count_used: bool,
    /// (where the bytes start, how many were used, structure). Addresses,
    /// not slices: a slice would make ReadCx invariant in 'g.
    used: RefCell<Vec<(usize, u32, Structure)>>,
}

impl<'g> ReadCx<'g> {
    /// A context over `engine` for the life of `guard`, which must come from
    /// [`Engine::pin`] on the same engine. It keeps no record of the pages it
    /// serves (as [`Hold::none`]).
    pub fn new(engine: &'g Engine, guard: &'g Guard) -> ReadCx<'g> {
        ReadCx::with(engine, guard, false, false, &NO_PAGES, None, true)
    }

    pub(crate) fn with(
        engine: &'g Engine,
        guard: &'g Guard,
        keep_touched: bool,
        quiet: bool,
        held: &'g PageSet,
        private: Option<&'g Private>,
        count_used: bool,
    ) -> ReadCx<'g> {
        ReadCx {
            guard,
            engine,
            keep_touched,
            quiet,
            held,
            last: Cell::new(None),
            private,
            missing: RefCell::new(PageSet::default()),
            touched: RefCell::new(PageSet::default()),
            stats: Cell::new(ReadStats::default()),
            by: Default::default(),
            count_used,
            used: RefCell::new(Vec::new()),
        }
    }

    /// Records that a decode used `how` of `bytes`, which this pass served
    /// from a page of structure `s`. A block counts once per pass however
    /// often it's decoded, keyed by where its bytes start.
    #[inline]
    pub(crate) fn used(&self, bytes: &[u8], s: Structure, how: Use) {
        if self.count_used && !bytes.is_empty() {
            self.used.borrow_mut().push((bytes.as_ptr() as usize, how.len(bytes) as u32, s));
        }
    }

    /// Bytes the pass's decodes used so far, by structure: what requests
    /// actually read of the lists and blocks their pages hold, against which
    /// the bytes read from disk are an amplification.
    pub fn bytes_used(&self) -> UsedBytes {
        let mut out = [0u64; 7];
        let mut used = self.used.borrow_mut();
        if used.is_empty() {
            return out;
        }
        used.sort_unstable_by_key(|u| u.0);
        used.dedup_by_key(|u| u.0);
        for &(_, n, s) in used.iter() {
            out[s as usize] += n as u64;
        }
        out
    }

    /// Page `p`, if it's in memory; otherwise records it as missing and
    /// returns [`Pending`]. Never waits. A page that's bad or past its file's
    /// end is missing too: the fetch reports why ([`StorageError`]).
    #[inline]
    pub fn page(&self, p: PageId) -> R<&'g Page> {
        self.page_of(p, Structure::Other)
    }

    /// [`ReadCx::page`] for a page of structure `s`, counted under it.
    #[inline]
    pub fn page_of(&self, p: PageId, s: Structure) -> R<&'g Page> {
        let mut st = self.stats.get();
        st.pages += 1;
        // a read past the cache isn't a use: it mustn't keep pages cached
        let bump = !self.quiet && self.private.is_none() && self.last.get() != Some(p) && !self.held.contains(p);
        self.last.set(Some(p));
        let found = self
            .engine
            .lookup(p, self.guard, bump, !self.quiet)
            .or_else(|| Some((self.private?.get(p)?, Found::Other)));
        let r = match found {
            Some((page, how)) => {
                if self.keep_touched {
                    self.touched.borrow_mut().insert(p);
                }
                let c = &self.by[s as usize][how as usize];
                c.set(c.get() + 1);
                Ok(page)
            }
            None => {
                st.misses += 1;
                self.missing.borrow_mut().insert(p);
                let c = &self.by[s as usize][Found::Miss as usize];
                c.set(c.get() + 1);
                Err(Pending)
            }
        };
        self.stats.set(st);
        r
    }

    /// Lookups so far by structure and how they came out.
    pub fn lookups(&self) -> LookupCounts {
        std::array::from_fn(|s| std::array::from_fn(|h| self.by[s][h].get()))
    }

    /// Nothing has been missing so far: a pass whose closure succeeds is
    /// clean only if this still holds at its end.
    pub fn clean(&self) -> bool {
        self.missing.borrow().is_empty()
    }

    /// Pages recorded missing so far, repeats possible.
    pub fn missing(&self) -> usize {
        self.missing.borrow().len()
    }

    pub fn stats(&self) -> ReadStats {
        self.stats.get()
    }

    pub fn guard(&self) -> &'g Guard {
        self.guard
    }

    pub fn engine(&self) -> &'g Engine {
        self.engine
    }

    pub(crate) fn into_sets(self) -> (PageSet, PageSet) {
        let mut missing = self.missing.into_inner();
        missing.normalize();
        let mut touched = self.touched.into_inner();
        touched.normalize();
        (missing, touched)
    }
}

/// A run of blocks of one column (an out list's subjects or rkeys, an in
/// set's blocks): what the codecs decode, wherever the bytes live. Paged
/// implementations read through [`ReadCx::page`]; in-memory ones (the
/// overrides built from deltas, pinned heads) never miss.
///
/// The contract: a method that will read blocks first calls [`Blocks::need`]
/// on all of them, which it can work out from the pinned head, and only then
/// decodes, so one pass names every page a scan misses instead of one per
/// round. `b` is a block number of this run; one past the end is a caller's
/// bug and may panic.
pub trait Blocks<'g> {
    /// Block `b`'s bytes. A block never crosses a page, so this is one slice
    /// of one page (or of memory).
    fn block(&self, cx: &ReadCx<'g>, b: usize) -> R<&'g [u8]>;

    /// Registers every missing page among blocks `bs`, then returns
    /// [`Pending`] if there was one. Doesn't stop at the first.
    fn need(&self, cx: &ReadCx<'g>, bs: Range<usize>) -> R<()> {
        need_each(bs, |b| self.block(cx, b).is_ok())
    }
}

/// [`Blocks::need`] over `get`, which reads block b without decoding it and
/// says whether it was there: every block is tried, Pending if one wasn't.
pub fn need_each(bs: Range<usize>, mut get: impl FnMut(usize) -> bool) -> R<()> {
    let mut ok = true;
    for b in bs {
        ok &= get(b);
    }
    if ok {
        Ok(())
    } else {
        Err(Pending)
    }
}

/// Where one block is: a page, and the bytes `off..off + len` of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockAt {
    pub page: PageId,
    pub off: u16,
    pub len: u16,
}

impl BlockAt {
    /// Panics if the block crosses the page's end.
    pub fn new(page: PageId, off: u16, len: u16) -> BlockAt {
        assert!(off as usize + len as usize <= PAGE, "a block crosses its page: {off} + {len}");
        BlockAt { page, off, len }
    }
}

/// [`Blocks`] at known places in pages: the simplest paged implementation,
/// for tests and as the model for the codecs'.
#[derive(Clone, Copy, Debug)]
pub struct PageBlocks<'g> {
    at: &'g [BlockAt],
}

impl<'g> PageBlocks<'g> {
    pub fn new(at: &'g [BlockAt]) -> PageBlocks<'g> {
        PageBlocks { at }
    }

    pub fn len(&self) -> usize {
        self.at.len()
    }

    pub fn is_empty(&self) -> bool {
        self.at.is_empty()
    }
}

impl<'g> Blocks<'g> for PageBlocks<'g> {
    fn block(&self, cx: &ReadCx<'g>, b: usize) -> R<&'g [u8]> {
        let a = self.at[b];
        let page = cx.page(a.page)?;
        Ok(&page[a.off as usize..a.off as usize + a.len as usize])
    }
}

/// The pages a request keeps pinned between passes, so a page it has read
/// stays in memory while it fetches the rest. The miss pool, the writer and
/// other blocking readers make one with [`Hold::new`] per request and drop it
/// when the request is done; the first pass on a tokio worker uses
/// [`Hold::none`], since a request that misses there starts over on the pool.
/// Dropping it unpins everything.
pub struct Hold {
    pub(crate) keep: bool,
    /// Its passes are a write's reads ([`Hold::write`]).
    pub(crate) quiet: bool,
    pub(crate) pins: PageSet,
    /// The frame behind each pin, in the order of `pins` ([`NO_FRAME`] for a
    /// page of a file kept whole, which needs no pin).
    frames: Vec<u32>,
    cache: Option<Arc<Cache>>,
    /// Most pins it takes; pages past that rely on the cache's small queue.
    max_pins: usize,
    /// Most pages one pass may need at once ([`Hold::request`]).
    pub(crate) page_limit: usize,
    /// Fetch rounds run and pages they asked for, over the hold's life.
    pub(crate) rounds: u32,
    pub(crate) fetched: u64,
    /// Pages it read past the cache, if it reads that way ([`Hold::past`]).
    pub(crate) private: Option<Box<Private>>,
    /// Its clean passes add to the engine's bytes used: a request's, not a
    /// job's or a write's.
    pub(crate) count_used: bool,
    pub(crate) source: Source,
}

/// Pins a request holds at most (8 MiB of pages): with 64 requests on the
/// miss pool, pins stay under 512 MiB.
pub const MAX_PINS: usize = 2048;

impl Hold {
    /// A hold that pins what a missing pass touched, up to [`MAX_PINS`].
    pub fn new() -> Hold {
        Hold::with_max_pins(MAX_PINS)
    }

    /// A hold that pins up to `max` pages.
    pub fn with_max_pins(max: usize) -> Hold {
        Hold {
            keep: true,
            quiet: false,
            pins: PageSet::default(),
            frames: Vec::new(),
            cache: None,
            max_pins: max,
            page_limit: usize::MAX,
            rounds: 0,
            fetched: 0,
            private: None,
            count_used: false,
            source: Source::Job,
        }
    }

    /// A blocking job's hold on `engine` (tools, admin jobs, a write's
    /// reads, checkpoints): a pass that needs more than half the cache at
    /// once fails with [`StorageError::TooBig`], since it could never finish.
    pub fn job(engine: &Engine) -> Hold {
        let mut h = Hold::new();
        h.page_limit = engine.job_limit();
        h
    }

    /// A request's hold on `engine`: up to [`MAX_PINS`] pins, and a pass
    /// that needs more than [`Engine::page_limit`] pages at once fails with
    /// [`StorageError::TooBig`] rather than thrash.
    pub fn request(engine: &Engine) -> Hold {
        let mut h = Hold::new();
        h.page_limit = engine.page_limit();
        h.count_used = true;
        h.source = Source::Request;
        h
    }

    /// A blocking job's hold that reads past the cache, on the background
    /// lane ([`Engine::read_past`]): what it misses is read into memory of its
    /// own, borrowed from the budget until the hold drops, and never enters
    /// the cache; what's cached it reads without counting a use. For scans
    /// that would otherwise flush the cache (list stats, compaction).
    pub fn past(engine: &Engine) -> Hold {
        let mut h = Hold::none();
        h.page_limit = engine.job_limit();
        h.private = engine.cache().map(|c| Box::new(Private::new(c.clone())));
        h.count_used = false;
        h
    }

    /// Whether it reads past the cache ([`Hold::past`]).
    pub fn is_past(&self) -> bool {
        self.private.is_some()
    }

    /// The first pass of a write's read, on the writer: pins nothing, and
    /// reads as quietly as it can. The writer is one thread waiting on each
    /// load in turn, so the frame's state a request's lookup also loads (for
    /// the policy and the hit counts) is a cache miss it pays for every page.
    pub fn write() -> Hold {
        let mut h = Hold::none();
        h.quiet = true;
        h.count_used = false;
        h.source = Source::Write;
        h
    }

    /// A hold that pins nothing: a request's fast pass on a worker, or a
    /// tool's read.
    pub fn none() -> Hold {
        let mut h = Hold::with_max_pins(0);
        h.keep = false;
        h.count_used = true;
        h
    }

    /// The same hold, its lookups counted under `source`.
    pub fn source(mut self, source: Source) -> Hold {
        self.source = source;
        self
    }

    /// Whether it holds as many pins as it takes.
    pub(crate) fn full(&self) -> bool {
        self.pins.len() >= self.max_pins
    }

    /// Pages pinned.
    pub fn pins(&self) -> &PageSet {
        &self.pins
    }

    /// Fetch rounds run through this hold.
    pub fn rounds(&self) -> u32 {
        self.rounds
    }

    /// Pages those rounds asked for: what the request read from disk, or
    /// waited on another reader to read.
    pub fn fetched(&self) -> u64 {
        self.fetched
    }

    /// Unpins everything.
    pub fn release(&mut self) {
        if let Some(c) = &self.cache {
            if self.frames.iter().any(|&f| f != NO_FRAME) {
                let guard = crossbeam_epoch::pin();
                for &f in &self.frames {
                    if f != NO_FRAME {
                        c.unpin(f, &guard);
                    }
                }
            }
        }
        self.pins = PageSet::default();
        self.frames.clear();
    }

    /// Takes pins already made on `(page, frame)`s that aren't held yet;
    /// past its limit, it drops them at once instead.
    pub(crate) fn add(&mut self, mut add: Vec<(PageId, u32)>, cache: Option<&Arc<Cache>>) {
        if add.is_empty() {
            return;
        }
        match (&self.cache, cache) {
            (None, Some(c)) => self.cache = Some(c.clone()),
            (Some(a), Some(b)) => assert!(Arc::ptr_eq(a, b), "a Hold is used on two engines"),
            _ => {}
        }
        let room = self.max_pins.saturating_sub(self.pins.len());
        if add.len() > room {
            let over = add.split_off(room);
            if let Some(c) = cache {
                let guard = crossbeam_epoch::pin();
                for (_, f) in over {
                    if f != NO_FRAME {
                        c.unpin(f, &guard);
                    }
                }
            }
            if add.is_empty() {
                return;
            }
        }
        add.extend(self.pins.0.iter().copied().zip(self.frames.iter().copied()));
        add.sort_unstable_by_key(|x| x.0);
        self.pins.0 = add.iter().map(|x| x.0).collect();
        self.frames = add.into_iter().map(|x| x.1).collect();
    }
}

impl Default for Hold {
    fn default() -> Hold {
        Hold::new()
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        self.release();
    }
}

impl fmt::Debug for Hold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hold").field("keep", &self.keep).field("pins", &self.pins).finish()
    }
}

/// What a pass missed: hand it to [`Engine::fetch_blocking`].
#[derive(Debug)]
pub struct Misses {
    pub(crate) pages: PageSet,
    /// Pages the pass found in memory, when its hold keeps pins (0 otherwise).
    pub(crate) touched: usize,
}

impl Misses {
    /// Sorted and unique. Empty only when a closure returned [`Pending`]
    /// without a page recorded, which is a bug.
    pub fn pages(&self) -> &PageSet {
        &self.pages
    }

    /// Pages the pass needed at once: those it found and those it missed.
    pub fn needed(&self) -> usize {
        self.touched + self.pages.len()
    }
}

/// A page the store couldn't give a reader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BadPage {
    /// The file's name in the snapshot directory.
    pub file: String,
    /// The section holding the page, when the file's footer is known.
    pub section: Option<String>,
    /// The page's number in its file.
    pub page: u32,
}

impl fmt::Display for BadPage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.section {
            Some(s) => write!(f, "{} {s} page {}", self.file, self.page),
            None => write!(f, "{} page {}", self.file, self.page),
        }
    }
}

/// Why a blocking read failed. A request that gets one fails whole (a batch
/// too: a partial answer would read as "no edge"); RPCs map `Checksum` to
/// `data_loss`, `TooBig` to `resource_exhausted` and the rest to
/// `unavailable`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    /// The page failed its sum.
    Checksum(BadPage),
    /// The read kept failing with EIO.
    Eio(BadPage),
    /// The read came back short: the page is past its file's end.
    Short(BadPage),
    /// A pass returned [`Pending`] without recording a page, so there was
    /// nothing to fetch: a bug in a [`Blocks`] implementation or a caller.
    NoPage,
    /// A pass needed more pages at once than a request may hold
    /// ([`Engine::page_limit`]): it would only thrash.
    TooBig { pages: usize, limit: usize },
}

impl StorageError {
    /// The `kind` label of marco_store_read_errors_total.
    pub fn kind(&self) -> &'static str {
        match self {
            StorageError::Checksum(_) => "checksum",
            StorageError::Eio(_) => "eio",
            StorageError::Short(_) => "short",
            StorageError::NoPage => "no_page",
            StorageError::TooBig { .. } => "too_big",
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StorageError::Checksum(p) | StorageError::Eio(p) | StorageError::Short(p) => {
                write!(f, "{p}: {}", self.kind())
            }
            StorageError::NoPage => f.write_str("a pass was pending without a missing page"),
            StorageError::TooBig { pages, limit } => write!(
                f,
                "this request needs {} MiB of pages at once, more than the {} MiB one may hold",
                (*pages as u64 * PAGE as u64).div_ceil(1 << 20),
                (*limit as u64 * PAGE as u64) >> 20,
            ),
        }
    }
}

impl std::error::Error for StorageError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{FileKind, PagedFooter, PagedWriter};
    use crate::store::page::FileId;

    /// Three pages, every byte of page i equal to i + 1.
    fn engine() -> (Engine, FileId) {
        let e = Engine::new();
        let bytes = (0..3u8).flat_map(|i| [i + 1; PAGE]).collect();
        let f = e.add("shard-000-00000000.marco", bytes);
        (e, f)
    }

    fn at(f: FileId, page: u32, off: u16, len: u16) -> BlockAt {
        BlockAt::new(PageId::new(f, page), off, len)
    }

    #[test]
    fn a_clean_pass_returns_its_answer() {
        let (e, f) = engine();
        let blocks = [at(f, 0, 0, 8), at(f, 2, 4000, 96)];
        let b = PageBlocks::new(&blocks);
        let sum = e
            .pass(&mut Hold::none(), |cx| {
                b.need(cx, 0..2)?;
                let s = b.block(cx, 0)?.iter().map(|&x| x as u32).sum::<u32>() + b.block(cx, 1)?[95] as u32;
                assert_eq!(cx.stats(), ReadStats { pages: 4, misses: 0 });
                Ok(s)
            })
            .unwrap();
        assert_eq!(sum, 8 + 3);
    }

    #[test]
    fn a_pass_names_every_page_it_missed() {
        let (e, f) = engine();
        let g = FileId(9);
        let blocks = [at(f, 1, 0, 1), at(f, 7, 0, 1), at(f, 5, 0, 1), at(g, 0, 0, 1), at(f, 5, 8, 1)];
        let b = PageBlocks::new(&blocks);
        let mut hold = Hold::new();
        let m = e
            .pass(&mut hold, |cx| {
                let r = b.need(cx, 0..5);
                assert_eq!(r, Err(Pending));
                assert!(!cx.clean());
                assert_eq!(cx.stats(), ReadStats { pages: 5, misses: 4 });
                Ok(())
            })
            .unwrap_err();
        let want = [PageId::new(f, 5), PageId::new(f, 7), PageId::new(g, 0)];
        assert_eq!(m.pages().iter().collect::<Vec<_>>(), want, "sorted, once each, though the closure said Ok");
        assert_eq!(hold.pins().iter().collect::<Vec<_>>(), [PageId::new(f, 1)], "the pages it touched are pinned");
        let mut none = Hold::none();
        assert!(e.pass(&mut none, |cx| b.need(cx, 0..5)).is_err());
        assert!(none.pins().is_empty());
        hold.release();
        assert!(hold.pins().is_empty());
    }

    #[test]
    fn lookups_count_under_the_holds_source() {
        let (e, f) = engine();
        let one = |cx: &ReadCx<'_>| cx.page_of(PageId::new(f, 0), Structure::InSmall).map(|_| ());
        e.pass(&mut Hold::request(&e), one).unwrap();
        e.pass(&mut Hold::none().source(Source::Request), one).unwrap();
        e.pass(&mut Hold::write(), one).unwrap();
        e.pass(&mut Hold::new().source(Source::Write), one).unwrap();
        e.read_past(one).unwrap();
        e.read(one).unwrap();
        e.pass(&mut Hold::job(&e), one).unwrap();
        let st = e.stats();
        let n = |src: Source| st.lookups[src as usize][Structure::InSmall as usize][Found::Other as usize];
        assert_eq!((n(Source::Request), n(Source::Write), n(Source::Job)), (2, 2, 3));
    }

    #[test]
    fn a_swallowed_pending_still_fails_the_pass() {
        let (e, f) = engine();
        let blocks = [at(f, 0, 0, 1), at(f, 3, 0, 1)];
        let b = PageBlocks::new(&blocks);
        // a batch catches Pending per item and carries on
        let r =
            e.pass(&mut Hold::none(), |cx| Ok((0..2).map(|i| b.block(cx, i).ok().map(|s| s[0])).collect::<Vec<_>>()));
        assert_eq!(r.unwrap_err().pages().iter().collect::<Vec<_>>(), [PageId::new(f, 3)]);
    }

    #[test]
    fn read_blocking_reports_what_it_cannot_fetch() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("shard-001-00000000.marco");
        let mut w = PagedWriter::create(&p, FileKind::Base).unwrap();
        w.pinned("r0.out.idx", &[1, 2, 3]).unwrap();
        w.paged("r0.out.small", 0, &[5u8; PAGE + 1]).unwrap();
        w.finish("shard", &()).unwrap();
        let e = Engine::new();
        let bytes = std::fs::read(&p).unwrap();
        let footer = PagedFooter::read(&bytes).unwrap();
        let f = e.add("shard-001-00000000.marco", bytes);
        assert_eq!(e.name(f), Some("shard-001-00000000.marco"));
        let small = footer.section("r0.out.small").unwrap().clone();
        let page = |i: u32| PageId::new(f, small.first_page() + i);
        let mut hold = Hold::new();
        let v = e.read_blocking(&mut hold, |cx| Ok(cx.page(page(1))?[0])).unwrap();
        assert_eq!(v, 5);
        let err = e.read_blocking(&mut hold, |cx| cx.page(PageId::new(f, 1 << 20)).map(|p| p[0])).unwrap_err();
        assert_eq!(err.to_string(), format!("shard-001-00000000.marco page {}: short", 1 << 20));
        let blocks = [at(f, small.first_page() + 1, 0, 1)];
        let b = PageBlocks::new(&blocks);
        assert_eq!(e.read_blocking(&mut hold, |cx| b.block(cx, 0).map(|s| s[0])), Ok(5));
        let fid = FileId(f.0 + 1);
        let err = e.read_blocking(&mut hold, |cx| cx.page(PageId::new(fid, 0)).map(|_| ())).unwrap_err();
        assert_eq!(err, StorageError::Short(BadPage { file: format!("file {}", fid.0), section: None, page: 0 }));
        assert_eq!(e.read_blocking(&mut hold, |_| R::<()>::Err(Pending)), Err(StorageError::NoPage));
        let named = StorageError::Checksum(BadPage { file: "f".into(), section: Some("r0.in.blocks".into()), page: 7 });
        assert_eq!(named.to_string(), "f r0.in.blocks page 7: checksum");
    }
}
