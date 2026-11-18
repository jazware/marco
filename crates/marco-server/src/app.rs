//! The server's state: two copies of the graph, the writer thread that
//! applies every write to both, the sources and their cursors, readiness,
//! the recent-writes ring and the jobs.
//!
//! Readers load the published copy and never wait. One writer thread owns
//! the other copy: it applies a batch of writes to it, publishes it, waits
//! for readers to leave the old one and applies the same batch there.
//!
//! Durability (writer.rs): every few seconds the writes applied since the
//! last change segment are written as one, with every source's cursor at
//! that cut. A checkpoint folds the segments into new shard files when a
//! restart would replay too many of them, or daily. Restart opens the
//! snapshot, replays the segments after it, and the sources resume from the
//! newest segment's cursors; marco is ready once they've caught up.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use arc_swap::{ArcSwap, Guard};
use marco_core::did::classify;
use marco_core::read::{Engine, Hold};
use marco_core::relation::parse_uri;
use marco_core::store::Storage;
use marco_core::{segment, Added, Graph, Rec, RelationshipSet, Removed};

pub use marco_core::{Op, Status};

use crate::auth::AuthConfig;
use crate::writer::{self, Req};

pub const MAX_BATCH: usize = 1000;
const RECENT: usize = 256;
/// A source is caught up once it reports a lag under this.
pub const READY_LAG_SECONDS: f64 = 1.0;
/// The source of Graph writes.
pub const API_SOURCE: &str = "api";
/// The source of the operator's SetActorStatus.
pub const ADMIN_SOURCE: &str = "admin";

/// A flag as Settings shows it.
#[derive(Clone, Debug)]
pub struct FlagInfo {
    pub name: String,
    pub value: String,
    pub source: String,
}

pub struct Config {
    pub snapshot: PathBuf,
    pub read_only: bool,
    /// The API listener doesn't route Graph writes at all, token or not; the
    /// admin listener and the sources still write.
    pub api_read_only: bool,
    pub merge_ratio: f64,
    pub compact_min: usize,
    pub api_listen: String,
    pub admin_listen: String,
    /// /metrics and pprof without a token, for a scraper; off when None.
    pub ops_listen: Option<String>,
    /// A second API listener (the same routes as `api_listen`), for a port the host can publish; off when None.
    pub api_listen_extra: Option<String>,
    pub auth: AuthConfig,
    pub flags: Vec<FlagInfo>,
    /// How often the writes applied are written as a change segment.
    pub segment_interval: Duration,
    /// A checkpoint starts once the segments since the last hold this many ops…
    pub checkpoint_ops: u64,
    /// …or the last is this old.
    pub checkpoint_age: Duration,
    /// Answer Graph reads before every expected source has caught up.
    pub serve_before_ready: bool,
    /// Checkpoints drop tombstones older than this.
    pub tombstone_ttl: Duration,
    /// Sources readiness waits for (expect_source adds more).
    pub expected_sources: Vec<String>,
    /// Load every page into the cache at open, before a checkpoint's switch
    /// and after a compaction, when the budget holds them all.
    pub load: bool,
    /// The store's budget (`--memory`).
    pub storage: Storage,
    /// Threads a request that misses moves to (`--miss-threads`).
    pub miss_threads: usize,
    /// The most actors an EdgesAmong request takes (`--edges-among-max-actors`).
    pub edges_among_max: usize,
    /// The process's memory limit, which the store keeps it under by giving back
    /// cache ([`crate::memory`]); None: no limit to keep.
    pub memory_limit: Option<u64>,
    /// When shards are compacted, after open and after each checkpoint.
    pub compaction: marco_core::CompactPolicy,
    /// The pace of background I/O (`--background-io`): compactions,
    /// checkpoint writes, verify and list stats.
    pub background: marco_core::store::io::Rate,
    /// The most of a subscriber's newest records a via reads
    /// (`--via-max-records`).
    pub via_max_records: usize,
    /// The relationships config (`--relationships`, its TOML); None is
    /// [`marco_core::relationship::DEFAULT_CONFIG`] less what the snapshot
    /// has no relations for.
    pub relationships: Option<String>,
}

/// Distinct pages failing on the device within the storm window that take
/// `/readyz` down.
pub const STORM_PAGES: usize = 64;

/// Memory a store budget must leave the cache beyond the pinned tier.
pub const MIN_CACHE: u64 = 2 << 30;

impl Config {
    pub fn new(snapshot: PathBuf) -> Config {
        Config {
            snapshot,
            read_only: false,
            api_read_only: false,
            merge_ratio: 0.125,
            compact_min: 0,
            api_listen: "127.0.0.1:0".into(),
            admin_listen: "127.0.0.1:0".into(),
            ops_listen: None,
            api_listen_extra: None,
            auth: AuthConfig::default(),
            flags: Vec::new(),
            segment_interval: Duration::from_secs(5),
            checkpoint_ops: 3_000_000,
            checkpoint_age: Duration::from_secs(86_400),
            serve_before_ready: false,
            tombstone_ttl: Duration::from_secs(7 * 86_400),
            expected_sources: Vec::new(),
            load: true,
            storage: Storage::from_env().unwrap_or_else(|e| panic!("{e:#}")),
            miss_threads: 64,
            edges_among_max: crate::graph_svc::EDGES_AMONG_MAX,
            memory_limit: None,
            compaction: marco_core::CompactPolicy::default(),
            background: marco_core::store::io::Rate::default(),
            via_max_records: marco_core::graph::VIA_MAX_RECORDS,
            relationships: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Added,
    Exists,
    Deleted,
    Removed(String),
    NotFound,
    Error(String),
}

impl Outcome {
    pub fn word(&self) -> &'static str {
        match self {
            Outcome::Added => "added",
            Outcome::Exists => "exists",
            Outcome::Deleted => "deleted",
            Outcome::Removed(_) => "removed",
            Outcome::NotFound => "not_found",
            Outcome::Error(_) => "error",
        }
    }
}

/// Applies one op.
pub fn apply_op(g: &mut Graph, op: &Op) -> Outcome {
    apply_op_noting(g, op, &mut None)
}

/// Whether `e` came from a page the store couldn't read, rather than from
/// the op itself.
fn is_storage(e: &anyhow::Error) -> bool {
    e.chain().any(|c| c.is::<marco_core::read::StorageError>())
}

/// [`apply_op`], keeping the first error a bad page caused in `storage`.
fn apply_op_noting(g: &mut Graph, op: &Op, storage: &mut Option<String>) -> Outcome {
    let mut fail = |e: anyhow::Error| {
        let m = format!("{e:#}");
        if storage.is_none() && is_storage(&e) {
            *storage = Some(m.clone());
        }
        Outcome::Error(e.to_string())
    };
    match op {
        Op::Add { uri, subject, list, time_us } => match g.add_rec(uri, subject, list.as_deref(), *time_us) {
            Ok(Added::Added) => Outcome::Added,
            Ok(Added::Exists) => Outcome::Exists,
            Ok(Added::Deleted) => Outcome::Deleted,
            Err(e) => fail(e),
        },
        Op::Delete { uri, time_us } => match g.delete_at(uri, *time_us) {
            Ok(Removed::Subject(s)) => Outcome::Removed(s),
            Ok(Removed::NotFound) => Outcome::NotFound,
            Err(e) => fail(e),
        },
    }
}

pub enum Applied {
    Ops(Vec<Outcome>),
    Status(Result<u64, String>),
}

/// Applies one record: the writer, the second copy, a restart's replay and
/// a checkpoint's replay all go through here, so they agree.
pub fn apply_rec(g: &mut Graph, rec: &Rec) -> Applied {
    apply_rec_noting(g, rec, &mut None)
}

fn apply_rec_noting(g: &mut Graph, rec: &Rec, storage: &mut Option<String>) -> Applied {
    match rec {
        Rec::Apply { source, ops, .. } => {
            // a stamp holds only against other sources: the record's own source applies in order
            g.set_source(source);
            Applied::Ops(ops.iter().map(|op| apply_op_noting(g, op, storage)).collect())
        }
        Rec::Status { did, status, time_us, .. } => {
            Applied::Status(g.set_status(did, *status, *time_us).map_err(|e| {
                if storage.is_none() && is_storage(&e) {
                    *storage = Some(format!("{e:#}"));
                }
                e.to_string()
            }))
        }
    }
}

#[derive(Clone, Debug)]
pub struct Recent {
    pub seq: u64,
    pub at_ms: i64,
    pub rel: String,
    pub add: bool,
    pub actor: String,
    pub subject: String,
    pub outcome: &'static str,
    pub uri: String,
}

#[derive(Clone, Debug, Default)]
pub struct JobState {
    pub kind: &'static str,
    pub running: bool,
    pub started_ms: i64,
    pub finished_ms: i64,
    pub error: String,
    pub done: u32,
    pub total: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Verify {
    #[default]
    Unverified,
    Verifying,
    Ok,
    Mismatch(String),
}

impl Verify {
    pub fn word(&self) -> &'static str {
        match self {
            Verify::Unverified => "unverified",
            Verify::Verifying => "verifying",
            Verify::Ok => "ok",
            Verify::Mismatch(_) => "mismatch",
        }
    }
}

/// What a source's consumer reports about itself.
#[derive(Clone, Debug, Default)]
pub struct SourceReport {
    pub url: String,
    pub state: String,
    pub cursor_kind: String,
    /// How far the source's newest applied event is behind its head.
    pub lag_seconds: f64,
    pub errors: u64,
    pub reconnects: u64,
}

#[derive(Clone, Debug, Default)]
pub struct SourceState {
    /// The cursor of the last write applied.
    pub cursor: String,
    /// The cursor in the newest change segment.
    pub durable: String,
    pub expected: bool,
    pub caught_up: bool,
    pub ops: u64,
    pub last_ms: i64,
    pub report: Option<SourceReport>,
}

#[derive(Clone, Debug, Default)]
pub struct Durability {
    pub segment: u64,
    pub segment_ms: i64,
    pub segment_write_seconds: f64,
    pub segments: u32,
    pub segment_bytes: u64,
    pub segment_ops: u64,
    pub pending_ops: u64,
    pub checkpoint: u64,
    pub checkpoint_ms: i64,
    pub checkpoint_seconds: f64,
    pub checkpoint_bytes: u64,
    pub checkpoint_shards: u32,
    /// The last checkpoint's patch bytes (R) and their pinned bytes (P_new).
    pub checkpoint_patch_bytes: u64,
    pub checkpoint_pinned_bytes: u64,
    pub compactions: u64,
    pub compaction_bytes: u64,
    /// The last compaction's error, cleared by the next that commits.
    pub compaction_error: String,
    pub replayed_segments: u32,
    pub replayed_ops: u64,
    pub replay_seconds: f64,
    /// The last segment write's error, while it's being retried.
    pub segment_error: String,
    /// The last checkpoint's error, cleared by the next that commits.
    pub checkpoint_error: String,
    pub checkpoint_failures: u64,
}

pub struct App {
    pub cfg: Config,
    /// The page store every copy of the graph and every snapshot opened
    /// reads through.
    pub engine: Arc<Engine>,
    pub miss_pool: crate::pool::MissPool,
    /// What the process holds beside the store, as the memory watcher saw it.
    pub beside: Arc<crate::memory::Beside>,
    graph: ArcSwap<Graph>,
    tx: Mutex<Option<mpsc::Sender<Req>>>,
    pub recent: Mutex<VecDeque<Recent>>,
    pub write_seq: AtomicU64,
    pub job: Mutex<JobState>,
    /// Per file of the snapshot being served: the directory, then each shard file.
    pub verify: Mutex<Vec<Verify>>,
    pub list_stats: Arc<crate::liststats::Cache>,
    pub series: Mutex<crate::series::Ring>,
    pub sources: Mutex<BTreeMap<String, SourceState>>,
    pub durability: Mutex<Durability>,
    pub reads_before_ready: AtomicU64,
    ready: AtomicBool,
    /// Per source: (ms, ops) at the last sample, and ops/s since the one before.
    rates: Mutex<BTreeMap<String, (i64, u64, f64)>>,
    pub started_ms: i64,
    pub started: Instant,
    pub open_seconds: f64,
    /// Open loaded every page: the budget holds the whole snapshot.
    pub resident: AtomicBool,
    /// Paged bytes in the shards' files no index names, as of the last
    /// checkpoint or compaction.
    pub garbage_bytes: AtomicU64,
    /// When compactions committed, the last day's.
    pub compacted_at: Mutex<VecDeque<i64>>,
    /// What Relationships and BlockPairs answer.
    pub relationships: RelationshipSet,
    /// The listeners' clean stop (`--shutdown-drain-secs`): `/readyz` is 503 once it begins.
    pub drain: marco_ops::drain::Drain,
}

pub fn now_ms() -> i64 {
    marco_core::store::now_ms()
}

/// Warns when the memory limit leaves under 2 GiB beside the store budget:
/// the deltas, the directory's live DIDs and the request heap live outside
/// it, and what they take past the room comes out of the cache.
fn warn_limit(memory: u64, limit: u64) {
    if limit.saturating_sub(memory) < MIN_CACHE {
        eprintln!(
            "marco: warning: the memory limit ({} MiB) leaves under {} MiB beside --memory ({} MiB) for the \
             graph's deltas and the process; what they take past it comes out of the cache",
            limit >> 20,
            MIN_CACHE >> 20,
            memory >> 20
        );
    }
}

/// A snapshot with the change segments after it replayed.
pub struct Replayed {
    pub graph: Graph,
    /// The newest segment's cursors (the snapshot's, with none).
    pub cursors: BTreeMap<String, String>,
    pub segments: u32,
    pub ops: u64,
    /// The newest segment replayed (the snapshot's, with none).
    pub last: u64,
    /// The first op skipped on a bad page, if one was.
    pub skipped: Option<String>,
}

/// Applies `recs` in order to `g`, and to its twin `t` if given, planning
/// each run of them first so their reads wait on disk once per dependency
/// level ([`Graph::plan`]); the plan's pins last through both applies.
/// Returns the first error a bad page caused: that op was skipped.
pub fn apply_planned(g: &mut Graph, mut t: Option<&mut Graph>, recs: &[Rec]) -> Option<String> {
    let mut storage = None;
    let mut at = 0;
    while at < recs.len() {
        let mut ops = 0;
        let end = recs[at..].iter().position(|r| {
            ops += r.ops();
            ops > PLAN_OPS
        });
        let end = at + end.map_or(recs.len() - at, |e| e.max(1));
        let mut hold = Hold::new();
        g.plan(&recs[at..end].iter().collect::<Vec<_>>(), &mut hold);
        for r in &recs[at..end] {
            apply_rec_noting(g, r, &mut storage);
            if let Some(t) = t.as_deref_mut() {
                apply_rec_noting(t, r, &mut storage);
            }
        }
        at = end;
    }
    storage
}

/// Ops a replay plans at once.
const PLAN_OPS: usize = 1000;

/// Opens the snapshot through `engine` and replays the segments after it;
/// with `upto`, the segments after that are left out.
pub fn open_replayed(cfg: &Config, engine: &Arc<Engine>, upto: Option<u64>) -> Result<Replayed> {
    let path = &cfg.snapshot;
    let mut g = Graph::open_on(path, engine)?;
    g.merge_ratio = cfg.merge_ratio;
    g.compact_min = cfg.compact_min;
    g.tombstone_ttl_us = cfg.tombstone_ttl.as_micros() as i64;
    let m = g.manifest().context("no manifest")?.clone();
    let mut cursors = m.cursors.clone();
    let (mut n, mut ops, mut last) = (0u32, 0u64, m.segment);
    let mut skipped = None;
    g.quiet = true;
    for (k, p) in segment::list(path)? {
        if k <= m.segment {
            continue;
        }
        if upto.is_some_and(|u| k > u) {
            break;
        }
        if k != last + 1 {
            bail!("change segment {} is missing ({} follows {last})", last + 1, p.display());
        }
        let s = segment::read(&p)?;
        if s.header.build != m.build {
            bail!("{} belongs to another snapshot", p.display());
        }
        if let Some(e) = apply_planned(&mut g, None, &s.records) {
            // a checkpoint folds these segments in: an op skipped now would be lost
            if upto.is_some() {
                bail!("replaying {}: {e}", p.display());
            }
            eprintln!("marco: replaying {}: an op skipped on a bad page: {e}", p.display());
            skipped.get_or_insert(e);
        }
        ops += s.records.iter().map(|r| r.ops() as u64).sum::<u64>();
        cursors = s.header.cursors;
        last = k;
        n += 1;
    }
    g.quiet = false;
    Ok(Replayed { graph: g, cursors, segments: n, ops, last, skipped })
}

/// The relationships a marco serves: `--relationships`, every relation it
/// names in the snapshot, or the default less what the snapshot lacks.
fn relationships_of(cfg: &Config, rels: &marco_core::Relations) -> Result<RelationshipSet> {
    match &cfg.relationships {
        Some(toml) => {
            let defs = marco_core::relationship::parse_toml(toml)?;
            RelationshipSet::resolve(defs, rels).context("--relationships")
        }
        None => {
            let (set, left_out) = RelationshipSet::bsky_default(rels);
            if !left_out.is_empty() {
                eprintln!(
                    "marco: relationships: {} left out, the snapshot has no relations for it",
                    left_out.join(", ")
                );
            }
            Ok(set)
        }
    }
}

impl App {
    pub fn open(cfg: Config) -> Result<Arc<App>> {
        let engine = cfg.storage.engine_paced(cfg.background)?;
        App::open_on(cfg, engine)
    }

    /// Opens on an engine the caller made (tests give it a faulty device);
    /// it must have `cfg.storage`'s budget.
    pub fn open_on(cfg: Config, engine: Arc<Engine>) -> Result<Arc<App>> {
        let t = Instant::now();
        if !cfg.read_only {
            let m = marco_core::store::Manifest::read(&cfg.snapshot)?;
            if let Err(e) = marco_core::store::remove_unlisted(&cfg.snapshot, &m, true) {
                eprintln!("marco: removing leftovers in {}: {e:#}", cfg.snapshot.display());
            }
        }
        let Replayed { graph: g, cursors, segments: segs, ops, last, .. } = open_replayed(&cfg, &engine, None)?;
        let mut resident = false;
        let beside = Arc::new(crate::memory::Beside::default());
        if let Some(s) = g.snapshot() {
            let Storage { memory, max_frames } = &cfg.storage;
            // a checkpoint that patches every shard reads each one's index and heads
            // again beside the old until its switch; the directory it never rewrites
            let pinned = engine.bytes().borrowed;
            let again = s.shard_pinned_bytes();
            let min = pinned + again + MIN_CACHE;
            if min > *memory && max_frames.is_none() {
                bail!(
                    "--memory {} MiB is too small: this snapshot's pinned tier takes {} MiB, a checkpoint reads {} \
                     MiB of it again, and the cache needs {} MiB more; give it at least {} MiB",
                    memory >> 20,
                    pinned >> 20,
                    again >> 20,
                    MIN_CACHE >> 20,
                    min.div_ceil(1 << 20)
                );
            }
            if let (None, Some(limit)) = (max_frames, cfg.memory_limit) {
                warn_limit(*memory, limit);
                crate::memory::spawn(engine.clone(), *memory, limit, beside.clone());
            }
            if cfg.load {
                let t = Instant::now();
                resident = s.load();
                if resident {
                    eprintln!(
                        "marco: loaded {} MiB of pages in {:.1} s",
                        (s.paged_pages() * marco_core::store::page::PAGE as u64) >> 20,
                        t.elapsed().as_secs_f64()
                    );
                }
            }
        }
        let open_seconds = t.elapsed().as_secs_f64();
        let m = g.manifest().cloned().unwrap_or_default();
        let files = g.snapshot().map_or(0, |s| s.files().count());
        let mut dur = Durability {
            segment: last,
            segments: segs,
            segment_ops: ops,
            checkpoint: m.checkpoint,
            checkpoint_ms: m.written_ms,
            replayed_segments: segs,
            replayed_ops: ops,
            replay_seconds: open_seconds,
            ..Default::default()
        };
        for (k, p) in segment::list(&cfg.snapshot)? {
            if k > m.segment {
                dur.segment_bytes += std::fs::metadata(&p).map_or(0, |x| x.len());
                if k == last {
                    dur.segment_ms = std::fs::metadata(&p)
                        .and_then(|x| x.modified())
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map_or(0, |d| d.as_millis() as i64);
                }
            }
        }
        let mut sources: BTreeMap<String, SourceState> = cursors
            .iter()
            .map(|(k, v)| (k.clone(), SourceState { cursor: v.clone(), durable: v.clone(), ..Default::default() }))
            .collect();
        for s in &cfg.expected_sources {
            sources.entry(s.clone()).or_default().expected = true;
        }
        let ready = cfg.expected_sources.is_empty();
        let relationships = relationships_of(&cfg, g.relations())?;
        let spare = (!cfg.read_only).then(|| g.clone());
        let miss_pool = crate::pool::MissPool::new(cfg.miss_threads);
        let app = Arc::new(App {
            relationships,
            cfg,
            engine,
            miss_pool,
            beside,
            graph: ArcSwap::from_pointee(g),
            tx: Mutex::new(None),
            recent: Mutex::new(VecDeque::with_capacity(RECENT)),
            write_seq: AtomicU64::new(0),
            job: Mutex::new(JobState::default()),
            verify: Mutex::new(vec![Verify::Unverified; files]),
            list_stats: Default::default(),
            series: Mutex::new(Default::default()),
            sources: Mutex::new(sources),
            durability: Mutex::new(dur),
            reads_before_ready: AtomicU64::new(0),
            ready: AtomicBool::new(ready),
            rates: Mutex::new(BTreeMap::new()),
            started_ms: now_ms(),
            started: Instant::now(),
            open_seconds,
            resident: AtomicBool::new(resident),
            garbage_bytes: AtomicU64::new(0),
            compacted_at: Mutex::new(VecDeque::new()),
            drain: marco_ops::drain::Drain::new(),
        });
        if let Some(spare) = spare {
            let tx = writer::spawn(app.clone(), spare, writer::Init { cursors, segment: last, ops });
            *app.tx.lock().unwrap() = Some(tx);
        }
        crate::metrics::READY.set(ready as i64);
        Ok(app)
    }

    /// The published copy. Hold it briefly: the writer waits for every
    /// reader of a copy to leave before it writes that copy again.
    pub fn graph(&self) -> Guard<Arc<Graph>> {
        self.graph.load()
    }

    pub(crate) fn publish(&self, g: Arc<Graph>) -> Arc<Graph> {
        self.graph.swap(g)
    }

    fn send(&self, r: Req) -> Result<()> {
        match &*self.tx.lock().unwrap() {
            Some(tx) => tx.send(r).map_err(|_| anyhow::anyhow!("the writer has stopped")),
            None => bail!("this marco is --read-only"),
        }
    }

    pub fn status_of(&self, did: &str) -> Option<Status> {
        self.graph().status_of(did)
    }

    pub fn any_hidden(&self) -> bool {
        self.graph().any_status()
    }

    pub fn hidden(&self, did: &str) -> bool {
        self.graph().hidden(did)
    }

    /// Checks ops before any applies, so a bad batch changes nothing. The
    /// subject and list rules are the ones every ingest path applies; an op
    /// carries no record, so a relation's `when` isn't checked here.
    pub fn validate(g: &Graph, ops: &[Op]) -> Result<(), String> {
        for (i, op) in ops.iter().enumerate() {
            let uri = match op {
                Op::Add { uri, .. } | Op::Delete { uri, .. } => uri,
            };
            let Some((_, coll, _)) = parse_uri(uri) else { return Err(format!("op {i}: {uri:?} isn't a record URI")) };
            let Some(rel) = g.relations().by_collection(coll) else {
                return Err(format!("op {i}: no relation for {coll}"));
            };
            if let Op::Add { subject, list, .. } = op {
                let r = g.relations().get(rel);
                if r.check_subject(uri, subject).is_err() {
                    return Err(format!("op {i}: subject {subject:?} isn't {}", r.subject_desc()));
                }
                if r.check_list(uri, list.as_deref()).is_err() {
                    return Err(format!("op {i}: {coll} needs a list in the record's own repo"));
                }
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------ the seam the sources call

    /// Applies `ops` from `source` in order and advances its cursor with
    /// them: on return they're applied and visible, and they're durable with
    /// the next change segment (a few seconds). An empty `cursor` leaves the
    /// source's cursor as it was; empty `ops` just advance it. A source's
    /// first call that sets a cursor returns once that's in a segment, so a
    /// source never restarts from nothing after it began: that segment is
    /// written before its ops apply. An op's `time_us` of 0 becomes now.
    pub async fn apply_from(&self, source: &str, cursor: &str, ops: &[Op]) -> Result<Vec<Outcome>> {
        let (reply, rx) = tokio::sync::oneshot::channel();
        let now = now_ms() * 1000;
        let ops = ops.iter().map(|o| o.clone().stamped(now)).collect();
        let rec = Rec::Apply { source: source.into(), cursor: cursor.into(), ops };
        self.send(Req::Apply { rec, durable: false, reply })?;
        match rx.await.context("the writer dropped the write")?? {
            Applied::Ops(o) => Ok(o),
            Applied::Status(_) => unreachable!("an apply answers with outcomes"),
        }
    }

    /// Sets an actor's status from `source`, with the cursor that carried it
    /// and the time the source saw it (0: now). Deleted drops its records,
    /// with tombstones at that time. Returns how many it removed.
    pub async fn status_from(
        &self,
        source: &str,
        cursor: &str,
        did: &str,
        status: Option<Status>,
        time_us: i64,
    ) -> Result<u64> {
        self.status_(source, cursor, did, status, time_us, false).await
    }

    async fn status_(
        &self,
        source: &str,
        cursor: &str,
        did: &str,
        status: Option<Status>,
        time_us: i64,
        durable: bool,
    ) -> Result<u64> {
        if classify(did).is_none() {
            bail!("{did:?} isn't a DID");
        }
        let (reply, rx) = tokio::sync::oneshot::channel();
        let rec = Rec::Status {
            source: source.into(),
            cursor: cursor.into(),
            did: did.into(),
            status,
            time_us: if time_us == 0 { now_ms() * 1000 } else { time_us },
        };
        self.send(Req::Apply { rec, durable, reply })?;
        match rx.await.context("the writer dropped the write")?? {
            Applied::Status(r) => r.map_err(anyhow::Error::msg),
            Applied::Ops(_) => unreachable!("a status answers with a count"),
        }
    }

    /// The cursor a restart resumes `source` from: the one in the newest
    /// change segment (or the snapshot's, before any).
    pub fn source_cursor(&self, source: &str) -> Option<String> {
        self.sources.lock().unwrap().get(source).map(|s| s.durable.clone()).filter(|c| !c.is_empty())
    }

    /// The cursor of `source`'s last applied write.
    pub fn latest_cursor(&self, source: &str) -> Option<String> {
        self.sources.lock().unwrap().get(source).map(|s| s.cursor.clone()).filter(|c| !c.is_empty())
    }

    /// Declares a configured source: marco isn't ready until it has caught
    /// up. Call it before serving.
    pub fn expect_source(&self, source: &str) {
        let mut m = self.sources.lock().unwrap();
        let s = m.entry(source.into()).or_default();
        s.expected = true;
        if !s.caught_up {
            self.ready.store(false, Ordering::Relaxed);
            crate::metrics::READY.set(0);
        }
    }

    /// A source's consumer reports its state; a lag under a second after
    /// start counts as caught up.
    pub fn report_source(&self, source: &str, r: SourceReport) {
        let mut m = self.sources.lock().unwrap();
        let s = m.entry(source.into()).or_default();
        if r.lag_seconds < READY_LAG_SECONDS {
            s.caught_up = true;
        }
        s.report = Some(r);
        drop(m);
        self.readiness();
    }

    /// Ready, or why not. Once every expected source has caught up after a
    /// start, marco stays ready: a later lag is the source's, not a restart's.
    pub fn readiness(&self) -> (bool, String) {
        if self.ready.load(Ordering::Relaxed) {
            return (true, String::new());
        }
        let m = self.sources.lock().unwrap();
        let behind: Vec<&str> = m.iter().filter(|(_, s)| s.expected && !s.caught_up).map(|(k, _)| k.as_str()).collect();
        if behind.is_empty() {
            self.ready.store(true, Ordering::Relaxed);
            crate::metrics::READY.set(1);
            return (true, String::new());
        }
        (false, format!("catching up: {}", behind.join(", ")))
    }

    /// What `/readyz` says: [`App::readiness`], unless reads are failing on
    /// the device across many pages, which means the disk is going. Isolated
    /// bad pages leave marco ready: their requests fail, the rest serve.
    pub fn ready_status(&self) -> (bool, String) {
        let n = self.engine.failing_pages(marco_core::store::engine::STORM_WINDOW);
        if n >= STORM_PAGES {
            return (false, format!("storage failing: {n} pages failed their reads in the last 60 s"));
        }
        self.readiness()
    }

    pub fn is_ready(&self) -> bool {
        self.readiness().0
    }

    /// Whether a Graph read may be answered: before ready, only with
    /// --serve-before-ready, and counted.
    pub fn read_gate(&self) -> Result<(), String> {
        let (ok, why) = self.readiness();
        if ok {
            return Ok(());
        }
        self.reads_before_ready.fetch_add(1, Ordering::Relaxed);
        crate::metrics::READS_BEFORE_READY.inc();
        if self.cfg.serve_before_ready {
            Ok(())
        } else {
            Err(why)
        }
    }

    // ------------------------------------------------------------ the API's writes

    /// Graph writes: source "api". The caller validated them.
    pub async fn apply(&self, ops: &[Op], cursor: &str) -> Result<Vec<Outcome>> {
        self.apply_from(API_SOURCE, cursor, ops).await
    }

    /// The operator's status: durable when it answers.
    pub async fn set_status(&self, did: &str, status: Option<Status>) -> Result<u64, String> {
        self.status_(ADMIN_SOURCE, "", did, status, 0, true).await.map_err(|e| format!("{e:#}"))
    }

    /// Writes a change segment now and waits for it.
    pub async fn flush(&self) -> Result<()> {
        let (reply, rx) = tokio::sync::oneshot::channel();
        self.send(Req::Flush { reply })?;
        rx.await.context("the writer stopped")?
    }

    /// Writes the last change segment and stops the writer.
    pub async fn shutdown(&self) -> Result<()> {
        let (reply, rx) = tokio::sync::oneshot::channel();
        self.send(Req::Shutdown { reply })?;
        let r = rx.await.context("the writer stopped")?;
        self.tx.lock().unwrap().take();
        r
    }

    pub(crate) fn note_applied(&self, rec: &Rec, out: &Applied) {
        let now = now_ms();
        let (source, cursor, n) = match rec {
            Rec::Apply { source, cursor, ops } => (source, cursor, ops.len()),
            Rec::Status { source, cursor, .. } => (source, cursor, 1),
        };
        {
            let mut m = self.sources.lock().unwrap();
            let s = m.entry(source.clone()).or_default();
            if !cursor.is_empty() {
                s.cursor = cursor.clone();
            }
            s.ops += n as u64;
            s.last_ms = now;
        }
        let (Rec::Apply { ops, .. }, Applied::Ops(outs)) = (rec, out) else { return };
        let g = self.graph();
        let mut ring = self.recent.lock().unwrap();
        for (op, o) in ops.iter().zip(outs) {
            let (uri, add, subject_in) = match op {
                Op::Add { uri, subject, .. } => (uri, true, subject.as_str()),
                Op::Delete { uri, .. } => (uri, false, ""),
            };
            let subject = match o {
                Outcome::Removed(s) => s.clone(),
                _ => subject_in.to_string(),
            };
            let (actor, rel) = match parse_uri(uri) {
                Some((d, c, _)) => (
                    d.to_string(),
                    g.relations().by_collection(c).map(|r| g.relations().get(r).name.clone()).unwrap_or_default(),
                ),
                None => (String::new(), String::new()),
            };
            let seq = self.write_seq.fetch_add(1, Ordering::Relaxed) + 1;
            if ring.len() == RECENT {
                ring.pop_front();
            }
            ring.push_back(Recent { seq, at_ms: now, rel, add, actor, subject, outcome: o.word(), uri: uri.clone() });
        }
    }

    /// Samples each source's op count (every 10 s, from series.rs).
    pub fn sample_rates(&self) {
        let now = now_ms();
        let ops: Vec<(String, u64)> = self.sources.lock().unwrap().iter().map(|(k, s)| (k.clone(), s.ops)).collect();
        let mut r = self.rates.lock().unwrap();
        for (k, n) in ops {
            let e = r.entry(k).or_insert((now, n, 0.0));
            if now > e.0 {
                *e = (now, n, (n - e.1) as f64 * 1000.0 / (now - e.0) as f64);
            }
        }
    }

    pub fn source_rates(&self) -> BTreeMap<String, f64> {
        self.rates.lock().unwrap().iter().map(|(k, v)| (k.clone(), v.2)).collect()
    }

    pub(crate) fn forget_source(&self, source: &str) {
        self.sources.lock().unwrap().remove(source);
    }

    pub(crate) fn note_durable(&self, cursors: &BTreeMap<String, String>) {
        let mut m = self.sources.lock().unwrap();
        for (k, v) in cursors {
            m.entry(k.clone()).or_default().durable = v.clone();
        }
    }

    // ------------------------------------------------------------ jobs

    pub fn job(&self) -> JobState {
        self.job.lock().unwrap().clone()
    }

    pub(crate) fn begin_job(&self, kind: &'static str, total: u32) -> Result<(), String> {
        let mut j = self.job.lock().unwrap();
        if j.running {
            return Err(format!("a {} is running", j.kind));
        }
        *j = JobState { kind, running: true, started_ms: now_ms(), total, ..Default::default() };
        Ok(())
    }

    pub(crate) fn end_job(&self, r: Result<()>) {
        let mut j = self.job.lock().unwrap();
        j.running = false;
        j.finished_ms = now_ms();
        if let Err(e) = r {
            j.error = format!("{e:#}");
        } else {
            j.done = j.total;
        }
    }

    /// Starts a checkpoint now: the segments folded into new shard files.
    pub fn start_checkpoint(self: &Arc<Self>) -> Result<JobState, String> {
        if self.cfg.read_only {
            return Err("this marco is --read-only".into());
        }
        let n = self.graph().n_shards() as u32;
        self.begin_job("checkpoint", n)?;
        if let Err(e) = self.send(Req::Checkpoint) {
            self.end_job(Err(e));
        }
        Ok(self.job())
    }

    /// Writes a checkpoint and waits for it (tests, benches).
    pub async fn checkpoint(self: &Arc<Self>) -> Result<()> {
        self.start_checkpoint().map_err(anyhow::Error::msg)?;
        loop {
            let j = self.job();
            if !j.running {
                if j.error.is_empty() {
                    return Ok(());
                }
                bail!("{}", j.error);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// When open loaded the whole snapshot into the cache:
    /// the free room in the budget, and what the next checkpoint will take
    /// of it (the last one's P_new + R). Compaction clears cached garbage
    /// to keep the first above the second.
    pub fn headroom(&self) -> Option<marco_core::Headroom> {
        if !self.resident.load(Ordering::Relaxed) {
            return None;
        }
        let room = self.engine.room() as u64;
        let b = self.engine.bytes();
        // every frame of a committed group that isn't on the free list: cached,
        // retired, or a stage's or a past read's
        let used = (b.committed / marco_core::store::page::PAGE as u64).saturating_sub(b.free_frames);
        let d = self.durability.lock().unwrap();
        Some(marco_core::Headroom {
            free: room.saturating_sub(used) * marco_core::store::page::PAGE as u64,
            need: d.checkpoint_patch_bytes + d.checkpoint_pinned_bytes,
        })
    }

    /// Compactions committed since `ms`, within the last day.
    pub fn compactions_since(&self, ms: i64) -> u32 {
        self.compacted_at.lock().unwrap().iter().filter(|&&t| t >= ms).count() as u32
    }

    pub(crate) fn note_compacted(&self) {
        let now = now_ms();
        let mut q = self.compacted_at.lock().unwrap();
        while q.front().is_some_and(|&t| t < now - 86_400_000) {
            q.pop_front();
        }
        q.push_back(now);
    }

    pub(crate) fn snapshot_files(&self) -> usize {
        self.graph().snapshot().map_or(0, |s| s.files().count())
    }

    /// Checks each file's checksums, reading it from disk again, over the
    /// served snapshot held apart from the graph, so no copy of the graph is
    /// held for the minute a whole-network check takes. On the engine, a
    /// page that fails is marked bad and one that reads clean loses its mark.
    pub fn start_verify(self: &Arc<Self>) -> Result<JobState, String> {
        let Some(snap) = self.graph().shared_snapshot() else { return Err("no snapshot".into()) };
        let n = snap.files().count();
        self.begin_job("verify", n as u32)?;
        *self.verify.lock().unwrap() = vec![Verify::Verifying; n];
        let app = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut failed = 0;
            for (i, (_, f)) in snap.files().enumerate() {
                let v = match f.verify_checksums(&snap.engine) {
                    Ok(()) => Verify::Ok,
                    Err(e) => {
                        failed += 1;
                        Verify::Mismatch(format!("{e:#}"))
                    }
                };
                if let Some(x) = app.verify.lock().unwrap().get_mut(i) {
                    *x = v;
                }
                app.job.lock().unwrap().done = i as u32 + 1;
            }
            app.end_job(if failed == 0 {
                Ok(())
            } else {
                Err(anyhow::anyhow!("{failed} files failed their checksums"))
            });
        });
        Ok(self.job())
    }
}
