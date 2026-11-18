//! `marco-load scan-scylla`: a parallel full scan of the tables in a Scylla
//! config into `marco-load build`'s input, then (with --snapshot) the build.
//!
//! Each range is read by token(pk) bounds, routed to the replica shard that
//! owns it, with BYPASS CACHE, paged. Rows are bucketed by repo DID prefix
//! and committed in chunks at partition boundaries (see spill.rs), so a
//! crashed scan resumes from its last checkpoint. The scan isn't a point in
//! time: it records when it began, and the input's cursors.json tells the
//! server to replay Jetstream from a little before that.

use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use marco_core::relation::{json_path, Skip};
use marco_core::Relation;
use prometheus::{register_int_counter_vec, register_int_gauge_vec, IntCounterVec, IntGaugeVec};
use scylla::client::session::Session;
use scylla::deserialize::value::DeserializeValue;
use scylla::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
use scylla::frame::response::result::{ColumnType, NativeType};
use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::response::{PagingState, PagingStateResponse};
use scylla::statement::prepared::PreparedStatement;
use serde_json::{json, Value};

use super::config::{ident, Config};
use super::plan::{self, width, Plan, Range};
use super::spill::{bucket_of, Pos, Spill, State, BUCKETS, PLAN, STATE};
use crate::build::{own_list, CURSORS, SNAPSHOT_TIME, START_MARGIN};
use crate::rows::put_row;
use crate::sys;

#[derive(clap::Args)]
pub struct Args {
    /// The Scylla config (TOML): nodes, keyspace, tables and their columns.
    #[arg(long)]
    pub config: PathBuf,
    /// The scan's output, which is the build's input; resumes the scan already there.
    #[arg(long)]
    pub out: PathBuf,
    /// Relations config (TOML) the tables' relations come from; default the built-in one.
    /// The snapshot holds only the relations scanned.
    #[arg(long)]
    pub relations: Option<PathBuf>,
    /// Build a snapshot here once the scan is whole, then delete the scan's output.
    #[arg(long)]
    pub snapshot: Option<PathBuf>,
    /// With --snapshot, keep the scan's output.
    #[arg(long)]
    pub keep_spill: bool,
    /// Discard a scan already in --out and start over.
    #[arg(long)]
    pub fresh: bool,
    /// Override the config's contact points (comma-separated host:port).
    #[arg(long, value_delimiter = ',')]
    pub nodes: Vec<String>,
    #[arg(long)]
    pub ranges_per_shard: Option<usize>,
    #[arg(long)]
    pub in_flight_per_shard: Option<usize>,
    #[arg(long)]
    pub page_size: Option<i32>,
    /// The snapshot's time is the scan's start less this: for clock skew, and for
    /// the table's own lag behind the network (an indexer behind by more loses edits).
    #[arg(long, default_value_t = 300.0)]
    pub cursor_margin_secs: f64,
    /// How far back the source the server will follow replays (measure it with
    /// marco-harness retention; Jetstream kept 36.6 h): warn while the scan heads past
    /// it, and the build refuses a time that leaves under 2 h to start the server.
    #[arg(long, default_value_t = 36.0)]
    pub retention_hours: f64,
    /// Build even if the cursor is past the retention (the snapshot will miss what changed).
    #[arg(long)]
    pub allow_gap: bool,
    /// Rows a range buffers before it commits them (at the next partition boundary),
    /// at most: --memory-gb lowers it when many ranges are in flight.
    #[arg(long, default_value_t = 262_144)]
    pub chunk_rows: usize,
    /// What the ranges in flight may buffer in all, GB: sets the chunk size
    /// (a big cluster has many shards, each with ranges in flight).
    #[arg(long, default_value_t = 4.0)]
    pub memory_gb: f64,
    /// Seconds between checkpoints: what a crash loses and rescans.
    #[arg(long, default_value_t = 10.0)]
    pub checkpoint_secs: f64,
    #[arg(long, default_value_t = 1)]
    pub zstd_level: i32,
    /// Seconds between progress lines.
    #[arg(long, default_value_t = 5.0)]
    pub progress_secs: f64,
    /// For the crash test: abort the process after this many commits.
    #[arg(long, hide = true)]
    pub crash_after_commits: Option<u64>,
}

static ROWS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!("marco_load_scan_rows_total", "Rows read from Scylla", &["relation"]).unwrap()
});
static BYTES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_load_scan_bytes_total",
        "Row bytes read (DIDs and rkeys, before compression)",
        &["relation"]
    )
    .unwrap()
});
static PAGES: LazyLock<IntCounterVec> =
    LazyLock::new(|| register_int_counter_vec!("marco_load_scan_pages_total", "Pages read", &["relation"]).unwrap());
static RETRIES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!("marco_load_scan_retries_total", "Page reads retried, by error kind", &["kind"]).unwrap()
});
static RANGES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!("marco_load_scan_ranges", "Scan ranges: total, done, in_flight", &["state"]).unwrap()
});
static SPILL: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!("marco_load_scan_spill_bytes", "Committed spill bytes", &["relation"]).unwrap()
});

/// A buffered row's bytes, about: two DIDs, an rkey, lengths, time and op.
const ROW_BYTES: usize = 100;
/// What a list item's row adds: the list's AT URI.
const LIST_BYTES: usize = 80;

fn now_us() -> i64 {
    marco_core::store::now_ms() * 1000
}

struct Ctx {
    cfg: Config,
    plan: Plan,
    rels: Vec<String>,
    /// Each table's relation.
    defs: Vec<Relation>,
    session: Session,
    stmts: Vec<PreparedStatement>,
    spill: Spill,
    /// The committed state: spill lengths and range positions move together.
    commit: Mutex<State>,
    chunk_rows: usize,
    zstd_level: i32,
    crash_after: Option<u64>,
    crash_armed: AtomicBool,
    /// An append failed part-way: the spill is ahead of the state.
    poisoned: AtomicBool,
    /// A bucket buffer bigger than this is freed after its commit.
    bucket_cap: usize,
    dir: PathBuf,
    /// One checkpoint at a time: they share the state file's temp name.
    ck: Mutex<()>,
    rows: AtomicU64,
    bytes: AtomicU64,
    pages: AtomicU64,
    retries: AtomicU64,
    in_flight: AtomicU64,
    stop: AtomicBool,
}

/// Rows skipped, by why.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Skipped {
    /// A column the row needs is null.
    nulls: u64,
    /// A list in another repo.
    cross_repo: u64,
    /// A record the relation doesn't keep (`when`).
    unmatched: u64,
    /// A record that doesn't parse as a JSON object, or isn't an edge.
    bad: u64,
}

impl Skipped {
    fn add(&mut self, o: Skipped) {
        self.nulls += o.nulls;
        self.cross_repo += o.cross_repo;
        self.unmatched += o.unmatched;
        self.bad += o.bad;
    }
}

/// A record column's bytes, from a blob or a text column.
pub struct RecordCol<'a>(&'a [u8]);

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for RecordCol<'frame> {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        <&[u8]>::type_check(typ).or_else(|_| <&str>::type_check(typ))
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        match typ {
            ColumnType::Native(NativeType::Blob) => <&[u8]>::deserialize(typ, v).map(RecordCol),
            _ => <&str>::deserialize(typ, v).map(|s| RecordCol(s.as_bytes())),
        }
    }
}

/// One range's rows since its last commit: whole partitions by bucket, and
/// the partition being read.
struct Buf {
    buckets: Vec<Vec<u8>>,
    rows: u64,
    bytes: u64,
    skipped: Skipped,
    cur: Vec<u8>,
    cur_rows: Vec<(u16, u64)>,
    /// The current partition's rows skipped.
    cur_skipped: Skipped,
    cur_token: Option<i64>,
}

impl Buf {
    fn new() -> Buf {
        Buf {
            buckets: vec![Vec::new(); BUCKETS],
            rows: 0,
            bytes: 0,
            skipped: Skipped::default(),
            cur: Vec::new(),
            cur_rows: Vec::new(),
            cur_skipped: Skipped::default(),
            cur_token: None,
        }
    }

    /// One row of the partition being read. `list` is the list column, on a
    /// list table. Returns the bytes kept.
    fn push(&mut self, a: Option<&str>, k: Option<&str>, s: Option<&str>, list: Option<Option<&str>>) -> u64 {
        let (Some(actor), Some(rkey), Some(subject)) = (a, k, s) else {
            self.cur_skipped.nulls += 1;
            return 0;
        };
        let list = match list {
            None => None,
            Some(None) => {
                self.cur_skipped.nulls += 1;
                return 0;
            }
            // the build would refuse it too: a list item counts only on its owner's list
            Some(Some(l)) if own_list(actor.as_bytes(), l.as_bytes()).is_none() => {
                self.cur_skipped.cross_repo += 1;
                return 0;
            }
            Some(Some(l)) => Some(l),
        };
        self.put(actor, rkey, subject, list)
    }

    /// One row of a record table: the record (JSON) is an edge by its
    /// relation's rule, read as the live path reads it, with the record's
    /// URI from the actor and rkey columns. Returns the bytes kept.
    fn push_record(&mut self, rel: &Relation, a: Option<&str>, k: Option<&str>, rec: Option<&[u8]>) -> u64 {
        let (Some(actor), Some(rkey), Some(rec)) = (a, k, rec) else {
            self.cur_skipped.nulls += 1;
            return 0;
        };
        let Some(v) = serde_json::from_slice::<Value>(rec).ok().filter(Value::is_object) else {
            self.cur_skipped.bad += 1;
            return 0;
        };
        let uri = format!("at://{actor}/{}/{rkey}", rel.collection);
        match rel.edge(&uri, |p| json_path(&v, p)) {
            Ok(e) => self.put(actor, rkey, e.subject, e.list),
            Err(why) => {
                match why {
                    Skip::Unmatched => self.cur_skipped.unmatched += 1,
                    Skip::CrossRepo => self.cur_skipped.cross_repo += 1,
                    _ => self.cur_skipped.bad += 1,
                }
                0
            }
        }
    }

    fn put(&mut self, actor: &str, rkey: &str, subject: &str, list: Option<&str>) -> u64 {
        put_row(&mut self.cur, actor.as_bytes(), rkey.as_bytes(), subject.as_bytes(), list.map(str::as_bytes), 0, 1);
        self.cur_rows.push((bucket_of(actor) as u16, self.cur.len() as u64));
        let n = (actor.len() + rkey.len() + subject.len() + list.map_or(0, str::len)) as u64;
        self.bytes += n;
        n
    }

    /// The current partition is whole: into the buckets.
    /// `cap`: a partition buffer grown past this goes back after.
    fn seal(&mut self, cap: usize) {
        let mut at = 0usize;
        for &(b, end) in &self.cur_rows {
            self.buckets[b as usize].extend_from_slice(&self.cur[at..end as usize]);
            at = end as usize;
        }
        self.rows += self.cur_rows.len() as u64;
        self.skipped.add(self.cur_skipped);
        self.cur_skipped = Skipped::default();
        self.cur.clear();
        self.cur_rows.clear();
        // a big partition's buffer isn't kept for the next: the budget counts chunks
        if self.cur.capacity() > cap {
            self.cur = Vec::new();
            self.cur_rows = Vec::new();
        }
    }
}

impl Ctx {
    /// Compresses `buf`'s buckets, appends them and moves range `ri` to `pos`, as one step.
    fn commit(&self, ri: usize, buf: &mut Buf, pos: Pos) -> Result<()> {
        let t = self.plan.ranges[ri].table;
        let frames: Vec<(usize, Vec<u8>)> = tokio::task::block_in_place(|| -> Result<_> {
            let mut z = zstd::bulk::Compressor::new(self.zstd_level)?;
            let mut v = Vec::new();
            for (b, raw) in buf.buckets.iter().enumerate() {
                if !raw.is_empty() {
                    v.push((b, z.compress(raw)?));
                }
            }
            Ok(v)
        })?;
        let commits = {
            let mut c = self.commit.lock().unwrap();
            if self.poisoned.load(Relaxed) {
                bail!("an earlier append failed; the scan resumes from its last checkpoint");
            }
            for (b, f) in &frames {
                if let Err(e) = self.spill.append(t, *b, f) {
                    // the files now hold bytes no state counts: no more commits or checkpoints
                    self.poisoned.store(true, Relaxed);
                    return Err(e);
                }
            }
            for (b, f) in &frames {
                c.lens[t][*b] += f.len() as u64;
            }
            c.pos[ri] = pos;
            c.rows[t] += buf.rows;
            c.row_bytes[t] += buf.bytes;
            c.null_rows[t] += buf.skipped.nulls;
            c.cross_repo_rows[t] += buf.skipped.cross_repo;
            c.unmatched_rows[t] += buf.skipped.unmatched;
            c.bad_rows[t] += buf.skipped.bad;
            c.commits += 1;
            SPILL.with_label_values(&[&self.rels[t]]).set(c.lens[t].iter().sum::<u64>() as i64);
            c.commits
        };
        if let Some(n) = self.crash_after {
            // a checkpoint, then commits it doesn't cover, then the crash
            if commits == n {
                self.checkpoint(&self.dir, 0.0, 0.0)?;
                self.crash_armed.store(true, Relaxed);
            } else if commits > n + 4 && self.crash_armed.load(Relaxed) {
                eprintln!("scan-scylla: aborting after {commits} commits (--crash-after-commits)");
                std::process::abort();
            }
        }
        for b in &mut buf.buckets {
            b.clear();
            if b.capacity() > self.bucket_cap {
                *b = Vec::new();
            }
        }
        buf.rows = 0;
        buf.bytes = 0;
        buf.skipped = Skipped::default();
        Ok(())
    }

    /// The state as of now, made durable: the files' bytes first, then the state that counts them.
    fn checkpoint(&self, dir: &Path, run_secs: f64, prior_secs: f64) -> Result<()> {
        let _one = self.ck.lock().unwrap();
        if self.poisoned.load(Relaxed) {
            bail!("not checkpointing after a failed append");
        }
        let mut st = self.commit.lock().unwrap().clone();
        self.spill.sync()?;
        st.scan_secs = prior_secs + run_secs;
        st.write(dir)
    }

    fn statement(&self, r: &Range, routed: bool) -> PreparedStatement {
        let mut s = self.stmts[r.table].clone();
        if routed {
            if let Some(h) = r.host.as_deref().and_then(|h| h.parse::<uuid::Uuid>().ok()) {
                s.set_load_balancing_policy(Some(SingleTargetLoadBalancingPolicy::new(
                    NodeIdentifier::HostId(h),
                    r.shard,
                )));
            }
        }
        s
    }

    async fn page(
        &self,
        r: &Range,
        stmt: &mut PreparedStatement,
        bounds: (i64, i64),
        paging: &PagingState,
    ) -> Result<(scylla::response::query_result::QueryRowsResult, PagingStateResponse)> {
        let mut backoff = Duration::from_millis(200);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let res = self.session.execute_single_page(stmt, bounds, paging.clone()).await;
            let err = match res {
                Ok((q, next)) => match q.into_rows_result() {
                    Ok(rows) => {
                        if attempt > 1 {
                            *stmt = self.statement(r, true);
                        }
                        return Ok((rows, next));
                    }
                    Err(e) => anyhow::anyhow!("not rows: {e}"),
                },
                Err(e) => anyhow::Error::new(e),
            };
            if attempt >= self.cfg.retries.max(1) || self.stop.load(Relaxed) {
                return Err(err.context(format!(
                    "range of {:?} at tokens ({}, {}]: {attempt} attempts",
                    self.plan.tables[r.table].name, bounds.0, bounds.1
                )));
            }
            let kind = format!("{err:#}");
            let kind = kind.split(':').next().unwrap_or("error").trim().to_string();
            RETRIES.with_label_values(&[&kind]).inc();
            self.retries.fetch_add(1, Relaxed);
            eprintln!("scan-scylla: retrying a page in {:?} ({attempt}): {err:#}", backoff);
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
            // the owner may be the trouble: let the driver pick another replica
            // (in the datacenter, with one set) for this page
            *stmt = self.statement(r, false);
        }
    }

    /// A row of partition `tok`: if it starts a new one, the last is whole,
    /// and a full chunk commits.
    fn boundary(&self, ri: usize, pi: usize, buf: &mut Buf, tok: i64) -> Result<()> {
        if buf.cur_token != Some(tok) {
            if let Some(prev) = buf.cur_token {
                buf.seal(self.bucket_cap.max(1 << 20));
                if buf.rows >= self.chunk_rows as u64 {
                    self.commit(ri, buf, Pos { piece: pi as u32, after: Some(prev), done: false })?;
                }
            }
            buf.cur_token = Some(tok);
        }
        Ok(())
    }

    async fn range(&self, ri: usize) -> Result<()> {
        let r = &self.plan.ranges[ri];
        let start = self.commit.lock().unwrap().pos[ri];
        if start.done {
            return Ok(());
        }
        let rel = &self.rels[r.table];
        let (rows_c, bytes_c, pages_c) =
            (ROWS.with_label_values(&[rel]), BYTES.with_label_values(&[rel]), PAGES.with_label_values(&[rel]));
        let tb = &self.plan.tables[r.table];
        let (listed, record) = (tb.list.is_some(), tb.record.is_some());
        let def = &self.defs[r.table];
        let mut stmt = self.statement(r, true);
        let mut buf = Buf::new();
        for pi in start.piece as usize..r.pieces.len() {
            let (mut lo, hi) = r.pieces[pi];
            if pi == start.piece as usize {
                if let Some(a) = start.after {
                    lo = a;
                }
            }
            let mut paging = PagingState::start();
            while lo < hi {
                if self.stop.load(Relaxed) {
                    bail!("stopped");
                }
                let (rows, next) = self.page(r, &mut stmt, (lo, hi), &paging).await?;
                pages_c.inc();
                self.pages.fetch_add(1, Relaxed);
                let (mut n, mut nb) = (0u64, 0u64);
                if record {
                    for row in rows.rows::<(i64, Option<&str>, Option<&str>, Option<RecordCol>)>()? {
                        let (tok, a, k, rec) = row?;
                        n += 1;
                        self.boundary(ri, pi, &mut buf, tok)?;
                        nb += buf.push_record(def, a, k, rec.map(|r| r.0));
                    }
                } else if listed {
                    for row in rows.rows::<(i64, Option<&str>, Option<&str>, Option<&str>, Option<&str>)>()? {
                        let (tok, a, k, s, l) = row?;
                        n += 1;
                        self.boundary(ri, pi, &mut buf, tok)?;
                        nb += buf.push(a, k, s, Some(l));
                    }
                } else {
                    for row in rows.rows::<(i64, Option<&str>, Option<&str>, Option<&str>)>()? {
                        let (tok, a, k, s) = row?;
                        n += 1;
                        self.boundary(ri, pi, &mut buf, tok)?;
                        nb += buf.push(a, k, s, None);
                    }
                }
                rows_c.inc_by(n);
                bytes_c.inc_by(nb);
                self.rows.fetch_add(n, Relaxed);
                self.bytes.fetch_add(nb, Relaxed);
                match next.into_paging_control_flow() {
                    std::ops::ControlFlow::Break(()) => lo = hi,
                    std::ops::ControlFlow::Continue(p) => paging = p,
                }
            }
            buf.seal(self.bucket_cap.max(1 << 20));
            buf.cur_token = None;
            let last = pi + 1 == r.pieces.len();
            if last || buf.rows >= self.chunk_rows as u64 {
                self.commit(ri, &mut buf, Pos { piece: pi as u32 + 1, after: None, done: last })?;
            }
        }
        if r.pieces.is_empty() {
            self.commit(ri, &mut buf, Pos { piece: 0, after: None, done: true })?;
        }
        Ok(())
    }
}

/// The share of the ring's tokens the ranges have committed, 0 to 1.
fn done_share(plan: &Plan, pos: &[Pos]) -> f64 {
    let mut done = 0u128;
    let mut total = 0u128;
    for (r, p) in plan.ranges.iter().zip(pos) {
        for (i, &pc) in r.pieces.iter().enumerate() {
            let w = width(pc);
            total += w;
            if p.done || i < p.piece as usize {
                done += w;
            } else if i == p.piece as usize {
                if let Some(a) = p.after {
                    done += width((pc.0, a.clamp(pc.0, pc.1)));
                }
            }
        }
    }
    if total == 0 {
        1.0
    } else {
        done as f64 / total as f64
    }
}

fn select(plan: &Plan, t: usize, bypass: bool) -> String {
    let tb = &plan.tables[t];
    let pk: Vec<String> = plan.partition_keys[t].iter().map(|c| ident(c)).collect();
    let tok = format!("token({})", pk.join(", "));
    // a record table's subject and list are in its record
    let rest = match &tb.record {
        Some(rec) => ident(rec),
        None => ident(&tb.subject) + &tb.list.as_deref().map_or(String::new(), |l| format!(", {}", ident(l))),
    };
    format!(
        "SELECT {tok}, {}, {}, {rest} FROM {}.{} WHERE {tok} > ? AND {tok} <= ?{}",
        ident(&tb.actor),
        ident(&tb.rkey),
        ident(&plan.keyspace),
        ident(&tb.name),
        if bypass { " BYPASS CACHE" } else { "" }
    )
}

fn hms(s: f64) -> String {
    let s = s.max(0.0) as u64;
    format!("{}h{:02}m{:02}s", s / 3600, s / 60 % 60, s % 60)
}

/// Removes what a scan wrote in `dir`: its state, plan and cursors, and the relation dirs' bucket files.
fn clear(dir: &Path, rels: &[String]) -> Result<()> {
    for f in [STATE, PLAN, CURSORS, RELATIONS] {
        let _ = std::fs::remove_file(dir.join(f));
    }
    crate::spill::remove_buckets(dir, rels)
}

/// The relations scanned, for `marco-load build --relations` on the output.
pub const RELATIONS: &str = "relations.toml";

pub fn run(a: &Args) -> Result<Value> {
    let mut cfg = Config::read(&a.config)?;
    if !a.nodes.is_empty() {
        cfg.nodes = a.nodes.clone();
    }
    if let Some(v) = a.ranges_per_shard {
        cfg.ranges_per_shard = v.max(1);
    }
    if let Some(v) = a.in_flight_per_shard {
        cfg.in_flight_per_shard = v.max(1);
    }
    if let Some(v) = a.page_size {
        cfg.page_size = v.max(1);
    }
    let relations = match &a.relations {
        Some(p) => marco_core::Relations::parse_toml(&std::fs::read_to_string(p)?)?,
        None => marco_core::Relations::bsky_default(),
    };
    cfg.resolve(&relations)?;
    let rels: Vec<String> = cfg.tables.iter().map(|t| t.relation.clone()).collect();
    // the snapshot holds the relations scanned (a list relation's companion
    // too), in the config's order: one left out would be empty and then
    // half-filled by the catch-up
    let defs: Vec<Relation> =
        rels.iter().filter_map(|r| relations.by_name(r)).map(|id| relations.get(id).clone()).collect();
    let relations = marco_core::Relations::new(defs.clone())?;
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let mut report = rt.block_on(scan(a, cfg, rels.clone(), defs))?;
    drop(rt);
    marco_core::store::write_atomic(&a.out, RELATIONS, toml::to_string(&relations)?.as_bytes(), true)?;
    if let Some(snap) = &a.snapshot {
        let b = crate::build::build_scanned(&a.out, snap, relations, a.retention_hours, a.allow_gap)?;
        report["build"] = serde_json::to_value(b)?;
        if !a.keep_spill {
            clear(&a.out, &rels)?;
            let _ = std::fs::remove_dir(&a.out);
        }
    }
    Ok(report)
}

async fn scan(a: &Args, cfg: Config, rels: Vec<String>, defs: Vec<Relation>) -> Result<Value> {
    let dir = &a.out;
    // a file per bucket per table stays open, past the usual soft limit of 1024
    let nofile = sys::raise_nofile();
    std::fs::create_dir_all(dir)?;
    if a.fresh {
        clear(dir, &rels)?;
    }
    let t_connect = Instant::now();
    let session = cfg.connect().await?;
    let connect_secs = t_connect.elapsed().as_secs_f64();
    let (plan, mut state) = match (State::read(dir)?, std::fs::read(dir.join(PLAN))) {
        (Some(st), Ok(p)) => {
            let plan: Plan = serde_json::from_slice(&p).context("parse the scan plan")?;
            if plan.keyspace != cfg.keyspace || plan.tables != cfg.tables {
                bail!("{} holds a scan of other tables; --fresh to start over", dir.display());
            }
            // a record table's rows were kept or dropped by its relation's rule
            if plan.record_rules != plan::record_rules(&plan.tables, &defs) {
                bail!(
                    "{} holds a scan whose record tables were read by other relation rules (--relations); --fresh to start over",
                    dir.display()
                );
            }
            if plan.datacenter != cfg.datacenter {
                bail!(
                    "{} holds a scan of datacenter {:?}, the config says {:?}; --fresh to start over",
                    dir.display(),
                    plan.datacenter,
                    cfg.datacenter
                );
            }
            if st.pos.len() != plan.ranges.len() {
                bail!("{}: the scan state doesn't match its plan; --fresh to start over", dir.display());
            }
            let mut st = st;
            for v in [&mut st.cross_repo_rows, &mut st.unmatched_rows, &mut st.bad_rows] {
                v.resize(plan.tables.len(), 0);
            }
            eprintln!(
                "resuming the scan in {} (begun {:.1} h ago, {:.1}% of tokens done)",
                dir.display(),
                (now_us() - st.started_us) as f64 / 3.6e9,
                100.0 * done_share(&plan, &st.pos)
            );
            (plan, st)
        }
        _ => {
            if std::fs::read_dir(dir)?.next().is_some() {
                bail!("{} isn't empty and holds no scan; use an empty directory or --fresh", dir.display());
            }
            // the start comes before the plan: every row read is newer than it
            let started = now_us();
            let mut plan = plan::make(&session, &cfg).await?;
            plan.record_rules = plan::record_rules(&plan.tables, &defs);
            marco_core::store::write_atomic(dir, PLAN, &serde_json::to_vec(&plan)?, true)?;
            let st = State::new(plan.ranges.len(), plan.tables.len(), started);
            st.write(dir)?;
            (plan, st)
        }
    };
    for (t, tb) in plan.tables.iter().enumerate() {
        eprintln!(
            "{}.{} -> {}: {}, key {}, {} ranges",
            plan.keyspace,
            tb.name,
            tb.relation,
            plan.modes[t],
            plan.keys[t],
            plan.ranges.iter().filter(|r| r.table == t).count()
        );
        if !plan.partition_keys[t].contains(&tb.rkey) && !plan.clustering_keys[t].contains(&tb.rkey) {
            eprintln!(
                "warning: {}.{}'s primary key {} leaves out {}: an actor with two records naming one subject has one row, so marco keeps one of them",
                plan.keyspace, tb.name, plan.keys[t], tb.rkey
            );
        }
    }
    eprintln!("{} ranges over {} replica shards on {} nodes", plan.ranges.len(), plan.shards, plan.nodes);
    // the bucket files, and the driver's connection per shard per node
    let sockets: u64 = session
        .get_cluster_state()
        .get_nodes_info()
        .iter()
        .map(|n| n.sharder().map_or(1, |s| s.nr_shards.get() as u64))
        .sum();
    let need = (rels.len() * BUCKETS) as u64 + sockets + 256;
    if nofile < need {
        bail!("scan-scylla needs {need} open files ({} bucket files, {sockets} connections) and the limit is {nofile}: raise it (ulimit -n, LimitNOFILE)", rels.len() * BUCKETS);
    }
    let mut stmts = Vec::new();
    for t in 0..plan.tables.len() {
        let q = select(&plan, t, cfg.bypass_cache);
        let mut s = session.prepare(q.as_str()).await.with_context(|| format!("prepare {q}"))?;
        s.set_consistency(cfg.consistency()?);
        s.set_page_size(cfg.page_size);
        s.set_request_timeout(Some(cfg.timeout()));
        s.set_is_idempotent(true);
        stmts.push(s);
    }
    state.runs += 1;
    state.write(dir)?;
    let prior_secs = state.scan_secs;
    let spill = Spill::open(dir, &rels, &state.lens)?;
    let total = plan.ranges.len();
    let mut queues: BTreeMap<(Option<String>, Option<u32>), VecDeque<usize>> = BTreeMap::new();
    for (i, r) in plan.ranges.iter().enumerate() {
        if !state.pos[i].done {
            queues.entry((r.host.clone(), r.shard)).or_default().push_back(i);
        }
    }
    let first_share = done_share(&plan, &state.pos);
    let already_done = total - queues.values().map(|q| q.len()).sum::<usize>();
    let finished_before = state.finished_us.is_some();
    let workers = queues.len() * cfg.in_flight_per_shard;
    let uris = defs.iter().any(|r| r.list.is_some() || r.subject_records().is_some());
    let row_bytes = ROW_BYTES + if uris { LIST_BYTES } else { 0 };
    // a worker's buckets, their slack, and the compressed frames: ~3 bytes per row byte
    let budget_rows = ((a.memory_gb * 1e9) as usize / (workers.max(1) * 3 * row_bytes)).max(1024);
    let chunk_rows = a.chunk_rows.clamp(1, budget_rows);
    if chunk_rows < a.chunk_rows {
        eprintln!("{workers} ranges in flight: {chunk_rows} rows a chunk to stay in --memory-gb {}", a.memory_gb);
    }
    let ctx = Arc::new(Ctx {
        chunk_rows,
        zstd_level: a.zstd_level,
        crash_after: a.crash_after_commits,
        crash_armed: AtomicBool::new(false),
        poisoned: AtomicBool::new(false),
        bucket_cap: (chunk_rows * row_bytes / 64).max(64 << 10),
        dir: dir.clone(),
        ck: Mutex::new(()),
        commit: Mutex::new(state),
        plan,
        rels: rels.clone(),
        defs,
        session,
        stmts,
        spill,
        cfg,
        rows: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        pages: AtomicU64::new(0),
        retries: AtomicU64::new(0),
        in_flight: AtomicU64::new(0),
        stop: AtomicBool::new(false),
    });
    RANGES.with_label_values(&["total"]).set(total as i64);
    let t0 = Instant::now();
    let cpu0 = sys::cpu_secs();
    let mut workers = tokio::task::JoinSet::new();
    for q in queues.into_values() {
        let q = Arc::new(Mutex::new(q));
        for _ in 0..ctx.cfg.in_flight_per_shard {
            let (ctx, q) = (ctx.clone(), q.clone());
            workers.spawn(async move {
                loop {
                    let Some(ri) = q.lock().unwrap().pop_front() else { return Ok::<(), anyhow::Error>(()) };
                    ctx.in_flight.fetch_add(1, Relaxed);
                    let r = ctx.range(ri).await;
                    ctx.in_flight.fetch_sub(1, Relaxed);
                    r?;
                }
            });
        }
    }
    // checkpoints and progress
    let ticker = {
        let ctx = ctx.clone();
        let dir = dir.clone();
        let (ck, pr) =
            (Duration::from_secs_f64(a.checkpoint_secs.max(0.05)), Duration::from_secs_f64(a.progress_secs.max(0.5)));
        let retention = Duration::from_secs_f64(a.retention_hours * 3600.0);
        let margin_us = (a.cursor_margin_secs * 1e6) as i64;
        tokio::spawn(async move {
            let (mut last_ck, mut last_pr) = (Instant::now(), Instant::now());
            let (mut last_rows, mut last_bytes) = (0u64, 0u64);
            let mut warned = false;
            loop {
                tokio::time::sleep(Duration::from_millis(250)).await;
                if last_ck.elapsed() >= ck {
                    last_ck = Instant::now();
                    let (c, d, s) = (ctx.clone(), dir.clone(), t0.elapsed().as_secs_f64());
                    match tokio::task::spawn_blocking(move || c.checkpoint(&d, s, prior_secs)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => eprintln!("scan-scylla: checkpoint failed: {e:#}"),
                        Err(e) => eprintln!("scan-scylla: checkpoint panicked: {e}"),
                    }
                }
                if last_pr.elapsed() >= pr {
                    let dt = last_pr.elapsed().as_secs_f64();
                    last_pr = Instant::now();
                    let (rows, bytes) = (ctx.rows.load(Relaxed), ctx.bytes.load(Relaxed));
                    let (done, share, spill, started) = {
                        let c = ctx.commit.lock().unwrap();
                        (
                            c.pos.iter().filter(|p| p.done).count(),
                            done_share(&ctx.plan, &c.pos),
                            c.spill_bytes(),
                            c.started_us,
                        )
                    };
                    RANGES.with_label_values(&["done"]).set(done as i64);
                    RANGES.with_label_values(&["in_flight"]).set(ctx.in_flight.load(Relaxed) as i64);
                    let el = t0.elapsed().as_secs_f64();
                    let eta = if share > first_share { el * (1.0 - share) / (share - first_share) } else { f64::NAN };
                    eprintln!(
                        "scan: {:.1}M rows ({:.0}k/s, {:.0} MB/s), ranges {done}/{total} ({} reading), tokens {:.1}%, spill {:.2} GB, retries {}, {} in, eta {}",
                        rows as f64 / 1e6,
                        (rows - last_rows) as f64 / dt / 1e3,
                        (bytes - last_bytes) as f64 / dt / 1e6,
                        ctx.in_flight.load(Relaxed),
                        share * 100.0,
                        spill as f64 / 1e9,
                        ctx.retries.load(Relaxed),
                        hms(el),
                        if eta.is_finite() { hms(eta) } else { "?".into() },
                    );
                    (last_rows, last_bytes) = (rows, bytes);
                    // the cursor will be the start less the margin: will the source still have it?
                    let end_us = now_us() + if eta.is_finite() { (eta * 1e6) as i64 } else { 0 };
                    let age = Duration::from_micros((end_us - (started - margin_us)).max(0) as u64);
                    if age + START_MARGIN > retention && !warned {
                        warned = true;
                        eprintln!(
                            "warning: at this rate the scan ends {:.1} h after its cursor, past the source's {:.0} h retention; \
                             the build will refuse it without --allow-gap. More ranges in flight, or fewer tables per scan, would help.",
                            age.as_secs_f64() / 3600.0,
                            retention.as_secs_f64() / 3600.0
                        );
                    }
                }
            }
        })
    };
    let mut failed: Option<anyhow::Error> = None;
    while let Some(r) = workers.join_next().await {
        let r = r.map_err(anyhow::Error::from).and_then(|r| r);
        if let Err(e) = r {
            if failed.is_none() {
                ctx.stop.store(true, Relaxed);
                failed = Some(e);
            }
        }
    }
    ticker.abort();
    let run_secs = t0.elapsed().as_secs_f64();
    let cpu = sys::cpu_secs() - cpu0;
    let ck = {
        let c = ctx.clone();
        let d = dir.clone();
        tokio::task::spawn_blocking(move || c.checkpoint(&d, run_secs, prior_secs)).await?
    };
    if let Some(e) = failed {
        // the stop's own cause first: a poisoned spill refuses this checkpoint anyway
        if let Err(c) = ck {
            eprintln!("scan-scylla: no final checkpoint: {c:#}");
        }
        return Err(e.context(format!(
            "the scan stopped; run the same command again to resume from {}",
            dir.join(STATE).display()
        )));
    }
    ck?;
    let mut st = ctx.commit.lock().unwrap().clone();
    st.scan_secs = prior_secs + run_secs;
    if st.finished_us.is_none() {
        st.finished_us = Some(now_us());
    }
    {
        let _one = ctx.ck.lock().unwrap();
        ctx.spill.sync()?;
        st.write(dir)?;
    }
    ctx.spill.remove_empty()?;
    let cursor = st.started_us - (a.cursor_margin_secs * 1e6) as i64;
    let cursors: BTreeMap<String, String> = [(SNAPSHOT_TIME.to_string(), cursor.to_string())].into();
    marco_core::store::write_atomic(dir, CURSORS, &serde_json::to_vec_pretty(&cursors)?, true)?;
    let rows: u64 = ctx.rows.load(Relaxed);
    let bytes: u64 = ctx.bytes.load(Relaxed);
    let tables: Vec<Value> = ctx
        .plan
        .tables
        .iter()
        .enumerate()
        .map(|(t, tb)| {
            json!({
                "relation": tb.relation,
                "table": format!("{}.{}", ctx.plan.keyspace, tb.name),
                "mode": ctx.plan.modes[t],
                "primary_key": ctx.plan.keys[t],
                "ranges": ctx.plan.ranges.iter().filter(|r| r.table == t).count(),
                "pieces": ctx.plan.ranges.iter().filter(|r| r.table == t).map(|r| r.pieces.len()).sum::<usize>(),
                "rows": st.rows[t],
                "null_rows": st.null_rows[t],
                "cross_repo_rows": st.cross_repo_rows[t],
                "unmatched_rows": st.unmatched_rows[t],
                "bad_rows": st.bad_rows[t],
                "row_bytes": st.row_bytes[t],
                "spill_bytes": st.lens[t].iter().sum::<u64>(),
            })
        })
        .collect();
    let age_h = (now_us() - cursor) as f64 / 3.6e9;
    if age_h + START_MARGIN.as_secs_f64() / 3600.0 > a.retention_hours {
        eprintln!(
            "warning: the cursor is {age_h:.1} h old, within 2 h of the {:.0} h retention: the build refuses it without --allow-gap",
            a.retention_hours
        );
    }
    for (t, tb) in ctx.plan.tables.iter().enumerate() {
        if st.cross_repo_rows[t] > 0 {
            eprintln!(
                "{}.{}: {} list items name a list in another repo; dropped, as marco never keeps them",
                ctx.plan.keyspace, tb.name, st.cross_repo_rows[t]
            );
        }
        if tb.record.is_some() {
            eprintln!(
                "{}.{}: {} records {} doesn't keep (when), {} that don't parse or aren't an edge; dropped",
                ctx.plan.keyspace, tb.name, st.unmatched_rows[t], tb.relation, st.bad_rows[t]
            );
        }
    }
    let total_rows: u64 = st.rows.iter().sum();
    eprintln!(
        "scan done: {total_rows} rows in {} ({} this run), spill {:.2} GB, cursor {cursor} ({:.1} min before the start)",
        hms(st.scan_secs),
        hms(run_secs),
        st.spill_bytes() as f64 / 1e9,
        a.cursor_margin_secs / 60.0
    );
    Ok(json!({
        "keyspace": ctx.plan.keyspace,
        "nodes": ctx.plan.nodes,
        "replica_shards": ctx.plan.shards,
        "ranges": total,
        "ranges_done_before": already_done,
        "already_finished": finished_before,
        "ranges_per_shard": ctx.cfg.ranges_per_shard,
        "in_flight_per_shard": ctx.cfg.in_flight_per_shard,
        "page_size": ctx.cfg.page_size,
        "consistency": ctx.cfg.consistency_name(),
        "bypass_cache": ctx.cfg.bypass_cache,
        "compression": ctx.cfg.compression,
        "chunk_rows": ctx.chunk_rows,
        "tables": tables,
        "started_us": st.started_us,
        "finished_us": st.finished_us,
        "cursor_us": cursor,
        "runs": st.runs,
        "connect_secs": connect_secs,
        "scan_secs": st.scan_secs,
        "run_secs": run_secs,
        "run_rows": rows,
        "run_row_bytes": bytes,
        "run_rows_per_s": rows as f64 / run_secs,
        "run_mb_per_s": bytes as f64 / run_secs / 1e6,
        "rows": total_rows,
        "spill_bytes": st.spill_bytes(),
        "pages": ctx.pages.load(Relaxed),
        "retries": ctx.retries.load(Relaxed),
        "client_cpu_secs": cpu,
        "client_cores_busy": cpu / run_secs,
        "peak_rss_gb": sys::peak_rss_gb(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Rows;

    #[test]
    fn list_items_keep_their_list_and_drop_other_repos() {
        let (a, b) = ("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb");
        let own = format!("at://{a}/app.bsky.graph.list/l1");
        let theirs = format!("at://{b}/app.bsky.graph.list/l1");
        let mut buf = Buf::new();
        assert!(buf.push(Some(a), Some("3kaaaaaaaaa22"), Some(b), Some(Some(&own))) > 0);
        assert_eq!(buf.push(Some(a), Some("3kaaaaaaaaa23"), Some(b), Some(Some(&theirs))), 0);
        assert_eq!(buf.push(Some(a), Some("3kaaaaaaaaa24"), Some(b), Some(None)), 0);
        assert_eq!(buf.push(Some(a), Some("3kaaaaaaaaa25"), None, None), 0);
        // counted with their partition, so a resume that rereads it doesn't count them twice
        assert_eq!(buf.skipped, Skipped::default());
        buf.seal(1 << 20);
        assert_eq!((buf.rows, buf.skipped.nulls, buf.skipped.cross_repo), (1, 2, 1));
        let tmp = tempfile::tempdir().unwrap();
        let f = tmp.path().join("p-aa.rb.zst");
        std::fs::write(&f, zstd::bulk::compress(&buf.buckets[bucket_of(a)], 1).unwrap()).unwrap();
        let mut rows = Rows::open(&f).unwrap();
        let mut cols = vec![Vec::new(); 4];
        assert!(rows.next(&mut cols).unwrap());
        assert_eq!((rows.i64().unwrap(), rows.u8().unwrap()), (0, 1));
        assert_eq!(cols[3], own.as_bytes());
        assert!(!rows.next(&mut cols).unwrap());
    }

    #[test]
    fn a_list_table_selects_its_list_column() {
        let t = |list: Option<&str>, subject: &str, record: Option<&str>| super::super::config::Table {
            relation: "listitem".into(),
            name: "list_items".into(),
            actor: "actor_did".into(),
            rkey: "rkey".into(),
            subject: subject.into(),
            list: list.map(String::from),
            record: record.map(String::from),
        };
        let plan = Plan {
            keyspace: "bsky".into(),
            datacenter: None,
            tables: vec![
                t(Some("list"), "subject", None),
                t(None, "subject", None),
                t(None, "list", None),
                t(None, "target_did", Some("raw")),
            ],
            modes: vec![],
            keys: vec![],
            partition_keys: vec![vec!["actor_did".into()]; 4],
            clustering_keys: vec![vec!["rkey".into()]; 4],
            shards: 0,
            nodes: 0,
            ranges: vec![],
            record_rules: vec![],
        };
        assert_eq!(
            select(&plan, 0, true),
            "SELECT token(actor_did), actor_did, rkey, subject, list FROM bsky.list_items \
             WHERE token(actor_did) > ? AND token(actor_did) <= ? BYPASS CACHE"
        );
        assert!(!select(&plan, 1, false).contains("list,") && !select(&plan, 1, false).contains(", list "));
        // a list subscription's table: the list column is its subject
        assert!(select(&plan, 2, false).starts_with("SELECT token(actor_did), actor_did, rkey, list FROM"));
        // a record table reads the record in place of the subject
        assert!(select(&plan, 3, false).starts_with("SELECT token(actor_did), actor_did, rkey, raw FROM"));
    }

    /// The relations of the list subjects: a list's purpose (its own URI,
    /// while it's a mod list) and a list item read from its record.
    fn record_rels() -> (Relation, Relation) {
        let r = marco_core::Relations::parse_toml(
            r#"
[[relation]]
name = "listitem"
collection = "app.bsky.graph.listitem"
subject = "subject"
list = "list"

[[relation]]
name = "modlist"
collection = "app.bsky.graph.list"
subject = "$self"
when = { purpose = "app.bsky.graph.defs#modlist" }
"#,
        )
        .unwrap();
        (r.get(r.by_name("modlist").unwrap()).clone(), r.get(r.by_name("listitem").unwrap()).clone())
    }

    /// A record table's rows go through the relation's rule: a mod list is
    /// an edge to its own URI, a list of another purpose is unmatched, and a
    /// record that doesn't parse, or isn't an edge, is bad. Each is counted
    /// with its partition.
    #[test]
    fn a_record_table_keeps_the_edges_its_relation_keeps() {
        let (modlist, listitem) = record_rels();
        let (a, b) = ("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb");
        let rec = |purpose: &str| {
            format!(
                r#"{{"$type":"app.bsky.graph.list","name":"x","purpose":"{purpose}","createdAt":"2024-01-01T00:00:00Z"}}"#
            )
        };
        let mut buf = Buf::new();
        let k = "3kaaaaaaaaa22";
        assert!(buf.push_record(&modlist, Some(a), Some(k), Some(rec("app.bsky.graph.defs#modlist").as_bytes())) > 0);
        for p in ["app.bsky.graph.defs#curatelist", "app.bsky.graph.defs#referencelist"] {
            assert_eq!(buf.push_record(&modlist, Some(a), Some("3kaaaaaaaaa23"), Some(rec(p).as_bytes())), 0);
        }
        assert_eq!(buf.push_record(&modlist, Some(a), Some("3kaaaaaaaaa24"), Some(br#"{"name":"no purpose"}"#)), 0);
        assert_eq!(buf.push_record(&modlist, Some(a), Some("3kaaaaaaaaa25"), Some(b"{not json")), 0);
        assert_eq!(buf.push_record(&modlist, Some(a), Some("3kaaaaaaaaa26"), Some(b"\"a string\"")), 0);
        // an rkey no URI can hold: not an edge
        let bad_rkey = rec("app.bsky.graph.defs#modlist");
        assert_eq!(buf.push_record(&modlist, Some(a), Some("a/b"), Some(bad_rkey.as_bytes())), 0);
        assert_eq!(buf.push_record(&modlist, Some(a), None, Some(b"{}")), 0);
        assert_eq!(buf.push_record(&modlist, Some(a), Some(k), None), 0);
        // a list item's record: its own list kept, another repo's dropped as cross-repo
        let item = |list: &str| format!(r#"{{"subject":"{b}","list":"{list}"}}"#);
        let own = format!("at://{a}/app.bsky.graph.list/l1");
        assert!(buf.push_record(&listitem, Some(a), Some(k), Some(item(&own).as_bytes())) > 0);
        let theirs = format!("at://{b}/app.bsky.graph.list/l1");
        assert_eq!(buf.push_record(&listitem, Some(a), Some(k), Some(item(&theirs).as_bytes())), 0);
        assert_eq!(buf.skipped, Skipped::default());
        buf.seal(1 << 20);
        assert_eq!(buf.rows, 2);
        assert_eq!(buf.skipped, Skipped { nulls: 2, cross_repo: 1, unmatched: 3, bad: 3 });
        let tmp = tempfile::tempdir().unwrap();
        let f = tmp.path().join("p-aa.rb.zst");
        std::fs::write(&f, zstd::bulk::compress(&buf.buckets[bucket_of(a)], 1).unwrap()).unwrap();
        let mut rows = Rows::open(&f).unwrap();
        // the mod list's row: its own URI as the subject, as the build takes a $self row
        let mut cols = vec![Vec::new(); 3];
        assert!(rows.next(&mut cols).unwrap());
        assert_eq!(cols[2], format!("at://{a}/app.bsky.graph.list/{k}").as_bytes());
        assert_eq!((rows.i64().unwrap(), rows.u8().unwrap()), (0, 1));
        // the list item's: its subject and list
        let mut cols = vec![Vec::new(); 4];
        assert!(rows.next(&mut cols).unwrap());
        assert_eq!((cols[2].as_slice(), cols[3].as_slice()), (b.as_bytes(), own.as_bytes()));
    }
}
