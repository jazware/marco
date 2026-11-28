//! The writer thread, the segment thread and checkpoints.
//!
//! The writer drains every write waiting, applies them to the spare copy of
//! the graph, publishes it, answers, then waits for readers to leave the old
//! copy and applies the same writes there. Every `segment_interval` it hands
//! the writes since the last cut to the segment thread with every source's
//! cursor at that cut; the segment thread writes them whole (temp file,
//! fsync, rename) in order and retries a failed write until it lands, so
//! segment numbers never skip.
//!
//! A checkpoint cuts a segment, and a thread of its own opens the snapshot,
//! replays the segments up to that cut, writes the changed shards and the
//! manifest, deletes what the manifest no longer names, then opens the new
//! snapshot and replays the segments written since. The writer folds in the
//! writes after those and switches to the new copies: writes and reads never
//! wait on the checkpoint. Every checkpoint takes a new id, so its files
//! never land on names a committed manifest uses, and anything that goes
//! wrong after the manifest's rename is cleanup, not a failed checkpoint.
//!
//! After open and after each checkpoint, a compaction thread rewrites the
//! shards whose patches have left too much garbage, one at a time, each as
//! a new base with the same lists, committed by a manifest of its own. The
//! writer swaps each into both copies as it does a write. A checkpoint that
//! comes due gives up the compaction, the shard being compacted included,
//! and runs once its thread has ended; a verify stops it after its current
//! shard. A failed checkpoint is retried after a backoff, a minute doubling
//! to an hour.
//!
//! If the writer or segment thread panics, the process aborts: a writer
//! that answers writes no segment will hold must not keep running.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use marco_core::store::{remove_unlisted, Manifest};
use marco_core::{segment, Compacted, Graph, Rec};

use marco_core::read::{Hold, Source};

use crate::app::{apply_planned, apply_rec, now_ms, open_replayed, App, Applied};

pub enum Req {
    Apply {
        rec: Rec,
        durable: bool,
        reply: tokio::sync::oneshot::Sender<Result<Applied>>,
    },
    Flush {
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    Checkpoint,
    Shutdown {
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    Rewritten(Box<Result<Rewritten>>),
    /// A shard compacted, or why it failed.
    Compacted(Box<Result<Compacted>>),
    /// The compaction thread ended.
    CompactionDone,
}

/// A committed checkpoint.
pub struct Rewritten {
    manifest: Manifest,
    seconds: f64,
    bytes: u64,
    shards: u32,
    patch_bytes: u64,
    pinned_bytes: u64,
    base_bytes: u64,
    /// Pages written through into the engine's cache.
    written_through: u64,
    /// The new copies to serve, or why opening them failed after the commit.
    switch: Result<Switch, String>,
    /// What failed in the cleanup after the commit.
    cleanup: Option<String>,
}

struct Switch {
    g: Graph,
    twin: Graph,
    /// The newest segment the new copies replayed.
    last: u64,
    segments_after: (u32, u64),
}

/// Aborts the process if the thread that holds it panics.
struct AbortOnPanic(&'static str);

impl Drop for AbortOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("marco: the {} thread panicked; stopping rather than answer writes no segment holds", self.0);
            std::process::abort();
        }
    }
}

enum Waiter {
    Apply(tokio::sync::oneshot::Sender<Result<Applied>>, Applied),
    Unit(tokio::sync::oneshot::Sender<Result<()>>),
}

impl Waiter {
    fn done(self) {
        match self {
            Waiter::Apply(r, a) => {
                let _ = r.send(Ok(a));
            }
            Waiter::Unit(r) => {
                let _ = r.send(Ok(()));
            }
        }
    }
}

struct SegJob {
    /// None: nothing to write, only waiters behind the segments queued.
    header: Option<segment::Header>,
    recs: Arc<Vec<Rec>>,
    waiters: Vec<Waiter>,
}

pub struct Init {
    pub cursors: BTreeMap<String, String>,
    pub segment: u64,
    pub ops: u64,
}

pub fn spawn(app: Arc<App>, spare: Graph, init: Init) -> mpsc::Sender<Req> {
    let (tx, rx) = mpsc::channel();
    let (seg_tx, seg_rx) = mpsc::channel::<SegJob>();
    let a = app.clone();
    std::thread::Builder::new().name("marco-segments".into()).spawn(move || segments(a, seg_rx)).unwrap();
    let m = spare.manifest().cloned().unwrap_or_default();
    let age = Duration::from_millis((now_ms() - m.written_ms).max(0) as u64);
    let w = Writer {
        interval: app.cfg.segment_interval,
        deadline: Instant::now() + app.cfg.segment_interval,
        app,
        tx: tx.clone(),
        spare: Some(Arc::new(spare)),
        buf: Vec::new(),
        buf_ops: 0,
        next_seg: init.segment + 1,
        cursors: init.cursors,
        build: m.build,
        seg_tx,
        since_cut: None,
        ops_since_ckpt: init.ops,
        ckpt_at: Instant::now().checked_sub(age).unwrap_or_else(Instant::now),
        ckpt_running: false,
        next_ckpt: m.checkpoint + 1,
        compaction: None,
        ckpt_wanted: None,
        ckpt_retry: None,
    };
    std::thread::Builder::new().name("marco-writer".into()).spawn(move || w.run(rx)).unwrap();
    tx
}

/// Writes segments in order; a failed write is retried until it lands.
fn segments(app: Arc<App>, rx: mpsc::Receiver<SegJob>) {
    let _guard = AbortOnPanic("segment");
    let dir = app.cfg.snapshot.clone();
    for job in rx {
        if let Some(h) = &job.header {
            loop {
                let t = Instant::now();
                match segment::write(&dir, h, &job.recs) {
                    Ok(bytes) => {
                        let mut d = app.durability.lock().unwrap();
                        d.segment = h.segment;
                        d.segment_ms = h.written_ms;
                        d.segment_write_seconds = t.elapsed().as_secs_f64();
                        d.segments += 1;
                        d.segment_bytes += bytes;
                        d.segment_ops += h.ops;
                        d.segment_error.clear();
                        crate::metrics::SEGMENT_SECONDS.observe(t.elapsed().as_secs_f64());
                        crate::metrics::SEGMENT_BYTES.inc_by(bytes);
                        break;
                    }
                    Err(e) => {
                        let msg = format!("change segment {}: {e:#}", h.segment);
                        eprintln!("marco: {msg}; retrying");
                        crate::metrics::SEGMENT_ERRORS.inc();
                        app.durability.lock().unwrap().segment_error = msg;
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
            }
            app.note_durable(&h.cursors);
        }
        job.waiters.into_iter().for_each(Waiter::done);
    }
}

/// Waits until no reader holds `g`.
fn exclusive(g: &mut Arc<Graph>) -> &mut Graph {
    let mut spins = 0u32;
    while Arc::get_mut(g).is_none() {
        spins += 1;
        if spins < 64 {
            std::hint::spin_loop();
        } else if spins < 1024 {
            std::thread::yield_now();
        } else {
            std::thread::sleep(Duration::from_micros(20));
        }
    }
    Arc::get_mut(g).unwrap()
}

/// A running compaction thread.
struct Compaction {
    /// Give up, the shard being compacted too.
    abort: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<()>,
}

struct Writer {
    app: Arc<App>,
    tx: mpsc::Sender<Req>,
    spare: Option<Arc<Graph>>,
    buf: Vec<Rec>,
    buf_ops: u64,
    next_seg: u64,
    /// Every source's cursor after the writes applied.
    cursors: BTreeMap<String, String>,
    build: u64,
    seg_tx: mpsc::Sender<SegJob>,
    /// While a checkpoint runs: the segments cut since its cut.
    since_cut: Option<Vec<(u64, Arc<Vec<Rec>>)>>,
    ops_since_ckpt: u64,
    ckpt_at: Instant,
    ckpt_running: bool,
    next_ckpt: u64,
    compaction: Option<Compaction>,
    /// A checkpoint waiting for the compaction to stop: whether it was asked for.
    ckpt_wanted: Option<bool>,
    /// After a failed checkpoint: when the next automatic one may start, and the backoff.
    ckpt_retry: Option<(Instant, Duration)>,
    interval: Duration,
    deadline: Instant,
}

impl Writer {
    fn run(mut self, rx: mpsc::Receiver<Req>) {
        let _guard = AbortOnPanic("writer");
        self.start_compaction();
        loop {
            let wait = self.deadline.saturating_duration_since(Instant::now());
            let mut batch = Vec::new();
            match rx.recv_timeout(wait) {
                Ok(r) => batch.push(r),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.cut(Vec::new());
                    self.stop_compaction();
                    self.wait_checkpoint(&rx);
                    return;
                }
            }
            while batch.len() < 4096 {
                match rx.try_recv() {
                    Ok(r) => batch.push(r),
                    Err(_) => break,
                }
            }
            let mut applies = Vec::new();
            let mut waiters = Vec::new();
            let mut checkpoint = false;
            let mut shutdown = false;
            for r in batch {
                match r {
                    Req::Apply { rec, durable, reply } => applies.push((rec, durable, reply)),
                    Req::Flush { reply } => waiters.push(Waiter::Unit(reply)),
                    Req::Checkpoint => checkpoint = true,
                    Req::Shutdown { reply } => {
                        waiters.push(Waiter::Unit(reply));
                        shutdown = true;
                    }
                    Req::Rewritten(r) => self.rewritten(*r),
                    Req::Compacted(r) => self.compacted(*r),
                    Req::CompactionDone => {
                        if let Some(c) = self.compaction.take() {
                            let _ = c.thread.join();
                        }
                        crate::metrics::COMPACTION_PENDING.set(0);
                        if let Some(asked) = self.ckpt_wanted.take() {
                            self.start_checkpoint(asked);
                        }
                    }
                }
            }
            if !applies.is_empty() {
                self.apply(applies, &mut waiters);
            }
            if shutdown {
                // the writes go into a segment now; the reply waits until nothing
                // else writes the snapshot
                self.cut(Vec::new());
                self.stop_compaction();
                self.wait_checkpoint(&rx);
                self.cut(waiters);
                return;
            }
            let now = Instant::now();
            if !waiters.is_empty() || now >= self.deadline {
                self.cut(waiters);
                self.deadline = now + self.interval;
            }
            self.app.durability.lock().unwrap().pending_ops = self.buf_ops;
            let due = self.ops_since_ckpt + self.buf_ops >= self.app.cfg.checkpoint_ops
                || (self.ckpt_at.elapsed() >= self.app.cfg.checkpoint_age && self.ops_since_ckpt + self.buf_ops > 0);
            if checkpoint || due {
                self.start_checkpoint(checkpoint);
            }
        }
    }

    /// Waits for a running checkpoint's thread to report, so nothing writes
    /// the snapshot once the writer has gone. What else comes meanwhile is
    /// dropped, as it would be after the return.
    fn wait_checkpoint(&mut self, rx: &mpsc::Receiver<Req>) {
        while self.ckpt_running {
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(Req::Rewritten(r)) => {
                    self.ckpt_running = false;
                    let r = *r;
                    if let Ok(r) = &r {
                        self.app.durability.lock().unwrap().checkpoint = r.manifest.checkpoint;
                    }
                    self.finish_job(r.map(|_| ()));
                }
                Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {}
                // every sender is gone, the checkpoint thread's too
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    fn apply(&mut self, applies: Vec<(Rec, bool, Reply)>, waiters: &mut Vec<Waiter>) {
        let mut chunk = Vec::with_capacity(applies.len());
        for a in applies {
            let (source, cursor) = source_cursor(&a.0);
            if !cursor.is_empty() && !self.cursors.contains_key(source) {
                self.apply_chunk(std::mem::take(&mut chunk), waiters, true);
                self.first(a, waiters);
            } else {
                chunk.push(a);
            }
        }
        self.apply_chunk(chunk, waiters, true);
    }

    /// A source's first cursor: its record goes out in a segment, written,
    /// before it's applied, so a source never restarts from nothing.
    fn first(&mut self, a: (Rec, bool, Reply), waiters: &mut Vec<Waiter>) {
        let (source, cursor) = source_cursor(&a.0);
        self.cursors.insert(source.to_string(), cursor.to_string());
        // A stream source now has its own cursor, in this segment: the bulk
        // build's time has done its job, and a source added later mustn't
        // start from it.
        if ["jetstream:", "jetstreamv2:", "firehose:"].iter().any(|p| source.starts_with(p))
            && self.cursors.remove(marco_ingest::SNAPSHOT_TIME).is_some()
        {
            self.app.forget_source(marco_ingest::SNAPSHOT_TIME);
        }
        self.buf_ops += a.0.ops() as u64;
        self.buf.push(a.0.clone());
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cut(vec![Waiter::Unit(tx)]);
        let _ = rx.blocking_recv();
        self.apply_chunk(vec![a], waiters, false);
    }

    /// Applies records to both copies; `buffer`: and keeps them for the next segment.
    fn apply_chunk(&mut self, applies: Vec<(Rec, bool, Reply)>, waiters: &mut Vec<Waiter>, buffer: bool) {
        if applies.is_empty() {
            return;
        }
        let mut g = self.spare.take().expect("the spare copy");
        let mut outs = Vec::with_capacity(applies.len());
        // the plan's pins last through both copies' applies
        let mut hold = Hold::new().source(Source::Write);
        {
            let gm = exclusive(&mut g);
            gm.quiet = false;
            let recs: Vec<&Rec> = applies.iter().map(|a| &a.0).collect();
            gm.plan(&recs, &mut hold);
            for (rec, _, _) in &applies {
                outs.push(apply_rec(gm, rec));
                let (source, cursor) = source_cursor(rec);
                if !cursor.is_empty() {
                    self.cursors.insert(source.to_string(), cursor.to_string());
                }
            }
        }
        let mut old = self.app.publish(g);
        let mut recs = Vec::with_capacity(applies.len());
        for ((rec, durable, reply), out) in applies.into_iter().zip(outs) {
            self.app.note_applied(&rec, &out);
            if durable {
                waiters.push(Waiter::Apply(reply, out));
            } else {
                let _ = reply.send(Ok(out));
            }
            recs.push(rec);
        }
        let om = exclusive(&mut old);
        om.quiet = true;
        for rec in &recs {
            apply_rec(om, rec);
        }
        om.quiet = false;
        drop(hold);
        self.spare = Some(old);
        if buffer {
            self.buf_ops += recs.iter().map(|r| r.ops() as u64).sum::<u64>();
            self.buf.extend(recs);
        }
    }

    fn send_segment(&self, job: SegJob) {
        if self.seg_tx.send(job).is_err() {
            eprintln!("marco: the segment thread is gone; stopping rather than answer writes no segment holds");
            std::process::abort();
        }
    }

    fn cut(&mut self, waiters: Vec<Waiter>) {
        if self.buf.is_empty() {
            if !waiters.is_empty() {
                self.send_segment(SegJob { header: None, recs: Arc::default(), waiters });
            }
            return;
        }
        let recs = Arc::new(std::mem::take(&mut self.buf));
        let ops = std::mem::take(&mut self.buf_ops);
        let h = segment::Header {
            segment: self.next_seg,
            build: self.build,
            written_ms: now_ms(),
            cursors: self.cursors.clone(),
            records: recs.len() as u64,
            ops,
        };
        if let Some(v) = &mut self.since_cut {
            v.push((self.next_seg, recs.clone()));
        }
        self.next_seg += 1;
        self.ops_since_ckpt += ops;
        self.send_segment(SegJob { header: Some(h), recs, waiters });
    }

    fn start_checkpoint(&mut self, asked: bool) {
        if self.ckpt_running {
            return;
        }
        if !asked && self.ckpt_retry.is_some_and(|(at, _)| Instant::now() < at) {
            return;
        }
        if let Some(c) = &self.compaction {
            // give up the shard being compacted: finishing it held checkpoints up to a minute and a half
            c.abort.store(true, Ordering::Relaxed);
            self.ckpt_wanted = Some(asked || self.ckpt_wanted.unwrap_or(false));
            return;
        }
        self.cut(Vec::new());
        if !asked {
            // the operator's call took the slot already; with a verify in it, this one runs unlisted
            let _ = self.app.begin_job("checkpoint", self.app.graph().n_shards() as u32);
        }
        let upto = self.next_seg - 1;
        let cursors = self.cursors.clone();
        let id = self.next_ckpt;
        self.next_ckpt += 1;
        self.since_cut = Some(Vec::new());
        self.ckpt_running = true;
        let (app, tx) = (self.app.clone(), self.tx.clone());
        std::thread::Builder::new()
            .name("marco-checkpoint".into())
            .spawn(move || {
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| rewrite(&app, id, upto, cursors)))
                    .unwrap_or_else(|_| Err(anyhow!("the checkpoint thread panicked")));
                let _ = tx.send(Req::Rewritten(Box::new(r)));
            })
            .unwrap();
    }

    /// Clears the job slot if a checkpoint holds it, whoever started it.
    fn finish_job(&self, r: Result<()>) {
        let j = self.app.job();
        if j.running && j.kind == "checkpoint" {
            self.app.end_job(r);
        }
    }

    fn note_error(&self, stage: &str, msg: String) {
        eprintln!("marco: checkpoint {stage}: {msg}");
        crate::metrics::CHECKPOINT_ERRORS.with_label_values(&[stage]).inc();
        let mut d = self.app.durability.lock().unwrap();
        d.checkpoint_error = format!("{stage}: {msg}");
        d.checkpoint_failures += 1;
    }

    fn rewritten(&mut self, r: Result<Rewritten>) {
        let since = self.since_cut.take().unwrap_or_default();
        self.ckpt_running = false;
        let r = match r {
            Ok(r) => r,
            Err(e) => {
                self.note_error("failed", format!("{e:#}"));
                self.finish_job(Err(e));
                let wait =
                    self.ckpt_retry.map_or(Duration::from_secs(60), |(_, w)| (w * 2).min(Duration::from_secs(3600)));
                self.ckpt_retry = Some((Instant::now() + wait, wait));
                return;
            }
        };
        self.ckpt_retry = None;
        self.ckpt_at = Instant::now();
        let ops: u64 = since.iter().map(|(_, recs)| recs.iter().map(|r| r.ops() as u64).sum::<u64>()).sum();
        self.ops_since_ckpt = ops;
        {
            let mut d = self.app.durability.lock().unwrap();
            d.checkpoint = r.manifest.checkpoint;
            d.checkpoint_ms = r.manifest.written_ms;
            d.checkpoint_seconds = r.seconds;
            d.checkpoint_bytes = r.bytes;
            d.checkpoint_shards = r.shards;
            d.checkpoint_patch_bytes = r.patch_bytes;
            d.checkpoint_pinned_bytes = r.pinned_bytes;
            d.segment_ops = ops;
            d.checkpoint_error.clear();
        }
        crate::metrics::CHECKPOINT_SECONDS.observe(r.seconds);
        for (part, b) in [("patch", r.patch_bytes), ("pinned", r.pinned_bytes), ("base", r.base_bytes)] {
            crate::metrics::CHECKPOINT_BYTES.with_label_values(&[part]).set(b as i64);
        }
        eprintln!(
            "marco: checkpoint {} committed in {:.1} s: {} shards, R {} B patch, P_new {} B pinned, {} B new bases, \
             {} pages written through",
            r.manifest.checkpoint, r.seconds, r.shards, r.patch_bytes, r.pinned_bytes, r.base_bytes, r.written_through
        );
        if let Some(e) = r.cleanup {
            self.note_error("cleanup after commit", e);
        }
        let sw = match r.switch {
            Ok(sw) => sw,
            Err(e) => {
                // the committed snapshot holds what the copies served hold; they carry on
                self.note_error("reopen after commit", e.clone());
                self.finish_job(Err(anyhow!("committed checkpoint {}; reopen failed: {e}", r.manifest.checkpoint)));
                return;
            }
        };
        let (mut g, mut t) = (sw.g, sw.twin);
        g.quiet = true;
        t.quiet = true;
        let mut skipped = None;
        for (_, recs) in since.iter().filter(|(k, _)| *k > sw.last) {
            skipped = skipped.or(apply_planned(&mut g, Some(&mut t), recs));
        }
        skipped = skipped.or(apply_planned(&mut g, Some(&mut t), &self.buf));
        if let Some(e) = skipped {
            // the served copies applied it; the new ones would lack it
            self.note_error("reopen after commit", e.clone());
            self.finish_job(Err(anyhow!("committed checkpoint {}; reopen failed: {e}", r.manifest.checkpoint)));
            return;
        }
        g.quiet = false;
        t.quiet = false;
        let old = (self.app.publish(Arc::new(g)), self.spare.replace(Arc::new(t)));
        // freeing a day of deltas twice takes long enough to stall writes
        std::thread::Builder::new().name("marco-free".into()).spawn(move || drop(old)).ok();
        {
            let mut d = self.app.durability.lock().unwrap();
            d.segments = sw.segments_after.0;
            d.segment_bytes = sw.segments_after.1;
        }
        *self.app.verify.lock().unwrap() = vec![Default::default(); self.app.snapshot_files()];
        self.app.list_stats.clear();
        self.finish_job(Ok(()));
        self.start_compaction();
    }

    /// Starts compacting the shards that are due, if any are and nothing
    /// else is rewriting the snapshot.
    fn start_compaction(&mut self) {
        if self.ckpt_running || self.compaction.is_some() || self.app.cfg.read_only {
            return;
        }
        let Some(snap) = self.spare.as_ref().and_then(|g| g.shared_snapshot()) else { return };
        let policy = self.app.cfg.compaction;
        let garbage = snap.garbage();
        self.app.garbage_bytes.store(garbage.iter().map(|g| g.1.pages).sum(), Ordering::Relaxed);
        let due = marco_core::SharedSnapshot::due_of(garbage, &policy, self.app.headroom());
        if due.is_empty() || self.verifying() {
            return;
        }
        crate::metrics::COMPACTION_PENDING.set(due.len() as i64);
        drop(snap);
        let abort = Arc::new(AtomicBool::new(false));
        let (ab, tx, app) = (abort.clone(), self.tx.clone(), self.app.clone());
        let thread = std::thread::Builder::new()
            .name("marco-compaction".into())
            .spawn(move || {
                let path = &app.cfg.snapshot;
                for (k, _) in due {
                    // a verify reads the files by name, and a compaction removes some
                    if ab.load(Ordering::Relaxed) || verifying(&app) {
                        break;
                    }
                    // the served copy's snapshot, afresh for each shard: one held across the run would keep every
                    // compacted shard's old files open, and at full residency their cached pages with them
                    let Some(snap) = app.graph().shared_snapshot() else { break };
                    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| snap.compact(path, k, &ab)))
                        .unwrap_or_else(|_| Err(anyhow!("the compaction thread panicked")));
                    // another shard's compaction may still go: a shard that keeps failing
                    // mustn't hold up the rest. One that found the snapshot moved stops it.
                    let stale = r.as_ref().err().is_some_and(|e| {
                        let m = e.to_string();
                        m == marco_core::COMPACTION_STOPPED
                            || m.contains("changed since")
                            || m.contains("changed during")
                    });
                    if let Ok(c) = &r {
                        if let Err(e) = remove_unlisted(path, &c.manifest, false) {
                            eprintln!("marco: removing shard {k}'s old files after its compaction: {e:#}");
                        }
                    }
                    if tx.send(Req::Compacted(Box::new(r))).is_err() || stale {
                        break;
                    }
                }
                let _ = tx.send(Req::CompactionDone);
            })
            .expect("spawn the compaction thread");
        self.compaction = Some(Compaction { abort, thread });
    }

    fn verifying(&self) -> bool {
        verifying(&self.app)
    }

    /// Stops the compaction, if one runs, and waits for its thread: nothing
    /// may write the snapshot once the writer has gone.
    fn stop_compaction(&mut self) {
        if let Some(c) = self.compaction.take() {
            c.abort.store(true, Ordering::Relaxed);
            let _ = c.thread.join();
        }
    }

    /// Swaps a compacted shard into both copies, as a write goes in.
    fn compacted(&mut self, r: Result<Compacted>) {
        crate::metrics::COMPACTION_PENDING.set((crate::metrics::COMPACTION_PENDING.get() - 1).max(0));
        let c = match r {
            Ok(c) => c,
            Err(e) if e.to_string() == marco_core::COMPACTION_STOPPED => return,
            Err(e) => {
                self.note_compaction_error(format!("{e:#}"));
                return;
            }
        };
        let k = c.shard;
        let swapped = c.file.clone();
        let swap = match self.spare.as_ref().expect("the spare copy").compacted(k, &c.was, c.file, c.manifest) {
            Ok(s) => s,
            Err(e) => {
                // committed: a restart opens the compacted shard, which reads the same
                self.note_compaction_error(format!("{e:#}"));
                return;
            }
        };
        let mut g = self.spare.take().expect("the spare copy");
        exclusive(&mut g).swap_base(&swap);
        let mut old = self.app.publish(g);
        exclusive(&mut old).swap_base(&swap);
        self.spare = Some(old);
        // only this thread sets it
        let g = self.app.garbage_bytes.load(Ordering::Relaxed);
        self.app.garbage_bytes.store(g.saturating_sub(c.before.pages), Ordering::Relaxed);
        self.app.note_compacted();
        crate::metrics::COMPACTION_SHARDS.inc();
        crate::metrics::COMPACTION_BYTES.inc_by(c.bytes);
        crate::metrics::COMPACTION_GARBAGE.inc_by(c.before.bytes());
        crate::metrics::COMPACTION_SECONDS.observe(c.seconds);
        eprintln!(
            "marco: compacted shard {k} in {:.1} s: {} B of files, {} B live, into a {} B base",
            c.seconds, c.before.disk, c.before.live, c.bytes
        );
        {
            let mut d = self.app.durability.lock().unwrap();
            d.compactions += 1;
            d.compaction_bytes += c.bytes;
            d.compaction_error.clear();
        }
        if !self.verifying() {
            *self.app.verify.lock().unwrap() = vec![Default::default(); self.app.snapshot_files()];
        }
        if self.app.cfg.load {
            // before its first reads would miss every page
            let (f, engine) = (swapped, self.app.engine.clone());
            std::thread::Builder::new()
                .name("marco-load".into())
                .spawn(move || {
                    engine.load(&[&*f.base().file.file]);
                })
                .ok();
        }
    }

    fn note_compaction_error(&self, msg: String) {
        eprintln!("marco: compaction: {msg}");
        crate::metrics::CHECKPOINT_ERRORS.with_label_values(&["compaction"]).inc();
        self.app.durability.lock().unwrap().compaction_error = msg;
    }
}

/// Whether a verify job is running.
fn verifying(app: &App) -> bool {
    let j = app.job();
    j.running && j.kind == "verify"
}

fn source_cursor(r: &Rec) -> (&str, &str) {
    match r {
        Rec::Apply { source, cursor, .. } | Rec::Status { source, cursor, .. } => (source, cursor),
    }
}

type Reply = tokio::sync::oneshot::Sender<Result<Applied>>;

/// Writes and commits checkpoint `id` at segment `upto`. An error means
/// nothing was committed; what fails after the commit comes back in
/// `Rewritten`.
fn rewrite(app: &App, id: u64, upto: u64, cursors: BTreeMap<String, String>) -> Result<Rewritten> {
    let t = Instant::now();
    let cfg = &app.cfg;
    while app.durability.lock().unwrap().segment < upto {
        std::thread::sleep(Duration::from_millis(5));
    }
    let g = open_replayed(cfg, &app.engine, Some(upto))?.graph;
    let mut rep = g.write_checkpoint(&cfg.snapshot, id, upto, cursors)?;
    drop(g);
    // the new files stay registered, their written-through pages cached, until
    // the new copies open them
    let written = std::mem::take(&mut rep.files);
    let manifest = rep.manifest;
    let (bytes, shards) = (rep.file_bytes, rep.patches + rep.bases);
    let cleanup = remove_unlisted(&cfg.snapshot, &manifest, false).err().map(|e| format!("{e:#}"));
    let switch = (|| -> Result<Switch> {
        let r = open_replayed(cfg, &app.engine, None)?;
        // the served copies applied that op: keep serving them
        if let Some(e) = &r.skipped {
            bail!("replaying the segments after the cut: {e}");
        }
        if cfg.load {
            // before the switch: the new files' first reads would miss every page
            if let Some(s) = r.graph.snapshot() {
                s.load();
            }
        }
        let seg_bytes = segment::list(&cfg.snapshot)?
            .iter()
            .filter(|(k, _)| *k > upto)
            .map(|(_, p)| std::fs::metadata(p).map_or(0, |x| x.len()))
            .sum();
        let twin = r.graph.clone();
        Ok(Switch { g: r.graph, twin, last: r.last, segments_after: (r.segments, seg_bytes) })
    })()
    .map_err(|e| format!("{e:#}"));
    drop(written);
    Ok(Rewritten {
        manifest,
        seconds: t.elapsed().as_secs_f64(),
        bytes,
        shards,
        patch_bytes: rep.patch_bytes,
        pinned_bytes: rep.pinned_bytes,
        base_bytes: rep.base_bytes,
        written_through: rep.written_through,
        switch,
        cleanup,
    })
}
