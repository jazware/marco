//! The pod: `bsky.Service/SearchActorsTypeahead` over the newest artifact and
//! its live overlay. One task owns every change: Jetstream's events, handle
//! verifications and artifact swaps go through it in order, so a swap replays
//! the recent changes into the new overlay and nothing is lost between them.
//! Queries read the current generation through an `ArcSwap`: a swap never
//! pauses them, and the old generation's map goes on a blocking thread once
//! its last query ends.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use arc_swap::ArcSwapOption;
use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use parking_lot::RwLock;
use seek_api::bsky::{SearchActorResult, SearchActorsTypeaheadRequest, SearchActorsTypeaheadResponse};
use seek_core::did::{key_of, DidKey};
use seek_core::index::{Index, NO_ID};
use seek_core::overlay::Overlay;
use seek_core::query::{self, alloc_size, Params, Viewer, ViewerBuild};
use tokio::sync::mpsc;

use crate::jetstream::Ev;
use crate::marco::Marco;
use crate::metrics::*;
use crate::verify::{Outcome, Queue};

pub const DEFAULT_LIMIT: usize = 10;
pub const MAX_LIMIT: usize = 100;
/// Extra candidates past the limit, so a page the block filter thins is still full.
const MARGIN: usize = 8;
/// A request header that traces the query whatever the sample rate.
pub const FORCE_TRACE: &str = "x-seek-trace";
/// Follows folded per hold of the overlay's read lock while a viewer's keys are built.
const VIEWER_CHUNK: usize = 1024;

/// One artifact and the changes since it.
pub struct Gen {
    pub id: u64,
    pub version: String,
    pub index: Index,
    pub overlay: RwLock<Overlay>,
    path: PathBuf,
    bytes: u64,
}

impl Drop for Gen {
    fn drop(&mut self) {
        self.index.artifact().release();
        MAPPED.sub(self.bytes as i64);
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A change in the order it happened, kept for replay into the next generation.
#[derive(Clone, Debug)]
pub enum Change {
    Name(String, Option<String>),
    Handle(String, Option<String>),
    Hidden(String, bool),
}

/// A viewer's side of their logged-in queries, as the cache keeps it.
pub enum ViewerEntry {
    /// More follows than `--max-follows`: no follows first. Kept so the next keystroke doesn't ask
    /// marco again.
    Capped {
        read_ms: u64,
    },
    Keyed(Keyed),
}

/// A viewer's follows as one generation's ids. Ids are per artifact: [`Rekey`] moves an entry onto
/// the next artifact's without asking marco again.
pub struct Keyed {
    pub gen: u64,
    /// When marco's follows were read (unix ms), kept across re-keys.
    pub read_ms: u64,
    pub viewer: Viewer,
    /// Follows no generation has had an id for yet (no handle or name seen), each with whether it's
    /// a mutual: tried again at each re-key.
    pub unresolved: Box<[(DidKey, bool)]>,
}

/// What the cache spends on an entry besides its value: moka's entry and its deque node, the key
/// (a DID) and the Arc (fitted to RSS growth over 60,000 entries).
const ENTRY_BYTES: usize = 512;

impl ViewerEntry {
    fn weight(&self) -> u32 {
        let bytes = match self {
            ViewerEntry::Capped { .. } => ENTRY_BYTES,
            ViewerEntry::Keyed(k) => {
                let other = |d: &DidKey| match d {
                    DidKey::Other(s) => alloc_size(s.len()),
                    DidKey::Plc(_) => 0,
                };
                ENTRY_BYTES
                    + k.viewer.bytes()
                    + alloc_size(std::mem::size_of::<(DidKey, bool)>() * k.unresolved.len())
                    + k.unresolved.iter().map(|(d, _)| other(d)).sum::<usize>()
            }
        };
        bytes.min(u32::MAX as usize) as u32
    }

    /// When marco was read for it (unix ms).
    fn read_ms(&self) -> u64 {
        match self {
            ViewerEntry::Capped { read_ms } => *read_ms,
            ViewerEntry::Keyed(k) => k.read_ms,
        }
    }
}

impl Keyed {
    /// Folds the follows' keys against `g`, taking the overlay's read lock a chunk of follows at
    /// a time, so a heavy follower never holds writers off for long.
    fn build(g: &Gen, follows: Vec<u32>, mutual: Vec<u32>, unresolved: Vec<(DidKey, bool)>, read_ms: u64) -> Keyed {
        let t = Instant::now();
        let mut b = ViewerBuild::new(follows, mutual);
        let mut at = 0;
        while at < b.len() {
            let to = (at + VIEWER_CHUNK).min(b.len());
            b.fold(&g.index, &g.overlay.read(), at, to);
            at = to;
        }
        let viewer = b.finish();
        VIEWER_KEYS.with(&[]).since(t);
        Keyed { gen: g.id, read_ms, viewer, unresolved: unresolved.into() }
    }

    /// From marco's answer: each DID's id in `g`, base or overlay.
    fn resolve(g: &Gen, follows: &[String], mutuals: &[String], read_ms: u64) -> Keyed {
        let m: rustc_hash::FxHashSet<&str> = mutuals.iter().map(String::as_str).collect();
        let (mut fi, mut mi, mut un) = (Vec::with_capacity(follows.len()), Vec::new(), Vec::new());
        for c in follows.chunks(VIEWER_CHUNK) {
            let ov = g.overlay.read();
            for d in c {
                let k = key_of(d);
                let mutual = m.contains(d.as_str());
                match ov.id_of(&g.index, &k) {
                    Some(id) => {
                        fi.push(id);
                        if mutual {
                            mi.push(id)
                        }
                    }
                    None => un.push((k, mutual)),
                }
            }
        }
        Keyed::build(g, fi, mi, un, read_ms)
    }

    /// This entry on `g`'s ids, by `rk` (built from this entry's generation to `g`).
    fn rekey(&self, rk: &Rekey, g: &Gen) -> Keyed {
        let v = &self.viewer;
        let (mut fi, mut mi, mut un) = (Vec::with_capacity(v.follows.len()), Vec::new(), Vec::new());
        for &id in v.follows.iter() {
            let mutual = v.mutual(id);
            match rk.ids.get(id as usize) {
                Some(&n) if n != NO_ID => {
                    fi.push(n);
                    if mutual {
                        mi.push(n)
                    }
                }
                _ => {
                    if let Some(k) = rk.gone.get(&id) {
                        un.push((k.clone(), mutual))
                    }
                }
            }
        }
        // the map's misses and the follows no generation had: the overlay may have them now
        let ask: Vec<(DidKey, bool)> = un.drain(..).chain(self.unresolved.iter().cloned()).collect();
        let ov = g.overlay.read();
        for (k, mutual) in ask.iter() {
            match ov.id_of(&g.index, k) {
                Some(n) => {
                    fi.push(n);
                    if *mutual {
                        mi.push(n)
                    }
                }
                None => un.push((k.clone(), *mutual)),
            }
        }
        drop(ov);
        Keyed::build(g, fi, mi, un, self.read_ms)
    }
}

/// Old ids to new across one swap: built while both generations are mapped, kept until the sweep
/// has moved the cache onto the new ids.
struct Rekey {
    from: u64,
    to: u64,
    /// Indexed by the old generation's ids (base, then overlay): the new id, or `NO_ID`.
    ids: Vec<u32>,
    /// The DIDs of old ids with no new one, so an entry keeps them as unresolved.
    gone: rustc_hash::FxHashMap<u32, DidKey>,
}

impl Rekey {
    fn new(old: &Gen, new: &Gen) -> Rekey {
        let t = Instant::now();
        let mut ids = old.index.ids_in(&new.index);
        let (oov, nov) = (old.overlay.read(), new.overlay.read());
        let mut gone = rustc_hash::FxHashMap::default();
        for (i, n) in ids.iter_mut().enumerate() {
            if *n == NO_ID {
                if let Some(k) = old.index.did(i as u32).map(|d| key_of(&d)) {
                    match nov.id_of(&new.index, &k) {
                        Some(id) => *n = id,
                        None => {
                            gone.insert(i as u32, k);
                        }
                    }
                }
            }
        }
        ids.reserve_exact(oov.new_dids().len());
        for k in oov.new_dids() {
            let n = nov.id_of(&new.index, k);
            if n.is_none() {
                gone.insert(ids.len() as u32, k.clone());
            }
            ids.push(n.unwrap_or(NO_ID));
        }
        tracing::info!(
            ids = ids.len(),
            gone = gone.len(),
            ms = t.elapsed().as_millis() as u64,
            "viewer ids mapped onto the new artifact"
        );
        Rekey { from: old.id, to: new.id, ids, gone }
    }
}

/// The viewer cache's expiry: `--viewer-idle-seconds` after an entry is built or read. Moving an
/// entry onto a new artifact's ids is an update and keeps its deadline, so the sweep at each swap
/// doesn't keep idle viewers alive.
struct Idle(Duration);

impl moka::Expiry<String, Arc<ViewerEntry>> for Idle {
    fn expire_after_create(&self, _: &String, _: &Arc<ViewerEntry>, _: std::time::Instant) -> Option<Duration> {
        Some(self.0)
    }

    fn expire_after_read(
        &self,
        _: &String,
        _: &Arc<ViewerEntry>,
        _: std::time::Instant,
        _: Option<Duration>,
        _: std::time::Instant,
    ) -> Option<Duration> {
        Some(self.0)
    }

    fn expire_after_update(
        &self,
        _: &String,
        _: &Arc<ViewerEntry>,
        _: std::time::Instant,
        left: Option<Duration>,
    ) -> Option<Duration> {
        left
    }
}

/// What the cache holds for a viewer (`Pod::cached_viewer`).
#[derive(Debug, Clone, PartialEq)]
pub enum CachedViewer {
    Capped,
    Keyed { gen: u64, follows: usize, mutuals: usize, unresolved: usize, read_ms: u64, weight: u32 },
}

pub struct Config {
    pub params: Params,
    /// Jetstream's replay window: an artifact whose cursor is older can't be caught up.
    pub replay_window: Duration,
    pub viewer_cache_bytes: u64,
    pub viewer_idle: Duration,
    /// Keep this long of changes for replay at a swap: longer than the builder's interval.
    pub replay_keep: Duration,
    /// Not ready once Jetstream is this far behind (or silent this long).
    pub max_lag: Duration,
    /// One viewer's follows and mutuals from marco, all pages, within this.
    pub viewer_deadline: Duration,
    /// How far marco's follows may trail Jetstream's. A build whose marco read starts before a
    /// follow event for the viewer plus this isn't cached, and the entry is dropped again at twice
    /// this after the event.
    pub marco_lag: Duration,
}

pub struct Pod {
    cur: ArcSwapOption<Gen>,
    marco: Option<Marco>,
    viewers: moka::future::Cache<String, Arc<ViewerEntry>>,
    hists: QueryHists,
    cfg: Config,
    why: Mutex<String>,
    ready: AtomicBool,
    caught_up: AtomicBool,
    stale: AtomicBool,
    lagging: AtomicBool,
    next_gen: AtomicU64,
    /// While marco fails, viewer builds wait until this (unix ms) instead of piling on.
    marco_down_until: AtomicU64,
    /// The last swap's id map, while the viewer cache may still hold entries on the old ids.
    rekey: ArcSwapOption<Rekey>,
    /// When each DID's entry was last dropped by a follow event (unix ms), for `marco_lag` and the
    /// longest build after it.
    dropped: parking_lot::Mutex<rustc_hash::FxHashMap<String, u64>>,
    /// The listeners' clean stop (`--shutdown-drain-secs`): not ready once it begins, while
    /// queries are still answered.
    pub drain: marco_ops::drain::Drain,
}

impl Pod {
    pub fn new(cfg: Config, marco: Option<Marco>) -> Arc<Pod> {
        let viewers = moka::future::Cache::builder()
            .max_capacity(cfg.viewer_cache_bytes)
            .weigher(|_: &String, v: &Arc<ViewerEntry>| v.weight())
            .expire_after(Idle(cfg.viewer_idle))
            .eviction_listener(|_, _, cause| {
                let c = match cause {
                    moka::notification::RemovalCause::Expired => "idle",
                    moka::notification::RemovalCause::Size => "size",
                    moka::notification::RemovalCause::Replaced => return,
                    moka::notification::RemovalCause::Explicit => "invalidated",
                };
                VIEWER_EVICTIONS.with_label_values(&[c]).inc();
            })
            .build();
        if marco.is_none() {
            tracing::warn!("no --marco: queries that name a viewer are refused (blocks can't be checked)");
        }
        Arc::new(Pod {
            cur: ArcSwapOption::empty(),
            marco,
            viewers,
            hists: QueryHists::new(),
            cfg,
            why: Mutex::new("no artifact yet".into()),
            ready: AtomicBool::new(false),
            caught_up: AtomicBool::new(false),
            stale: AtomicBool::new(false),
            lagging: AtomicBool::new(false),
            next_gen: AtomicU64::new(1),
            marco_down_until: AtomicU64::new(0),
            rekey: ArcSwapOption::empty(),
            dropped: Default::default(),
            drain: marco_ops::drain::Drain::new(),
        })
    }

    pub fn current(&self) -> Option<Arc<Gen>> {
        self.cur.load_full()
    }

    /// (ready, why not): `/readyz`. Not ready for good once the drain begins.
    pub fn readiness(&self) -> (bool, String) {
        if self.drain.draining() {
            return (false, "shutting down".into());
        }
        (self.ready.load(Ordering::Relaxed), self.why.lock().unwrap().clone())
    }

    fn set_state(&self) {
        let why = if self.cur.load().is_none() {
            "no artifact yet".to_string()
        } else if self.stale.load(Ordering::Relaxed) {
            "refusing to serve: the artifact's cursor is older than Jetstream's replay window, or Jetstream's replay skipped events; waiting for a newer artifact".into()
        } else if !self.caught_up.load(Ordering::Relaxed) {
            "replaying Jetstream from the artifact's cursor".into()
        } else if self.lagging.load(Ordering::Relaxed) {
            format!("Jetstream is more than {} s behind or silent", self.cfg.max_lag.as_secs())
        } else {
            String::new()
        };
        let ready = why.is_empty();
        if !ready && *self.why.lock().unwrap() != why {
            tracing::warn!(reason = %why, "not ready");
        }
        self.ready.store(ready, Ordering::Relaxed);
        READY.set(ready as i64);
        STALE.set(self.stale.load(Ordering::Relaxed) as i64);
        *self.why.lock().unwrap() = why;
    }

    /// Gauges a scrape reads.
    pub fn refresh_gauges(&self) {
        if let Some(g) = self.current() {
            ARTIFACT_AGE.set(now_secs() - g.index.meta.created_ms as f64 / 1e3);
            let st = g.overlay.read().stats();
            OVERLAY_ACTORS.set(st.actors as i64);
            OVERLAY_KEYS.set(st.keys as i64);
            OVERLAY_BYTES.set(st.key_bytes as i64);
        }
        VIEWER_ENTRIES.set(self.viewers.entry_count() as i64);
        VIEWER_BYTES.set(self.viewers.weighted_size() as i64);
    }

    /// Opens the artifact at `path` unless it was built before `newer_than` (ms), which is read
    /// from its meta before anything is verified or paged in. None: refused as older.
    fn open_gen(next: u64, path: PathBuf, version: String, newer_than: Option<i64>) -> Result<Option<Gen>> {
        let refuse = |e: anyhow::Error| {
            VERIFY_FAILURES.inc();
            REFUSED.with_label_values(&["invalid"]).inc();
            let _ = std::fs::remove_file(&path);
            Err(e)
        };
        let art = match seek_core::artifact::Artifact::open(&path, false) {
            Ok(a) => a,
            Err(e) => return refuse(e),
        };
        let created = art
            .raw("meta")
            .ok()
            .and_then(|m| serde_json::from_slice::<seek_core::index::Meta>(m).ok())
            .map(|m| m.created_ms);
        if let (Some(c), Some(cur)) = (created, newer_than) {
            if c <= cur {
                REFUSED.with_label_values(&["older"]).inc();
                drop(art);
                let _ = std::fs::remove_file(&path);
                return Ok(None);
            }
        }
        if let Err(e) = art.verify() {
            return refuse(e);
        }
        let index = match Index::from_artifact(art) {
            Ok(i) => i,
            Err(e) => return refuse(e),
        };
        index.artifact().prefetch();
        let bytes = index.artifact().len() as u64;
        MAPPED.add(bytes as i64);
        let overlay = RwLock::new(Overlay::new(&index));
        Ok(Some(Gen { id: next, version, index, overlay, path, bytes }))
    }

    /// Serves `g`; the generation it replaces goes on a blocking thread once its last query ends,
    /// so no query pays for the unmap.
    fn publish_gen(&self, g: Gen) {
        let m = &g.index.meta;
        ARTIFACT_INFO.reset();
        ARTIFACT_INFO.with_label_values(&[&g.version]).set(1);
        ARTIFACT_BUILT.set(m.created_ms as f64 / 1e3);
        ARTIFACT_CURSOR.set(m.cursor_us as f64 / 1e6);
        ARTIFACT_BUILD_SECS.set(m.build_secs);
        ARTIFACT_ACTORS.set(m.actors as i64);
        tracing::info!(version = %g.version, actors = m.actors, cursor_us = m.cursor_us, "serving artifact");
        if let Some(old) = self.cur.swap(Some(Arc::new(g))) {
            tokio::task::spawn_blocking(move || {
                let t = Instant::now();
                while Arc::strong_count(&old) > 1 && t.elapsed() < Duration::from_secs(60) {
                    std::thread::sleep(Duration::from_millis(20));
                }
                drop(old);
            });
        }
    }

    /// What the cache holds for `viewer`, for tests and debugging.
    pub async fn cached_viewer(&self, viewer: &str) -> Option<CachedViewer> {
        let e = self.viewers.get(viewer).await?;
        Some(match &*e {
            ViewerEntry::Capped { .. } => CachedViewer::Capped,
            ViewerEntry::Keyed(k) => CachedViewer::Keyed {
                gen: k.gen,
                follows: k.viewer.follows.len(),
                mutuals: k.viewer.mutual.len(),
                unresolved: k.unresolved.len(),
                read_ms: k.read_ms,
                weight: e.weight(),
            },
        })
    }

    /// `e` for a query on `g`: as it is when it's on `g`'s ids or a newer generation's (the query
    /// moves to that one), re-keyed by the last swap's map, or None when it's on older ids that
    /// map doesn't cover.
    async fn on_gen(&self, viewer: &str, e: Arc<ViewerEntry>, g: &Gen) -> Option<Arc<ViewerEntry>> {
        let ViewerEntry::Keyed(k) = &*e else { return Some(e) };
        if k.gen >= g.id {
            return Some(e);
        }
        let rk = self.rekey.load_full().filter(|r| r.from == k.gen && r.to == g.id)?;
        let n = Arc::new(ViewerEntry::Keyed(k.rekey(&rk, g)));
        self.replace(viewer, &e, Some(n.clone())).await;
        Some(n)
    }

    /// Puts `n` in `old`'s place (or removes it for None), unless the entry changed since (a follow
    /// event dropped it, or a build or a re-key replaced it). An update keeps the entry's idle
    /// deadline (`Idle`).
    async fn replace(&self, viewer: &str, old: &Arc<ViewerEntry>, n: Option<Arc<ViewerEntry>>) {
        use moka::ops::compute::{CompResult, Op};
        let r = self
            .viewers
            .entry_by_ref(viewer)
            .and_compute_with(|cur| async move {
                match (cur, n) {
                    (Some(c), Some(n)) if Arc::ptr_eq(c.value(), old) => Op::Put(n),
                    (Some(c), None) if Arc::ptr_eq(c.value(), old) => Op::Remove,
                    _ => Op::Nop,
                }
            })
            .await;
        if matches!(r, CompResult::ReplacedWith(_)) {
            VIEWER_REKEYS.inc();
        }
    }

    /// Drops `did`'s entry on a follow event, theirs or of them. Their next build isn't kept if its
    /// marco read starts within `marco_lag` of now, when marco may not have the follow yet; a build
    /// already reading isn't kept either.
    async fn drop_viewer(&self, did: &str) {
        self.dropped.lock().insert(did.to_string(), (now_secs() * 1e3) as u64);
        self.viewers.invalidate(did).await;
    }

    /// Whether a build whose marco read started at `read_ms` may have missed a follow event.
    fn read_before_drop(&self, viewer: &str, read_ms: u64) -> bool {
        let lag = self.cfg.marco_lag.as_millis() as u64;
        self.dropped.lock().get(viewer).is_some_and(|&t| read_ms < t + lag)
    }

    /// Moves every entry on the swapped-out generation's ids onto the new one's, a batch at a time
    /// on a blocking thread, so a viewer back hours later still finds their entry. It stops when a
    /// newer swap comes (that swap's sweep takes over, and entries it can't move are rebuilt when
    /// next used). Then the map goes.
    async fn sweep(self: Arc<Pod>, rk: Arc<Rekey>) {
        let t = Instant::now();
        let n = self.sweep_entries(&rk).await;
        let _ = self.rekey.compare_and_swap(&Some(rk), None);
        tracing::info!(entries = n, ms = t.elapsed().as_millis() as u64, "viewer cache moved onto the new artifact");
    }

    async fn sweep_entries(&self, rk: &Arc<Rekey>) -> usize {
        let todo: Vec<(Arc<String>, Arc<ViewerEntry>)> =
            self.viewers.iter().filter(|(_, e)| matches!(&**e, ViewerEntry::Keyed(k) if k.gen == rk.from)).collect();
        let mut it = todo.into_iter();
        let mut moved = 0;
        loop {
            let Some(g) = self.current().filter(|g| g.id == rk.to) else { break };
            let batch: Vec<_> = it.by_ref().take(256).collect();
            if batch.is_empty() {
                break;
            }
            let rk2 = rk.clone();
            let Ok(done) = tokio::task::spawn_blocking(move || {
                batch
                    .into_iter()
                    .filter_map(|(v, e)| match &*e {
                        ViewerEntry::Keyed(k) => {
                            let n = Arc::new(ViewerEntry::Keyed(k.rekey(&rk2, &g)));
                            Some((v, e, n))
                        }
                        ViewerEntry::Capped { .. } => None,
                    })
                    .collect::<Vec<_>>()
            })
            .await
            else {
                break;
            };
            moved += done.len();
            for (v, e, n) in done {
                self.replace(&v, &e, Some(n)).await;
            }
        }
        moved
    }

    async fn viewer_entry(&self, viewer: &str, g: &Arc<Gen>) -> Result<Arc<ViewerEntry>, String> {
        let Some(marco) = self.marco.as_ref() else { return Err("no marco".into()) };
        let t = Instant::now();
        let mut miss = false;
        let now_ms = (now_secs() * 1e3) as u64;
        let down = self.marco_down_until.load(Ordering::Relaxed) > now_ms;
        let max_age = self.cfg.viewer_idle.as_millis() as u64;
        let cached = match self.viewers.get(viewer).await {
            None => None,
            // marco read more than the idle time ago: read it again, whatever events were missed
            Some(e) if e.read_ms() + max_age < now_ms => {
                self.replace(viewer, &e, None).await;
                None
            }
            Some(e) => match self.on_gen(viewer, e.clone(), g).await {
                Some(e) => Some(e),
                None => {
                    self.replace(viewer, &e, None).await;
                    None
                }
            },
        };
        let r = match cached {
            Some(e) => Ok(e),
            None if self.cfg.viewer_deadline.is_zero() => Err("viewer builds off (deadline 0)".to_string()),
            None if down => Err("marco failing: viewer builds paused".to_string()),
            None => {
                let built = self
                    .viewers
                    .try_get_with(viewer.to_string(), async {
                        miss = true;
                        let tb = Instant::now();
                        let both = async {
                            let n = marco.follow_count(viewer, &self.hists).await?;
                            VIEWER_FOLLOWS.with(&[]).observe(n);
                            if n > marco.max_follows as u64 {
                                VIEWER_CAPPED.inc();
                                return Ok(None);
                            }
                            let fm = tokio::try_join!(
                                marco.follows(viewer, &self.hists),
                                marco.mutuals(viewer, &self.hists)
                            )?;
                            Ok::<_, anyhow::Error>(Some(fm))
                        };
                        let fm = tokio::time::timeout(self.cfg.viewer_deadline, both)
                            .await
                            .map_err(|_| anyhow::anyhow!("viewer build past {:?}", self.cfg.viewer_deadline))??;
                        VIEWER_BUILD.with(&[]).since(tb);
                        Ok::<_, anyhow::Error>(Arc::new(match fm {
                            None => ViewerEntry::Capped { read_ms: now_ms },
                            Some((f, m)) => ViewerEntry::Keyed(Keyed::resolve(g, &f, &m, now_ms)),
                        }))
                    })
                    .await
                    .map_err(|e| {
                        self.marco_down_until.store(now_ms + 2000, Ordering::Relaxed);
                        e.to_string()
                    });
                match built {
                    Ok(e) => {
                        if miss && self.read_before_drop(viewer, now_ms) {
                            // a follow event came during or just before the read: this query uses
                            // it, the cache doesn't keep it
                            VIEWER_UNKEPT.inc();
                            self.replace(viewer, &e, None).await;
                        }
                        // a build another query started on an artifact older than this query's
                        self.on_gen(viewer, e, g)
                            .await
                            .ok_or_else(|| "viewer built on an artifact older than the query's".to_string())
                    }
                    Err(e) => Err(e),
                }
            }
        };
        if miss {
            VIEWER_MISSES.inc()
        } else {
            VIEWER_HITS.inc()
        }
        self.hists.stage[0].since(t);
        r
    }

    /// One query. `trace` forces a span.
    pub async fn search(
        &self,
        q: &str,
        viewer: &str,
        limit: i32,
        trace: bool,
    ) -> Result<Vec<(String, f64)>, (&'static str, String)> {
        let t0 = Instant::now();
        let mode = (!viewer.is_empty()) as usize;
        let span = (trace || sampled()).then(|| {
            tracing::info_span!(
                "seek.query",
                otel.name = "seek.query",
                seek.mode = MODES[mode],
                seek.chars = q.chars().count() as u64,
                seek.results = tracing::field::Empty
            )
        });
        let fut = self.search_in(q, viewer, limit, None);
        let r = match &span {
            Some(s) => tracing::Instrument::instrument(fut, s.clone()).await,
            None => fut.await,
        };
        self.hists.mode[mode].since(t0);
        match &r {
            Ok(v) => {
                self.hists.queries[mode][v.is_empty() as usize].inc();
                self.hists.results[mode].inc_by(v.len() as u64);
                if let Some(s) = &span {
                    s.record("seek.results", v.len() as u64);
                }
            }
            Err((kind, _)) => {
                self.hists.queries[mode][2].inc();
                self.hists.error(kind);
            }
        }
        r
    }

    /// A query on `g`, as one that loaded `g` just before a swap runs: for tests.
    pub async fn search_on(
        &self,
        g: Arc<Gen>,
        q: &str,
        viewer: &str,
        limit: i32,
    ) -> Result<Vec<(String, f64)>, (&'static str, String)> {
        self.search_in(q, viewer, limit, Some(g)).await
    }

    async fn search_in(
        &self,
        q: &str,
        viewer: &str,
        limit: i32,
        on: Option<Arc<Gen>>,
    ) -> Result<Vec<(String, f64)>, (&'static str, String)> {
        if !self.ready.load(Ordering::Relaxed) {
            return Err(("not_ready", self.why.lock().unwrap().clone()));
        }
        let logged_in = !viewer.is_empty();
        if logged_in && self.marco.is_none() {
            return Err(("no_marco", "this seek has no --marco: it can't check blocks for a viewer".into()));
        }
        let k = if limit <= 0 { DEFAULT_LIMIT } else { (limit as usize).min(MAX_LIMIT) };
        let qq = query::prepare(q);
        if qq.folded.is_empty() {
            return Ok(Vec::new());
        }
        let Some(mut g) = on.or_else(|| self.current()) else { return Err(("not_ready", "no artifact".into())) };
        let mut entry = if logged_in {
            let fut = self.viewer_entry(viewer, &g);
            let r = match stage_span("viewer") {
                Some(s) => tracing::Instrument::instrument(fut, s).await,
                None => fut.await,
            };
            match r {
                Ok(e) => Some(e),
                Err(e) => {
                    // the follows can wait; the block filter below still runs
                    if FALLBACKS.get().is_multiple_of(100) {
                        tracing::warn!(error = %e, "viewer's follows unread: answering without follows first");
                    }
                    FALLBACKS.inc();
                    None
                }
            }
        } else {
            None
        };
        // the entry moved on to a newer artifact than this query loaded: the query does too
        let newer = match entry.as_deref() {
            Some(ViewerEntry::Keyed(k)) if k.gen != g.id => Some(k.gen),
            _ => None,
        };
        if let Some(gen) = newer {
            match self.current() {
                Some(c) if c.id == gen => g = c,
                _ => {
                    FALLBACKS.inc();
                    entry = None;
                }
            }
        }
        let v = match entry.as_deref() {
            Some(ViewerEntry::Keyed(k)) => Some(&k.viewer),
            _ => None,
        };
        let want = if logged_in { k + MARGIN } else { k };
        let hits = {
            let s = stage_span("match");
            let _e = s.as_ref().map(|s| s.enter());
            let ov = g.overlay.read();
            let parts = query::parts(&g.index, &ov, v, &qq, want, &self.cfg.params);
            let hits = query::merge(&parts, want);
            let hits = hits.into_iter().filter_map(|h| ov.did(&g.index, h.id).map(|d| (d, h))).collect::<Vec<_>>();
            drop(ov);
            self.hists.stage[1].observe(parts.prefix_ns);
            self.hists.stage[2].observe(parts.personal_ns);
            self.hists.stage[3].observe(parts.global_ns);
            if let Some(s) = &s {
                s.record("seek.prefix_ns", parts.prefix_ns);
                s.record("seek.personal_ns", parts.personal_ns);
                s.record("seek.global_ns", parts.global_ns);
            }
            hits
        };
        let page = if logged_in {
            let m = self.marco.as_ref().expect("checked above");
            let t = Instant::now();
            let dids: Vec<String> = hits.iter().map(|(d, _)| d.clone()).collect();
            let fut = m.unblocked(viewer, dids, &self.hists);
            let keep = match stage_span("filter") {
                Some(s) => tracing::Instrument::instrument(fut, s).await,
                None => fut.await,
            };
            let keep = keep.map_err(|e| ("marco", e.to_string()))?;
            self.hists.stage[4].since(t);
            let keep: rustc_hash::FxHashSet<&str> = keep.iter().map(String::as_str).collect();
            let mut out = Vec::with_capacity(k);
            for (d, h) in hits {
                // an exact handle match stays even when a block would drop it, as the reference AppView keeps it
                match (keep.contains(d.as_str()), h.exact) {
                    (true, _) => out.push((d, h)),
                    (false, true) => {
                        EXACT_KEPT.inc();
                        out.push((d, h))
                    }
                    (false, false) => BLOCK_DROPPED.inc(),
                }
            }
            out.truncate(k);
            out
        } else {
            hits.into_iter().take(k).collect()
        };
        Ok(page.into_iter().map(|(d, h)| (d, h.score as f64)).collect())
    }

    /// The task that owns every change. `art` brings new artifacts, `js` Jetstream's events,
    /// `verified` the verifier's outcomes.
    pub async fn run_ingest(
        self: Arc<Pod>,
        mut art: mpsc::Receiver<(PathBuf, String)>,
        mut js: mpsc::Receiver<Ev>,
        mut verified: mpsc::Receiver<(String, Outcome)>,
        queue: Option<Queue>,
        jetstream_start: tokio::sync::oneshot::Sender<u64>,
    ) {
        let mut ring: VecDeque<(u64, Change)> = VecDeque::new();
        // the ring holds every change after this time: an artifact from before it can't be caught up
        let mut ring_from = 0u64;
        // Jetstream's replay skipped from asked to first: only an artifact at or past it can serve
        let mut gap_until: Option<u64> = None;
        let mut last_event = Instant::now();
        let mut start = Some(jetstream_start);
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                Some((path, version)) = art.recv() => {
                    let t = Instant::now();
                    let next = self.next_gen.fetch_add(1, Ordering::Relaxed);
                    let newer_than = self.current().map(|c| c.index.meta.created_ms);
                    let v2 = version.clone();
                    let opened = tokio::task::spawn_blocking(move || Pod::open_gen(next, path, v2, newer_than)).await;
                    let g = match opened {
                        Ok(Ok(Some(g))) => g,
                        Ok(Ok(None)) => { tracing::warn!(version = %version, "artifact older than the one served: not swapped in"); continue }
                        Ok(Err(e)) => { tracing::error!(error = %format!("{e:#}"), "artifact refused"); continue }
                        Err(e) => { tracing::error!(error = %e, "artifact open panicked"); continue }
                    };
                    let cursor = g.index.meta.cursor_us;
                    if let Some(s) = start.take() {
                        if now_us().saturating_sub(cursor) > self.cfg.replay_window.as_micros() as u64 {
                            tracing::error!(version = %g.version, cursor_us = cursor, "artifact older than Jetstream's replay window: refusing to serve until a newer one");
                            self.stale.store(true, Ordering::Relaxed);
                        }
                        ring_from = cursor;
                        let _ = s.send(cursor);
                    } else {
                        let reason = if cursor < ring_from {
                            Some("uncovered")
                        } else if gap_until.is_some_and(|gu| cursor < gu) {
                            Some("gap")
                        } else {
                            None
                        };
                        if let Some(r) = reason {
                            REFUSED.with_label_values(&[r]).inc();
                            tracing::warn!(version = %g.version, cursor_us = cursor, ring_from, reason = r, "artifact not swapped in");
                            // its unmap and unlink go on a blocking thread, as a swapped-out one's do
                            tokio::task::spawn_blocking(move || drop(g));
                            continue;
                        }
                        // the ring holds what happened since; replay what the artifact doesn't have
                        let mut ov = g.overlay.write();
                        let mut n = 0;
                        for (at, c) in ring.iter() {
                            if *at > cursor {
                                apply(&g.index, &mut ov, c);
                                n += 1;
                            }
                        }
                        ov.cursor_us = ov.cursor_us.max(ring.back().map_or(0, |r| r.0));
                        drop(ov);
                        SWAP_REPLAYED.set(n);
                        gap_until = None;
                        self.stale.store(false, Ordering::Relaxed);
                    }
                    // the viewer cache's entries move onto the new ids: a sweep after the swap, and
                    // any query that gets to one first
                    let (g, rk) = match (self.current(), self.marco.is_some()) {
                        (Some(old), true) => {
                            let (g, rk) = tokio::task::spawn_blocking(move || {
                                let rk = Rekey::new(&old, &g);
                                (g, rk)
                            })
                            .await
                            .expect("rekey panicked");
                            let rk = Arc::new(rk);
                            self.rekey.store(Some(rk.clone()));
                            (g, Some(rk))
                        }
                        _ => (g, None),
                    };
                    self.publish_gen(g);
                    if let Some(rk) = rk {
                        tokio::spawn(self.clone().sweep(rk));
                    }
                    SWAPS.inc();
                    SWAP_SECONDS.with(&[]).since(t);
                    self.set_state();
                }
                Some(ev) = js.recv() => {
                    last_event = Instant::now();
                    let Some(g) = self.current() else { continue };
                    let t = ev.time_us();
                    let change = match ev {
                        Ev::Gap { asked_us, first_us } => {
                            tracing::error!(asked_us, first_us, "Jetstream replay started past the cursor: refusing to serve until a newer artifact");
                            gap_until = Some(first_us);
                            self.stale.store(true, Ordering::Relaxed);
                            self.set_state();
                            None
                        }
                        Ev::Follow { did, subject, .. } => {
                            // the follower's follows changed, and the subject's mutuals
                            let who: Vec<String> = std::iter::once(did).chain(subject).collect();
                            if self.caught_up.load(Ordering::Relaxed) {
                                for d in &who {
                                    self.drop_viewer(d).await;
                                }
                                // marco may apply the follow later than a build that started past
                                // the guard: drop them once more when it surely has
                                let (pod, again) = (self.clone(), self.cfg.marco_lag * 2);
                                tokio::spawn(async move {
                                    tokio::time::sleep(again).await;
                                    for d in &who {
                                        pod.drop_viewer(d).await;
                                    }
                                });
                            } else {
                                for d in &who {
                                    self.viewers.invalidate(d).await;
                                }
                            }
                            None
                        }
                        Ev::Profile { did, name, .. } => Some(Change::Name(did, name)),
                        Ev::Account { did, active, .. } => Some(Change::Hidden(did, !active)),
                        Ev::Identity { did, handle, .. } => {
                            let now = g.overlay.read().handle_key(&g.index, &did);
                            let claimed = handle.as_deref().map(|h| seek_core::keys::cut(&seek_core::fold::fold(h)).as_bytes().to_vec());
                            if now.is_some() && now == claimed {
                                None
                            } else {
                                if let Some(q) = &queue { q.push(&did); }
                                now.is_some().then_some(Change::Handle(did, None))
                            }
                        }
                    };
                    if let Some(c) = change {
                        let mut ov = g.overlay.write();
                        if t > g.index.meta.cursor_us {
                            apply(&g.index, &mut ov, &c);
                        }
                        ov.cursor_us = ov.cursor_us.max(t);
                        drop(ov);
                        ring.push_back((t, c));
                    }
                    let lag = now_us().saturating_sub(t);
                    if !self.caught_up.load(Ordering::Relaxed) && lag < 10_000_000 {
                        self.caught_up.store(true, Ordering::Relaxed);
                        tracing::info!("caught up with Jetstream");
                        self.set_state();
                    }
                    if self.lagging.load(Ordering::Relaxed) && lag < self.cfg.max_lag.as_micros() as u64 / 2 {
                        self.lagging.store(false, Ordering::Relaxed);
                        self.set_state();
                    }
                }
                Some((did, o)) = verified.recv() => {
                    let Some(g) = self.current() else { continue };
                    let c = match o {
                        Outcome::Valid(h) => Change::Handle(did, Some(h)),
                        Outcome::Invalid => Change::Handle(did, None),
                        Outcome::Unknown => continue,
                    };
                    let mut ov = g.overlay.write();
                    let at = ov.cursor_us;
                    apply(&g.index, &mut ov, &c);
                    drop(ov);
                    ring.push_back((at, c));
                }
                _ = tick.tick() => {
                    let hold = (self.cfg.marco_lag + self.cfg.viewer_deadline).as_millis() as u64 + 1000;
                    let now_ms = (now_secs() * 1e3) as u64;
                    self.dropped.lock().retain(|_, t| now_ms < *t + hold);
                    let keep = self.cfg.replay_keep.as_micros() as u64;
                    while ring.front().is_some_and(|(t, _)| now_us().saturating_sub(*t) > keep) {
                        if let Some((t, _)) = ring.pop_front() {
                            ring_from = ring_from.max(t);
                        }
                    }
                    RING.set(ring.len() as i64);
                    let behind = self.current().map(|g| now_us().saturating_sub(g.overlay.read().cursor_us)).unwrap_or(0);
                    let silent = last_event.elapsed() > self.cfg.max_lag;
                    let lagging = self.caught_up.load(Ordering::Relaxed)
                        && (silent || behind > self.cfg.max_lag.as_micros() as u64);
                    if lagging != self.lagging.load(Ordering::Relaxed) {
                        self.lagging.store(lagging, Ordering::Relaxed);
                        self.set_state();
                    }
                }
                else => return,
            }
        }
    }
}

/// Applies one change; a panic (a bug) skips the change instead of taking the pod down.
fn apply(base: &Index, ov: &mut Overlay, c: &Change) {
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match c {
        Change::Name(d, n) => ov.set_name(base, d, n.as_deref()),
        Change::Handle(d, h) => ov.set_handle(base, d, h.as_deref()),
        Change::Hidden(d, h) => ov.set_hidden(base, d, *h),
    }));
    if r.is_err() {
        INGEST_PANICS.inc();
        QUERY_ERRORS_TOTAL.with_label_values(&["internal"]).inc();
        tracing::error!(change = ?c, "a change panicked: skipped");
    }
}

/// Whether this query is traced: marco's trace switch (`/debug/tracing`) sets the share. At 0
/// it's one relaxed load.
#[inline]
fn sampled() -> bool {
    let r = marco_ops::trace_ratio();
    if r <= 0.0 {
        return false;
    }
    if r >= 1.0 {
        return true;
    }
    thread_local! {
        static RNG: std::cell::Cell<u64> = std::cell::Cell::new(((&RNG as *const _ as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)) | 1);
    }
    RNG.with(|c| {
        let mut x = c.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        c.set(x);
        (x as f64) < r * u64::MAX as f64
    })
}

/// A child span of the query's, only when the query is traced. Names match the stage label of
/// seek_query_stage_seconds (match covers prefix, personal and global, as span fields).
fn stage_span(stage: &'static str) -> Option<tracing::Span> {
    let cur = tracing::Span::current();
    if cur.is_disabled() || cur.is_none() {
        return None;
    }
    Some(tracing::info_span!(
        "seek.stage",
        otel.name = stage,
        seek.prefix_ns = tracing::field::Empty,
        seek.personal_ns = tracing::field::Empty,
        seek.global_ns = tracing::field::Empty
    ))
}

/// The API: the dataplane's typeahead method.
pub struct Api(pub Arc<Pod>);

impl seek_api::connect::bsky::Service for Api {
    #[allow(refining_impl_trait)]
    async fn search_actors_typeahead(
        &self,
        ctx: RequestContext,
        req: ServiceRequest<'_, SearchActorsTypeaheadRequest>,
    ) -> ServiceResult<SearchActorsTypeaheadResponse> {
        IN_FLIGHT.inc();
        let _g = Dec;
        let r = req.to_owned_message();
        let p = r.params.as_option().cloned().unwrap_or_default();
        if p.query.len() > 512 || p.viewer.len() > 2048 {
            self.0.hists.error("bad_request");
            return Err(ConnectError::invalid_argument("query or viewer too long"));
        }
        if !p.viewer.is_empty() && !p.viewer.starts_with("did:") {
            self.0.hists.error("bad_request");
            return Err(ConnectError::invalid_argument("viewer must be a DID"));
        }
        let trace = ctx.header(FORCE_TRACE).is_some();
        match self.0.search(&p.query, &p.viewer, p.limit, trace).await {
            Ok(v) => Response::ok(SearchActorsTypeaheadResponse {
                actors: v
                    .into_iter()
                    .map(|(did, score)| SearchActorResult { did, score, ..Default::default() })
                    .collect(),
                ..Default::default()
            }),
            Err(("no_marco", why)) => Err(ConnectError::failed_precondition(why)),
            Err((_, why)) => Err(ConnectError::unavailable(why)),
        }
    }
}

struct Dec;
impl Drop for Dec {
    fn drop(&mut self) {
        IN_FLIGHT.dec();
    }
}

/// `l` with Nagle off on every connection: an RPC's response goes out as several small writes, and
/// Nagle with the client's delayed ack holds each for tens of milliseconds.
pub fn nodelay(l: tokio::net::TcpListener) -> impl axum::serve::Listener<Addr = std::net::SocketAddr> {
    use axum::serve::ListenerExt;
    l.tap_io(|s| {
        let _ = s.set_nodelay(true);
    })
}

/// The API listener's routes: the service, /healthz and /readyz. While the pod drains, an HTTP/1
/// response says `Connection: close` ([`marco_ops::drain::close_http1`]).
pub fn api_router(pod: &Arc<Pod>) -> axum::Router {
    use seek_api::connect::bsky::ServiceExt;
    let svc = Arc::new(Api(pod.clone())).register(connectrpc::Router::new()).into_axum_service();
    let p = pod.clone();
    axum::Router::new()
        .route_service("/bsky.Service/{*method}", svc)
        .route("/healthz", axum::routing::get(|| async { "ok\n" }))
        .route(
            "/readyz",
            axum::routing::get(move || {
                let p = p.clone();
                async move {
                    match p.readiness() {
                        (true, _) => (http::StatusCode::OK, "ready\n".to_string()),
                        (false, why) => (http::StatusCode::SERVICE_UNAVAILABLE, format!("{why}\n")),
                    }
                }
            }),
        )
        .layer(axum::middleware::from_fn_with_state(pod.drain.clone(), marco_ops::drain::close_http1))
}
