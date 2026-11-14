//! The pod: `bsky.Service/SearchActorsTypeahead` over the newest artifact and
//! its live overlay. One task owns every change: Jetstream's events, handle
//! verifications and artifact swaps go through it in order, so a swap replays
//! the recent changes into the new overlay and nothing is lost between them.
//! Queries read the current generation through an `ArcSwap`: a swap never
//! pauses them, and the old generation's map goes when its last query ends.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::Result;
use arc_swap::ArcSwapOption;
use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use seek_api::bsky::{SearchActorResult, SearchActorsTypeaheadRequest, SearchActorsTypeaheadResponse};
use seek_core::did::{key_of, DidKey};
use seek_core::index::Index;
use seek_core::overlay::Overlay;
use seek_core::query::{self, Params, Viewer};
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

struct ViewerEntry {
    follows: Vec<DidKey>,
    mutuals: Vec<DidKey>,
    keys: Mutex<Option<(u64, Arc<Viewer>)>>,
}

impl ViewerEntry {
    fn weight(&self) -> u32 {
        (self.follows.len() * 40 + self.mutuals.len() * 24 + 64).min(u32::MAX as usize) as u32
    }

    /// The viewer's keys against `g`, folded once per generation.
    fn viewer(&self, g: &Gen) -> Arc<Viewer> {
        if let Some((id, v)) = &*self.keys.lock().unwrap() {
            if *id == g.id {
                return v.clone();
            }
        }
        let t = Instant::now();
        let ov = g.overlay.read().unwrap();
        let ids = |ks: &[DidKey]| -> Vec<u32> { ks.iter().filter_map(|k| ov.id_of(&g.index, k)).collect() };
        let v = Arc::new(Viewer::build(&g.index, &ov, &ids(&self.follows), &ids(&self.mutuals)));
        drop(ov);
        VIEWER_KEYS.with(&[]).since(t);
        *self.keys.lock().unwrap() = Some((g.id, v.clone()));
        v
    }
}

pub struct Config {
    pub params: Params,
    /// Jetstream's replay window: an artifact whose cursor is older can't be caught up.
    pub replay_window: Duration,
    pub viewer_cache_bytes: u64,
    pub viewer_idle: Duration,
    /// Keep this long of changes for replay at a swap: longer than the builder's interval.
    pub replay_keep: Duration,
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
    next_gen: AtomicU64,
}

impl Pod {
    pub fn new(cfg: Config, marco: Option<Marco>) -> Arc<Pod> {
        let viewers = moka::future::Cache::builder()
            .max_capacity(cfg.viewer_cache_bytes)
            .weigher(|_: &String, v: &Arc<ViewerEntry>| v.weight())
            .time_to_idle(cfg.viewer_idle)
            .eviction_listener(|_, _, cause| {
                let c = match cause {
                    moka::notification::RemovalCause::Expired => "idle",
                    moka::notification::RemovalCause::Size => "size",
                    _ => "invalidated",
                };
                VIEWER_EVICTIONS.with_label_values(&[c]).inc();
            })
            .build();
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
            next_gen: AtomicU64::new(1),
        })
    }

    pub fn current(&self) -> Option<Arc<Gen>> {
        self.cur.load_full()
    }

    /// (ready, why not).
    pub fn readiness(&self) -> (bool, String) {
        (self.ready.load(Ordering::Relaxed), self.why.lock().unwrap().clone())
    }

    fn set_state(&self) {
        let why = if self.cur.load().is_none() {
            "no artifact yet".to_string()
        } else if self.stale.load(Ordering::Relaxed) {
            "refusing to serve: the artifact's cursor is older than Jetstream's replay window; waiting for a newer artifact".into()
        } else if !self.caught_up.load(Ordering::Relaxed) {
            "replaying Jetstream from the artifact's cursor".into()
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
            let st = g.overlay.read().unwrap().stats();
            OVERLAY_ACTORS.set(st.actors as i64);
            OVERLAY_KEYS.set(st.keys as i64);
            OVERLAY_BYTES.set(st.key_bytes as i64);
        }
        VIEWER_ENTRIES.set(self.viewers.entry_count() as i64);
        VIEWER_BYTES.set(self.viewers.weighted_size() as i64);
    }

    fn open_gen(&self, path: PathBuf, version: String) -> Result<Gen> {
        let index = match Index::open(&path, true) {
            Ok(i) => i,
            Err(e) => {
                VERIFY_FAILURES.inc();
                let _ = std::fs::remove_file(&path);
                return Err(e);
            }
        };
        index.artifact().prefetch();
        let bytes = index.artifact().len() as u64;
        MAPPED.add(bytes as i64);
        let overlay = RwLock::new(Overlay::new(&index));
        Ok(Gen { id: self.next_gen.fetch_add(1, Ordering::Relaxed), version, index, overlay, path, bytes })
    }

    fn publish_gen(&self, g: Gen) {
        let m = &g.index.meta;
        ARTIFACT_INFO.reset();
        ARTIFACT_INFO.with_label_values(&[&g.version]).set(1);
        ARTIFACT_BUILT.set(m.created_ms as f64 / 1e3);
        ARTIFACT_CURSOR.set(m.cursor_us as f64 / 1e6);
        ARTIFACT_BUILD_SECS.set(m.build_secs);
        ARTIFACT_ACTORS.set(m.actors as i64);
        tracing::info!(version = %g.version, actors = m.actors, cursor_us = m.cursor_us, "serving artifact");
        self.cur.store(Some(Arc::new(g)));
    }

    async fn viewer_entry(&self, viewer: &str) -> Option<Arc<ViewerEntry>> {
        let marco = self.marco.as_ref()?;
        let t = Instant::now();
        let mut miss = false;
        let r = self
            .viewers
            .try_get_with(viewer.to_string(), async {
                miss = true;
                let tb = Instant::now();
                let (f, m) = tokio::join!(marco.follows(viewer, &self.hists), marco.mutuals(viewer, &self.hists));
                let (f, m) = (f?, m?);
                VIEWER_BUILD.with(&[]).since(tb);
                VIEWER_FOLLOWS.with(&[]).observe(f.len() as u64);
                Ok::<_, anyhow::Error>(Arc::new(ViewerEntry {
                    follows: f.iter().map(|d| key_of(d)).collect(),
                    mutuals: m.iter().map(|d| key_of(d)).collect(),
                    keys: Mutex::new(None),
                }))
            })
            .await;
        if miss {
            VIEWER_MISSES.inc()
        } else {
            VIEWER_HITS.inc()
        }
        self.hists.stage[0].since(t);
        match r {
            Ok(e) => Some(e),
            Err(e) => {
                tracing::warn!(error = %e, "viewer's follows unread: answering logged out");
                FALLBACKS.inc();
                None
            }
        }
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
        let logged_in = !viewer.is_empty() && self.marco.is_some();
        let mode = logged_in as usize;
        let span = (trace || sampled()).then(|| {
            tracing::info_span!(
                "seek.query",
                otel.name = "seek.query",
                seek.mode = MODES[mode],
                seek.chars = q.chars().count() as u64,
                seek.results = tracing::field::Empty
            )
        });
        let fut = self.search_in(q, viewer, limit, logged_in);
        let r = match &span {
            Some(s) => tracing::Instrument::instrument(fut, s.clone()).await,
            None => fut.await,
        };
        self.hists.mode[mode].since(t0);
        match &r {
            Ok(v) => {
                QUERIES.with_label_values(&[MODES[mode], if v.is_empty() { "empty" } else { "ok" }]).inc();
                RESULTS.with_label_values(&[MODES[mode]]).inc_by(v.len() as u64);
                if let Some(s) = &span {
                    s.record("seek.results", v.len() as u64);
                }
            }
            Err((kind, _)) => {
                QUERIES.with_label_values(&[MODES[mode], "error"]).inc();
                QUERY_ERRORS_TOTAL.with_label_values(&[kind]).inc();
            }
        }
        r
    }

    async fn search_in(
        &self,
        q: &str,
        viewer: &str,
        limit: i32,
        logged_in: bool,
    ) -> Result<Vec<(String, f64)>, (&'static str, String)> {
        if !self.ready.load(Ordering::Relaxed) {
            return Err(("not_ready", self.why.lock().unwrap().clone()));
        }
        let k = if limit <= 0 { DEFAULT_LIMIT } else { (limit as usize).min(MAX_LIMIT) };
        let fq = query::prepare(q);
        if fq.is_empty() {
            return Ok(Vec::new());
        }
        let entry = if logged_in {
            match stage_span("viewer") {
                Some(s) => tracing::Instrument::instrument(self.viewer_entry(viewer), s).await,
                None => self.viewer_entry(viewer).await,
            }
        } else {
            None
        };
        let Some(g) = self.current() else { return Err(("not_ready", "no artifact".into())) };
        let v = entry.as_ref().map(|e| e.viewer(&g));
        let hits = {
            let t = Instant::now();
            let _s = stage_span("match").map(|s| s.entered());
            let ov = g.overlay.read().unwrap();
            let want = if v.is_some() { k + MARGIN } else { k };
            let parts = query::parts(&g.index, &ov, v.as_deref(), fq.as_bytes(), want, &self.cfg.params);
            self.hists.stage[1].since(t);
            let hits = query::merge(&parts, want);
            hits.into_iter().filter_map(|h| ov.did(&g.index, h.id).map(|d| (d, h))).collect::<Vec<_>>()
        };
        let page = match (&self.marco, entry.is_some()) {
            (Some(m), true) => {
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
                // an exact handle match stays even when a block would drop it, as the reference AppView keeps it
                hits.into_iter().filter(|(d, h)| h.exact || keep.contains(d.as_str())).take(k).collect::<Vec<_>>()
            }
            _ => hits.into_iter().take(k).collect(),
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
        let mut start = Some(jetstream_start);
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                Some((path, version)) = art.recv() => {
                    let t = Instant::now();
                    match self.open_gen(path, version.clone()) {
                        Ok(g) => {
                            let cursor = g.index.meta.cursor_us;
                            let too_old = now_us().saturating_sub(cursor) > self.cfg.replay_window.as_micros() as u64;
                            if let Some(s) = start.take() {
                                if too_old {
                                    tracing::error!(version = %version, cursor_us = cursor, "artifact older than Jetstream's replay window: refusing to serve until a newer one");
                                    self.stale.store(true, Ordering::Relaxed);
                                }
                                let _ = s.send(cursor);
                            } else {
                                // the ring holds what happened since; replay what the artifact doesn't have
                                let mut ov = g.overlay.write().unwrap();
                                for (at, c) in ring.iter() {
                                    if *at > cursor {
                                        apply(&g.index, &mut ov, c);
                                    }
                                }
                                ov.cursor_us = ov.cursor_us.max(ring.back().map_or(0, |r| r.0));
                                drop(ov);
                                self.stale.store(false, Ordering::Relaxed);
                            }
                            self.publish_gen(g);
                            SWAPS.inc();
                            SWAP_SECONDS.with(&[]).since(t);
                            self.set_state();
                        }
                        Err(e) => tracing::error!(error = %e, version = %version, "artifact refused"),
                    }
                }
                Some(ev) = js.recv() => {
                    let Some(g) = self.current() else { continue };
                    let t = ev.time_us();
                    let change = match ev {
                        Ev::Gap { asked_us, first_us } => {
                            tracing::error!(asked_us, first_us, "Jetstream replay started past the cursor: refusing to serve until a newer artifact");
                            self.stale.store(true, Ordering::Relaxed);
                            self.set_state();
                            None
                        }
                        Ev::Follow { did, .. } => {
                            self.viewers.invalidate(&did).await;
                            None
                        }
                        Ev::Profile { did, name, .. } => Some(Change::Name(did, name)),
                        Ev::Account { did, active, .. } => Some(Change::Hidden(did, !active)),
                        Ev::Identity { did, handle, .. } => {
                            let now = g.overlay.read().unwrap().handle_key(&g.index, &did);
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
                        let mut ov = g.overlay.write().unwrap();
                        if t > g.index.meta.cursor_us {
                            apply(&g.index, &mut ov, &c);
                        }
                        ov.cursor_us = ov.cursor_us.max(t);
                        drop(ov);
                        ring.push_back((t, c));
                    }
                    if !self.caught_up.load(Ordering::Relaxed) && now_us().saturating_sub(t) < 10_000_000 {
                        self.caught_up.store(true, Ordering::Relaxed);
                        tracing::info!("caught up with Jetstream");
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
                    let at = g.overlay.read().unwrap().cursor_us;
                    apply(&g.index, &mut g.overlay.write().unwrap(), &c);
                    ring.push_back((at, c));
                }
                _ = tick.tick() => {
                    let keep = self.cfg.replay_keep.as_micros() as u64;
                    while ring.front().is_some_and(|(t, _)| now_us().saturating_sub(*t) > keep) {
                        ring.pop_front();
                    }
                }
                else => return,
            }
        }
    }
}

fn apply(base: &Index, ov: &mut Overlay, c: &Change) {
    match c {
        Change::Name(d, n) => ov.set_name(base, d, n.as_deref()),
        Change::Handle(d, h) => ov.set_handle(base, d, h.as_deref()),
        Change::Hidden(d, h) => ov.set_hidden(base, d, *h),
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

/// A child span of the query's, only when the query is traced.
fn stage_span(stage: &'static str) -> Option<tracing::Span> {
    let cur = tracing::Span::current();
    if cur.is_disabled() || cur.is_none() {
        return None;
    }
    Some(tracing::info_span!("seek.stage", otel.name = stage))
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
            QUERY_ERRORS_TOTAL.with_label_values(&["bad_request"]).inc();
            return Err(ConnectError::invalid_argument("query or viewer too long"));
        }
        if !p.viewer.is_empty() && !p.viewer.starts_with("did:") {
            QUERY_ERRORS_TOTAL.with_label_values(&["bad_request"]).inc();
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
            Err(("not_ready", why)) => Err(ConnectError::unavailable(why)),
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

/// The API listener's routes: the service, /healthz and /readyz.
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
}
