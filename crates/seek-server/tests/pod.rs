//! A pod end to end against a real marco server (in-process, on a small
//! graph) and a builder-made artifact, over Connect and gRPC: logged out by
//! rank, logged in with follows first, blocks dropped both ways (spaced
//! queries' matches too), an exact handle kept through a block, the overlay's
//! changes, a swap, and the drain on the way out. And seek's marco client
//! through a Service-like proxy, its marco going away under it.

use std::sync::Arc;
use std::time::Duration;

use connectrpc::client::{ClientConfig, HttpClient};
use connectrpc::Protocol;
use seek_api::bsky::{SearchActorsTypeaheadRequest, SearchTypeaheadParams};
use seek_api::connect::bsky::ServiceClient;
use seek_server::builder::{self, IndexSet, State};
use seek_server::jetstream::Ev;
use seek_server::metrics::now_us;
use seek_server::serve::{nodelay, Config, Pod};
use tokio::sync::mpsc;

fn did(i: u32) -> String {
    marco_server::synth::did(i)
}

const V: u32 = 0;
const A: u32 = 1; // V follows A, A follows V: a mutual, low rank
const B: u32 = 2; // a stranger, high rank
const C: u32 = 3; // blocks V
const D: u32 = 4; // V blocks D; D's handle is typed out in full below
const E: u32 = 5; // V follows E, not mutual
const F: u32 = 6; // V blocks F, whose handle folds like D's
const G: u32 = 7; // V follows G, which isn't in the first artifact (only the overlay, once named)
const H: u32 = 8; // V follows H, which nothing names until the second artifact
const I: u32 = 9; // V blocks I, found by its handle typed with a space or its name's words reversed

fn tid(k: u64) -> String {
    vlatproto::tid::Tid::from_parts(1_704_067_200_000_000 + k * 1_000_000, 0).to_string()
}

async fn marco() -> (std::net::SocketAddr, tempfile::TempDir) {
    let (_, api, dir) = marco_app(true).await;
    (api, dir)
}

/// A marco on the small graph. Without `block_d`, V follows D instead of blocking them, so a
/// filter's answer shows which marco gave it.
async fn marco_app(block_d: bool) -> (Arc<marco_server::app::App>, std::net::SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut g = marco_core::Graph::new(marco_core::Relations::bsky_default());
    let mut k = 0;
    let mut rec = |g: &mut marco_core::Graph, from: u32, coll: &str, to: u32| {
        k += 1;
        g.add(&format!("at://{}/{coll}/{}", did(from), tid(k)), &did(to)).unwrap();
    };
    rec(&mut g, V, "app.bsky.graph.follow", A);
    rec(&mut g, A, "app.bsky.graph.follow", V);
    rec(&mut g, V, "app.bsky.graph.follow", E);
    rec(&mut g, V, "app.bsky.graph.follow", G);
    rec(&mut g, V, "app.bsky.graph.follow", H);
    rec(&mut g, C, "app.bsky.graph.block", V);
    rec(&mut g, V, if block_d { "app.bsky.graph.block" } else { "app.bsky.graph.follow" }, D);
    rec(&mut g, V, "app.bsky.graph.block", F);
    rec(&mut g, V, "app.bsky.graph.block", I);
    rec(&mut g, B, "app.bsky.graph.follow", C);
    let snap = dir.path().join("snap");
    g.write_snapshot(&snap).unwrap();
    let app = marco_server::app::App::open(marco_server::app::Config::new(snap)).unwrap();
    let ui = Arc::new(marco_server::ui::Ui::load(None).unwrap());
    let (api, _admin) = marco_server::serve(app.clone(), ui).await.unwrap();
    (app, api, dir)
}

fn state(cursor_us: u64) -> State {
    state_with(cursor_us, &[])
}

fn state_with(cursor_us: u64, more: &[(u32, &str, &str)]) -> State {
    let mut s = State::default();
    let base = [
        (V, "viewer.bsky.social", "Viewer"),
        (A, "jo-a.bsky.social", "Jo Mutual"),
        (B, "jo-b.bsky.social", "Jo Stranger"),
        (C, "jo-c.bsky.social", "Jo Blocker"),
        (D, "jo.example.com", "Jo Blocked"),
        (E, "jo-e.bsky.social", "Jo Followed"),
        (F, "jo-example.com", "Lookalike"),
        (I, "kimrook.bsky.social", "Sam Ostrander"),
    ];
    for &(i, h, n) in base.iter().chain(more) {
        let a = s.get_mut(&did(i));
        a.handle = Some(h.into());
        a.verified_us = 1;
        a.name = Some(n.into());
        a.profile = true;
    }
    s.cursor_us = cursor_us;
    s.merge_added();
    s
}

fn ranks(dir: &std::path::Path) -> seek_core::ranks::RanksFile {
    let p = dir.join("r.ranks");
    let mut rows: Vec<seek_core::ranks::Row> =
        [(V, 1), (A, 10), (B, 5000), (C, 6000), (D, 100), (E, 20), (F, 7000), (I, 50)]
            .into_iter()
            .map(|(i, r)| seek_core::ranks::Row {
                did: seek_core::did::key_of(&did(i)),
                rank16: r,
                followers: 0,
                hidden: false,
            })
            .collect();
    seek_core::ranks::write(&p, &Default::default(), &mut rows).unwrap();
    seek_core::ranks::RanksFile::open(&p).unwrap()
}

struct Pods {
    addr: std::net::SocketAddr,
    pod: Arc<Pod>,
    art: mpsc::Sender<(std::path::PathBuf, String)>,
    js: mpsc::Sender<Ev>,
}

async fn pod(marco_addr: Option<std::net::SocketAddr>, art_dir: &std::path::Path, viewer_deadline: Duration) -> Pods {
    pod_with(marco_addr, art_dir, viewer_deadline, Duration::from_secs(300), 10_000).await
}

async fn pod_with(
    marco_addr: Option<std::net::SocketAddr>,
    art_dir: &std::path::Path,
    viewer_deadline: Duration,
    max_lag: Duration,
    max_follows: usize,
) -> Pods {
    seek_server::metrics::init();
    std::fs::create_dir_all(art_dir).unwrap();
    let m = marco_addr.map(|a| {
        seek_server::marco::Marco::new(&format!("http://{a}"), 2, Duration::from_secs(5), max_follows).unwrap()
    });
    let cfg = Config {
        params: Default::default(),
        replay_window: Duration::from_secs(3600),
        viewer_cache_bytes: 1 << 20,
        viewer_idle: Duration::from_secs(60),
        replay_keep: Duration::from_secs(3600),
        max_lag,
        viewer_deadline,
        marco_lag: Duration::from_millis(500),
    };
    let pod = Pod::new(cfg, m);
    let (art, art_rx) = mpsc::channel(1);
    let (js, js_rx) = mpsc::channel(100);
    let (_vtx, vrx) = mpsc::channel(10);
    let (start, start_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(pod.clone().run_ingest(art_rx, js_rx, vrx, None, start));
    let mut st = state(now_us());
    let rf = ranks(art_dir);
    let out = builder::build(&mut st, Some(&rf), IndexSet::Profiles, art_dir).unwrap();
    art.send((out.path.clone(), out.meta.version.clone())).await.unwrap();
    let cursor = start_rx.await.unwrap();
    assert_eq!(cursor, out.meta.cursor_us);
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let r = seek_server::serve::api_router(&pod);
    pod.drain.spawn(axum::serve(nodelay(l), r).with_graceful_shutdown(pod.drain.stopped()));
    Pods { addr, pod, art, js }
}

async fn ask(addr: std::net::SocketAddr, proto: Protocol, q: &str, viewer: u32) -> Vec<String> {
    try_ask(addr, proto, q, viewer, 10).await.unwrap()
}

async fn try_ask(
    addr: std::net::SocketAddr,
    proto: Protocol,
    q: &str,
    viewer: u32,
    limit: i32,
) -> Result<Vec<String>, connectrpc::ConnectError> {
    let cfg = ClientConfig::new(format!("http://{addr}").parse().unwrap()).with_protocol(proto);
    let http = if proto == Protocol::Grpc { HttpClient::plaintext_http2_only() } else { HttpClient::plaintext() };
    let c = ServiceClient::new(http, cfg);
    let viewer = if viewer == u32::MAX { String::new() } else { did(viewer) };
    let r = c
        .search_actors_typeahead(SearchActorsTypeaheadRequest {
            params: SearchTypeaheadParams { query: q.into(), viewer, limit, ..Default::default() }.into(),
            ..Default::default()
        })
        .await?
        .into_owned();
    Ok(r.actors.into_iter().map(|a| a.did).collect())
}

async fn ready(p: &Pods) {
    p.js.send(Ev::Profile { did: did(V), time_us: now_us(), name: Some("Viewer".into()), deleted: false })
        .await
        .unwrap();
    for _ in 0..100 {
        if p.pod.readiness().0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{}", p.pod.readiness().1);
}

const OUT: u32 = u32::MAX;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pod_against_marco() {
    let (maddr, _mdir) = marco().await;
    let dir = tempfile::tempdir().unwrap();
    let p = pod(Some(maddr), dir.path(), Duration::from_secs(5)).await;
    // not caught up with Jetstream yet: not ready
    assert!(!p.pod.readiness().0);
    ready(&p).await;

    for proto in [Protocol::Connect, Protocol::Grpc] {
        // logged out: by rank
        assert_eq!(
            ask(p.addr, proto, "jo", OUT).await,
            vec![did(F), did(C), did(B), did(D), did(E), did(A)],
            "{proto:?}"
        );
        // logged in: follows first (the mutual ahead), strangers by rank, the blocker and the blocked gone
        assert_eq!(ask(p.addr, proto, "jo", V).await, vec![did(A), did(E), did(B)], "{proto:?}");
        // the blocked account's exact handle stays; one that only folds the same doesn't
        assert_eq!(ask(p.addr, proto, "jo.example.com", V).await, vec![did(D)]);
        assert_eq!(ask(p.addr, proto, "jo-example.com", V).await, vec![did(F)]);
        // a spaced query's matches go through the block filter too
        for q in ["kim ro", "ostrander sam"] {
            assert_eq!(ask(p.addr, proto, q, OUT).await, vec![did(I)], "{proto:?} {q}");
            assert!(ask(p.addr, proto, q, V).await.is_empty(), "{proto:?} {q}");
        }
        // the API's largest page on a one-letter query answers from the top list
        assert_eq!(try_ask(p.addr, proto, "j", OUT, 100).await.unwrap().len(), 6);
        assert!(ask(p.addr, proto, "", V).await.is_empty());
    }

    // the overlay: B renamed, C taken down
    let t = now_us();
    p.js.send(Ev::Profile { did: did(B), time_us: t, name: Some("Zed".into()), deleted: false }).await.unwrap();
    p.js.send(Ev::Account { did: did(C), time_us: t + 1, active: false }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(ask(p.addr, Protocol::Connect, "zed", OUT).await, vec![did(B)]);
    assert_eq!(ask(p.addr, Protocol::Connect, "jo", OUT).await, vec![did(F), did(B), did(D), did(E), did(A)]);

    // a newer artifact from before those changes: the swap replays them into its overlay
    let old = p.pod.current().unwrap();
    let mut st = state(t - 1);
    std::fs::create_dir_all(dir.path().join("o")).unwrap();
    let older = builder::build(&mut st, Some(&ranks(dir.path())), IndexSet::Profiles, &dir.path().join("o")).unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let out = builder::build(&mut st, Some(&ranks(dir.path())), IndexSet::Profiles, dir.path()).unwrap();
    p.art.send((out.path.clone(), out.meta.version.clone())).await.unwrap();
    for _ in 0..100 {
        if p.pod.current().unwrap().version == out.meta.version {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(p.pod.current().unwrap().version, out.meta.version);
    assert_eq!(ask(p.addr, Protocol::Connect, "zed", OUT).await, vec![did(B)]);
    assert!(!ask(p.addr, Protocol::Connect, "jo", OUT).await.contains(&did(C)));
    drop(old);
    // an artifact built before the one served is refused
    p.art.send((older.path.clone(), older.meta.version.clone())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(p.pod.current().unwrap().version, out.meta.version);
    assert_eq!(seek_server::metrics::REFUSED.with_label_values(&["older"]).get(), 1);

    // marco slow to give the follows: the query still runs the block filter
    let slow = pod(Some(maddr), &dir.path().join("slow"), Duration::from_millis(0)).await;
    ready(&slow).await;
    let got = ask(slow.addr, Protocol::Connect, "jo", V).await;
    assert!(!got.contains(&did(C)) && !got.contains(&did(D)) && !got.contains(&did(F)), "{got:?}");
    assert!(seek_server::metrics::FALLBACKS.get() >= 1);
    // no marco: a query naming a viewer is refused, one without is answered
    let none = pod(None, &dir.path().join("none"), Duration::from_secs(1)).await;
    ready(&none).await;
    let e = try_ask(none.addr, Protocol::Connect, "jo", V, 10).await.unwrap_err();
    assert_eq!(e.code, connectrpc::ErrorCode::FailedPrecondition);
    assert_eq!(ask(none.addr, Protocol::Connect, "jo", OUT).await.len(), 6);

    let text = marco_ops::metrics_text();
    for m in [
        "seek_queries_total{mode=\"logged_in\",result=\"ok\"}",
        "seek_marco_seconds_count{method=\"filter\"}",
        "seek_artifact_refused_total{reason=\"older\"} 1",
        "seek_query_exact_kept_total",
        "seek_query_blocked_total",
    ] {
        assert!(text.contains(m), "{m} missing");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_answers_then_refuses() {
    let (maddr, _mdir) = marco().await;
    let dir = tempfile::tempdir().unwrap();
    let p = pod(Some(maddr), dir.path(), Duration::from_secs(5)).await;
    ready(&p).await;
    p.pod.drain.lame_duck();
    let r = reqwest::get(format!("http://{}/readyz", p.addr)).await.unwrap();
    assert_eq!(r.status(), 503);
    assert_eq!(r.text().await.unwrap(), "shutting down\n");
    // not ready, but queries are still answered, logged in too
    assert_eq!(ask(p.addr, Protocol::Grpc, "jo", V).await, vec![did(A), did(E), did(B)]);
    assert!(p.pod.drain.stop(Duration::from_secs(5)).await);
    let e = std::net::TcpStream::connect(p.addr).unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::ConnectionRefused);
}

async fn send(p: &Pods, dir: &std::path::Path, cursor: u64) -> String {
    tokio::time::sleep(Duration::from_millis(3)).await;
    let mut st = state(cursor);
    let out = builder::build(&mut st, Some(&ranks(dir)), IndexSet::Profiles, dir).unwrap();
    p.art.send((out.path.clone(), out.meta.version.clone())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    out.meta.version
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn swap_guards_and_lag() {
    let (maddr, _mdir) = marco().await;
    let dir = tempfile::tempdir().unwrap();
    let p = pod_with(Some(maddr), dir.path(), Duration::from_secs(5), Duration::from_secs(1), 10_000).await;
    ready(&p).await;
    let first = p.pod.current().unwrap().version.clone();
    let start = p.pod.current().unwrap().index.meta.cursor_us;
    let refused = |r: &str| seek_server::metrics::REFUSED.with_label_values(&[r]).get();

    // newer, but from before what the pod has replayed since it started: refused
    let u0 = refused("uncovered");
    send(&p, dir.path(), start - 1_000_000).await;
    assert_eq!(p.pod.current().unwrap().version, first);
    assert_eq!(refused("uncovered"), u0 + 1);

    // a replay gap: not serving, and only an artifact past the gap is taken
    let gap_at = now_us();
    p.js.send(Ev::Gap { asked_us: start, first_us: gap_at }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!p.pod.readiness().0);
    let g0 = refused("gap");
    send(&p, dir.path(), gap_at - 1).await;
    assert_eq!(refused("gap"), g0 + 1);
    assert_eq!(p.pod.current().unwrap().version, first);
    let v = send(&p, dir.path(), gap_at + 1).await;
    assert_eq!(p.pod.current().unwrap().version, v);
    p.js.send(Ev::Profile { did: did(V), time_us: now_us(), name: Some("Viewer".into()), deleted: false })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(p.pod.readiness().0, "{}", p.pod.readiness().1);

    // Jetstream silent past --max-lag-seconds: not ready, saying why; an event brings it back
    tokio::time::sleep(Duration::from_secs(6)).await;
    let (ok, why) = p.pod.readiness();
    assert!(!ok && why.contains("behind or silent"), "{why}");
    p.js.send(Ev::Profile { did: did(V), time_us: now_us(), name: Some("Viewer".into()), deleted: false })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(p.pod.readiness().0, "{}", p.pod.readiness().1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viewer_entries_overlay_swap_and_cap() {
    use seek_server::serve::CachedViewer;
    let (maddr, _mdir) = marco().await;
    let dir = tempfile::tempdir().unwrap();
    let p = pod(Some(maddr), dir.path(), Duration::from_secs(5)).await;
    ready(&p).await;

    // G exists only in the overlay: still a follow, ahead of every stranger
    p.js.send(Ev::Profile { did: did(G), time_us: now_us(), name: Some("Jo New".into()), deleted: false })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let g1 = p.pod.current().unwrap();
    assert!(g1.overlay.read().id_of(&g1.index, &seek_core::did::key_of(&did(G))).unwrap() >= g1.index.len());
    assert_eq!(ask(p.addr, Protocol::Connect, "jo", V).await, vec![did(A), did(E), did(G), did(B)]);
    let Some(CachedViewer::Keyed { gen, follows: 3, mutuals: 1, unresolved: 1, read_ms, weight }) =
        p.pod.cached_viewer(&did(V)).await
    else {
        panic!("{:?}", p.pod.cached_viewer(&did(V)).await)
    };
    assert_eq!(gen, g1.id);
    assert!(weight > 320 && weight < 1024, "{weight}");

    // a swap to an artifact whose ids moved (twenty more actors) and that names H: the sweep after
    // the swap moves the entry onto the new ids without asking marco again
    let more: Vec<(u32, String)> = (100..120).map(|i| (i, format!("x{i}.bsky.social"))).collect();
    let mut extra: Vec<(u32, &str, &str)> = more.iter().map(|(i, h)| (*i, h.as_str(), "")).collect();
    extra.push((G, "jo-g.bsky.social", "Jo New"));
    extra.push((H, "jo-h.bsky.social", "Jo Late"));
    tokio::time::sleep(Duration::from_millis(3)).await;
    let mut st = state_with(now_us(), &extra);
    let out = builder::build(&mut st, Some(&ranks(dir.path())), IndexSet::Profiles, dir.path()).unwrap();
    p.art.send((out.path.clone(), out.meta.version.clone())).await.unwrap();
    for _ in 0..100 {
        if p.pod.current().unwrap().version == out.meta.version {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let g2 = p.pod.current().unwrap();
    assert_eq!(g2.version, out.meta.version);
    assert!(
        [A, E].iter().any(|&x| g1.index.id_of(&did(x)) != g2.index.id_of(&did(x))),
        "the new artifact's ids should differ"
    );
    drop(g1);
    for _ in 0..100 {
        if matches!(p.pod.cached_viewer(&did(V)).await, Some(CachedViewer::Keyed { gen, .. }) if gen == g2.id) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let got = ask(p.addr, Protocol::Connect, "jo", V).await;
    assert_eq!(got[..2], [did(A), did(E)]);
    let mut mid = got[2..4].to_vec();
    mid.sort();
    let mut want = vec![did(G), did(H)];
    want.sort();
    assert_eq!(mid, want, "{got:?}");
    assert_eq!(got[4], did(B));
    assert_eq!(
        p.pod.cached_viewer(&did(V)).await,
        Some(CachedViewer::Keyed {
            gen: g2.id,
            follows: 4,
            mutuals: 1,
            unresolved: 0,
            read_ms,
            weight: match p.pod.cached_viewer(&did(V)).await {
                Some(CachedViewer::Keyed { weight, .. }) => weight,
                _ => 0,
            },
        })
    );
    assert!(seek_server::metrics::VIEWER_REKEYS.get() >= 1);

    // past --max-follows: no follows first, blocks still filtered, and the verdict is cached
    let capped =
        pod_with(Some(maddr), &dir.path().join("capped"), Duration::from_secs(5), Duration::from_secs(300), 3).await;
    ready(&capped).await;
    for _ in 0..2 {
        assert_eq!(ask(capped.addr, Protocol::Connect, "jo", V).await, vec![did(B), did(E), did(A)]);
        assert_eq!(capped.pod.cached_viewer(&did(V)).await, Some(CachedViewer::Capped));
    }
    assert!(seek_server::metrics::VIEWER_CAPPED.get() >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viewer_drops_old_queries_and_double_swaps() {
    use seek_server::serve::CachedViewer;
    use std::time::Instant;
    let (maddr, _mdir) = marco().await;
    let dir = tempfile::tempdir().unwrap();
    let p = pod(Some(maddr), dir.path(), Duration::from_secs(5)).await;
    ready(&p).await;
    let cached = |v: u32| {
        let pod = p.pod.clone();
        async move { pod.cached_viewer(&did(v)).await }
    };
    let follow = |from: u32, to: Option<u32>| Ev::Follow { did: did(from), time_us: now_us(), subject: to.map(did) };
    let want = vec![did(A), did(E), did(B)];

    // a follow event while E's first entry is being built, or just before: whichever comes first,
    // the cache doesn't keep a build that may have read marco without the follow (marco_lag 500 ms)
    assert_eq!(cached(E).await, None);
    let t = Instant::now();
    let (got, sent) = tokio::join!(ask(p.addr, Protocol::Connect, "jo", E), p.js.send(follow(E, None)));
    sent.unwrap();
    assert!(!got.is_empty());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(cached(E).await, None);
    // past the margin a build is kept, until the second drop at twice it
    tokio::time::sleep(Duration::from_millis(700).saturating_sub(t.elapsed())).await;
    ask(p.addr, Protocol::Connect, "jo", E).await;
    assert!(cached(E).await.is_some(), "kept past the margin");
    tokio::time::sleep(Duration::from_millis(1250).saturating_sub(t.elapsed())).await;
    assert_eq!(cached(E).await, None, "dropped again at twice the margin");
    // and a build inside the margin after that drop is used but not kept
    let unkept = seek_server::metrics::VIEWER_UNKEPT.get();
    ask(p.addr, Protocol::Connect, "jo", E).await;
    assert_eq!(cached(E).await, None);
    assert!(seek_server::metrics::VIEWER_UNKEPT.get() > unkept);

    // a follow of A changes A's mutuals: A's entry goes too
    tokio::time::sleep(Duration::from_millis(600)).await;
    ask(p.addr, Protocol::Connect, "jo", A).await;
    assert!(cached(A).await.is_some());
    p.js.send(follow(B, Some(A))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(cached(A).await, None);

    // a query that loaded the old artifact, after the sweep moved V's entry to the new one: it
    // answers from the new one and leaves the entry alone
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(ask(p.addr, Protocol::Connect, "jo", V).await, want);
    let Some(CachedViewer::Keyed { read_ms, .. }) = cached(V).await else { panic!("{:?}", cached(V).await) };
    let g1 = p.pod.current().unwrap();
    let v2 = send(&p, dir.path(), now_us()).await;
    let g2 = p.pod.current().unwrap();
    assert_eq!(g2.version, v2);
    for _ in 0..100 {
        if matches!(cached(V).await, Some(CachedViewer::Keyed { gen, .. }) if gen == g2.id) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let old: Vec<String> =
        p.pod.search_on(g1.clone(), "jo", &did(V), 10).await.unwrap().into_iter().map(|(d, _)| d).collect();
    assert_eq!(old, want);
    let Some(CachedViewer::Keyed { gen, read_ms: r2, .. }) = cached(V).await else { panic!() };
    assert_eq!((gen, r2), (g2.id, read_ms), "the newer entry stays, unbuilt");
    drop((g1, g2));

    // two swaps back to back: V's entry ends on the last one's ids, moved or rebuilt, and answers
    // the same
    let mut st = state(now_us());
    let a = builder::build(&mut st, Some(&ranks(dir.path())), IndexSet::Profiles, dir.path()).unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    let mut st = state(now_us());
    let b = builder::build(&mut st, Some(&ranks(dir.path())), IndexSet::Profiles, dir.path()).unwrap();
    p.art.send((a.path.clone(), a.meta.version.clone())).await.unwrap();
    p.art.send((b.path.clone(), b.meta.version.clone())).await.unwrap();
    for _ in 0..200 {
        if p.pod.current().unwrap().version == b.meta.version {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let g3 = p.pod.current().unwrap();
    assert_eq!(g3.version, b.meta.version);
    assert_eq!(ask(p.addr, Protocol::Connect, "jo", V).await, want);
    assert!(matches!(cached(V).await, Some(CachedViewer::Keyed { gen, .. }) if gen == g3.id));
}

fn load_args(logged_in: bool) -> seek_server::load::LoadArgs {
    seek_server::load::LoadArgs {
        logged_in,
        connections: 2,
        concurrency: vec![1, 4],
        rates: vec![200.0, 1e6],
        step_seconds: 0.4,
        warmup_seconds: 0.1,
        max_in_flight: 16,
        marco_metrics: None,
        marco_hist: "marco_rpc_seconds".into(),
        marco_hist_filter: String::new(),
        health: vec![],
        stop_error_ratio: 0.01,
        stop_p99_ms: None,
        sample_ms: 100,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn load_sweeps() {
    let (maddr, _mdir) = marco().await;
    let dir = tempfile::tempdir().unwrap();
    let p = pod(Some(maddr), dir.path(), Duration::from_secs(5)).await;
    ready(&p).await;
    for logged_in in [false, true] {
        let l = seek_server::load::Load {
            args: load_args(logged_in),
            target: format!("http://{}", p.addr),
            pod_metrics: None,
            queries: Arc::new(vec!["jo".into(), "jo-".into(), "view".into()]),
            viewers: Arc::new(vec![did(V)]),
            seed: 7,
        };
        let closed = l.run(false).await.unwrap();
        let steps = closed["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 2, "{closed}");
        for s in steps {
            assert_eq!(s["errors"], 0, "{s}");
            assert!(s["per_s"].as_f64().unwrap() > 10.0, "{s}");
            assert!(s["latency"]["p99_us"].as_f64().unwrap() > 0.0, "{s}");
        }
        assert!(closed["stopped"].is_null(), "{closed}");
        let open = l.run(true).await.unwrap();
        let steps = open["steps"].as_array().unwrap();
        // 200/s is met; an absurd rate saturates and stops the sweep
        assert_eq!(steps.len(), 2, "{open}");
        let s = &steps[0];
        assert_eq!(s["errors"], 0, "{s}");
        assert!((s["per_s"].as_f64().unwrap() - 200.0).abs() < 30.0, "{s}");
        assert!(s["latency"]["n"].as_u64().unwrap() >= 70, "{s}");
        assert!(open["stopped"].as_str().unwrap().starts_with("saturated"), "{open}");
    }
}

/// A TCP proxy, as a Kubernetes Service is to its pods: each new connection goes to the next of its
/// backends in turn and stays there. [`Proxy::cut`] kills the connections open now, as a pod dying
/// under them would: the next bytes a client sends on one get a reset.
struct Proxy {
    addr: std::net::SocketAddr,
    to: Arc<std::sync::Mutex<(Vec<std::net::SocketAddr>, usize)>>,
    cut: Arc<tokio::sync::watch::Sender<u64>>,
}

impl Proxy {
    async fn start(to: Vec<std::net::SocketAddr>) -> Proxy {
        use tokio::io::AsyncReadExt;
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let to = Arc::new(std::sync::Mutex::new((to, 0)));
        let cut = Arc::new(tokio::sync::watch::channel(0u64).0);
        let (routes, cuts) = (to.clone(), cut.clone());
        tokio::spawn(async move {
            loop {
                let (client, _) = l.accept().await.unwrap();
                let backend = {
                    let mut r = routes.lock().unwrap();
                    r.1 += 1;
                    r.0[(r.1 - 1) % r.0.len()]
                };
                let mut cut = cuts.subscribe();
                tokio::spawn(async move {
                    let Ok(up) = tokio::net::TcpStream::connect(backend).await else { return };
                    let (mut cr, mut cw) = client.into_split();
                    let (mut ur, mut uw) = up.into_split();
                    let was_cut = tokio::select! {
                        _ = tokio::io::copy(&mut cr, &mut uw) => false,
                        _ = tokio::io::copy(&mut ur, &mut cw) => false,
                        _ = cut.changed() => true,
                    };
                    if was_cut {
                        drop((ur, uw));
                        let _ = cr.read(&mut [0u8; 1]).await;
                        let _ = cr.reunite(cw).unwrap().set_zero_linger();
                    }
                });
            }
        });
        Proxy { addr, to, cut }
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Connections made since the last [`Proxy::route`].
    fn conns(&self) -> usize {
        self.to.lock().unwrap().1
    }

    /// New connections go to `to` from now on.
    fn route(&self, to: Vec<std::net::SocketAddr>) {
        *self.to.lock().unwrap() = (to, 0);
    }

    fn cut(&self) {
        self.cut.send_modify(|n| *n += 1);
    }
}

/// A server answering everything 503: a proxy on the way whose upstream is gone.
async fn unavailable() -> std::net::SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let r = axum::Router::new().fallback(|| async { (http::StatusCode::SERVICE_UNAVAILABLE, "no healthy upstream") });
    tokio::spawn(async move { axum::serve(l, r).await });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn marco_calls_survive_their_marco_going() {
    use seek_server::marco::Marco;
    seek_server::metrics::init();
    let h = seek_server::metrics::QueryHists::new();
    let retries = || seek_server::metrics::MARCO_RETRIES.with_label_values(&["filter"]).get();
    let client = |p: &Proxy| Marco::new(&p.url(), 1, Duration::from_secs(5), 10_000).unwrap();
    // V blocks D on A, and follows D on B
    let (a, a_addr, _ad) = marco_app(true).await;
    let (_b, b_addr, _bd) = marco_app(false).await;
    let (on_a, on_b) = (vec![did(E)], vec![did(D), did(E)]);
    async fn filter(m: &Marco, h: &seek_server::metrics::QueryHists) -> Vec<String> {
        m.unblocked(&did(V), vec![did(D), did(E)], h).await.unwrap()
    }

    // A dies under the open connection: the call meets a reset, and its retry dials again and lands
    // on B
    let p = Proxy::start(vec![a_addr]).await;
    let m = client(&p);
    assert_eq!(filter(&m, &h).await, on_a);
    p.route(vec![b_addr]);
    p.cut();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let r0 = retries();
    assert_eq!(filter(&m, &h).await, on_b);
    assert_eq!(retries(), r0 + 1);
    // and the slot keeps the fresh connection
    assert_eq!(filter(&m, &h).await, on_b);
    assert_eq!(retries(), r0 + 1);

    // a proxy on the way answers 503: the retry's fresh connection goes to the next backend, B
    let gone = unavailable().await;
    let p = Proxy::start(vec![gone, b_addr]).await;
    let m = client(&p);
    let r0 = retries();
    assert_eq!(filter(&m, &h).await, on_b);
    assert_eq!(retries(), r0 + 1);

    // 503 everywhere: a slot's client is replaced at most once a second, so failing calls don't
    // dial a connection each
    let p = Proxy::start(vec![gone, gone, b_addr]).await;
    let m = client(&p);
    let v = did(V);
    let ask = || m.unblocked(&v, vec![did(D), did(E)], &h);
    let r0 = retries();
    assert!(ask().await.is_err());
    assert_eq!((p.conns(), retries()), (2, r0 + 1), "the first failure replaces the client");
    assert!(ask().await.is_err());
    assert_eq!((p.conns(), retries()), (2, r0 + 2), "a second within the second retries on it");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(ask().await.unwrap(), on_b);
    assert_eq!((p.conns(), retries()), (3, r0 + 3));

    // A stops cleanly: a call during its drain still answers there, and after its GOAWAY the next
    // one dials again and lands on B, with no retry
    let p = Proxy::start(vec![a_addr]).await;
    let m = client(&p);
    assert_eq!(filter(&m, &h).await, on_a);
    p.route(vec![b_addr]);
    a.drain.lame_duck();
    assert_eq!(filter(&m, &h).await, on_a);
    assert!(a.drain.stop(Duration::from_secs(5)).await);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let r0 = retries();
    assert_eq!(filter(&m, &h).await, on_b);
    assert_eq!(retries(), r0, "a GOAWAY needs no retry");
}
