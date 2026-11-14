//! A pod end to end against a real marco server (in-process, on a small
//! graph) and a builder-made artifact, over Connect and gRPC: logged out by
//! rank, logged in with follows first, blocks dropped both ways, an exact
//! handle kept through a block, the overlay's changes, and a swap.

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

fn tid(k: u64) -> String {
    vlatproto::tid::Tid::from_parts(1_704_067_200_000_000 + k * 1_000_000, 0).to_string()
}

async fn marco() -> (std::net::SocketAddr, tempfile::TempDir) {
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
    rec(&mut g, C, "app.bsky.graph.block", V);
    rec(&mut g, V, "app.bsky.graph.block", D);
    rec(&mut g, V, "app.bsky.graph.block", F);
    rec(&mut g, B, "app.bsky.graph.follow", C);
    let snap = dir.path().join("snap");
    g.write_snapshot(&snap).unwrap();
    let app = marco_server::app::App::open(marco_server::app::Config::new(snap)).unwrap();
    let ui = Arc::new(marco_server::ui::Ui::load(None).unwrap());
    let (api, _admin) = marco_server::serve(app, ui).await.unwrap();
    (api, dir)
}

fn state(cursor_us: u64) -> State {
    let mut s = State::default();
    for (i, h, n) in [
        (V, "viewer.bsky.social", "Viewer"),
        (A, "jo-a.bsky.social", "Jo Mutual"),
        (B, "jo-b.bsky.social", "Jo Stranger"),
        (C, "jo-c.bsky.social", "Jo Blocker"),
        (D, "jo.example.com", "Jo Blocked"),
        (E, "jo-e.bsky.social", "Jo Followed"),
        (F, "jo-example.com", "Lookalike"),
    ] {
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
    let mut rows: Vec<seek_core::ranks::Row> = [(V, 1), (A, 10), (B, 5000), (C, 6000), (D, 100), (E, 20), (F, 7000)]
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
    pod_with(marco_addr, art_dir, viewer_deadline, Duration::from_secs(300)).await
}

async fn pod_with(
    marco_addr: Option<std::net::SocketAddr>,
    art_dir: &std::path::Path,
    viewer_deadline: Duration,
    max_lag: Duration,
) -> Pods {
    seek_server::metrics::init();
    std::fs::create_dir_all(art_dir).unwrap();
    let m = marco_addr
        .map(|a| seek_server::marco::Marco::new(&format!("http://{a}"), Duration::from_secs(5), 10_000).unwrap());
    let cfg = Config {
        params: Default::default(),
        replay_window: Duration::from_secs(3600),
        viewer_cache_bytes: 1 << 20,
        viewer_idle: Duration::from_secs(60),
        replay_keep: Duration::from_secs(3600),
        max_lag,
        viewer_deadline,
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
    tokio::spawn(async move { axum::serve(nodelay(l), r).await });
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
    let p = pod_with(Some(maddr), dir.path(), Duration::from_secs(5), Duration::from_secs(1)).await;
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
