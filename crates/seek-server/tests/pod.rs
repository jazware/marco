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
use seek_server::serve::{Config, Pod};
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
    let mut rows: Vec<seek_core::ranks::Row> = [(V, 1), (A, 10), (B, 5000), (C, 6000), (D, 100), (E, 20)]
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

async fn pod(marco_addr: std::net::SocketAddr, art_dir: &std::path::Path) -> Pods {
    seek_server::metrics::init();
    let m = seek_server::marco::Marco::new(&format!("http://{marco_addr}"), Duration::from_secs(5), 10_000).unwrap();
    let cfg = Config {
        params: Default::default(),
        replay_window: Duration::from_secs(3600),
        viewer_cache_bytes: 1 << 20,
        viewer_idle: Duration::from_secs(60),
        replay_keep: Duration::from_secs(3600),
    };
    let pod = Pod::new(cfg, Some(m));
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
    tokio::spawn(async move { axum::serve(l, r).await });
    Pods { addr, pod, art, js }
}

async fn ask(addr: std::net::SocketAddr, proto: Protocol, q: &str, viewer: u32) -> Vec<String> {
    let cfg = ClientConfig::new(format!("http://{addr}").parse().unwrap()).with_protocol(proto);
    let http = if proto == Protocol::Grpc { HttpClient::plaintext_http2_only() } else { HttpClient::plaintext() };
    let c = ServiceClient::new(http, cfg);
    let viewer = if viewer == u32::MAX { String::new() } else { did(viewer) };
    let r = c
        .search_actors_typeahead(SearchActorsTypeaheadRequest {
            params: SearchTypeaheadParams { query: q.into(), viewer, limit: 10, ..Default::default() }.into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    r.actors.into_iter().map(|a| a.did).collect()
}

const OUT: u32 = u32::MAX;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pod_against_marco() {
    let (maddr, _mdir) = marco().await;
    let dir = tempfile::tempdir().unwrap();
    let p = pod(maddr, dir.path()).await;
    // not caught up with Jetstream yet: not ready
    assert!(!p.pod.readiness().0);
    p.js.send(Ev::Profile { did: did(V), time_us: now_us(), name: Some("Viewer".into()), deleted: false })
        .await
        .unwrap();
    for _ in 0..100 {
        if p.pod.readiness().0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(p.pod.readiness().0, "{}", p.pod.readiness().1);

    for proto in [Protocol::Connect, Protocol::Grpc] {
        // logged out: by rank
        assert_eq!(ask(p.addr, proto, "jo", OUT).await, vec![did(C), did(B), did(D), did(E), did(A)], "{proto:?}");
        // logged in: follows first (the mutual ahead), strangers by rank, the blocker and the blocked gone
        assert_eq!(ask(p.addr, proto, "jo", V).await, vec![did(A), did(E), did(B)], "{proto:?}");
        // the blocked account's exact handle stays
        assert_eq!(ask(p.addr, proto, "jo.example.com", V).await[0], did(D));
        assert!(ask(p.addr, proto, "", V).await.is_empty());
    }

    // the overlay: B renamed, C taken down
    let t = now_us();
    p.js.send(Ev::Profile { did: did(B), time_us: t, name: Some("Zed".into()), deleted: false }).await.unwrap();
    p.js.send(Ev::Account { did: did(C), time_us: t + 1, active: false }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(ask(p.addr, Protocol::Connect, "zed", OUT).await, vec![did(B)]);
    assert_eq!(ask(p.addr, Protocol::Connect, "jo", OUT).await, vec![did(B), did(D), did(E), did(A)]);

    // a newer artifact from before those changes: the swap replays them into its overlay
    let old = p.pod.current().unwrap();
    let mut st = state(t - 1);
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

    let text = marco_ops::metrics_text();
    for m in [
        "seek_queries_total{mode=\"logged_in\",result=\"ok\"}",
        "seek_marco_seconds_count{method=\"filter\"}",
        "seek_artifact_swaps_total 2",
    ] {
        assert!(text.contains(m), "{m} missing");
    }
}
