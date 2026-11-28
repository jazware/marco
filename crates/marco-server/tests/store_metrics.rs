//! The engine's metrics at /metrics, and the /readyz storm rule. Its own
//! test binary: the store metrics describe one App per process.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use connectrpc::client::{ClientConfig, HttpClient};
use marco_core::read::Engine;
use marco_core::store::engine::Config as EngineConfig;
use marco_core::store::io::{default_device, Fault, Times};
use marco_core::store::{shard_file, Storage};
use marco_proto::connect::marco::v1::{AdminClient, GraphClient};
use marco_proto::v1::*;
use marco_server::app::{App, Config};
use marco_server::auth::AuthConfig;
use marco_server::synth::did;

const TOKEN: &str = "test-token-0123456789";
const FRAMES: u32 = 4096;

struct Srv {
    admin: SocketAddr,
    app: Arc<App>,
}

async fn serve(snap: PathBuf, e: Arc<Engine>) -> Srv {
    let auth = AuthConfig { token: Some(TOKEN.into()), ..Default::default() };
    let storage = Storage { memory: 1 << 30, max_frames: Some(FRAMES) };
    let cfg = Config { auth, storage, load: false, ..Config::new(snap) };
    let app = App::open_on(cfg, e).unwrap();
    let ui = Arc::new(marco_server::ui::Ui::load(None).unwrap());
    let (_, admin) = marco_server::serve(app.clone(), ui).await.unwrap();
    Srv { admin, app }
}

fn client_cfg(addr: SocketAddr) -> ClientConfig {
    ClientConfig::new(format!("http://{addr}").parse().unwrap()).with_default_header(
        http::header::AUTHORIZATION,
        format!("Bearer {TOKEN}").parse::<http::HeaderValue>().unwrap(),
    )
}

/// GET `path` on the admin listener with the token: status and body.
async fn get(addr: SocketAddr, path: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req =
        format!("GET {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\nauthorization: Bearer {TOKEN}\r\n\r\n");
    c.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    c.read_to_end(&mut out).await.unwrap();
    let out = String::from_utf8_lossy(&out).into_owned();
    (out[9..12].parse().unwrap(), out.split_once("\r\n\r\n").map_or(String::new(), |x| x.1.to_string()))
}

/// The value of the first sample of `name` with `labels` in a scrape.
fn sample(text: &str, name: &str, labels: &str) -> Option<f64> {
    let key = if labels.is_empty() { format!("{name} ") } else { format!("{name}{{{labels}}} ") };
    text.lines().find_map(|l| l.strip_prefix(&key)).and_then(|v| v.trim().parse().ok())
}

/// The engine's metrics describe its budget and reads, and failing reads
/// across many pages take /readyz down where one bad page doesn't.
#[test]
fn store_metrics_and_the_storm_rule() {
    let rt = marco_server::runtime(Some(2)).unwrap();
    rt.block_on(async {
        let d = tempfile::tempdir().unwrap();
        let fault = Fault::new(default_device());
        let mut cfg = EngineConfig::new(1 << 30).device(fault.clone());
        cfg.max_frames = Some(FRAMES);
        let e = Arc::new(Engine::with_config(cfg).unwrap());
        let snap = d.path().join("s");
        marco_server::synth::write(&snap, 40_000, 8).unwrap();
        let s = serve(snap, e).await;
        let g = GraphClient::new(HttpClient::plaintext(), client_cfg(s.admin));
        for i in 0..200 {
            let r = ListOutRequest { rel: "follow".into(), actor: did(i), limit: 20, ..Default::default() };
            g.list_out(r).await.unwrap();
        }
        let (st, m) = get(s.admin, "/metrics").await;
        assert_eq!(st, 200);
        assert_eq!(sample(&m, "marco_store_bytes", r#"part="budget""#), Some((1u64 << 30) as f64));
        assert!(sample(&m, "marco_store_bytes", r#"part="pinned_dir""#).unwrap() > 0.0, "{m}");
        assert!(sample(&m, "marco_store_bytes", r#"part="pinned_index""#).unwrap() > 0.0);
        assert!(sample(&m, "marco_store_bytes", r#"part="cache_small""#).unwrap() > 0.0);
        let lookups = |labels: &str| sample(&m, "marco_store_lookups_total", labels).unwrap();
        let hits = lookups(r#"structure="out_small",result="hit",queue="small",source="request""#)
            + lookups(r#"structure="out_small",result="hit",queue="main",source="request""#);
        let misses = lookups(r#"structure="out_small",result="miss",queue="none",source="request""#);
        assert!(hits > 0.0 && misses > 0.0, "out_small: {hits} hits, {misses} misses");
        // every source's counters start at 0, so rate() sees a job's first scan
        assert_eq!(lookups(r#"structure="out_small",result="miss",queue="none",source="job""#), 0.0);
        assert!(sample(&m, "marco_store_io_seconds_count", r#"queue="request""#).unwrap() > 0.0);
        assert_eq!(
            sample(&m, "marco_store_io_queue_wait_seconds_count", r#"queue="request""#),
            sample(&m, "marco_store_io_seconds_count", r#"queue="request""#),
            "every read waited in the queue once"
        );
        let read = sample(&m, "marco_store_io_bytes_total", r#"queue="request""#).unwrap();
        assert!(read > 0.0);
        // 200 lists of about 8 records: each counted once, from its own section
        let used = sample(&m, "marco_store_bytes_used_total", r#"structure="out_small""#).unwrap();
        assert!(used > 200.0 * 8.0 * 4.0 && used < read, "{used} bytes used of {read} read");
        assert_eq!(sample(&m, "marco_store_bytes_used_total", r#"structure="other""#), Some(0.0));
        assert_eq!(sample(&m, "marco_store_bad_pages", ""), Some(0.0));
        assert_eq!(get(s.admin, "/readyz").await.0, 200);

        // 100 pages of a shard file failing, read by a verify: a storm
        let f0 = shard_file(0, 0);
        fault.eio(&f0, 0..100, Times::Always);
        AdminClient::new(HttpClient::plaintext(), client_cfg(s.admin))
            .verify_shards(VerifyShardsRequest::default())
            .await
            .unwrap();
        while s.app.job().running {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let (st, why) = get(s.admin, "/readyz").await;
        assert_eq!(st, 503, "{why}");
        assert!(why.starts_with("storage failing"), "{why}");
        let (_, m) = get(s.admin, "/metrics").await;
        assert!(sample(&m, "marco_store_failing_pages", "").unwrap() >= 64.0);
        assert!(sample(&m, "marco_store_bad_pages", "").unwrap() >= 64.0);
        assert!(sample(&m, "marco_store_read_errors_total", r#"kind="eio""#).unwrap() >= 64.0);
        assert!(sample(&m, "marco_store_io_seconds_count", r#"queue="background""#).unwrap() > 0.0);
    });
}
