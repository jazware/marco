//! A clean stop. In process: through the drain `/readyz` is 503 while every
//! listener keeps serving (an HTTP/1 answer says `Connection: close`), a
//! request in flight finishes across the stop, a connection after the stop is
//! refused, and the grace bounds the wait. The binary: SIGTERM drains and
//! stops, and a write taken during the drain is in the snapshot after the exit.

use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use connectrpc::client::{ClientConfig, HttpClient};
use marco_proto::connect::marco::v1::GraphClient;
use marco_proto::v1::__buffa::oneof::{write_op, write_result};
use marco_proto::v1::*;
use marco_server::app::{App, Config};
use marco_server::auth::AuthConfig;
use marco_server::synth::{did, tid, uri, FOLLOW};

const N: u32 = 300;
const TOKEN: &str = "test-token-0123456789";

/// One CPU profile at a time per process: the tests here that take one queue.
static PROFILE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn server(dir: &Path) -> (Arc<App>, SocketAddr, SocketAddr, SocketAddr) {
    let snap = dir.join("snap");
    marco_server::synth::write(&snap, N, 4).unwrap();
    let auth = AuthConfig { token: Some(TOKEN.into()), ..Default::default() };
    let app = App::open(Config { auth, ops_listen: Some("127.0.0.1:0".into()), ..Config::new(snap) }).unwrap();
    let ui = Arc::new(marco_server::ui::Ui::load(None).unwrap());
    let (api, admin) = marco_server::serve(app.clone(), ui).await.unwrap();
    let ops = marco_server::serve_ops(&app).await.unwrap().unwrap();
    (app, api, admin, ops)
}

fn graph(addr: SocketAddr, token: Option<&str>) -> GraphClient<HttpClient> {
    let mut c = ClientConfig::new(format!("http://{addr}").parse().unwrap());
    if let Some(t) = token {
        c = c.with_default_header(
            http::header::AUTHORIZATION,
            format!("Bearer {t}").parse::<http::HeaderValue>().unwrap(),
        );
    }
    GraphClient::new(HttpClient::plaintext_http2_only(), c)
}

fn has() -> HasRequest {
    HasRequest { rel: "follow".into(), from: did(3), to: did(4), ..Default::default() }
}

/// A CPU profile of `secs` on the ops listener over HTTP/2: a request in flight that long.
fn profile(ops: SocketAddr, secs: u64) -> tokio::task::JoinHandle<(u16, usize)> {
    let c = reqwest::Client::builder().http2_prior_knowledge().build().unwrap();
    tokio::spawn(async move {
        let r = c.get(format!("http://{ops}/debug/pprof/profile?seconds={secs}")).send().await.unwrap();
        let st = r.status().as_u16();
        (st, r.bytes().await.unwrap().len())
    })
}

/// A keep-alive HTTP/1.1 GET on a new connection: the status, the head
/// (lowercased), and whether the server closed the connection after the answer.
fn h1(addr: SocketAddr, path: &str) -> std::io::Result<(u16, String, bool)> {
    let mut s = TcpStream::connect(addr)?;
    s.set_read_timeout(Some(Duration::from_secs(10)))?;
    write!(s, "GET {path} HTTP/1.1\r\nhost: {addr}\r\n\r\n")?;
    let mut buf = Vec::new();
    let mut b = [0u8; 4096];
    let end = loop {
        let n = s.read(&mut b)?;
        if n == 0 {
            return Err(ErrorKind::UnexpectedEof.into());
        }
        buf.extend_from_slice(&b[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..end]).to_lowercase();
    let status = head[9..12].parse().unwrap();
    let len: usize =
        head.lines().find_map(|l| l.strip_prefix("content-length:")).map_or(0, |v| v.trim().parse().unwrap());
    let mut left = (end + len).saturating_sub(buf.len());
    while left > 0 {
        let n = s.read(&mut b[..left.min(4096)])?;
        if n == 0 {
            return Err(ErrorKind::UnexpectedEof.into());
        }
        left -= n;
    }
    s.set_read_timeout(Some(Duration::from_millis(500)))?;
    let closed = matches!(s.read(&mut b), Ok(0));
    Ok((status, head, closed))
}

async fn h1_async(addr: SocketAddr, path: &'static str) -> std::io::Result<(u16, String, bool)> {
    tokio::task::spawn_blocking(move || h1(addr, path)).await.unwrap()
}

fn refused(addr: SocketAddr) -> bool {
    matches!(TcpStream::connect(addr), Err(e) if e.kind() == ErrorKind::ConnectionRefused)
}

/// Waits for `addr` to refuse connections: a listener closes on its own task, just after its stop.
async fn until_refused(addr: SocketAddr, within: Duration) {
    let t = Instant::now();
    while !refused(addr) {
        assert!(t.elapsed() < within, "{addr} still accepts");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_serves_then_stops() {
    let _one = PROFILE.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let (app, api, admin, ops) = server(dir.path()).await;
    // an HTTP/2 connection from before the drain, as seek keeps them
    let g = graph(api, None);
    g.has(has()).await.unwrap();
    let (st, head, closed) = h1_async(api, "/readyz").await.unwrap();
    assert!(st == 200 && !head.contains("connection: close") && !closed, "{st} {head}");

    let slow = profile(ops, 3);
    tokio::time::sleep(Duration::from_millis(200)).await;
    app.drain.lame_duck();
    // not ready, but every listener still answers, and HTTP/1 answers close their connection
    for a in [api, admin] {
        let (st, head, closed) = h1_async(a, "/readyz").await.unwrap();
        assert_eq!(st, 503, "{a}");
        assert!(head.contains("connection: close") && closed, "{a}: {head}");
        assert_eq!(h1_async(a, "/healthz").await.unwrap().0, 200);
    }
    g.has(has()).await.unwrap();

    let t = Instant::now();
    assert!(app.drain.stop(Duration::from_secs(10)).await, "the profile ends within the grace");
    assert!(t.elapsed() >= Duration::from_millis(1500), "the stop waited for the profile: {:?}", t.elapsed());
    let (st, bytes) = slow.await.unwrap();
    assert!(st == 200 && bytes > 0, "{st} {bytes}");
    for a in [api, admin, ops] {
        until_refused(a, Duration::from_secs(1)).await;
    }
    // the HTTP/2 client got a GOAWAY: nothing here takes its next call
    assert!(g.has(has()).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grace_bounds_the_wait() {
    let _one = PROFILE.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let (app, api, _, ops) = server(dir.path()).await;
    let slow = profile(ops, 5);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let t = Instant::now();
    assert!(!app.drain.stop(Duration::from_millis(500)).await, "the profile outlasts the grace");
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
    for a in [api, ops] {
        until_refused(a, Duration::from_secs(1)).await;
    }
    // the stop gave up waiting, not the request
    assert_eq!(slow.await.unwrap().0, 200);
}

fn free_port() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_drains_then_exits() {
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("snap");
    marco_server::synth::write(&snap, N, 4).unwrap();
    let token = dir.path().join("token");
    std::fs::write(&token, TOKEN).unwrap();
    let (api, admin, ops) = (free_port(), free_port(), free_port());
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_marco"))
        .arg("serve")
        .arg("--snapshot")
        .arg(&snap)
        .args(["--memory", "3GiB", "--memory-limit", "none"])
        .args(["--listen", &api.to_string(), "--admin-listen", &admin.to_string(), "--ops-listen", &ops.to_string()])
        .arg("--admin-token-file")
        .arg(&token)
        .args(["--shutdown-drain-secs", "2.5", "--shutdown-grace-secs", "10"])
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let t = Instant::now();
    while !matches!(h1_async(api, "/readyz").await, Ok((200, _, _))) {
        assert!(t.elapsed() < Duration::from_secs(60), "marco never became ready");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let g = graph(api, None);
    assert_eq!(g.has(has()).await.unwrap().into_owned().uri, "");
    let slow = profile(ops, 5);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let t = Instant::now();
    assert_eq!(unsafe { libc::kill(child.id().unwrap() as libc::pid_t, libc::SIGTERM) }, 0);
    while h1_async(api, "/readyz").await.unwrap().0 != 503 {
        assert!(t.elapsed() < Duration::from_secs(1), "/readyz still 200");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // during the drain: a write on a new connection, and a read on the old one
    let u = uri(&did(3), FOLLOW, &tid(9_000_000, 1));
    let add = WriteOp {
        op: Some(write_op::Op::Add(Box::new(AddOp { uri: u.clone(), subject: did(4), ..Default::default() }))),
        ..Default::default()
    };
    let r = graph(admin, Some(TOKEN))
        .write_batch(WriteBatchRequest { ops: vec![add], ..Default::default() })
        .await
        .unwrap()
        .into_owned();
    assert!(matches!(r.results[0].outcome, Some(write_result::Outcome::Added(_))));
    assert_eq!(g.has(has()).await.unwrap().into_owned().uri, u);
    assert!(t.elapsed() < Duration::from_millis(2400), "the checks ran past the drain: {:?}", t.elapsed());

    // after the drain, new connections are refused while the profile finishes
    until_refused(api, Duration::from_secs(6)).await;
    assert!(t.elapsed() >= Duration::from_millis(2400), "stopped before the drain: {:?}", t.elapsed());
    for a in [admin, ops] {
        until_refused(a, Duration::from_secs(1)).await;
    }
    let (st, bytes) = slow.await.unwrap();
    assert!(st == 200 && bytes > 0, "{st} {bytes}");
    let exit = tokio::time::timeout(Duration::from_secs(30), child.wait()).await.unwrap().unwrap();
    assert!(exit.success(), "{exit}");

    // the write from the drain is in the last segment
    let app = App::open(Config::new(snap)).unwrap();
    let ui = Arc::new(marco_server::ui::Ui::load(None).unwrap());
    let (api, _) = marco_server::serve(app, ui).await.unwrap();
    assert_eq!(graph(api, None).has(has()).await.unwrap().into_owned().uri, u);
}
