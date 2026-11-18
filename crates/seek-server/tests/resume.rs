//! A pod's restart: the artifact it served is kept in `--data` and served again at start, checked
//! first (a truncated or corrupt one, or one from another source, is deleted, never served), with
//! the staleness rule as for a download; the poll doesn't fetch the same version again, and a newer
//! one swaps in and takes the old one's place on disk.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use seek_server::builder::{self, BuildOut, IndexSet, Published, State};
use seek_server::jetstream::Ev;
use seek_server::metrics::now_us;
use seek_server::serve::{self, Config, Pod};
use seek_server::store::{self, Latest, Source};
use tokio::sync::{mpsc, oneshot};

const SRC: &str = "http://builder.test/artifact/latest";

fn did(i: u32) -> String {
    marco_server::synth::did(i)
}

fn build(dir: &Path, cursor_us: u64) -> BuildOut {
    std::thread::sleep(Duration::from_millis(3));
    let mut st = State::default();
    for (i, h) in [(0, "jo-a.bsky.social"), (1, "jo-b.bsky.social")] {
        let a = st.get_mut(&did(i));
        a.handle = Some(h.into());
        a.verified_us = 1;
        a.name = Some(format!("Jo {i}").into());
        a.profile = true;
    }
    st.cursor_us = cursor_us;
    builder::build(&mut st, None, IndexSet::Profiles, dir).unwrap()
}

struct P {
    pod: Arc<Pod>,
    art: mpsc::Sender<(PathBuf, String)>,
    js: mpsc::Sender<Ev>,
    start: oneshot::Receiver<u64>,
    ingest: tokio::task::JoinHandle<()>,
}

fn pod() -> P {
    seek_server::metrics::init();
    let cfg = Config {
        params: Default::default(),
        replay_window: Duration::from_secs(3600),
        viewer_cache_bytes: 1 << 20,
        viewer_idle: Duration::from_secs(60),
        replay_keep: Duration::from_secs(3600),
        max_lag: Duration::from_secs(300),
        viewer_deadline: Duration::from_secs(1),
        marco_lag: Duration::from_millis(500),
    };
    let pod = Pod::new(cfg, None);
    let (art, art_rx) = mpsc::channel(1);
    let (js, js_rx) = mpsc::channel(100);
    let (_vtx, vrx) = mpsc::channel(10);
    let (start_tx, start) = oneshot::channel();
    let ingest = tokio::spawn(pod.clone().run_ingest(art_rx, js_rx, vrx, None, start_tx));
    P { pod, art, js, start, ingest }
}

/// A data directory as a pod on `SRC` leaves it, empty.
fn data() -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    assert!(store::on_disk(d.path(), SRC).unwrap().is_empty());
    d
}

fn files(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> =
        std::fs::read_dir(dir).unwrap().flatten().map(|e| e.file_name().to_string_lossy().into_owned()).collect();
    v.sort();
    v
}

async fn until(what: &str, f: impl Fn() -> bool) {
    for _ in 0..250 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kept_artifact_is_served_at_start_and_kept_at_stop() {
    let d = data();
    let a = build(d.path(), now_us());
    std::fs::write(d.path().join("cut-short.part"), b"half").unwrap();
    let found = store::on_disk(d.path(), SRC).unwrap();
    assert_eq!(found, vec![(a.path.clone(), a.meta.version.clone())]);
    assert!(!d.path().join("cut-short.part").exists(), "a download cut short goes at start");

    let mut p = pod();
    assert_eq!(serve::resume(&p.pod, &p.art, found).await.as_deref(), Some(a.meta.version.as_str()));
    // Jetstream replays from its cursor, as for a download
    assert_eq!((&mut p.start).await.unwrap(), a.meta.cursor_us);
    p.js.send(Ev::Profile { did: did(0), time_us: now_us(), name: Some("Jo".into()), deleted: false }).await.unwrap();
    until("ready", || p.pod.readiness().0).await;
    assert_eq!(p.pod.search("jo", "", 10, false).await.unwrap().len(), 2);

    // a stop drops the pod and its generation: the file stays for the next start
    let weak = Arc::downgrade(&p.pod);
    p.ingest.abort();
    let _ = p.ingest.await;
    drop((p.pod, p.art, p.js));
    until("the pod dropped", || weak.upgrade().is_none()).await;
    assert!(a.path.exists());
    assert_eq!(store::on_disk(d.path(), SRC).unwrap(), vec![(a.path.clone(), a.meta.version.clone())]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncated_or_corrupt_artifact_is_deleted_not_served() {
    // the newest truncated: deleted, and the older one is served
    let d = data();
    let older = build(d.path(), now_us());
    let newest = build(d.path(), now_us());
    let len = std::fs::metadata(&newest.path).unwrap().len();
    std::fs::OpenOptions::new().write(true).open(&newest.path).unwrap().set_len(len / 2).unwrap();
    let found = store::on_disk(d.path(), SRC).unwrap();
    assert_eq!(found[0].1, newest.meta.version, "newest first");
    let p = pod();
    assert_eq!(serve::resume(&p.pod, &p.art, found).await.as_deref(), Some(older.meta.version.as_str()));
    assert!(!newest.path.exists());
    assert_eq!(p.pod.current().unwrap().version, older.meta.version);

    // a flipped byte past the header: the section's xxh3 refuses it, nothing is served
    let d = data();
    let a = build(d.path(), now_us());
    let mut b = std::fs::read(&a.path).unwrap();
    let at = b.len() - 100;
    b[at] ^= 0xff;
    std::fs::write(&a.path, &b).unwrap();
    let p = pod();
    assert_eq!(serve::resume(&p.pod, &p.art, store::on_disk(d.path(), SRC).unwrap()).await, None);
    assert!(p.pod.current().is_none());
    assert!(!a.path.exists());
    assert_eq!(p.pod.readiness(), (false, "no artifact yet".into()));

    // a file whose name isn't the version its meta carries
    let d = data();
    let a = build(d.path(), now_us());
    let renamed = d.path().join("20990101T000000Z000-1.seek");
    std::fs::rename(&a.path, &renamed).unwrap();
    let p = pod();
    assert_eq!(serve::resume(&p.pod, &p.art, store::on_disk(d.path(), SRC).unwrap()).await, None);
    assert!(!renamed.exists());

    // the directory was filled from another source: everything goes before anything is mapped
    let d = data();
    build(d.path(), now_us());
    assert!(store::on_disk(d.path(), "http://another-builder.test/artifact/latest").unwrap().is_empty());
    assert_eq!(files(d.path()), vec!["source"]);
    // and the mark now names the new source
    assert!(store::on_disk(d.path(), SRC).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kept_artifact_past_the_replay_window_is_refused_until_a_newer_one() {
    let d = data();
    let old = build(d.path(), now_us() - 2 * 3600 * 1_000_000);
    let mut p = pod();
    let found = store::on_disk(d.path(), SRC).unwrap();
    assert_eq!(serve::resume(&p.pod, &p.art, found).await.as_deref(), Some(old.meta.version.as_str()));
    assert_eq!((&mut p.start).await.unwrap(), old.meta.cursor_us);
    p.js.send(Ev::Profile { did: did(0), time_us: now_us(), name: Some("Jo".into()), deleted: false }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (ok, why) = p.pod.readiness();
    assert!(!ok && why.starts_with("refusing to serve"), "{why}");

    // the builder's newer one swaps in: ready, and the stale one is gone from disk
    let b = build(d.path(), now_us());
    p.art.send((b.path.clone(), b.meta.version.clone())).await.unwrap();
    until("the swap", || p.pod.current().unwrap().version == b.meta.version).await;
    p.js.send(Ev::Profile { did: did(0), time_us: now_us(), name: Some("Jo".into()), deleted: false }).await.unwrap();
    until("ready", || p.pod.readiness().0).await;
    until("the old file deleted", || !old.path.exists()).await;
}

/// The builder's `/artifact/latest`, counting full answers and 304s.
async fn builder_http(published: Arc<Mutex<Published>>) -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let (full, same) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (f, s) = (full.clone(), same.clone());
    let r = builder::router(published, &marco_ops::drain::Drain::new()).layer(axum::middleware::map_response(
        move |r: axum::response::Response| {
            let (f, s) = (f.clone(), s.clone());
            async move {
                match r.status() {
                    http::StatusCode::OK => f.fetch_add(1, Ordering::Relaxed),
                    http::StatusCode::NOT_MODIFIED => s.fetch_add(1, Ordering::Relaxed),
                    _ => 0,
                };
                r
            }
        },
    ));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/artifact/latest", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, r).await });
    (url, full, same)
}

fn latest(b: &BuildOut) -> (PathBuf, Latest) {
    let l = Latest {
        version: b.meta.version.clone(),
        key: format!("{}.seek", b.meta.version),
        bytes: std::fs::metadata(&b.path).unwrap().len(),
        cursor_us: b.meta.cursor_us,
        created_ms: b.meta.created_ms,
    };
    (b.path.clone(), l)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_version_isnt_fetched_again_and_a_newer_one_replaces_it() {
    let builder_dir = tempfile::tempdir().unwrap();
    let published = Arc::new(Mutex::new(Published::default()));
    let (url, full, same) = builder_http(published.clone()).await;

    // the builder's artifact, kept by the last run under the same version
    let a = build(builder_dir.path(), now_us());
    published.lock().unwrap().latest = Some(latest(&a));
    let d = tempfile::tempdir().unwrap();
    assert!(store::on_disk(d.path(), &url).unwrap().is_empty());
    let kept = d.path().join(format!("{}.seek", a.meta.version));
    std::fs::copy(&a.path, &kept).unwrap();

    let p = pod();
    let found = store::on_disk(d.path(), &url).unwrap();
    let have = serve::resume(&p.pod, &p.art, found).await;
    assert_eq!(have.as_deref(), Some(a.meta.version.as_str()));
    let gen = p.pod.current().unwrap().id;
    let src = Source::parse(&url, None, "auto", None).unwrap();
    let poll = tokio::spawn(store::follow(src, d.path().to_path_buf(), have, Duration::from_millis(30), p.art.clone()));
    until("three polls", || same.load(Ordering::Relaxed) >= 3).await;
    assert_eq!(full.load(Ordering::Relaxed), 0, "the version served isn't downloaded again");
    assert_eq!(p.pod.current().unwrap().id, gen);
    assert!(kept.exists());

    // a newer build: downloaded, swapped in, and the old file deleted
    let b = build(builder_dir.path(), now_us());
    published.lock().unwrap().latest = Some(latest(&b));
    until("the swap", || p.pod.current().unwrap().version == b.meta.version).await;
    assert_eq!(full.load(Ordering::Relaxed), 1);
    until("the old file deleted", || !kept.exists()).await;
    assert_eq!(files(d.path()), vec![format!("{}.seek", b.meta.version), "source".into()]);
    poll.abort();
}
