//! The checkpoint export: a copy taken through a lease is the checkpoint the
//! lease named, though a checkpoint and a compaction land mid-copy; a file
//! resumes with Range; a lease expires and takes its links with it.

use std::sync::Arc;

use marco_server::app::{App, Config, Op};
use marco_server::auth::AuthConfig;
use marco_server::synth::{did, tid, uri, FOLLOW};

const N: u32 = 400;
const TOKEN: &str = "test-token-0123456789";
const EXPORT: &str = "export-token-0123456789";

async fn start() -> (std::net::SocketAddr, Arc<App>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("snap");
    marco_server::synth::write(&snap, N, 4).unwrap();
    let auth = AuthConfig { token: Some(TOKEN.into()), export_token: Some(EXPORT.into()), ..Default::default() };
    let cfg =
        Config { auth, compaction: marco_core::CompactPolicy { ratio: 1.0001, min_garbage: 0 }, ..Config::new(snap) };
    let app = App::open(cfg).unwrap();
    let ui = Arc::new(marco_server::ui::Ui::load(None).unwrap());
    let (_api, admin) = marco_server::serve(app.clone(), ui).await.unwrap();
    (admin, app, dir)
}

fn dump(g: &marco_core::Graph) -> Vec<String> {
    (0..N)
        .map(|i| {
            let d = did(i);
            let (o, _) = g.read(|cx| g.out_list(cx, 0, &d, 1_000_000, None)).unwrap();
            format!("{d} {o:?}")
        })
        .collect()
}

async fn writes(app: &App, r: u32) {
    let t0 = marco_server::app::now_ms() * 1000;
    let ops: Vec<Op> = (0..300u32)
        .map(|i| Op::Add {
            uri: uri(&did(i % N), FOLLOW, &tid(80_000_000 + (r * 1000 + i) as u64, 1)),
            subject: did((i * 7 + r) % N),
            list: None,
            time_us: t0 + i as i64,
        })
        .collect();
    for (i, c) in ops.chunks(50).enumerate() {
        app.apply(c, &format!("{r:04}{i:04}")).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lease_copies_one_checkpoint() {
    let (addr, app, dir) = start().await;
    let c = reqwest::Client::new();
    let base = format!("http://{addr}");
    // no token, no lease
    assert_eq!(c.get(format!("{base}/admin/checkpoint")).send().await.unwrap().status(), 401);

    let before = dump(&app.graph());
    let lease: serde_json::Value = c
        .get(format!("{base}/admin/checkpoint?ttl=300"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = lease["lease"].as_str().unwrap().to_string();
    let files = lease["files"].as_array().unwrap().clone();
    assert!(files.len() > 2);

    // a checkpoint and the compactions it sets off, mid-copy
    for r in 0..3 {
        writes(&app, r).await;
        app.checkpoint().await.unwrap();
    }
    let peek: serde_json::Value =
        c.get(format!("{base}/admin/checkpoint?peek=1")).bearer_auth(TOKEN).send().await.unwrap().json().await.unwrap();
    assert!(peek["checkpoint"].as_u64().unwrap() > lease["checkpoint"].as_u64().unwrap());

    let copy = dir.path().join("copy");
    std::fs::create_dir_all(&copy).unwrap();
    for (k, f) in files.iter().enumerate() {
        let name = f["name"].as_str().unwrap();
        let url = format!("{base}/admin/checkpoint/files/{name}?lease={id}");
        let bytes = if k == 0 {
            // a dropped download resumes where it stopped
            let whole = f["bytes"].as_u64().unwrap();
            let half = whole / 2;
            let a =
                c.get(&url).bearer_auth(TOKEN).header("range", format!("bytes=0-{}", half - 1)).send().await.unwrap();
            assert_eq!(a.status(), 206);
            let mut b = a.bytes().await.unwrap().to_vec();
            let r = c.get(&url).bearer_auth(TOKEN).header("range", format!("bytes={half}-")).send().await.unwrap();
            assert_eq!(r.status(), 206);
            b.extend_from_slice(&r.bytes().await.unwrap());
            b
        } else {
            c.get(&url)
                .bearer_auth(TOKEN)
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_vec()
        };
        assert_eq!(bytes.len() as u64, f["bytes"].as_u64().unwrap(), "{name}");
        assert_eq!(format!("{:016x}", xxhash_rust::xxh3::xxh3_64(&bytes)), f["xxh3"].as_str().unwrap(), "{name}");
        std::fs::write(copy.join(name), &bytes).unwrap();
    }
    std::fs::write(copy.join("manifest.json"), serde_json::to_vec(&lease["manifest"]).unwrap()).unwrap();
    let g = marco_core::Graph::open(&copy).unwrap();
    assert_eq!(dump(&g), before, "the copy is the leased checkpoint, not a later one");

    // released: its files go
    assert_eq!(
        c.delete(format!("{base}/admin/checkpoint?lease={id}")).bearer_auth(TOKEN).send().await.unwrap().status(),
        204
    );
    let name = files[0]["name"].as_str().unwrap();
    let gone =
        c.get(format!("{base}/admin/checkpoint/files/{name}?lease={id}")).bearer_auth(TOKEN).send().await.unwrap();
    assert_eq!(gone.status(), 404);

    // a lease past its ttl is gone too
    let short: serde_json::Value =
        c.get(format!("{base}/admin/checkpoint?ttl=1")).bearer_auth(TOKEN).send().await.unwrap().json().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let url = format!("{base}/admin/checkpoint/files/{name}?lease={}", short["lease"].as_str().unwrap());
    let r = c.get(url).bearer_auth(TOKEN).send().await.unwrap();
    assert_eq!(r.status(), 404);
    let text = c.get(format!("{base}/metrics")).bearer_auth(TOKEN).send().await.unwrap().text().await.unwrap();
    assert!(
        text.contains("marco_export_leases 0"),
        "{}",
        text.lines().filter(|l| l.contains("export")).collect::<Vec<_>>().join("\n")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_export_token_and_the_limits() {
    let (addr, _app, _dir) = start().await;
    let c = reqwest::Client::new();
    let base = format!("http://{addr}");
    // the export token opens the export, and is refused everywhere else on the admin listener
    let peek = c.get(format!("{base}/admin/checkpoint?peek=1")).bearer_auth(EXPORT).send().await.unwrap();
    assert_eq!(peek.status(), 200);
    assert_eq!(c.get(format!("{base}/metrics")).bearer_auth(EXPORT).send().await.unwrap().status(), 403);
    let rpc = c
        .post(format!("{base}/marco.v1.Admin/GetOverview"))
        .bearer_auth(EXPORT)
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(rpc.status(), 403);
    assert_eq!(c.get(format!("{base}/debug/pprof/heap")).bearer_auth(EXPORT).send().await.unwrap().status(), 403);

    // leases past the cap: 429, and room again after a release
    let mut ids = Vec::new();
    for _ in 0..marco_server::export::MAX_LEASES {
        let l: serde_json::Value =
            c.get(format!("{base}/admin/checkpoint")).bearer_auth(EXPORT).send().await.unwrap().json().await.unwrap();
        ids.push(l["lease"].as_str().unwrap().to_string());
    }
    let over = c.get(format!("{base}/admin/checkpoint")).bearer_auth(EXPORT).send().await.unwrap();
    assert_eq!(over.status(), 429);
    let id = ids.pop().unwrap();
    assert_eq!(
        c.delete(format!("{base}/admin/checkpoint?lease={id}")).bearer_auth(EXPORT).send().await.unwrap().status(),
        204
    );
    let again: serde_json::Value =
        c.get(format!("{base}/admin/checkpoint")).bearer_auth(EXPORT).send().await.unwrap().json().await.unwrap();
    let id = again["lease"].as_str().unwrap().to_string();
    let name = again["files"][0]["name"].as_str().unwrap().to_string();
    let bytes = again["files"][0]["bytes"].as_u64().unwrap();

    // names that would leave the lease's directory are refused; a range past the end is 416
    for bad in ["..%2Fmanifest.json", ".hidden", "a%2Fb"] {
        let r =
            c.get(format!("{base}/admin/checkpoint/files/{bad}?lease={id}")).bearer_auth(EXPORT).send().await.unwrap();
        assert!(r.status() == 400 || r.status() == 404, "{bad}: {}", r.status());
        assert_ne!(r.status(), 200);
    }
    let r = c
        .get(format!("{base}/admin/checkpoint/files/{name}?lease={id}"))
        .bearer_auth(EXPORT)
        .header("range", format!("bytes={}-", bytes + 10))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 416);
    let text = c.get(format!("{base}/metrics")).bearer_auth(TOKEN).send().await.unwrap().text().await.unwrap();
    assert!(
        text.contains(r#"marco_export_refused_total{reason="leases"} 1"#),
        "{}",
        text.lines().filter(|l| l.contains("export")).collect::<Vec<_>>().join("\n")
    );
}
