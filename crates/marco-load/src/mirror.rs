//! `marco-load mirror`: keeps a local copy of a running marco's current
//! checkpoint, through its admin listener's checkpoint export. It takes a
//! lease, downloads only the files it doesn't have (each checked against its
//! xxh3, a partial one resumed with Range), then links them into a snapshot
//! directory, `DIR/snap-<checkpoint>`, and points `DIR/current` at it. Files
//! no snapshot names any more go. PageRank and seek's builder read
//! `DIR/current` with `--snapshot`.
//!
//! The first run copies the whole snapshot; later ones only what the
//! checkpoints since wrote (their patches and new bases).

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(clap::Args)]
pub struct Args {
    /// marco's admin listener (http://host:7301).
    #[arg(long)]
    url: String,
    /// The admin token.
    #[arg(long)]
    token_file: Option<PathBuf>,
    /// The mirror: files/, snap-<checkpoint>/ and current.
    #[arg(long)]
    dir: PathBuf,
    /// How long the lease holds the checkpoint's files on marco's side.
    #[arg(long, default_value_t = 3600)]
    lease_seconds: u64,
    /// Only report what's there and what a sync would fetch.
    #[arg(long)]
    peek: bool,
}

#[derive(Deserialize)]
struct FileInfo {
    name: String,
    bytes: u64,
    xxh3: String,
}

#[derive(Deserialize)]
struct LeaseResp {
    lease: String,
    checkpoint: u64,
    manifest: Value,
    files: Vec<FileInfo>,
}

/// A name the manifest could have written: no path separator, nothing hidden or relative, so a
/// joined path stays inside the mirror.
fn safe_name(n: &str) -> bool {
    !n.is_empty()
        && n.len() <= 255
        && !n.starts_with('.')
        && n != "current"
        && !n.starts_with("snap-")
        && n.bytes().all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
}

fn sum_of(p: &Path) -> Result<u64> {
    use std::io::Read;
    let mut f = std::fs::File::open(p)?;
    let mut h = xxhash_rust::xxh3::Xxh3::new();
    let mut buf = vec![0u8; 4 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            return Ok(h.digest());
        }
        h.update(&buf[..n]);
    }
}

pub fn run(a: &Args) -> Result<Value> {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    rt.block_on(sync(a))
}

/// The checkpoint `DIR/current` holds, if any.
pub fn current_checkpoint(dir: &Path) -> Option<u64> {
    let t = std::fs::read_link(dir.join("current")).ok()?;
    t.to_str()?.strip_prefix("snap-")?.parse().ok()
}

async fn sync(a: &Args) -> Result<Value> {
    let t0 = Instant::now();
    let token = a.token_file.as_ref().map(std::fs::read_to_string).transpose()?.map(|t| t.trim().to_string());
    let base = a.url.trim_end_matches('/').to_string();
    // a hung marco can't hold a mirror forever: the builder that runs this also kills it past a limit
    let c = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .read_timeout(std::time::Duration::from_secs(120))
        .build()?;
    let auth = |r: reqwest::RequestBuilder| match &token {
        Some(t) => r.bearer_auth(t),
        None => r,
    };
    if a.peek {
        let v: Value =
            auth(c.get(format!("{base}/admin/checkpoint?peek=1"))).send().await?.error_for_status()?.json().await?;
        return Ok(json!({"marco": v, "mirror": current_checkpoint(&a.dir)}));
    }
    let lr: LeaseResp = auth(c.get(format!("{base}/admin/checkpoint?ttl={}", a.lease_seconds)))
        .send()
        .await?
        .error_for_status()
        .context("GET /admin/checkpoint")?
        .json()
        .await?;
    if let Some(f) = lr.files.iter().find(|f| !safe_name(&f.name)) {
        let _ = auth(c.delete(format!("{base}/admin/checkpoint?lease={}", lr.lease))).send().await;
        bail!("the server named a file {:?}: not a plain name in the mirror", f.name);
    }
    let files_dir = a.dir.join("files");
    std::fs::create_dir_all(&files_dir)?;
    let r = fetch(&c, &auth, &base, &lr, &files_dir).await;
    let _ = auth(c.delete(format!("{base}/admin/checkpoint?lease={}", lr.lease))).send().await;
    let (fetched, bytes, resumed) = r?;

    // the snapshot directory: links to the files, then its manifest, then `current`
    let snap = a.dir.join(format!("snap-{}", lr.checkpoint));
    let _ = std::fs::remove_dir_all(&snap);
    std::fs::create_dir_all(&snap)?;
    for f in &lr.files {
        std::fs::hard_link(files_dir.join(&f.name), snap.join(&f.name))?;
    }
    std::fs::write(snap.join("manifest.json"), serde_json::to_vec_pretty(&lr.manifest)?)?;
    let tmp = a.dir.join("current.tmp");
    let _ = std::fs::remove_file(&tmp);
    std::os::unix::fs::symlink(snap.file_name().unwrap(), &tmp)?;
    std::fs::rename(&tmp, a.dir.join("current"))?;
    // older snapshot dirs and files nothing names any more
    let keep: std::collections::HashSet<&str> = lr.files.iter().map(|f| f.name.as_str()).collect();
    let mut dropped = 0;
    for e in std::fs::read_dir(&a.dir)?.flatten() {
        let n = e.file_name().to_string_lossy().into_owned();
        if n.starts_with("snap-") && e.path() != snap {
            std::fs::remove_dir_all(e.path())?;
        }
    }
    for e in std::fs::read_dir(&files_dir)?.flatten() {
        let n = e.file_name().to_string_lossy().into_owned();
        let named = n.strip_prefix('.').and_then(|x| x.strip_suffix(".xxh3")).unwrap_or(&n);
        if !keep.contains(named) && !n.ends_with(".part") {
            std::fs::remove_file(e.path())?;
            dropped += 1;
        }
    }
    Ok(json!({
        "checkpoint": lr.checkpoint, "files": lr.files.len(), "fetched": fetched, "resumed": resumed,
        "bytes_fetched": bytes, "bytes_total": lr.files.iter().map(|f| f.bytes).sum::<u64>(), "dropped": dropped,
        "secs": t0.elapsed().as_secs_f64(), "snapshot": snap,
    }))
}

async fn fetch(
    c: &reqwest::Client,
    auth: &impl Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
    base: &str,
    lr: &LeaseResp,
    files_dir: &Path,
) -> Result<(usize, u64, usize)> {
    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;
    let (mut fetched, mut bytes, mut resumed) = (0, 0u64, 0);
    for f in &lr.files {
        let want = u64::from_str_radix(&f.xxh3, 16).context("xxh3")?;
        let p = files_dir.join(&f.name);
        // sums of files already here are kept beside them: a file never changes under its name
        let sum_p = files_dir.join(format!(".{}.xxh3", f.name));
        if p.exists() && std::fs::metadata(&p)?.len() == f.bytes {
            let have = match std::fs::read_to_string(&sum_p).ok().and_then(|s| u64::from_str_radix(s.trim(), 16).ok()) {
                Some(s) => s,
                None => sum_of(&p)?,
            };
            if have == want {
                std::fs::write(&sum_p, &f.xxh3)?;
                continue;
            }
        }
        let part = files_dir.join(format!("{}.part", f.name));
        let mut from = std::fs::metadata(&part).map(|m| m.len()).unwrap_or(0);
        if from > f.bytes {
            std::fs::remove_file(&part)?;
            from = 0;
        }
        if from < f.bytes {
            let mut req = auth(c.get(format!("{base}/admin/checkpoint/files/{}?lease={}", f.name, lr.lease)));
            if from > 0 {
                req = req.header(reqwest::header::RANGE, format!("bytes={from}-"));
                resumed += 1;
            }
            let resp = req.send().await?.error_for_status().with_context(|| format!("GET {}", f.name))?;
            if from > 0 && resp.status() != reqwest::StatusCode::PARTIAL_CONTENT {
                bail!("{}: the server ignored the range", f.name);
            }
            let mut out = tokio::fs::OpenOptions::new().create(true).append(true).open(&part).await?;
            let mut s = resp.bytes_stream();
            while let Some(chunk) = s.next().await {
                let chunk = chunk.with_context(|| format!("read {}", f.name))?;
                bytes += chunk.len() as u64;
                out.write_all(&chunk).await?;
            }
            out.sync_all().await?;
        }
        let got = sum_of(&part)?;
        if got != want {
            std::fs::remove_file(&part)?;
            bail!("{}: xxh3 {got:016x}, the lease says {}", f.name, f.xxh3);
        }
        std::fs::rename(&part, &p)?;
        std::fs::write(&sum_p, &f.xxh3)?;
        fetched += 1;
    }
    Ok((fetched, bytes, resumed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn names() {
        assert!(safe_name("shard-0001.marco") && safe_name("directory.marco"));
        for bad in ["", "../x", "a/b", ".x", "current", "snap-3", "a b"] {
            assert!(!safe_name(bad), "{bad}");
        }
    }

    /// A mirror of an in-process marco: the first sync copies it all and opens as the same graph,
    /// a second fetches nothing, and one after a checkpoint fetches only what it wrote.
    #[test]
    fn mirrors_a_running_marco() {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let snap = dir.path().join("snap");
        marco_server::synth::write(&snap, 300, 4).unwrap();
        let token = dir.path().join("export-token");
        std::fs::write(&token, "export-token-0123456789").unwrap();
        let (app, addr) = rt.block_on(async {
            let auth = marco_server::auth::AuthConfig {
                export_token: Some("export-token-0123456789".into()),
                ..Default::default()
            };
            let app = marco_server::app::App::open(marco_server::app::Config {
                auth,
                ..marco_server::app::Config::new(snap.clone())
            })
            .unwrap();
            let ui = Arc::new(marco_server::ui::Ui::load(None).unwrap());
            let (_api, admin) = marco_server::serve(app.clone(), ui).await.unwrap();
            (app, admin)
        });
        let args = |dir: &Path| Args {
            url: format!("http://{addr}"),
            token_file: Some(token.clone()),
            dir: dir.to_path_buf(),
            lease_seconds: 60,
            peek: false,
        };
        let m = dir.path().join("mirror");
        let first = rt.block_on(sync(&args(&m))).unwrap();
        assert_eq!(first["fetched"], first["files"]);
        let g = marco_core::Graph::open(&m.join("current")).unwrap();
        let orig = marco_core::Graph::open(&snap).unwrap();
        let d = marco_server::synth::did(1);
        let ins = |g: &marco_core::Graph| g.read(|cx| g.in_list(cx, 0, &d, 1_000_000, None)).unwrap().0;
        assert_eq!(ins(&g), ins(&orig));
        let again = rt.block_on(sync(&args(&m))).unwrap();
        assert_eq!(again["fetched"], 0);
        assert_eq!(again["bytes_fetched"], 0);
        rt.block_on(async {
            let ops = vec![marco_server::app::Op::Add {
                uri: format!("at://{}/app.bsky.graph.follow/3lzzzzzzzzzz2", marco_server::synth::did(7)),
                subject: d.clone(),
                list: None,
                time_us: marco_server::app::now_ms() * 1000,
            }];
            app.apply(&ops, "t1").await.unwrap();
            app.checkpoint().await.unwrap();
        });
        let third = rt.block_on(sync(&args(&m))).unwrap();
        assert!(third["fetched"].as_u64().unwrap() >= 1);
        assert!(third["bytes_fetched"].as_u64().unwrap() < first["bytes_fetched"].as_u64().unwrap());
        let g = marco_core::Graph::open(&m.join("current")).unwrap();
        assert!(ins(&g).contains(&marco_server::synth::did(7)));
        // a snapshot dir per checkpoint, the old one gone
        let snaps = std::fs::read_dir(&m)
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().starts_with("snap-"))
            .count();
        assert_eq!(snaps, 1);
    }
}
