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
    let c = reqwest::Client::builder().build()?;
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
