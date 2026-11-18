//! `marco-load mirror`: keeps a local copy of a running marco's current
//! checkpoint, through its admin listener's checkpoint export. It takes a
//! lease, downloads only the files it doesn't have by name and size (a
//! partial one resumed with Range; a local one whose footer names another
//! build is fetched again), checks each fetched file against its own
//! section checksums at local disk speed, then links them into a snapshot
//! directory, `DIR/snap-<checkpoint>`, and points `DIR/current` at it. Files
//! no snapshot names any more go. PageRank and seek's builder read
//! `DIR/current` with `--snapshot`.
//!
//! The first run copies the whole snapshot; later ones only what the
//! checkpoints since wrote (their patches and new bases).

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use marco_core::snapshot::{PagedFooter, TRAILER};
use marco_core::store::{ShardFiles, DIR_FILE};
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
    let r = match drop_stale(&files_dir, &lr) {
        Ok(()) => fetch(&c, &auth, &base, &lr, &files_dir).await,
        Err(e) => Err(e),
    };
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
        if !keep.contains(n.as_str()) && !n.ends_with(".part") {
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

/// A snapshot file's footer, read from its last page and the JSON before it.
fn read_footer(p: &Path) -> Result<PagedFooter> {
    let mut f = std::fs::File::open(p).with_context(|| format!("open {}", p.display()))?;
    let len = f.metadata()?.len();
    let mut trailer = [0u8; TRAILER];
    f.seek(SeekFrom::Start(len.saturating_sub(TRAILER as u64)))?;
    f.read_exact(&mut trailer)?;
    let r = PagedFooter::footer_range(len, &trailer)?;
    let mut json = vec![0u8; (r.end - r.start) as usize];
    f.seek(SeekFrom::Start(r.start))?;
    f.read_exact(&mut json)?;
    PagedFooter::parse(&json, r.start)
}

/// What a snapshot file's footer says it is: its build, and a shard file's
/// shard. None for one that doesn't read as a snapshot file.
fn file_build(p: &Path) -> Option<(u64, Option<u64>)> {
    let f = read_footer(p).ok()?;
    Some((f.meta.get("build")?.as_u64()?, f.meta.get("shard").and_then(Value::as_u64)))
}

/// Removes the files in `files_dir` that the lease names but another build
/// wrote. A file is kept by name and size, and two builds name theirs
/// alike: every build's directory file is `directory.marco`, and a fresh
/// build's shard files are `shard-NNN-00000000.marco`. So another build's
/// (a peer that rebuilt) of the same size would pass for the checkpoint's,
/// and the snapshot would never open. A local file of the lease's size is
/// kept only if its footer names the lease's build and, for a shard file,
/// the shard the manifest has it under, as the snapshot's open checks; one
/// whose footer doesn't read goes too. The same goes for a download that
/// finished but wasn't renamed into place. One still partial doesn't read
/// as a snapshot file yet and resumes: the fetched file's checksums cover
/// every page, so a resume onto another build's pages that differ fails
/// them and is removed.
fn drop_stale(files_dir: &Path, lr: &LeaseResp) -> Result<()> {
    let build = lr.manifest.get("build").and_then(Value::as_u64);
    let shards: BTreeMap<u32, ShardFiles> =
        lr.manifest.get("shards").and_then(|s| serde_json::from_value(s.clone()).ok()).unwrap_or_default();
    let shard_of: HashMap<&str, u64> =
        shards.iter().flat_map(|(&k, f)| f.files().map(move |n| (n, u64::from(k)))).collect();
    for f in &lr.files {
        // the shard its footer must name: none for the directory file, and
        // unchecked for a name the manifest doesn't place
        let want = if f.name == DIR_FILE { Some(None) } else { shard_of.get(f.name.as_str()).map(|&k| Some(k)) };
        let ours = |(b, shard): (u64, Option<u64>)| Some(b) == build && want.is_none_or(|w| shard == w);
        let whole = files_dir.join(&f.name);
        if std::fs::metadata(&whole).is_ok_and(|m| m.len() == f.bytes) && !file_build(&whole).is_some_and(ours) {
            std::fs::remove_file(&whole).with_context(|| format!("remove {}", whole.display()))?;
        }
        let part = files_dir.join(format!("{}.part", f.name));
        if part.exists() && file_build(&part).is_some_and(|got| !ours(got)) {
            std::fs::remove_file(&part).with_context(|| format!("remove {}", part.display()))?;
        }
    }
    Ok(())
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
    // unpaced: this reads the local disk, not marco's
    let engine = marco_core::store::Storage::from_env()?.engine_paced(marco_core::store::io::Rate::UNLIMITED)?;
    for f in &lr.files {
        let p = files_dir.join(&f.name);
        // a file is renamed into place only after it passed its checks, and never changes under its
        // name within a build (drop_stale took out another build's)
        if std::fs::metadata(&p).is_ok_and(|m| m.len() == f.bytes) {
            continue;
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
        if std::fs::metadata(&part)?.len() != f.bytes {
            // kept: the next run resumes it
            bail!("{}: {} bytes fetched, the lease says {}", f.name, std::fs::metadata(&part)?.len(), f.bytes);
        }
        let (part2, engine2) = (part.clone(), engine.clone());
        let ok = tokio::task::spawn_blocking(move || marco_core::store::verify_file(&part2, &engine2)).await?;
        if let Err(e) = ok {
            std::fs::remove_file(&part)?;
            bail!("{}: failed its checksums, removed: {e:#}", f.name);
        }
        std::fs::rename(&part, &p)?;
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

    fn start_server(
        rt: &tokio::runtime::Runtime,
        dir: &Path,
    ) -> (PathBuf, std::net::SocketAddr, PathBuf, Arc<marco_server::app::App>) {
        let snap = dir.join("snap");
        marco_server::synth::write(&snap, 300, 4).unwrap();
        let token = dir.join("export-token");
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
        (snap, addr, token, app)
    }

    fn biggest(snap: &Path) -> (String, Vec<u8>) {
        let mut best = (String::new(), Vec::new());
        for e in std::fs::read_dir(snap).unwrap().flatten() {
            let n = e.file_name().to_string_lossy().into_owned();
            if n.ends_with(".json") {
                continue;
            }
            let b = std::fs::read(e.path()).unwrap();
            if b.len() > best.1.len() {
                best = (n, b);
            }
        }
        best
    }

    fn args_for(addr: std::net::SocketAddr, token: &Path, dir: &Path) -> Args {
        Args {
            url: format!("http://{addr}"),
            token_file: Some(token.to_path_buf()),
            dir: dir.to_path_buf(),
            lease_seconds: 60,
            peek: false,
        }
    }

    /// A directory file another build left in the mirror, the same size as
    /// the checkpoint's, is fetched again rather than kept by its name and
    /// size, so the mirror opens.
    #[test]
    fn a_directory_of_another_build_is_fetched_again() {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (snap, addr, token, _app) = start_server(&rt, dir.path());
        let other = dir.path().join("other");
        marco_server::synth::write(&other, 300, 4).unwrap();
        let theirs = std::fs::read(snap.join(DIR_FILE)).unwrap();
        let stale = std::fs::read(other.join(DIR_FILE)).unwrap();
        assert!(stale.len() == theirs.len() && stale != theirs, "a same-sized directory of another build");
        let m = dir.path().join("stale");
        std::fs::create_dir_all(m.join("files")).unwrap();
        std::fs::write(m.join("files").join(DIR_FILE), &stale).unwrap();
        let r = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap();
        assert_eq!(r["fetched"], r["files"]);
        assert_eq!(std::fs::read(m.join("files").join(DIR_FILE)).unwrap(), theirs);
        assert!(marco_core::Graph::open(&m.join("current")).is_ok());
        // the checkpoint's own directory is kept
        let again = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap();
        assert_eq!(again["fetched"], 0);
        // a finished download of another build's, never renamed into place, is fetched again too
        let m = dir.path().join("stale-part");
        std::fs::create_dir_all(m.join("files")).unwrap();
        std::fs::write(m.join("files").join(format!("{DIR_FILE}.part")), &stale).unwrap();
        let r = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap();
        assert_eq!((r["fetched"].clone(), r["resumed"].clone()), (r["files"].clone(), json!(0)));
        assert_eq!(std::fs::read(m.join("files").join(DIR_FILE)).unwrap(), theirs);
        assert!(marco_core::Graph::open(&m.join("current")).is_ok());
    }

    /// `snap`'s shard files, by name, with their bytes.
    fn shard_files(snap: &Path) -> Vec<(String, Vec<u8>)> {
        let mut v: Vec<(String, Vec<u8>)> = std::fs::read_dir(snap)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("shard-"))
            .map(|n| {
                let b = std::fs::read(snap.join(&n)).unwrap();
                (n, b)
            })
            .collect();
        v.sort();
        v
    }

    /// Shard files another build left in the mirror under the checkpoint's
    /// names and sizes are fetched again rather than kept by name and size,
    /// so the mirror opens; the checkpoint's own directory file beside them
    /// is kept. A finished download of one that was never renamed into
    /// place is fetched again too.
    #[test]
    fn shard_files_of_another_build_are_fetched_again() {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (snap, addr, token, _app) = start_server(&rt, dir.path());
        let other = dir.path().join("other");
        marco_server::synth::write(&other, 300, 4).unwrap();
        let (ours, theirs) = (shard_files(&snap), shard_files(&other));
        let shape = |v: &[(String, Vec<u8>)]| v.iter().map(|(n, b)| (n.clone(), b.len())).collect::<Vec<_>>();
        assert_eq!(shape(&ours), shape(&theirs), "another build's shard files: the same names and sizes");
        assert!(ours.iter().zip(&theirs).all(|(a, b)| a.1 != b.1));
        let m = dir.path().join("stale");
        let files = m.join("files");
        std::fs::create_dir_all(&files).unwrap();
        std::fs::copy(snap.join(DIR_FILE), files.join(DIR_FILE)).unwrap();
        for (n, b) in &theirs {
            std::fs::write(files.join(n), b).unwrap();
        }
        let r = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap();
        assert_eq!(r["fetched"], json!(theirs.len()), "every shard file, and not the directory");
        for (n, b) in &ours {
            assert_eq!(&std::fs::read(files.join(n)).unwrap(), b, "{n}");
        }
        assert!(marco_core::Graph::open(&m.join("current")).is_ok());
        let again = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap();
        assert_eq!(again["fetched"], 0, "the checkpoint's own files are kept");
        // a finished download of another build's, never renamed into place
        let m = dir.path().join("stale-part");
        let files = m.join("files");
        std::fs::create_dir_all(&files).unwrap();
        let (name, stale) = &theirs[0];
        std::fs::write(files.join(format!("{name}.part")), stale).unwrap();
        let r = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap();
        assert_eq!((r["fetched"].clone(), r["resumed"].clone()), (r["files"].clone(), json!(0)));
        assert_eq!(std::fs::read(files.join(name)).unwrap(), ours[0].1);
        assert!(marco_core::Graph::open(&m.join("current")).is_ok());
    }

    /// A file of the checkpoint's size is kept only if its footer is the one
    /// the manifest names: one whose footer doesn't read, and this build's
    /// file of another shard under a shard's name, are fetched again. A
    /// checkpoint's patches read as their shard's and are kept. Without the
    /// manifest's shards, the build alone is checked.
    #[test]
    fn a_file_whose_footer_isnt_the_manifests_is_fetched_again() {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (snap, addr, token, app) = start_server(&rt, dir.path());
        let ours = shard_files(&snap);
        // a whole file of zeros: no footer
        let m = dir.path().join("zeros");
        let files = m.join("files");
        std::fs::create_dir_all(&files).unwrap();
        let (name, bytes) = &ours[0];
        std::fs::write(files.join(name), vec![0u8; bytes.len()]).unwrap();
        let r = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap();
        assert_eq!(r["fetched"], r["files"]);
        assert_eq!(&std::fs::read(files.join(name)).unwrap(), bytes);
        assert!(marco_core::Graph::open(&m.join("current")).is_ok());
        // this build's file of one shard under another's name, the same size
        let (a, b) = ours
            .iter()
            .enumerate()
            .flat_map(|(i, a)| ours[i + 1..].iter().map(move |b| (a, b)))
            .find(|(a, b)| a.1.len() == b.1.len())
            .expect("two shard files of one size");
        let m = dir.path().join("swapped");
        let files = m.join("files");
        std::fs::create_dir_all(&files).unwrap();
        std::fs::write(files.join(&a.0), &b.1).unwrap();
        let r = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap();
        assert_eq!(r["fetched"], r["files"]);
        assert_eq!(std::fs::read(files.join(&a.0)).unwrap(), a.1);
        assert!(marco_core::Graph::open(&m.join("current")).is_ok());
        // a checkpoint's patches are this build's files of their shards
        rt.block_on(async {
            let ops = vec![marco_server::app::Op::Add {
                uri: format!("at://{}/app.bsky.graph.follow/3lzzzzzzzzzz2", marco_server::synth::did(7)),
                subject: marco_server::synth::did(1),
                list: None,
                time_us: marco_server::app::now_ms() * 1000,
            }];
            app.apply(&ops, "t1").await.unwrap();
            app.checkpoint().await.unwrap();
        });
        let r = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap();
        assert!(r["fetched"].as_u64().unwrap() >= 1);
        let again = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap();
        assert_eq!(again["fetched"], 0, "the checkpoint's patches are kept");
        // a lease whose manifest doesn't place its files: the build alone counts
        let build = PagedFooter::read(&std::fs::read(snap.join(DIR_FILE)).unwrap()).unwrap().meta["build"].clone();
        let lease = |manifest: Value| LeaseResp {
            lease: String::new(),
            checkpoint: 0,
            manifest,
            files: vec![FileInfo { name: a.0.clone(), bytes: b.1.len() as u64 }],
        };
        let files = dir.path().join("unplaced");
        std::fs::create_dir_all(&files).unwrap();
        std::fs::write(files.join(&a.0), &b.1).unwrap();
        drop_stale(&files, &lease(json!({"build": build}))).unwrap();
        assert!(files.join(&a.0).exists(), "kept: this build's, and nothing says which shard");
        drop_stale(&files, &lease(json!({"build": build.as_u64().unwrap() + 1}))).unwrap();
        assert!(!files.join(&a.0).exists(), "another build's");
    }

    /// A partial download of another build's file resumes, since it doesn't
    /// read as a snapshot file yet. The fetched file's checksums cover every
    /// page, so a resume onto pages that differ fails them: the file is
    /// removed, and the next sync fetches it from the start.
    #[test]
    fn a_partial_download_of_another_build_fails_once() {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (snap, addr, token, _app) = start_server(&rt, dir.path());
        // another graph: two builds of the same one can share a file's pages
        let other = dir.path().join("other");
        marco_server::synth::write(&other, 301, 4).unwrap();
        let (name, bytes) = shard_files(&snap).into_iter().max_by_key(|(_, b)| b.len()).unwrap();
        let stale = std::fs::read(other.join(&name)).unwrap();
        let m = dir.path().join("mirror");
        let files = m.join("files");
        std::fs::create_dir_all(&files).unwrap();
        let half = stale.len().min(bytes.len()) / 2 / 4096 * 4096;
        assert!(half > 0 && stale[..half] != bytes[..half], "another build's pages, two or more");
        let part = files.join(format!("{name}.part"));
        std::fs::write(&part, &stale[..half]).unwrap();
        let e = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap_err();
        assert!(format!("{e:#}").contains("checksum"), "{e:#}");
        assert!(!part.exists() && !m.join("current").exists());
        let r = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap();
        assert_eq!(r["resumed"], 0);
        assert_eq!(std::fs::read(files.join(&name)).unwrap(), bytes);
        assert!(marco_core::Graph::open(&m.join("current")).is_ok());
    }

    /// A partial file is finished with Range, and a file already there by name and size isn't fetched.
    #[test]
    fn resumes_a_partial_file_and_skips_whole_ones() {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (snap, addr, token, _app) = start_server(&rt, dir.path());
        let (name, bytes) = biggest(&snap);
        let m = dir.path().join("mirror");
        let files = m.join("files");
        std::fs::create_dir_all(&files).unwrap();
        let half = bytes.len() / 2 / 4096 * 4096;
        std::fs::write(files.join(format!("{name}.part")), &bytes[..half]).unwrap();
        // a whole file under another name, kept as is: it's skipped by name and size, not read
        let (other, ob) = std::fs::read_dir(&snap)
            .unwrap()
            .flatten()
            .map(|e| (e.file_name().to_string_lossy().into_owned(), e.path()))
            .filter(|(n, _)| n != &name && !n.ends_with(".json"))
            .map(|(n, p)| (n, std::fs::read(p).unwrap()))
            .next()
            .unwrap();
        std::fs::write(files.join(&other), &ob).unwrap();
        let r = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap();
        assert_eq!(r["resumed"], 1);
        assert_eq!(r["fetched"].as_u64().unwrap() + 1, r["files"].as_u64().unwrap());
        assert_eq!(
            r["bytes_fetched"].as_u64().unwrap(),
            r["bytes_total"].as_u64().unwrap() - ob.len() as u64 - half as u64
        );
        assert_eq!(std::fs::read(files.join(&name)).unwrap(), bytes);
        assert!(!files.join(format!("{name}.part")).exists());
    }

    /// A fetched file that fails its section checksums fails the sync and is removed, so the next
    /// run fetches it again and succeeds.
    #[test]
    fn a_corrupt_fetched_file_is_removed() {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (snap, addr, token, _app) = start_server(&rt, dir.path());
        let (name, bytes) = biggest(&snap);
        let m = dir.path().join("mirror");
        let files = m.join("files");
        std::fs::create_dir_all(&files).unwrap();
        let half = bytes.len() / 2 / 4096 * 4096;
        let mut bad = bytes[..half].to_vec();
        for page in bad.chunks_mut(4096) {
            page[100] ^= 0xff;
        }
        let part = files.join(format!("{name}.part"));
        std::fs::write(&part, &bad).unwrap();
        let e = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap_err();
        assert!(format!("{e:#}").contains("checksum"), "{e:#}");
        assert!(!part.exists() && !files.join(&name).exists());
        assert!(!m.join("current").exists());
        let r = rt.block_on(sync(&args_for(addr, &token, &m))).unwrap();
        assert_eq!(r["resumed"], 0);
        assert_eq!(std::fs::read(files.join(&name)).unwrap(), bytes);
    }
}
