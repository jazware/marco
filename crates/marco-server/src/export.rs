//! Checkpoint export on the admin listener: another process (seek's
//! builder, a replica being seeded, a backup) copies the current checkpoint
//! file by file while this one keeps serving, checkpointing and compacting.
//!
//! ```text
//! GET    /admin/checkpoint?ttl=SECS        a lease: the manifest, every file it names (bytes, xxh3)
//! GET    /admin/checkpoint?peek=1          the checkpoint number only, no lease
//! GET    /admin/checkpoint/files/NAME?lease=ID   one file, Range for a resume
//! DELETE /admin/checkpoint?lease=ID        the lease's end
//! ```
//!
//! A lease hard-links the files its manifest names into a directory beside
//! the snapshot's (the same filesystem), so a compaction or a newer
//! checkpoint can delete them meanwhile and the copy still reads them. It
//! goes on DELETE or once its ttl passes. Every read goes through the
//! background I/O bucket (`--background-io`), so requests go first.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Path as UrlPath, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use marco_core::store::{Manifest, DIR_FILE, MANIFEST};
use prometheus::{register_int_counter, register_int_gauge, IntCounter, IntGauge};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::app::App;

static BYTES: LazyLock<IntCounter> =
    LazyLock::new(|| register_int_counter!("marco_export_bytes_total", "Checkpoint file bytes served").unwrap());
static FILES: LazyLock<IntCounter> =
    LazyLock::new(|| register_int_counter!("marco_export_files_total", "Checkpoint file reads served").unwrap());
static LEASES: LazyLock<IntGauge> =
    LazyLock::new(|| register_int_gauge!("marco_export_leases", "Checkpoint export leases open").unwrap());
static HASHED: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("marco_export_hashed_bytes_total", "Bytes read to sum checkpoint files for a lease").unwrap()
});

const MAX_TTL: u64 = 6 * 3600;
const CHUNK: usize = 4 << 20;

struct Lease {
    dir: PathBuf,
    expires: Instant,
}

pub struct Export {
    root: PathBuf,
    snapshot: PathBuf,
    leases: Mutex<HashMap<String, Lease>>,
    /// Shard files never change once written, so a sum is kept by (name, length).
    sums: Mutex<HashMap<(String, u64), u64>>,
    seq: std::sync::atomic::AtomicU64,
}

impl Export {
    /// Clears any leases a previous process left (`<snapshot>.export/`).
    pub fn new(snapshot: &Path) -> Arc<Export> {
        let mut root = snapshot.as_os_str().to_owned();
        root.push(".export");
        let root = PathBuf::from(root);
        let _ = std::fs::remove_dir_all(&root);
        LazyLock::force(&BYTES);
        LazyLock::force(&FILES);
        LazyLock::force(&LEASES);
        LazyLock::force(&HASHED);
        Arc::new(Export {
            root,
            snapshot: snapshot.to_path_buf(),
            leases: Mutex::new(HashMap::new()),
            sums: Mutex::new(HashMap::new()),
            seq: Default::default(),
        })
    }

    fn sweep(&self) {
        let mut l = self.leases.lock().unwrap();
        let now = Instant::now();
        l.retain(|_, x| {
            let keep = x.expires > now;
            if !keep {
                let _ = std::fs::remove_dir_all(&x.dir);
            }
            keep
        });
        LEASES.set(l.len() as i64);
    }

    /// Links the current checkpoint's files into a new lease directory.
    fn link(&self, id: &str) -> anyhow::Result<(PathBuf, Vec<u8>, Manifest)> {
        let dir = self.root.join(id);
        for attempt in 0.. {
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir)?;
            let raw = std::fs::read(self.snapshot.join(MANIFEST))?;
            let m: Manifest = serde_json::from_slice(&raw)?;
            let mut names: Vec<&str> = vec![DIR_FILE];
            names.extend(m.shards.values().flat_map(|s| s.files()));
            match names.iter().try_for_each(|f| std::fs::hard_link(self.snapshot.join(f), dir.join(f))) {
                Ok(()) => {
                    std::fs::write(dir.join(MANIFEST), &raw)?;
                    return Ok((dir, raw, m));
                }
                // a compaction removed a file the manifest named: there's a newer manifest
                Err(e) if e.kind() == std::io::ErrorKind::NotFound && attempt < 5 => continue,
                Err(e) => return Err(anyhow::anyhow!("hard-link the checkpoint (same filesystem?): {e}")),
            }
        }
        unreachable!()
    }
}

async fn paced(app: &App, n: usize) {
    let g = app.graph();
    let bucket = g.engine().background().clone();
    drop(g);
    while let Err(w) = bucket.try_take(n as u64) {
        tokio::time::sleep(w).await;
    }
}

async fn sum_file(app: &App, ex: &Export, dir: &Path, name: &str) -> std::io::Result<(u64, u64)> {
    let p = dir.join(name);
    let len = tokio::fs::metadata(&p).await?.len();
    if let Some(&s) = ex.sums.lock().unwrap().get(&(name.to_string(), len)) {
        return Ok((len, s));
    }
    let mut f = tokio::fs::File::open(&p).await?;
    let mut h = xxhash_rust::xxh3::Xxh3::new();
    let mut buf = vec![0u8; CHUNK];
    loop {
        paced(app, CHUNK).await;
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        HASHED.inc_by(n as u64);
        h.update(&buf[..n]);
    }
    let s = h.digest();
    ex.sums.lock().unwrap().insert((name.to_string(), len), s);
    Ok((len, s))
}

#[derive(Deserialize)]
pub struct Q {
    ttl: Option<u64>,
    peek: Option<u8>,
    lease: Option<String>,
}

type St = (Arc<App>, Arc<Export>);

fn err(code: StatusCode, m: impl std::fmt::Display) -> Response {
    (code, format!("{m}\n")).into_response()
}

async fn checkpoint(State((app, ex)): State<St>, Query(q): Query<Q>) -> Response {
    ex.sweep();
    if q.peek.is_some() {
        return match std::fs::read(ex.snapshot.join(MANIFEST))
            .map_err(anyhow::Error::from)
            .and_then(|b| Ok(serde_json::from_slice::<Manifest>(&b)?))
        {
            Ok(m) => axum::Json(
                serde_json::json!({"checkpoint": m.checkpoint, "build": m.build, "written_ms": m.written_ms}),
            )
            .into_response(),
            Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e),
        };
    }
    let id =
        format!("{:x}{:04x}", crate::app::now_ms(), ex.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed) & 0xffff);
    let ttl = q.ttl.unwrap_or(1800).clamp(1, MAX_TTL);
    let e2 = ex.clone();
    let id2 = id.clone();
    let linked = tokio::task::spawn_blocking(move || e2.link(&id2)).await;
    let (dir, raw, m) = match linked {
        Ok(Ok(x)) => x,
        Ok(Err(e)) => return err(StatusCode::INTERNAL_SERVER_ERROR, e),
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e),
    };
    // the lease exists before its sums are read, so a long first sum can't lose its links
    ex.leases
        .lock()
        .unwrap()
        .insert(id.clone(), Lease { dir: dir.clone(), expires: Instant::now() + Duration::from_secs(ttl) });
    LEASES.set(ex.leases.lock().unwrap().len() as i64);
    let mut names: Vec<String> = vec![DIR_FILE.to_string()];
    names.extend(m.shards.values().flat_map(|s| s.files().map(String::from)));
    let mut files = Vec::with_capacity(names.len());
    for n in names {
        match sum_file(&app, &ex, &dir, &n).await {
            Ok((bytes, sum)) => {
                files.push(serde_json::json!({"name": n, "bytes": bytes, "xxh3": format!("{sum:016x}")}))
            }
            Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, format!("{n}: {e}")),
        }
    }
    let manifest: serde_json::Value = serde_json::from_slice(&raw).unwrap_or_default();
    axum::Json(serde_json::json!({
        "lease": id, "ttl_seconds": ttl, "checkpoint": m.checkpoint, "build": m.build,
        "written_ms": m.written_ms, "manifest": manifest, "files": files,
    }))
    .into_response()
}

async fn release(State((_, ex)): State<St>, Query(q): Query<Q>) -> Response {
    let Some(id) = q.lease else { return err(StatusCode::BAD_REQUEST, "lease is required") };
    if let Some(l) = ex.leases.lock().unwrap().remove(&id) {
        let _ = std::fs::remove_dir_all(&l.dir);
    }
    LEASES.set(ex.leases.lock().unwrap().len() as i64);
    StatusCode::NO_CONTENT.into_response()
}

/// `bytes=a-` or `bytes=a-b`.
fn range(h: &HeaderMap, len: u64) -> Option<(u64, u64)> {
    let v = h.get(header::RANGE)?.to_str().ok()?.strip_prefix("bytes=")?;
    let (a, b) = v.split_once('-')?;
    let a: u64 = a.parse().ok()?;
    let b: u64 = if b.is_empty() { len.saturating_sub(1) } else { b.parse::<u64>().ok()?.min(len.saturating_sub(1)) };
    (a <= b && a < len).then_some((a, b))
}

async fn file(
    State((app, ex)): State<St>,
    UrlPath(name): UrlPath<String>,
    Query(q): Query<Q>,
    h: HeaderMap,
) -> Response {
    ex.sweep();
    let Some(id) = q.lease else { return err(StatusCode::BAD_REQUEST, "lease is required") };
    let Some(dir) = ex.leases.lock().unwrap().get(&id).map(|l| l.dir.clone()) else {
        return err(StatusCode::NOT_FOUND, "no such lease (expired?)");
    };
    if name.contains('/') || name.starts_with('.') {
        return err(StatusCode::BAD_REQUEST, "bad file name");
    }
    let p = dir.join(&name);
    let mut f = match tokio::fs::File::open(&p).await {
        Ok(f) => f,
        Err(_) => return err(StatusCode::NOT_FOUND, "not in this lease"),
    };
    let len = match f.metadata().await {
        Ok(m) => m.len(),
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e),
    };
    let (status, start, end) = match range(&h, len) {
        Some((a, b)) => (StatusCode::PARTIAL_CONTENT, a, b + 1),
        None if h.contains_key(header::RANGE) && len > 0 => return err(StatusCode::RANGE_NOT_SATISFIABLE, "bad range"),
        None => (StatusCode::OK, 0, len),
    };
    if f.seek(std::io::SeekFrom::Start(start)).await.is_err() {
        return err(StatusCode::INTERNAL_SERVER_ERROR, "seek");
    }
    FILES.inc();
    let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<bytes::Bytes>>(4);
    tokio::spawn(async move {
        let mut left = end - start;
        let mut buf = vec![0u8; CHUNK];
        while left > 0 {
            let want = (left as usize).min(CHUNK);
            paced(&app, want).await;
            match f.read(&mut buf[..want]).await {
                Ok(0) => return,
                Ok(n) => {
                    left -= n as u64;
                    BYTES.inc_by(n as u64);
                    if tx.send(Ok(bytes::Bytes::copy_from_slice(&buf[..n]))).await.is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            }
        }
    });
    let body =
        axum::body::Body::from_stream(futures::stream::unfold(
            rx,
            |mut rx| async move { rx.recv().await.map(|x| (x, rx)) },
        ));
    let mut r =
        (status, [(header::CONTENT_LENGTH, (end - start).to_string()), (header::ACCEPT_RANGES, "bytes".into())], body)
            .into_response();
    if status == StatusCode::PARTIAL_CONTENT {
        r.headers_mut().insert(header::CONTENT_RANGE, format!("bytes {start}-{}/{len}", end - 1).parse().unwrap());
    }
    r
}

/// The admin listener's export routes, behind its auth.
pub fn routes(app: &Arc<App>, ex: Arc<Export>) -> axum::Router {
    let e2 = ex.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            e2.sweep();
        }
    });
    axum::Router::new()
        .route("/admin/checkpoint", axum::routing::get(checkpoint).delete(release))
        .route("/admin/checkpoint/files/{name}", axum::routing::get(file))
        .with_state((app.clone(), ex))
}

#[cfg(test)]
mod tests {
    #[test]
    fn ranges() {
        let mut h = super::HeaderMap::new();
        assert_eq!(super::range(&h, 10), None);
        h.insert(super::header::RANGE, "bytes=3-".parse().unwrap());
        assert_eq!(super::range(&h, 10), Some((3, 9)));
        h.insert(super::header::RANGE, "bytes=3-100".parse().unwrap());
        assert_eq!(super::range(&h, 10), Some((3, 9)));
        h.insert(super::header::RANGE, "bytes=12-".parse().unwrap());
        assert_eq!(super::range(&h, 10), None);
    }
}
