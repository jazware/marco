//! Where artifacts come from and go to. A pod polls a source: the builder's
//! HTTP endpoint (`http(s)://…/artifact/latest`, with an ETag), an S3
//! prefix (`s3://bucket/prefix`, whose `latest.json` names the newest), or a
//! local file or directory. The builder writes a directory (newest few plus
//! `latest.seek`) and, when a bucket is configured, uploads there too.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::metrics::*;

/// The bucket's `latest.json`, and what the builder's HTTP endpoint says in headers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Latest {
    pub version: String,
    pub key: String,
    pub bytes: u64,
    pub cursor_us: u64,
    pub created_ms: i64,
}

#[derive(Deserialize)]
struct Creds {
    access_key_id: String,
    secret_access_key: String,
}

/// An S3-compatible bucket: `s3://bucket/prefix`, the endpoint, region and a credentials file
/// (`{"access_key_id": …, "secret_access_key": …}`).
pub struct Bucket {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl Bucket {
    pub fn open(url: &str, endpoint: Option<&str>, region: &str, creds: Option<&Path>) -> Result<Bucket> {
        let rest = url.strip_prefix("s3://").context("a bucket URL is s3://bucket/prefix")?;
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let mut b = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region)
            .with_virtual_hosted_style_request(false);
        if let Some(e) = endpoint {
            b = b.with_endpoint(e).with_allow_http(e.starts_with("http://"));
        }
        if let Some(p) = creds {
            let c: Creds = serde_json::from_slice(&std::fs::read(p).with_context(|| format!("read {}", p.display()))?)
                .context("credentials file")?;
            b = b.with_access_key_id(c.access_key_id).with_secret_access_key(c.secret_access_key);
        }
        Ok(Bucket { store: Arc::new(b.build()?), prefix: prefix.trim_matches('/').to_string() })
    }

    fn key(&self, name: &str) -> ObjPath {
        if self.prefix.is_empty() {
            ObjPath::from(name)
        } else {
            ObjPath::from(format!("{}/{name}", self.prefix))
        }
    }

    pub async fn latest(&self) -> Result<Option<Latest>> {
        match self.store.get(&self.key("latest.json")).await {
            Ok(r) => Ok(Some(serde_json::from_slice(&r.bytes().await?)?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Uploads `file` as `name`, then points `latest.json` at it.
    pub async fn publish(&self, file: &Path, latest: &Latest) -> Result<()> {
        let up = self.store.put_multipart(&self.key(&latest.key)).await?;
        let mut w = object_store::WriteMultipart::new_with_chunk_size(up, 32 << 20);
        let mut f = tokio::fs::File::open(file).await?;
        let mut buf = vec![0u8; 8 << 20];
        loop {
            let n = tokio::io::AsyncReadExt::read(&mut f, &mut buf).await?;
            if n == 0 {
                break;
            }
            w.wait_for_capacity(4).await?;
            w.write(&buf[..n]);
        }
        w.finish().await?;
        self.store.put(&self.key("latest.json"), serde_json::to_vec(latest)?.into()).await?;
        Ok(())
    }

    /// Deletes artifacts other than the newest `keep` (by key, which sorts by time).
    pub async fn prune(&self, keep: usize) -> Result<()> {
        let mut keys: Vec<ObjPath> = Vec::new();
        let mut l = self.store.list(Some(&self.key("")));
        while let Some(m) = l.next().await {
            let m = m?;
            if m.location.as_ref().ends_with(".seek") {
                keys.push(m.location);
            }
        }
        keys.sort();
        for k in &keys[..keys.len().saturating_sub(keep.max(1))] {
            self.store.delete(k).await?;
        }
        Ok(())
    }

    async fn download(&self, latest: &Latest, to: &Path) -> Result<u64> {
        let r = self.store.get(&self.key(&latest.key)).await?;
        let mut s = r.into_stream();
        let mut f = tokio::fs::File::create(to).await?;
        let mut n = 0u64;
        while let Some(chunk) = s.next().await {
            let c = chunk?;
            n += c.len() as u64;
            DOWNLOAD_BYTES.inc_by(c.len() as u64);
            f.write_all(&c).await?;
        }
        f.sync_all().await?;
        Ok(n)
    }
}

pub enum Source {
    Dir(PathBuf),
    Http { url: String, client: reqwest::Client },
    S3(Bucket),
}

/// A newer artifact, downloaded to `path`.
pub struct Fetched {
    pub path: PathBuf,
    pub version: String,
    pub bytes: u64,
}

impl Source {
    pub fn parse(src: &str, endpoint: Option<&str>, region: &str, creds: Option<&Path>) -> Result<Source> {
        if src.starts_with("http://") || src.starts_with("https://") {
            let client = vlatproto::http::base("seek").build()?;
            Ok(Source::Http { url: src.to_string(), client })
        } else if src.starts_with("s3://") {
            Ok(Source::S3(Bucket::open(src, endpoint, region, creds)?))
        } else {
            Ok(Source::Dir(PathBuf::from(src.strip_prefix("file://").unwrap_or(src))))
        }
    }

    /// The newest artifact if it isn't `have`, downloaded into `dir`.
    pub async fn poll(&self, have: Option<&str>, dir: &Path) -> Result<Option<Fetched>> {
        let t = Instant::now();
        let f = match self {
            Source::Dir(d) => {
                // a directory holds latest.seek, a link to the newest; a file is itself
                let p = if d.is_dir() { d.join("latest.seek") } else { d.clone() };
                let target = std::fs::canonicalize(&p).with_context(|| format!("{}", p.display()))?;
                let version = target.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
                if Some(version.as_str()) == have {
                    return Ok(None);
                }
                let to = dir.join(format!("{version}.seek"));
                if to != target {
                    tokio::fs::copy(&target, &to).await?;
                }
                let bytes = std::fs::metadata(&to)?.len();
                DOWNLOAD_BYTES.inc_by(bytes);
                Fetched { path: to, version, bytes }
            }
            Source::Http { url, client } => {
                let mut req = client.get(url.as_str());
                if let Some(h) = have {
                    req = req.header(http::header::IF_NONE_MATCH, format!("\"{h}\""));
                }
                let resp = req.send().await?;
                if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
                    return Ok(None);
                }
                if !resp.status().is_success() {
                    bail!("{url}: {}", resp.status());
                }
                let version = resp
                    .headers()
                    .get("x-seek-version")
                    .and_then(|v| v.to_str().ok())
                    .filter(|v| !v.is_empty() && !v.contains('/'))
                    .context("no x-seek-version header")?
                    .to_string();
                if Some(version.as_str()) == have {
                    return Ok(None);
                }
                let to = dir.join(format!("{version}.seek"));
                let tmp = to.with_extension("part");
                let mut f = tokio::fs::File::create(&tmp).await?;
                let mut s = resp.bytes_stream();
                let mut n = 0u64;
                while let Some(c) = s.next().await {
                    let c = c?;
                    n += c.len() as u64;
                    DOWNLOAD_BYTES.inc_by(c.len() as u64);
                    f.write_all(&c).await?;
                }
                f.sync_all().await?;
                tokio::fs::rename(&tmp, &to).await?;
                Fetched { path: to, version, bytes: n }
            }
            Source::S3(b) => {
                let Some(l) = b.latest().await? else { return Ok(None) };
                if Some(l.version.as_str()) == have {
                    return Ok(None);
                }
                let to = dir.join(format!("{}.seek", l.version));
                let tmp = to.with_extension("part");
                let n = b.download(&l, &tmp).await?;
                tokio::fs::rename(&tmp, &to).await?;
                Fetched { path: to, version: l.version, bytes: n }
            }
        };
        DOWNLOAD_SECONDS.with(&[]).since(t);
        Ok(Some(f))
    }
}
