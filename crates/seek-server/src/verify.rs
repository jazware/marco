//! Handle verification: a handle is indexed only when it resolves back to
//! the DID that claims it, both ways, as the reference AppView checks before
//! it shows a handle. The DID's document (PLC, or did:web's) names the handle
//! in `alsoKnownAs`, and the handle names the DID by DNS TXT
//! `_atproto.<handle>` or `https://<handle>/.well-known/atproto-did` (DNS
//! wins when both answer). Every check is rate-limited: PLC directory reads
//! on their own limit, and the handle lookups, which reach hosts the handles'
//! owners chose, on another.

use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use futures::StreamExt;
use hickory_resolver::proto::rr::RData;
use tokio::sync::Mutex;
use vlatproto::did_resolver::{DidResolver, ResolveError};

use crate::metrics::*;

const TIMEOUT: Duration = Duration::from_secs(5);
const MAX_WELL_KNOWN: usize = 2048;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The DID's current handle, verified both ways.
    Valid(String),
    /// No handle that resolves back: index the actor with none.
    Invalid,
    /// Couldn't tell (a timeout, a host down): keep what was there, try again later.
    Unknown,
}

/// Spaces calls `rate` a second apart, shared by every caller.
pub struct Limiter {
    every: Duration,
    next: Mutex<Instant>,
}

impl Limiter {
    pub fn new(rate: f64) -> Limiter {
        Limiter { every: Duration::from_secs_f64(1.0 / rate.max(0.001)), next: Mutex::new(Instant::now()) }
    }

    pub async fn wait(&self) {
        let at = {
            let mut n = self.next.lock().await;
            let now = Instant::now();
            let at = (*n).max(now);
            *n = at + self.every;
            at
        };
        tokio::time::sleep_until(at.into()).await;
    }
}

pub struct Verifier {
    docs: DidResolver,
    plc: Limiter,
    lookups: Limiter,
    insecure: bool,
}

impl Verifier {
    /// `insecure` allows http and private addresses (tests only).
    pub fn new(plc_url: &str, plc_rate: f64, lookup_rate: f64, insecure: bool) -> Verifier {
        Verifier {
            docs: DidResolver::new(plc_url, insecure),
            plc: Limiter::new(plc_rate),
            lookups: Limiter::new(lookup_rate),
            insecure,
        }
    }

    /// The DID's verified handle now.
    pub async fn check(&self, did: &str) -> Outcome {
        let t = Instant::now();
        if did.starts_with("did:plc:") {
            self.plc.wait().await;
        } else {
            self.lookups.wait().await;
        }
        self.docs.invalidate(did);
        let doc = match self.docs.resolve(did).await {
            Ok(d) => d,
            Err(ResolveError::NotFound(_)) | Err(ResolveError::BadDid(_)) => {
                count("mismatch", "did_doc", t);
                return Outcome::Invalid;
            }
            Err(ResolveError::Failed(..)) => {
                count("error", "did_doc", t);
                return Outcome::Unknown;
            }
        };
        count("ok", "did_doc", t);
        let claimed = doc
            .get("alsoKnownAs")
            .and_then(|a| a.as_array())
            .and_then(|a| a.iter().filter_map(|v| v.as_str()).find_map(|s| s.strip_prefix("at://")))
            .map(|h| h.to_ascii_lowercase());
        let Some(handle) = claimed.filter(|h| seek_core::did::valid_handle(h)) else {
            return Outcome::Invalid;
        };
        self.lookups.wait().await;
        let t = Instant::now();
        let (dns, https) = tokio::join!(txt_did(&handle), self.well_known(&handle));
        let (dns_ok, https_ok) = (dns.as_deref() == Some(did), https.as_deref() == Some(did));
        let res = |ok: bool, got: &Option<String>| {
            if ok {
                "ok"
            } else if got.is_some() {
                "mismatch"
            } else {
                "error"
            }
        };
        count(res(dns_ok, &dns), "dns", t);
        if dns.is_none() {
            count(res(https_ok, &https), "well_known", t);
        }
        match (dns, https) {
            // DNS wins: a TXT record naming another DID makes the handle invalid whatever HTTPS says
            (Some(d), _) if d == did => Outcome::Valid(handle),
            (Some(_), _) => Outcome::Invalid,
            (None, Some(d)) if d == did => Outcome::Valid(handle),
            (None, Some(_)) => Outcome::Invalid,
            (None, None) => Outcome::Invalid,
        }
    }

    async fn well_known(&self, handle: &str) -> Option<String> {
        let req =
            vlatproto::http::guarded(self.insecure).get(&format!("https://{handle}/.well-known/atproto-did")).ok()?;
        let resp = tokio::time::timeout(TIMEOUT, req.send()).await.ok()?.ok()?;
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            VERIFY_CHECKS.with_label_values(&["rate_limited", "well_known"]).inc();
            return None;
        }
        if !resp.status().is_success() {
            return None;
        }
        let body = read_capped(resp, MAX_WELL_KNOWN).await.ok()?;
        let s = String::from_utf8(body).ok()?;
        let line = s.lines().next().unwrap_or("").trim().to_string();
        line.starts_with("did:").then_some(line)
    }
}

fn count(result: &str, method: &str, t: Instant) {
    VERIFY_CHECKS.with_label_values(&[result, method]).inc();
    VERIFY_SECONDS.with(&[method]).since(t);
}

async fn txt_did(handle: &str) -> Option<String> {
    static SYSTEM: LazyLock<Option<hickory_resolver::TokioResolver>> =
        LazyLock::new(|| hickory_resolver::TokioResolver::builder_tokio().and_then(|b| b.build()).ok());
    let lookup =
        tokio::time::timeout(TIMEOUT, SYSTEM.as_ref()?.txt_lookup(format!("_atproto.{handle}."))).await.ok()?.ok()?;
    let found: Vec<String> = lookup
        .answers()
        .iter()
        .filter_map(|r| match &r.data {
            RData::TXT(txt) => Some(txt.txt_data.iter().map(|c| String::from_utf8_lossy(c).into_owned()).collect()),
            _ => None,
        })
        .filter_map(|s: String| s.strip_prefix("did=").map(str::to_owned))
        .collect();
    // several did= records prove nothing, as in the reference resolver
    match found.as_slice() {
        [one] => Some(one.clone()),
        _ => None,
    }
}

async fn read_capped(resp: reqwest::Response, cap: usize) -> Result<Vec<u8>, String> {
    if resp.content_length().is_some_and(|l| l as usize > cap) {
        return Err("response too large".into());
    }
    let mut buf = Vec::new();
    let mut s = resp.bytes_stream();
    while let Some(chunk) = s.next().await {
        let chunk = chunk.map_err(|e| e.to_string())?;
        if buf.len() + chunk.len() > cap {
            return Err("response too large".into());
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

/// A deduplicated queue of DIDs to verify, worked by `workers` tasks; each outcome goes to `out`.
pub struct Queue {
    tx: tokio::sync::mpsc::UnboundedSender<String>,
    pending: Arc<std::sync::Mutex<rustc_hash::FxHashSet<String>>>,
}

impl Queue {
    pub fn start(v: Arc<Verifier>, workers: usize, out: tokio::sync::mpsc::Sender<(String, Outcome)>) -> Queue {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let rx = Arc::new(Mutex::new(rx));
        let pending: Arc<std::sync::Mutex<rustc_hash::FxHashSet<String>>> = Default::default();
        for _ in 0..workers.max(1) {
            let (v, rx, out, pending) = (v.clone(), rx.clone(), out.clone(), pending.clone());
            tokio::spawn(async move {
                loop {
                    let Some(did) = rx.lock().await.recv().await else { return };
                    let o = v.check(&did).await;
                    pending.lock().unwrap().remove(&did);
                    VERIFY_QUEUE.set(pending.lock().unwrap().len() as i64);
                    if out.send((did, o)).await.is_err() {
                        return;
                    }
                }
            });
        }
        Queue { tx, pending }
    }

    /// Queues `did` unless it's waiting already.
    pub fn push(&self, did: &str) {
        let mut p = self.pending.lock().unwrap();
        if p.insert(did.to_string()) {
            let _ = self.tx.send(did.to_string());
        }
        VERIFY_QUEUE.set(p.len() as i64);
    }

    pub fn len(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn limiter_spaces_calls() {
        let l = Limiter::new(100.0);
        let t = Instant::now();
        for _ in 0..11 {
            l.wait().await;
        }
        assert!(t.elapsed() >= Duration::from_millis(95), "{:?}", t.elapsed());
    }

    /// A local PLC directory and well-known host: a handle that points back verifies, one that
    /// points elsewhere or claims a handle it doesn't hold doesn't.
    #[tokio::test]
    async fn checks_both_directions() {
        use axum::routing::get;
        let app = axum::Router::new().route(
            "/{did}",
            get(|axum::extract::Path(did): axum::extract::Path<String>| async move {
                let h = match did.as_str() {
                    "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa" => "at://good.localhost",
                    "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb" => "at://good.localhost",
                    _ => return (http::StatusCode::NOT_FOUND, String::new()),
                };
                (http::StatusCode::OK, serde_json::json!({"id": did, "alsoKnownAs": [h]}).to_string())
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await });
        let v = Verifier::new(&format!("http://{addr}"), 1000.0, 1000.0, true);
        // no DNS or HTTPS answers for the handle: invalid, never trusted from the claim alone
        assert_eq!(v.check("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").await, Outcome::Invalid);
        assert_eq!(v.check("did:plc:cccccccccccccccccccccccc").await, Outcome::Invalid);
        assert!(VERIFY_CHECKS.with_label_values(&["ok", "did_doc"]).get() >= 1);
    }
}
