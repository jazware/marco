//! `seek bench`: query latency through a running pod's API, the way an
//! AppView calls it (gRPC over h2c, one query at a time), logged out and in,
//! at each trace ratio the pod's trace switch is set to.
//!
//! Queries are every prefix (up to 12 characters) of the handle's first label
//! and the display name of actors drawn from the artifact: half from the top
//! million by rank ("popular"), half uniformly. Logged in, viewers come from
//! a file of DIDs (one a line), each asking a few targets' prefixes.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use connectrpc::client::{ClientConfig, HttpClient};
use connectrpc::Protocol;
use seek_api::bsky::{SearchActorsTypeaheadRequest, SearchTypeaheadParams};
use seek_api::connect::bsky::ServiceClient;
use seek_core::index::Index;
use serde_json::{json, Value};

#[derive(clap::Args)]
pub struct Args {
    /// The pod's API.
    #[arg(long, default_value = "http://127.0.0.1:7310")]
    target: String,
    /// The pod's ops listener, to set the trace ratio for each pass.
    #[arg(long, default_value = "http://127.0.0.1:7311")]
    ops: String,
    /// The artifact the pod serves (for query targets).
    #[arg(long)]
    artifact: PathBuf,
    /// Viewer DIDs for logged-in queries, one a line.
    #[arg(long)]
    viewers: Option<PathBuf>,
    #[arg(long, default_value_t = 1000)]
    targets: usize,
    #[arg(long, default_value_t = 300)]
    viewer_count: usize,
    /// Trace ratios to run each pass at.
    #[arg(long, value_delimiter = ',', default_value = "0")]
    ratios: Vec<f64>,
    #[arg(long, default_value_t = 7)]
    seed: u64,
}

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n.max(1)
    }
}

fn pct(mut v: Vec<u64>) -> Value {
    if v.is_empty() {
        return Value::Null;
    }
    v.sort_unstable();
    let p = |q: f64| v[((v.len() as f64 - 1.0) * q).round() as usize] as f64 / 1e3;
    json!({"n": v.len(), "p50_us": p(0.5), "p90_us": p(0.9), "p99_us": p(0.99), "max_us": p(1.0),
        "mean_us": v.iter().sum::<u64>() as f64 / v.len() as f64 / 1e3})
}

/// What a user would type for actor `id`: prefixes of its folded handle's first word and its name.
fn typed(ix: &Index, id: u32) -> Vec<String> {
    let mut out = Vec::new();
    ix.keys_of(id, |k, t| {
        if t == seek_core::keys::HANDLE || t == seek_core::keys::NAME {
            let s = String::from_utf8_lossy(k);
            let s = if t == seek_core::keys::HANDLE { s.split(' ').next().unwrap_or("").to_string() } else { s.to_string() };
            let mut acc = String::new();
            for c in s.chars().take(12) {
                acc.push(c);
                if !acc.ends_with(' ') {
                    out.push(acc.clone());
                }
            }
        }
    });
    out
}

pub async fn run(a: Args) -> Result<Value> {
    let ix = Index::open(&a.artifact, false)?;
    let n = ix.len();
    let mut order: Vec<u32> = (0..n).collect();
    let ranks = ix.ranks();
    order.sort_unstable_by_key(|&i| std::cmp::Reverse(ranks[i as usize]));
    let mut rng = Rng(a.seed | 1);
    let mut qs = Vec::new();
    for _ in 0..a.targets {
        qs.extend(typed(&ix, order[rng.below(order.len().min(1_000_000) as u64) as usize]));
        qs.extend(typed(&ix, rng.below(n as u64) as u32));
    }
    let viewers: Vec<String> = match &a.viewers {
        Some(p) => std::fs::read_to_string(p)?.lines().map(str::trim).filter(|l| l.starts_with("did:")).map(String::from).collect(),
        None => Vec::new(),
    };
    let cfg = ClientConfig::new(a.target.parse().context("--target")?).with_protocol(Protocol::Grpc);
    let c = ServiceClient::new(HttpClient::plaintext_http2_only(), cfg);
    let http = reqwest::Client::new();
    let ask = |q: String, viewer: String| {
        let c = &c;
        async move {
            let t = Instant::now();
            let r = c
                .search_actors_typeahead(SearchActorsTypeaheadRequest {
                    params: SearchTypeaheadParams { query: q, viewer, limit: 10, ..Default::default() }.into(),
                    ..Default::default()
                })
                .await;
            (t.elapsed().as_nanos() as u64, r.is_ok())
        }
    };
    for q in qs.iter().take(500) {
        ask(q.clone(), String::new()).await;
    }
    let mut out = serde_json::Map::new();
    for &ratio in &a.ratios {
        let _ = http.post(format!("{}/debug/tracing?ratio={ratio}", a.ops.trim_end_matches('/'))).send().await;
        let mut lo = Vec::with_capacity(qs.len());
        let mut errs = 0;
        let t = Instant::now();
        for q in &qs {
            let (ns, ok) = ask(q.clone(), String::new()).await;
            lo.push(ns);
            errs += !ok as u64;
        }
        let lo_secs = t.elapsed().as_secs_f64();
        let mut li = Vec::new();
        let mut first = Vec::new();
        let mut lerrs = 0;
        for v in viewers.iter().take(a.viewer_count) {
            let mut vq = Vec::new();
            for _ in 0..3 {
                vq.extend(typed(&ix, order[rng.below(order.len().min(1_000_000) as u64) as usize]).into_iter().take(6));
            }
            for (i, q) in vq.into_iter().enumerate() {
                let (ns, ok) = ask(q, v.clone()).await;
                lerrs += !ok as u64;
                if i == 0 { first.push(ns) } else { li.push(ns) }
            }
        }
        out.insert(
            format!("ratio_{ratio}"),
            json!({"logged_out": pct(lo), "logged_out_qps_one_client": qs.len() as f64 / lo_secs, "logged_out_errors": errs,
                "logged_in": pct(li), "logged_in_first_query": pct(first), "logged_in_errors": lerrs}),
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let _ = http.post(format!("{}/debug/tracing?ratio=0", a.ops.trim_end_matches('/'))).send().await;
    // what one histogram observation costs on the keystroke path (a query makes four to six)
    let h = crate::hist::seconds("seek_bench_hist_seconds", "the bench's own histogram", &[]).with(&[]);
    let t = Instant::now();
    for i in 0..10_000_000u64 {
        h.observe(std::hint::black_box(i & 0xffff));
    }
    let hist_ns = t.elapsed().as_nanos() as f64 / 1e7;
    Ok(json!({"histogram_observe_ns": hist_ns, "queries": qs.len(), "viewers": viewers.len().min(a.viewer_count), "actors": n, "passes": out}))
}
