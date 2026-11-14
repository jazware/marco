//! `seek eval`: ranking quality on recorded mentions, searched as their
//! author would: the author is the viewer (their follows and mutuals from
//! marco), the query every prefix of the mentioned account's handle. It
//! sweeps follows-first between a boost of a few nats and the strict wall,
//! and measures second degree (accounts the viewer's follows follow) with one
//! marco Intersect per global candidate.

use std::io::Read;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use rustc_hash::FxHashSet;
use seek_core::did::key_of;
use seek_core::index::{Index, NAT};
use seek_core::overlay::Overlay;
use seek_core::query::{self, FollowPolicy, Params, Viewer};
use serde_json::{json, Value};

use crate::marco::Marco;
use crate::metrics::QueryHists;

#[derive(clap::Args)]
pub struct Args {
    #[arg(long)]
    artifact: PathBuf,
    #[arg(long)]
    marco: String,
    /// (author, mentioned, time_us) rows, ClickHouse RowBinary, zstd.
    #[arg(long)]
    mentions: PathBuf,
    #[arg(long, default_value_t = 2000)]
    cases: usize,
    /// Follow boosts to sweep, in nats; `wall` is the strict wall.
    #[arg(long, value_delimiter = ',', default_value = "0,1,2,3,4,5,6,8,10,wall")]
    boosts: Vec<String>,
    #[arg(long, default_value_t = 400)]
    second_degree_cases: usize,
    /// Second degree's weight: this many nats per factor of e of common follows.
    #[arg(long, default_value_t = 1.0)]
    second_degree_nats: f64,
    #[arg(long, default_value_t = 7)]
    seed: u64,
}

const L: usize = 8;

fn rows(p: &PathBuf) -> Result<Vec<(String, String)>> {
    let mut raw = Vec::new();
    zstd::Decoder::new(std::fs::File::open(p)?)?.read_to_end(&mut raw)?;
    let mut out = Vec::new();
    let mut i = 0;
    let varint = |b: &[u8], i: &mut usize| -> usize {
        let (mut v, mut s) = (0usize, 0);
        loop {
            let c = b[*i];
            *i += 1;
            v |= ((c & 0x7f) as usize) << s;
            if c < 0x80 {
                return v;
            }
            s += 7;
        }
    };
    while i < raw.len() {
        let n = varint(&raw, &mut i);
        let a = String::from_utf8_lossy(&raw[i..i + n]).into_owned();
        i += n;
        let n = varint(&raw, &mut i);
        let t = String::from_utf8_lossy(&raw[i..i + n]).into_owned();
        i += n + 8;
        out.push((a, t));
    }
    Ok(out)
}

fn handle_prefixes(ix: &Index, id: u32) -> Vec<String> {
    let mut h = None;
    ix.keys_of(id, |k, t| {
        if t == seek_core::keys::HANDLE {
            h = Some(String::from_utf8_lossy(k).split(' ').next().unwrap_or("").to_string());
        }
    });
    let mut out = Vec::new();
    let mut acc = String::new();
    for c in h.unwrap_or_default().chars().take(L) {
        acc.push(c);
        out.push(acc.clone());
    }
    out
}

#[derive(Default)]
struct Tally {
    pos: Vec<[Option<usize>; L]>,
}

impl Tally {
    fn add(&mut self, mut p: [Option<usize>; L], n: usize) {
        for i in n.max(1)..L {
            p[i] = p[n.max(1) - 1];
        }
        self.pos.push(p);
    }
    fn report(&self) -> Value {
        let n = self.pos.len().max(1) as f64;
        let at = |k: usize, i: usize| self.pos.iter().filter(|p| p[i].is_some_and(|x| x < k)).count() as f64 / n;
        let chars: f64 = self
            .pos
            .iter()
            .map(|p| p.iter().position(|x| x.is_some_and(|x| x < 3)).map_or(L as f64 + 1.0, |i| i as f64 + 1.0))
            .sum::<f64>()
            / n;
        json!({"cases": self.pos.len(), "top1_at_2": at(1, 1), "top3_at_3": at(3, 2), "top10_at_4": at(10, 3),
            "top3_by_chars": (0..L).map(|i| at(3, i)).collect::<Vec<_>>(), "chars_to_top3_mean": chars})
    }
}

pub async fn run(a: Args) -> Result<Value> {
    let ix = Index::open(&a.artifact, false)?;
    let ov = Overlay::new(&ix);
    let marco = Marco::new(&a.marco, std::time::Duration::from_secs(10), 50_000)?;
    let hists = QueryHists::new();
    let mut all = rows(&a.mentions)?;
    let mut x = a.seed | 1;
    for i in (1..all.len()).rev() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        all.swap(i, (x % (i as u64 + 1)) as usize);
    }
    let policies: Vec<(String, FollowPolicy)> = a
        .boosts
        .iter()
        .map(|b| match b.as_str() {
            "wall" => ("wall".to_string(), FollowPolicy::Wall),
            n => (format!("boost_{n}"), FollowPolicy::Boost((n.parse::<f64>().unwrap_or(0.0) * NAT as f64) as i32)),
        })
        .collect();
    let mut out_t = Tally::default();
    let mut tallies: Vec<Tally> = policies.iter().map(|_| Tally::default()).collect();
    let (mut sd_base, mut sd_boost) = (Tally::default(), Tally::default());
    let mut sd_ns = Vec::new();
    let (mut cases, mut followed) = (0usize, 0usize);
    let t0 = Instant::now();
    for (author, target) in all {
        if cases >= a.cases {
            break;
        }
        let Some(tid) = ix.id_of(&target) else { continue };
        if ix.hidden(tid) {
            continue;
        }
        let ps = handle_prefixes(&ix, tid);
        if ps.is_empty() {
            continue;
        }
        let (Ok(f), Ok(m)) = (marco.follows(&author, &hists).await, marco.mutuals(&author, &hists).await) else {
            continue;
        };
        let ids = |v: &[String]| -> Vec<u32> { v.iter().filter_map(|d| ov.id_of(&ix, &key_of(d))).collect() };
        let (fi, mi) = (ids(&f), ids(&m));
        if fi.contains(&tid) {
            followed += 1;
        }
        let v = Viewer::build(&ix, &ov, &fi, &mi);
        cases += 1;
        let mut lo = [None; L];
        for (i, q) in ps.iter().enumerate() {
            let hits = query::merge(&query::parts(&ix, &ov, None, &query::prepare(q), 10, &Params::default()), 10);
            lo[i] = hits.iter().position(|h| h.id == tid);
        }
        out_t.add(lo, ps.len());
        for ((_, pol), t) in policies.iter().zip(tallies.iter_mut()) {
            let p = Params { policy: *pol, ..Default::default() };
            let mut li = [None; L];
            for (i, q) in ps.iter().enumerate() {
                let hits = query::merge(&query::parts(&ix, &ov, Some(&v), &query::prepare(q), 10, &p), 10);
                li[i] = hits.iter().position(|h| h.id == tid);
            }
            t.add(li, ps.len());
        }
        if cases <= a.second_degree_cases {
            // second degree on the global candidates, wall policy, the first four prefixes
            let p = Params::default();
            let (mut base, mut boosted) = ([None; L], [None; L]);
            let fset: FxHashSet<u32> = fi.iter().copied().collect();
            for (i, q) in ps.iter().take(4).enumerate() {
                let mut parts = query::parts(&ix, &ov, Some(&v), &query::prepare(q), 20, &p);
                base[i] = query::merge(&parts, 10).iter().position(|h| h.id == tid);
                let t = Instant::now();
                for h in parts.global.iter_mut().filter(|h| !fset.contains(&h.id)) {
                    let Some(d) = ix.did(h.id) else { continue };
                    let n = marco.common_follows(&author, &d, &hists).await.unwrap_or(0);
                    h.score += (a.second_degree_nats * NAT as f64 * (1.0 + n as f64).ln()) as i64;
                }
                sd_ns.push(t.elapsed().as_nanos() as u64);
                boosted[i] = query::merge(&parts, 10).iter().position(|h| h.id == tid);
            }
            sd_base.add(base, ps.len().min(4));
            sd_boost.add(boosted, ps.len().min(4));
        }
    }
    sd_ns.sort_unstable();
    let p = |q: f64| sd_ns.get(((sd_ns.len().max(1) - 1) as f64 * q) as usize).map(|&n| n as f64 / 1e3);
    let mut sweep = serde_json::Map::new();
    for ((name, _), t) in policies.iter().zip(&tallies) {
        sweep.insert(name.clone(), t.report());
    }
    Ok(json!({
        "cases": cases, "target_followed_by_author": followed as f64 / cases.max(1) as f64, "secs": t0.elapsed().as_secs_f64(),
        "logged_out": out_t.report(), "logged_in": sweep,
        "second_degree": {"cases": sd_base.pos.len(), "nats_per_e": a.second_degree_nats, "wall": sd_base.report(), "wall_plus_second_degree": sd_boost.report(),
            "intersects_per_query_us": {"p50": p(0.5), "p99": p(0.99)}},
    }))
}
