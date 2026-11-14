//! seek-proto: the typeahead design's measurements on a marco snapshot.
//!
//! ```text
//! seek-proto bench --snapshot SNAP --ranks R.u16 --indeg R.indeg --handles H --profiles P [--mentions M]
//!                  [--old-ranks O.u16 --old-indeg O.indeg --cutoff-us T]
//! ```
//!
//! Loads handles and display names (zstd RowBinary exports), builds the prefix
//! index over every actor in the snapshot's directory, then measures index
//! size, logged-out and logged-in query latency (follows, mutuals and blocks
//! from the snapshot in-process), prints the top results of some prefixes under
//! PageRank and under follower counts, and scores both rankings on recorded
//! mentions and on follows made after `--cutoff-us`.

mod fold;
mod index;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use rayon::prelude::*;
use serde_json::{json, Value};

use marco_core::rkey::NON_TID;
use marco_core::{Dir, Graph, ListRef};

use index::{actor, Index, Weights};

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const FOLLOW: u8 = 0;
const BLOCK: u8 = 1;
const K: usize = 10;
const MUTUAL: i32 = 4400;

#[derive(Parser)]
struct Cli {
    #[arg(long, global = true)]
    report: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Bench(Args),
}

#[derive(clap::Args)]
struct Args {
    #[arg(long)]
    snapshot: PathBuf,
    #[arg(long)]
    ranks: PathBuf,
    #[arg(long)]
    indeg: PathBuf,
    #[arg(long)]
    handles: PathBuf,
    #[arg(long)]
    profiles: PathBuf,
    /// Quality-weighted followers (marco-load pagerank's .wf.u16), a third ranking.
    #[arg(long)]
    wf: Option<PathBuf>,
    #[arg(long)]
    old_wf: Option<PathBuf>,
    /// The third ranking's name in the report.
    #[arg(long, default_value = "quality_followers")]
    alt_name: String,
    #[arg(long)]
    mentions: Option<PathBuf>,
    #[arg(long)]
    old_ranks: Option<PathBuf>,
    #[arg(long)]
    old_indeg: Option<PathBuf>,
    #[arg(long)]
    cutoff_us: Option<u64>,
    /// Prefixes covering more postings than this keep a top list.
    #[arg(long, default_value_t = 2048)]
    heavy: usize,
    #[arg(long, default_value_t = 3000)]
    targets: usize,
    #[arg(long, default_value_t = 1000)]
    viewers: usize,
    #[arg(long, default_value_t = 20000)]
    eval: usize,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "a,j,ja,jay,al,alex,mar,mark,pa,paul,the,nyt,news,bsky,art,sam,kat,dr,ma,ch"
    )]
    examples: Vec<String>,
    #[arg(long, default_value_t = 7)]
    seed: u64,
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

// ---------------------------------------------------------------- input

struct Rb<R: Read>(BufReader<R>);
impl<R: Read> Rb<R> {
    fn varint(&mut self) -> Option<u64> {
        let mut v = 0u64;
        let mut s = 0;
        loop {
            let mut b = [0u8];
            if self.0.read(&mut b).ok()? == 0 {
                return None;
            }
            v |= ((b[0] & 0x7f) as u64) << s;
            if b[0] & 0x80 == 0 {
                return Some(v);
            }
            s += 7;
        }
    }
    fn string(&mut self) -> Option<String> {
        let n = self.varint()? as usize;
        let mut b = vec![0u8; n];
        self.0.read_exact(&mut b).ok()?;
        Some(String::from_utf8_lossy(&b).into_owned())
    }
    fn u8(&mut self) -> Option<u8> {
        let mut b = [0u8];
        self.0.read_exact(&mut b).ok()?;
        Some(b[0])
    }
    fn i64(&mut self) -> Option<i64> {
        let mut b = [0u8; 8];
        self.0.read_exact(&mut b).ok()?;
        Some(i64::from_le_bytes(b))
    }
}

fn rb(p: &Path) -> Result<Rb<zstd::Decoder<'static, BufReader<std::fs::File>>>> {
    let f = std::fs::File::open(p).with_context(|| format!("open {}", p.display()))?;
    Ok(Rb(BufReader::with_capacity(1 << 20, zstd::Decoder::new(f)?)))
}

fn read_u16(p: &Path) -> Result<Vec<u16>> {
    let b = std::fs::read(p)?;
    Ok(b.as_chunks::<2>().0.iter().map(|c| u16::from_le_bytes(*c)).collect())
}
fn read_u32(p: &Path) -> Result<Vec<u32>> {
    let b = std::fs::read(p)?;
    Ok(b.as_chunks::<4>().0.iter().map(|c| u32::from_le_bytes(*c)).collect())
}

/// ln(1 + followers) scaled to u16, the follower-count ranking.
fn followers_rank(indeg: &[u32]) -> Vec<u16> {
    let hi = (1.0 + *indeg.iter().max().unwrap_or(&1) as f64).ln();
    indeg.par_iter().map(|&d| ((1.0 + d as f64).ln() / hi * 65535.0).round() as u16).collect()
}

struct Names {
    handle: Vec<Box<str>>,
    name: Vec<Box<str>>,
    hosting: HashSet<String>,
}

fn load_names(g: &Graph, n: u32, handles: &Path, profiles: &Path) -> Result<(Names, Value)> {
    let t = Instant::now();
    let mut handle: Vec<Box<str>> = vec![Box::from(""); n as usize];
    let mut name: Vec<Box<str>> = vec![Box::from(""); n as usize];
    let mut r = rb(handles)?;
    let (mut rows, mut hit) = (0u64, 0u64);
    loop {
        let mut batch = Vec::with_capacity(1 << 20);
        while batch.len() < 1 << 20 {
            let Some(d) = r.string() else { break };
            let Some(h) = r.string() else { break };
            batch.push((d, h));
        }
        if batch.is_empty() {
            break;
        }
        rows += batch.len() as u64;
        let ids: Vec<Option<u32>> = batch.par_iter().map(|(d, _)| g.id_of(d).filter(|&i| i < n)).collect();
        for ((_, h), id) in batch.into_iter().zip(ids) {
            if let Some(i) = id {
                handle[i as usize] = h.into_boxed_str();
                hit += 1;
            }
        }
    }
    let mut r = rb(profiles)?;
    let (mut prows, mut phit) = (0u64, 0u64);
    loop {
        let mut batch = Vec::with_capacity(1 << 20);
        while batch.len() < 1 << 20 {
            let Some(d) = r.string() else { break };
            let Some(dn) = r.string() else { break };
            let Some(last) = r.u8() else { break };
            if last != 3 && !dn.trim().is_empty() {
                batch.push((d, dn));
            }
            prows += 1;
        }
        if batch.is_empty() {
            break;
        }
        let ids: Vec<Option<u32>> = batch.par_iter().map(|(d, _)| g.id_of(d).filter(|&i| i < n)).collect();
        for ((_, dn), id) in batch.into_iter().zip(ids) {
            if let Some(i) = id {
                name[i as usize] = dn.into_boxed_str();
                phit += 1;
            }
        }
    }
    let mut suffix: HashMap<String, u64> = HashMap::new();
    for h in &handle {
        let l: Vec<&str> = h.split('.').collect();
        if l.len() > 2 {
            *suffix.entry(l[l.len() - 2..].join(".")).or_default() += 1;
        }
    }
    let mut hs: Vec<(String, u64)> = suffix.into_iter().filter(|(_, c)| *c >= 1000).collect();
    hs.sort_by_key(|x| std::cmp::Reverse(x.1));
    let names_bytes: usize = handle.iter().chain(name.iter()).map(|s| s.len()).sum();
    let report = json!({"secs": t.elapsed().as_secs_f64(), "handle_rows": rows, "handles_in_directory": hit,
        "profile_rows": prows, "names_in_directory": phit, "hosting_suffixes": hs.len(),
        "hosting_top": hs.iter().take(12).map(|(s, c)| json!([s, c])).collect::<Vec<_>>(), "name_bytes": names_bytes});
    let hosting = hs.into_iter().map(|(s, _)| s).collect();
    Ok((Names { handle, name, hosting }, report))
}

fn build_index(names: &Names, n: u32, rank: &[u16], w: &Weights, heavy: usize) -> (Index, Value) {
    let t = Instant::now();
    let idx = index::build(
        n,
        |id, buf, out| index::keys(&names.handle[id as usize], &names.name[id as usize], &names.hosting, buf, out),
        rank,
        w,
        heavy,
    );
    let secs = t.elapsed().as_secs_f64();
    let s = idx.sizes();
    let actors = names.handle.iter().zip(&names.name).filter(|(h, d)| !h.is_empty() || !d.is_empty()).count();
    let total = s.blob + s.heads + s.post + s.heavy_bytes;
    (
        idx,
        json!({"secs": secs, "actors_indexed": actors, "entries": s.entries, "keys_per_actor": s.entries as f64 / actors as f64,
        "blob_bytes": s.blob, "heads_bytes": s.heads, "post_bytes": s.post, "heavy_prefixes": s.heavy_prefixes,
        "heavy_bytes": s.heavy_bytes, "total_bytes": total, "bytes_per_actor": total as f64 / actors as f64,
        "rank_bytes": rank.len() * 2}),
    )
}

// ---------------------------------------------------------------- queries

fn pct(mut v: Vec<u64>) -> Value {
    if v.is_empty() {
        return json!(null);
    }
    v.sort_unstable();
    let p = |q: f64| v[((v.len() as f64 - 1.0) * q).round() as usize] as f64 / 1e3;
    json!({"n": v.len(), "p50_us": p(0.5), "p90_us": p(0.9), "p99_us": p(0.99), "p999_us": p(0.999), "max_us": p(1.0),
        "mean_us": v.iter().sum::<u64>() as f64 / v.len() as f64 / 1e3})
}

fn prefixes(s: &str, max: usize) -> Vec<String> {
    let f = fold::fold(s);
    let mut out = Vec::new();
    let mut acc = String::new();
    for c in f.chars().take(max) {
        acc.push(c);
        if !acc.ends_with(' ') {
            out.push(acc.clone());
        }
    }
    out
}

/// What a user would type for `id`: the first label of the handle, and the display name.
fn typed(names: &Names, id: u32) -> Vec<String> {
    let mut v = Vec::new();
    let h = &names.handle[id as usize];
    if !h.is_empty() && &**h != "handle.invalid" {
        v.push(h.split('.').next().unwrap_or("").to_string());
    }
    if !names.name[id as usize].is_empty() {
        v.push(names.name[id as usize].to_string());
    }
    v
}

/// The viewer's side of a logged-in query: their follows' keys, with mutuals marked.
struct Personal {
    follows: Vec<u32>,
    mutual: HashSet<u32>,
    keys: Vec<(Box<[u8]>, u32)>,
}

fn personal(g: &Graph, names: &Names, v: u32, drop: Option<u32>) -> (Personal, u64, u64) {
    let t = Instant::now();
    let mut follows = g.read(|cx| g.out_set(cx, FOLLOW, v)).expect("out_set");
    if let Some(d) = drop {
        follows.retain(|&f| f != d);
    }
    let mutual: HashSet<u32> = g
        .read(|cx| {
            g.intersect(
                cx,
                ListRef { rel: FOLLOW, dir: Dir::Out, actor: v },
                ListRef { rel: FOLLOW, dir: Dir::In, actor: v },
                usize::MAX,
            )
        })
        .expect("intersect")
        .into_iter()
        .collect();
    let marco_ns = t.elapsed().as_nanos() as u64;
    let mut keys = Vec::new();
    let mut buf = String::new();
    let mut ks = Vec::new();
    for &f in &follows {
        if (f as usize) < names.handle.len() {
            index::keys(&names.handle[f as usize], &names.name[f as usize], &names.hosting, &mut buf, &mut ks);
            for (k, tr) in ks.drain(..) {
                keys.push((k.into_bytes().into_boxed_slice(), f | tr << 30));
            }
        }
    }
    keys.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    (Personal { follows, mutual, keys }, marco_ns, t.elapsed().as_nanos() as u64)
}

fn personal_bytes(p: &Personal) -> usize {
    p.keys.iter().map(|(k, _)| k.len() + 4 + 16).sum::<usize>() + p.follows.len() * 4 + p.mutual.len() * 8
}

#[derive(Default)]
struct QStat {
    marco_ns: u64,
}

/// Logged-in: matching follows first (mutuals boosted), then the global fill,
/// then one block filter over the page (exact handle matches kept, as the reference AppView does).
#[allow(clippy::too_many_arguments)]
fn search_in(
    g: &Graph,
    idx: &Index,
    p: &Personal,
    v: u32,
    q: &str,
    rank: &[u16],
    w: &Weights,
    st: &mut QStat,
    out: &mut Vec<u32>,
) {
    out.clear();
    let qb = q.as_bytes();
    let lo = p.keys.partition_point(|(k, _)| &**k < qb);
    let mut mine: Vec<(i32, u32)> = Vec::new();
    for (k, post) in &p.keys[lo..] {
        if !k.starts_with(qb) {
            break;
        }
        let a = actor(*post);
        let mut s = index::score(rank, w, *post) + if p.mutual.contains(&a) { MUTUAL } else { 0 };
        if &**k == qb && index::tier(*post) == index::HANDLE {
            s += 1 << 20;
        }
        mine.push((s, *post));
    }
    mine.sort_unstable_by_key(|x| std::cmp::Reverse(x.0));
    let mut seen = HashSet::new();
    let mut page: Vec<u32> = Vec::with_capacity(K + 4);
    let mut exact_global = Vec::new();
    {
        let (lo, hi) = idx.range(qb);
        idx.exact_handles(qb, lo, hi, &mut exact_global);
    }
    for p in exact_global.iter().map(|&p| actor(p)) {
        if seen.insert(p) {
            page.push(p);
        }
    }
    for (_, post) in mine {
        if page.len() >= K + 4 {
            break;
        }
        if seen.insert(actor(post)) {
            page.push(actor(post));
        }
    }
    let mut fill = Vec::new();
    if page.len() < K + 4 {
        idx.search(qb, K + 4, rank, w, |a| p.follows.binary_search(&a).is_ok() || seen.contains(&a), &mut fill);
        for f in fill {
            if page.len() >= K + 4 {
                break;
            }
            if seen.insert(actor(f)) {
                page.push(actor(f));
            }
        }
    }
    let t = Instant::now();
    let keep: HashSet<u32> = g
        .read(|cx| g.filter_id(cx, v, &page, &[(BLOCK, Dir::Out), (BLOCK, Dir::In)]))
        .expect("filter")
        .into_iter()
        .collect();
    st.marco_ns += t.elapsed().as_nanos() as u64;
    let exact: HashSet<u32> = exact_global.iter().map(|&p| actor(p)).collect();
    out.extend(page.into_iter().filter(|a| keep.contains(a) || exact.contains(a)).take(K));
}

fn search_out(idx: &Index, q: &str, rank: &[u16], w: &Weights, out: &mut Vec<u32>) -> (usize, bool) {
    let mut v = Vec::new();
    let r = idx.search(q.as_bytes(), K, rank, w, |_| false, &mut v);
    out.clear();
    out.extend(v.into_iter().map(actor));
    r
}

fn len_bucket(l: usize) -> &'static str {
    match l {
        1 => "1",
        2 => "2",
        3 => "3",
        4..=6 => "4-6",
        _ => "7+",
    }
}

fn latency_out(idx: &Index, qs: &[String], rank: &[u16], w: &Weights) -> Value {
    let mut out = Vec::new();
    for q in qs.iter().take(2000) {
        search_out(idx, q, rank, w, &mut out);
    }
    let mut all = Vec::new();
    let mut by: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
    let (mut heavy, mut scanned) = (0usize, Vec::new());
    for q in qs {
        let t = Instant::now();
        let (n, h) = search_out(idx, q, rank, w, &mut out);
        let ns = t.elapsed().as_nanos() as u64;
        all.push(ns);
        by.entry(len_bucket(q.chars().count())).or_default().push(ns);
        if h {
            heavy += 1;
        } else {
            scanned.push(n as u64 * 1000);
        }
    }
    let t = Instant::now();
    let n: usize = qs
        .par_iter()
        .map_init(Vec::new, |o, q| {
            search_out(idx, q, rank, w, o);
            o.len()
        })
        .sum();
    let par = t.elapsed().as_secs_f64();
    let mut sc = scanned.clone();
    sc.sort_unstable();
    json!({"all": pct(all), "by_prefix_chars": by.into_iter().map(|(k, v)| (k.to_string(), pct(v))).collect::<BTreeMap<_, _>>(),
        "served_from_top_list": heavy as f64 / qs.len() as f64,
        "scanned_postings_p50": sc.get(sc.len() / 2).map(|x| x / 1000), "scanned_postings_p99": sc.get(sc.len() * 99 / 100).map(|x| x / 1000),
        "parallel_qps": qs.len() as f64 / par, "parallel_threads": rayon::current_num_threads(), "results": n})
}

// ---------------------------------------------------------------- run

fn run(a: &Args) -> Result<Value> {
    let mut rep = BTreeMap::new();
    let t = Instant::now();
    let g = Graph::open(&a.snapshot)?;
    let loaded = g.snapshot().unwrap().load();
    rep.insert("graph_open_secs", json!(t.elapsed().as_secs_f64()));
    rep.insert("graph_loaded", json!(loaded));
    let n = g.base_n();
    let pr = read_u16(&a.ranks)?;
    let indeg = read_u32(&a.indeg)?;
    if pr.len() != n as usize || indeg.len() != n as usize {
        bail!("ranks {} / indeg {} for {} ids", pr.len(), indeg.len(), n);
    }
    let fol = followers_rank(&indeg);
    let (names, nrep) = load_names(&g, n, &a.handles, &a.profiles)?;
    eprintln!("names: {nrep}");
    rep.insert("names", nrep);
    let w = Weights::default();

    let (idx, irep) = build_index(&names, n, &pr, &w, a.heavy);
    eprintln!("index: {irep}");
    rep.insert("index", irep);

    // PageRank order, for reading examples
    let mut order: Vec<u32> = (0..n).collect();
    order.par_sort_unstable_by(|&x, &y| pr[y as usize].cmp(&pr[x as usize]));
    let mut pos = vec![0u32; n as usize];
    for (i, &id) in order.iter().enumerate() {
        pos[id as usize] = i as u32;
    }
    let mut by_fol: Vec<u32> = (0..n).collect();
    by_fol.par_sort_unstable_by(|&x, &y| indeg[y as usize].cmp(&indeg[x as usize]));
    let mut fpos = vec![0u32; n as usize];
    for (i, &id) in by_fol.iter().enumerate() {
        fpos[id as usize] = i as u32;
    }
    let top_fol_1k_outside_pr_10k = by_fol[..1000].iter().filter(|&&i| pos[i as usize] >= 10_000).count();
    let top_fol_10k_outside_pr_100k = by_fol[..10_000].iter().filter(|&&i| pos[i as usize] >= 100_000).count();
    let top_pr_1k_outside_fol_10k = order[..1000].iter().filter(|&&i| fpos[i as usize] >= 10_000).count();
    // the most-followed accounts PageRank puts furthest down: anonymised shape only
    let mut demoted: Vec<(u32, u32, u32)> =
        by_fol[..10_000].iter().map(|&i| (i, fpos[i as usize], pos[i as usize])).collect();
    demoted.sort_by_key(|x| std::cmp::Reverse(x.2));
    rep.insert(
        "pr_vs_followers",
        json!({"top_followers_1k_outside_pr_top_10k": top_fol_1k_outside_pr_10k,
            "top_followers_10k_outside_pr_top_100k": top_fol_10k_outside_pr_100k,
            "top_pr_1k_outside_followers_top_10k": top_pr_1k_outside_fol_10k,
            "most_demoted": demoted.iter().take(25).map(|&(i, f, p)| json!({"handle": &*names.handle[i as usize], "followers": indeg[i as usize], "followers_pos": f, "pr_pos": p})).collect::<Vec<_>>()}),
    );

    // query sets: popular targets (from PageRank's top million) and uniform ones (any actor with a follower)
    let mut rng = Rng(a.seed | 1);
    let with_follower: Vec<u32> =
        (0..n).into_par_iter().filter(|&i| indeg[i as usize] > 0 && !names.handle[i as usize].is_empty()).collect();
    let mut qs_pop = Vec::new();
    let mut qs_uni = Vec::new();
    for _ in 0..a.targets {
        let t = order[rng.below(1_000_000) as usize];
        for s in typed(&names, t) {
            qs_pop.extend(prefixes(&s, 12));
        }
        let t = with_follower[rng.below(with_follower.len() as u64) as usize];
        for s in typed(&names, t) {
            qs_uni.extend(prefixes(&s, 12));
        }
    }
    let lo_pop = latency_out(&idx, &qs_pop, &pr, &w);
    let lo_uni = latency_out(&idx, &qs_uni, &pr, &w);
    eprintln!("logged out: popular {lo_pop}\nuniform {lo_uni}");
    rep.insert("logged_out", json!({"popular_targets": lo_pop, "uniform_targets": lo_uni}));

    // mention cases: (author, mentioned) pairs, parsed up front so their authors are also a viewer pool
    let mut mention_rows = 0u64;
    let mention_cases: Option<Vec<(u32, u32)>> = match &a.mentions {
        None => None,
        Some(m) => {
            let mut r = rb(m)?;
            let mut cases = Vec::new();
            let mut prng = Rng(a.seed.wrapping_mul(31) | 1);
            while let (Some(au), Some(tg), Some(_)) = (r.string(), r.string(), r.i64()) {
                mention_rows += 1;
                if prng.below(8) != 0 && mention_rows > 2000 {
                    continue;
                }
                if let (Some(v), Some(t)) = (g.id_of(&au), g.id_of(&tg)) {
                    if v < n && t < n && v != t && !names.handle[t as usize].is_empty() {
                        cases.push((v, t));
                    }
                }
            }
            shuffle(&mut cases, &mut prng);
            cases.truncate(a.eval);
            Some(cases)
        }
    };
    let active: Vec<u32> = {
        let mut v: Vec<u32> = mention_cases.iter().flatten().map(|&(au, _)| au).collect();
        v.sort_unstable();
        v.dedup();
        v
    };
    // logged in: viewers uniform over actors who follow someone, active posters, and the heaviest followers
    let outdeg: Vec<u32> = {
        let mut d = vec![0u32; n as usize];
        for (i, x) in d.iter_mut().enumerate() {
            *x = g.read(|cx| g.out_count(cx, FOLLOW, i as u32)).unwrap_or(0) as u32;
        }
        d
    };
    let follows_someone: Vec<u32> = (0..n).filter(|&i| outdeg[i as usize] > 0).collect();
    let mut heavy_viewers: Vec<u32> = follows_someone.clone();
    heavy_viewers.par_sort_unstable_by(|&x, &y| outdeg[y as usize].cmp(&outdeg[x as usize]));
    heavy_viewers.truncate(10_000);
    let mut li = BTreeMap::new();
    let mut pools =
        vec![("uniform_viewers", &follows_someone, a.viewers), ("heaviest_10k_viewers", &heavy_viewers, a.viewers / 5)];
    if !active.is_empty() {
        pools.insert(1, ("mention_authors", &active, a.viewers));
    }
    for (label, pool, nv) in pools {
        let (mut build_ns, mut marco_build_ns, mut bytes, mut nf) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let (mut q_ns, mut q_marco, mut by) = (Vec::new(), Vec::new(), BTreeMap::<&str, Vec<u64>>::new());
        let (mut sd_ns, mut out) = (Vec::new(), Vec::new());
        for _ in 0..nv {
            let v = pool[rng.below(pool.len() as u64) as usize];
            let (p, mns, ns) = personal(&g, &names, v, None);
            build_ns.push(ns);
            marco_build_ns.push(mns);
            bytes.push(personal_bytes(&p) as u64 * 1000);
            nf.push(p.follows.len() as u64 * 1000);
            let mut qs = Vec::new();
            for _ in 0..3 {
                if !p.follows.is_empty() {
                    let f = p.follows[rng.below(p.follows.len() as u64) as usize];
                    for s in typed(&names, f) {
                        qs.extend(prefixes(&s, 8));
                    }
                }
                let t = order[rng.below(1_000_000) as usize];
                for s in typed(&names, t) {
                    qs.extend(prefixes(&s, 8));
                }
            }
            for q in &qs {
                let mut st = QStat::default();
                let t = Instant::now();
                search_in(&g, &idx, &p, v, q, &pr, &w, &mut st, &mut out);
                let ns = t.elapsed().as_nanos() as u64;
                q_ns.push(ns);
                q_marco.push(st.marco_ns);
                by.entry(len_bucket(q.chars().count())).or_default().push(ns);
            }
            // second degree: how many of the viewer's follows follow each of the global fill's first 20
            for q in qs.iter().take(4) {
                let mut fill = Vec::new();
                idx.search(q.as_bytes(), 20, &pr, &w, |x| p.follows.binary_search(&x).is_ok(), &mut fill);
                let t = Instant::now();
                let mut tot = 0usize;
                for c in fill.iter().map(|&x| actor(x)) {
                    tot += g.read(|cx| g.in_intersect(cx, FOLLOW, c, &p.follows, 1000)).map(|v| v.len()).unwrap_or(0);
                }
                std::hint::black_box(tot);
                sd_ns.push(t.elapsed().as_nanos() as u64);
            }
        }
        let thousand = |v: Vec<u64>| {
            let mut v = v;
            v.sort_unstable();
            let p = |q: f64| v[((v.len() as f64 - 1.0) * q).round() as usize] / 1000;
            json!({"p50": p(0.5), "p90": p(0.9), "p99": p(0.99), "max": p(1.0)})
        };
        li.insert(
            label,
            json!({"viewers": nv, "follows": thousand(nf), "personal_build": pct(build_ns), "personal_build_marco": pct(marco_build_ns),
                "personal_bytes": thousand(bytes), "query": pct(q_ns), "query_block_filter": pct(q_marco),
                "query_by_prefix_chars": by.into_iter().map(|(k, v)| (k.to_string(), pct(v))).collect::<BTreeMap<_, _>>(),
                "second_degree_20_candidates": pct(sd_ns)}),
        );
        eprintln!("logged in {label}: {}", li[label]);
    }
    rep.insert("logged_in", json!(li));

    // examples: PageRank against follower counts on the same index shape
    let (fidx, _) = build_index(&names, n, &fol, &w, a.heavy);
    let wf = match &a.wf {
        Some(p) => read_u16(p)?,
        None => fol.clone(),
    };
    let (widx, _) = build_index(&names, n, &wf, &w, a.heavy);
    let show = |ids: &[u32]| -> Vec<Value> {
        ids.iter()
            .map(|&i| json!({"handle": &*names.handle[i as usize], "name": &*names.name[i as usize], "followers": indeg[i as usize], "pr_pos": pos[i as usize], "followers_pos": fpos[i as usize]}))
            .collect()
    };
    let mut ex = BTreeMap::new();
    for q in &a.examples {
        let f = fold::fold(q);
        let (mut x, mut y, mut z) = (Vec::new(), Vec::new(), Vec::new());
        search_out(&idx, &f, &pr, &w, &mut x);
        search_out(&fidx, &f, &fol, &w, &mut y);
        search_out(&widx, &f, &wf, &w, &mut z);
        let mut e = json!({"pagerank": show(&x), "followers": show(&y)});
        e[a.alt_name.as_str()] = json!(show(&z));
        ex.insert(q.clone(), e);
    }
    rep.insert("examples", json!(ex));

    // mentions: what people typed into a composer's typeahead, on the current graph
    if let Some(cases) = &mention_cases {
        let cases = cases.clone();
        let e = evaluate(
            &g,
            &names,
            &cases,
            &[("pagerank", &idx, &pr), ("followers", &fidx, &fol), (a.alt_name.as_str(), &widx, &wf)],
            &w,
            false,
        );
        rep.insert("eval_mentions", json!({"rows": mention_rows, "cases": cases.len(), "result": e}));
        eprintln!("mentions: {}", rep["eval_mentions"]);
    }
    drop(widx);
    drop(fidx);
    drop(idx);

    // follows made after the cutoff, ranked by the graph before it
    if let (Some(or), Some(oi), Some(cut)) = (&a.old_ranks, &a.old_indeg, a.cutoff_us) {
        let opr = read_u16(or)?;
        let ofol = followers_rank(&read_u32(oi)?);
        let (oidx, _) = build_index(&names, n, &opr, &w, a.heavy);
        let (ofidx, _) = build_index(&names, n, &ofol, &w, a.heavy);
        let owf = match &a.old_wf {
            Some(p) => read_u16(p)?,
            None => ofol.clone(),
        };
        let (owidx, _) = build_index(&names, n, &owf, &w, a.heavy);
        let mut cases = Vec::new();
        let mut tries = 0;
        while cases.len() < a.eval && tries < 50_000_000 {
            tries += 1;
            let v = follows_someone[rng.below(follows_someone.len() as u64) as usize];
            let recs = g.read(|cx| g.out_all(cx, FOLLOW, v)).unwrap_or_default();
            let new: Vec<u32> =
                recs.iter().filter(|(k, s)| k & NON_TID == 0 && (k >> 10) >= cut && *s != v).map(|&(_, s)| s).collect();
            if new.is_empty() {
                continue;
            }
            let t = new[rng.below(new.len() as u64) as usize];
            if t < n && !names.handle[t as usize].is_empty() {
                cases.push((v, t));
            }
        }
        let e = evaluate(
            &g,
            &names,
            &cases,
            &[("pagerank", &oidx, &opr), ("followers", &ofidx, &ofol), (a.alt_name.as_str(), &owidx, &owf)],
            &w,
            true,
        );
        rep.insert("eval_new_follows", json!({"cutoff_us": cut, "cases": cases.len(), "result": e}));
        eprintln!("new follows: {}", rep["eval_new_follows"]);
    }
    Ok(json!(rep))
}

fn shuffle<T>(v: &mut [T], rng: &mut Rng) {
    for i in (1..v.len()).rev() {
        v.swap(i, rng.below(i as u64 + 1) as usize);
    }
}

/// For each (viewer, target) and each typed prefix of the target's handle, where the target lands
/// in the top K: logged out under each ranking, and logged in (follows first) under each.
fn evaluate(
    g: &Graph,
    names: &Names,
    cases: &[(u32, u32)],
    variants: &[(&str, &Index, &[u16])],
    w: &Weights,
    drop_target: bool,
) -> Value {
    const L: usize = 8;
    let res: Vec<Vec<[Option<usize>; L]>> = cases
        .par_iter()
        .map(|&(v, t)| {
            let h = names.handle[t as usize].split('.').next().unwrap_or("").to_string();
            let ps = prefixes(&h, L);
            let (p, _, _) = personal(g, names, v, if drop_target { Some(t) } else { None });
            let mut rows = Vec::new();
            let mut out = Vec::new();
            for (_, idx, rank) in variants {
                let mut lo = [None; L];
                let mut li = [None; L];
                for (i, q) in ps.iter().enumerate() {
                    search_out(idx, q, rank, w, &mut out);
                    lo[i] = out.iter().position(|&x| x == t);
                    let mut st = QStat::default();
                    search_in(g, idx, &p, v, q, rank, w, &mut st, &mut out);
                    li[i] = out.iter().position(|&x| x == t);
                }
                // prefixes past the handle's end keep the full handle's result
                for i in ps.len()..L {
                    lo[i] = if ps.is_empty() { None } else { lo[ps.len() - 1] };
                    li[i] = if ps.is_empty() { None } else { li[ps.len() - 1] };
                }
                rows.push(lo);
                rows.push(li);
            }
            let followed = p.follows.binary_search(&t).is_ok();
            let mut r = rows;
            r.push([if followed { Some(0) } else { None }; L]);
            r
        })
        .collect();
    let mut out = BTreeMap::new();
    let followed = res.iter().filter(|r| r.last().unwrap()[0].is_some()).count();
    out.insert("target_followed_by_viewer".to_string(), json!(followed as f64 / res.len().max(1) as f64));
    for (vi, (name, _, _)) in variants.iter().enumerate() {
        for (mi, mode) in ["logged_out", "logged_in"].iter().enumerate() {
            let col = vi * 2 + mi;
            let at = |k: usize, i: usize| {
                res.iter().filter(|r| r[col][i].is_some_and(|p| p < k)).count() as f64 / res.len().max(1) as f64
            };
            let ks: Vec<f64> = res
                .iter()
                .map(|r| {
                    r[col].iter().position(|p| p.is_some_and(|p| p < 3)).map_or(L as f64 + 1.0, |i| i as f64 + 1.0)
                })
                .collect();
            let mut ksorted = ks.clone();
            ksorted.sort_by(f64::total_cmp);
            out.insert(
                format!("{name}/{mode}"),
                json!({"top1_by_chars": (0..L).map(|i| at(1, i)).collect::<Vec<_>>(),
                    "top3_by_chars": (0..L).map(|i| at(3, i)).collect::<Vec<_>>(),
                    "top10_by_chars": (0..L).map(|i| at(10, i)).collect::<Vec<_>>(),
                    "chars_to_top3_mean": ks.iter().sum::<f64>() / ks.len().max(1) as f64,
                    "chars_to_top3_median": ksorted.get(ksorted.len() / 2)}),
            );
        }
    }
    json!(out)
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let r = match cli.cmd {
        Cmd::Bench(a) => run(&a)?,
    };
    let s = serde_json::to_string_pretty(&r)?;
    if let Some(p) = cli.report {
        std::fs::write(p, &s)?;
    }
    println!("{s}");
    Ok(())
}
