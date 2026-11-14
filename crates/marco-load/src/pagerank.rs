//! PageRank over a snapshot's follows, written as a DID-keyed ranks file
//! (`ranks.rs`) for seek's typeahead.
//!
//! The model: damping 0.85, uniform teleport, dangling mass spread uniformly,
//! self-follows dropped, one edge per (follower, subject). marco's in sets
//! leave hidden accounts out, so their follows pass nothing. What one
//! follower passes along one edge is capped at `--max-pass` average ranks
//! (1/N each) and the excess goes to the teleport: without it the sign-up
//! hub's tens of millions of followers flow to the handful of accounts it
//! follows. Iterations are pull-based over an in-edge CSR (u32 sources per
//! subject), so each pass writes each rank once with no atomics. Sources
//! are renumbered by out-degree first, hottest first, so the contributions
//! most rows read share cache lines (0.87 s a pass to 0.34 s on the whole
//! network). It stops once the L1 change is below `--tol`.
//!
//! The daily run: `--out-dir DIR` warm-starts from `DIR/latest.ranks`, writes
//! `DIR/ranks-<UTC time>.ranks`, points `latest.ranks` at it and keeps the
//! last `--keep`. `--link DIR` first hard-links the snapshot's current
//! checkpoint into DIR, so the run can read a directory a live `marco serve`
//! keeps checkpointing and compacting.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use serde_json::{json, Value};

use marco_core::rkey::NON_TID;
use marco_core::store::{Manifest, DIR_FILE, MANIFEST};
use marco_core::Graph;

use crate::ranks::{self, Header, Row, HIDDEN};
use crate::sys;

const FOLLOW: u8 = 0;

#[derive(clap::Args, Clone)]
pub struct Args {
    #[arg(long)]
    pub snapshot: PathBuf,
    /// Hard-link the snapshot's current checkpoint into this directory (same filesystem, removed
    /// after) and read that: for a directory a live server writes.
    #[arg(long)]
    pub link: Option<PathBuf>,
    /// csr-inset: CSR from the in sets (~25 GB peak on the whole network); inset: no CSR, iterate
    /// over the in sets (~10 GB, ~6x slower a pass); csr-out: CSR from the out logs, which alone
    /// honours --before-us.
    #[arg(long, default_value = "csr-inset")]
    pub source: String,
    /// Keep only follow records whose TID rkey is older than this (microseconds): the graph as it
    /// was then, for evaluations. Needs csr-out.
    #[arg(long)]
    pub before_us: Option<u64>,
    #[arg(long, default_value_t = 0.85)]
    pub damping: f64,
    /// Stop when the L1 change of the rank vector (summing to 1) is below this.
    #[arg(long, default_value_t = 1e-5)]
    pub tol: f64,
    #[arg(long, default_value_t = 200)]
    pub max_iter: usize,
    /// Cap what one follower passes along one edge at this many average ranks; 0 turns it off.
    #[arg(long, default_value_t = 1000.0)]
    pub max_pass: f64,
    /// Renumber sources by out-degree before iterating (CSR sources only).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub renumber: bool,
    /// Start from an earlier ranks file instead of uniform.
    #[arg(long)]
    pub warm: Option<PathBuf>,
    /// Write the ranks file here.
    #[arg(long)]
    pub out: Option<PathBuf>,
    /// The daily layout: warm from DIR/latest.ranks unless --warm or --cold, write a timestamped
    /// file, repoint latest.ranks, keep the newest --keep.
    #[arg(long)]
    pub out_dir: Option<PathBuf>,
    #[arg(long)]
    pub cold: bool,
    #[arg(long, default_value_t = 8)]
    pub keep: usize,
}

struct Csr {
    off: Vec<u64>,
    src: Vec<u32>,
}

impl Csr {
    fn row(&self, v: usize) -> &[u32] {
        &self.src[self.off[v] as usize..self.off[v + 1] as usize]
    }
    fn rows_mut(&mut self) -> Vec<&mut [u32]> {
        let mut rows = Vec::with_capacity(self.off.len().saturating_sub(1));
        let mut rest: &mut [u32] = &mut self.src;
        for w in self.off.windows(2) {
            let (row, tail) = rest.split_at_mut((w[1] - w[0]) as usize);
            rows.push(row);
            rest = tail;
        }
        rows
    }
}

/// Taken-down and deleted actors, one bit per id: their follows pass nothing and they get none.
/// The in sets still hold them, so every read filters.
pub struct Hidden(Vec<u64>);

impl Hidden {
    pub fn of(g: &Graph, n: u32) -> Hidden {
        if !g.any_status() {
            return Hidden(Vec::new());
        }
        let words: Vec<u64> = (0..n.div_ceil(64))
            .into_par_iter()
            .map(|w| (0..64).filter(|b| w * 64 + b < n && g.hidden_id(w * 64 + b)).fold(0u64, |x, b| x | 1 << b))
            .collect();
        Hidden(words)
    }
    #[inline]
    pub fn has(&self, id: u32) -> bool {
        self.0.get(id as usize / 64).is_some_and(|w| w >> (id % 64) & 1 == 1)
    }
}

fn in_followers(g: &Graph, h: &Hidden, s: u32, n: u32) -> Vec<u32> {
    if h.has(s) {
        return Vec::new();
    }
    let mut v = g.read(|cx| g.in_set(cx, FOLLOW, s)).expect("in_set");
    v.retain(|&a| a != s && a < n && !h.has(a));
    v
}

fn csr_from_inset(g: &Graph, h: &Hidden, n: u32) -> Csr {
    let chunk = 1u32 << 14;
    let parts: Vec<(Vec<u32>, Vec<u32>)> = (0..n.div_ceil(chunk))
        .into_par_iter()
        .map(|c| {
            let (lo, hi) = (c * chunk, ((c + 1) * chunk).min(n));
            let mut lens = Vec::with_capacity((hi - lo) as usize);
            let mut src = Vec::new();
            for s in lo..hi {
                let v = in_followers(g, h, s, n);
                lens.push(v.len() as u32);
                src.extend_from_slice(&v);
            }
            (lens, src)
        })
        .collect();
    let mut off = Vec::with_capacity(n as usize + 1);
    let mut src = Vec::with_capacity(parts.iter().map(|p| p.1.len()).sum());
    off.push(0u64);
    for (lens, s) in parts {
        let mut o = *off.last().unwrap();
        for l in lens {
            o += l as u64;
            off.push(o);
        }
        src.extend_from_slice(&s);
    }
    Csr { off, src }
}

fn keep_key(key: u64, before_us: Option<u64>) -> bool {
    match before_us {
        None => true,
        Some(b) => key & NON_TID != 0 || (key >> 10) < b,
    }
}

fn out_subjects(g: &Graph, h: &Hidden, u: u32, n: u32, before_us: Option<u64>) -> Vec<u32> {
    let mut v: Vec<u32> = g
        .read(|cx| g.out_all(cx, FOLLOW, u))
        .expect("out_all")
        .into_iter()
        .filter(|&(k, s)| s != u && s < n && !h.has(s) && keep_key(k, before_us))
        .map(|(_, s)| s)
        .collect();
    v.sort_unstable();
    v.dedup();
    v
}

fn csr_from_out(g: &Graph, h: &Hidden, n: u32, before_us: Option<u64>) -> Csr {
    let atomics = |len: usize| -> Vec<AtomicU32> { (0..len).map(|_| AtomicU32::new(0)).collect() };
    let skip = |u: u32| h.has(u);
    let indeg = atomics(n as usize);
    (0..n).into_par_iter().with_min_len(4096).filter(|&u| !skip(u)).for_each(|u| {
        for s in out_subjects(g, h, u, n, before_us) {
            indeg[s as usize].fetch_add(1, Ordering::Relaxed);
        }
    });
    let mut off = Vec::with_capacity(n as usize + 1);
    off.push(0u64);
    for d in &indeg {
        off.push(*off.last().unwrap() + d.load(Ordering::Relaxed) as u64);
    }
    drop(indeg);
    let src = atomics(*off.last().unwrap() as usize);
    let cur = atomics(n as usize);
    (0..n).into_par_iter().with_min_len(4096).filter(|&u| !skip(u)).for_each(|u| {
        for s in out_subjects(g, h, u, n, before_us) {
            let i = cur[s as usize].fetch_add(1, Ordering::Relaxed) as u64;
            src[(off[s as usize] + i) as usize].store(u, Ordering::Relaxed);
        }
    });
    drop(cur);
    let mut c = Csr { off, src: src.into_iter().map(AtomicU32::into_inner).collect() };
    // the second pass's order is a race; sorted rows make the sums deterministic
    c.rows_mut().par_iter_mut().for_each(|r| r.sort_unstable());
    c
}

fn degrees_csr(csr: &Csr, n: u32) -> (Vec<u32>, Vec<u32>) {
    let d: Vec<AtomicU32> = (0..n).map(|_| AtomicU32::new(0)).collect();
    csr.src.par_chunks(1 << 20).for_each(|c| {
        for &u in c {
            d[u as usize].fetch_add(1, Ordering::Relaxed);
        }
    });
    let indeg = csr.off.windows(2).map(|w| (w[1] - w[0]) as u32).collect();
    (d.into_iter().map(AtomicU32::into_inner).collect(), indeg)
}

fn degrees_inset(g: &Graph, h: &Hidden, n: u32) -> (Vec<u32>, Vec<u32>) {
    let d: Vec<AtomicU32> = (0..n).map(|_| AtomicU32::new(0)).collect();
    let indeg: Vec<u32> = (0..n)
        .into_par_iter()
        .with_min_len(4096)
        .map(|s| {
            let v = in_followers(g, h, s, n);
            for &a in &v {
                d[a as usize].fetch_add(1, Ordering::Relaxed);
            }
            v.len() as u32
        })
        .collect();
    (d.into_iter().map(AtomicU32::into_inner).collect(), indeg)
}

/// Log-scale u16: 0 is the smallest positive rank, 65535 the largest.
pub fn quantise(r: &[f64]) -> (Vec<u16>, f64, f64) {
    let lo = r.par_iter().copied().filter(|&x| x > 0.0).reduce(|| f64::MAX, f64::min).ln();
    let hi = r.par_iter().copied().reduce(|| 0.0, f64::max).ln();
    let span = (hi - lo).max(f64::MIN_POSITIVE);
    let q =
        r.par_iter().map(|&x| (((x.max(1e-300).ln() - lo) / span).clamp(0.0, 1.0) * 65535.0).round() as u16).collect();
    (q, lo, hi)
}

/// The starting vector: an earlier run's ranks by DID, 1/N for ids it doesn't have, renormalised.
fn warm_start(g: &Graph, n: u32, path: &Path) -> Result<(Vec<f64>, usize)> {
    let (_, rows) = ranks::read(path)?;
    let nf = n as f64;
    let mut r = vec![1.0 / nf; n as usize];
    let ids: Vec<Option<u32>> = rows.par_iter().map(|row| g.id_of(&row.did).filter(|&i| i < n)).collect();
    let mut hit = 0;
    for (row, id) in rows.iter().zip(ids) {
        if let Some(i) = id {
            r[i as usize] = row.rank as f64;
            hit += 1;
        }
    }
    let s: f64 = r.par_iter().sum();
    r.par_iter_mut().for_each(|x| *x /= s);
    Ok((r, hit))
}

pub struct Ranks {
    pub r: Vec<f64>,
    pub indeg: Vec<u32>,
    pub iterations: usize,
    pub converged: bool,
    pub l1: f64,
    pub report: Value,
}

/// The iterations, on ids 0..n of `g`.
pub fn compute(g: &Graph, a: &Args, warm: Option<Vec<f64>>) -> Result<Ranks> {
    let n = g.n_ids();
    let t = Instant::now();
    let h = Hidden::of(g, n);
    let mut csr = match a.source.as_str() {
        "csr-inset" | "inset" if a.before_us.is_some() => bail!("--before-us needs --source csr-out"),
        "csr-inset" => Some(csr_from_inset(g, &h, n)),
        "csr-out" => Some(csr_from_out(g, &h, n, a.before_us)),
        "inset" => None,
        s => bail!("unknown --source {s}"),
    };
    let (outdeg, indeg) = match &csr {
        Some(c) => degrees_csr(c, n),
        None => degrees_inset(g, &h, n),
    };
    let export_s = t.elapsed().as_secs_f64();
    let edges: u64 = outdeg.iter().map(|&d| d as u64).sum();
    let dangling = outdeg.iter().filter(|&&d| d == 0).count();
    let export_rss = sys::peak_rss_gb();
    eprintln!("{export_s:.1}s export: {n} ids, {edges} edges, {dangling} dangling, peak RSS {export_rss:.1} GB");

    let t = Instant::now();
    let old_of_new: Option<Vec<u32>> = match (&mut csr, a.renumber) {
        (Some(c), true) => {
            let mut order: Vec<u32> = (0..n).collect();
            order.par_sort_by_key(|&u| std::cmp::Reverse(outdeg[u as usize]));
            let mut new_of_old = vec![0u32; n as usize];
            for (i, &u) in order.iter().enumerate() {
                new_of_old[u as usize] = i as u32;
            }
            c.src.par_iter_mut().for_each(|s| *s = new_of_old[*s as usize]);
            c.rows_mut().par_iter_mut().for_each(|r| r.sort_unstable());
            Some(order)
        }
        _ => None,
    };
    let renumber_s = t.elapsed().as_secs_f64();

    let nf = n as f64;
    let mut r = warm.unwrap_or_else(|| vec![1.0 / nf; n as usize]);
    let inv: Vec<f32> = outdeg.par_iter().map(|&d| if d == 0 { 0.0 } else { 1.0 / d as f32 }).collect();
    let cap = if a.max_pass > 0.0 { (a.max_pass / nf) as f32 } else { f32::INFINITY };
    let mut contrib = vec![0f32; n as usize];
    let mut gathered = if old_of_new.is_some() { vec![0f32; n as usize] } else { Vec::new() };
    let mut next = vec![0f64; n as usize];
    let d = a.damping;
    let mut iters = Vec::new();
    let (mut converged, mut l1) = (false, f64::INFINITY);
    let t_iter = Instant::now();
    for it in 0..a.max_iter {
        let ti = Instant::now();
        contrib
            .par_iter_mut()
            .zip(r.par_iter())
            .zip(inv.par_iter())
            .for_each(|((c, &x), &i)| *c = ((x as f32) * i).min(cap));
        // rank that isn't passed on: dangling actors' whole rank, and what the cap holds back
        let held: f64 = r
            .par_iter()
            .zip(outdeg.par_iter())
            .zip(contrib.par_iter())
            .map(|((&x, &o), &c)| if o == 0 { x } else { (x - c as f64 * o as f64).max(0.0) })
            .sum();
        let base = (1.0 - d) / nf + d * held / nf;
        if let Some(o) = &old_of_new {
            gathered.par_iter_mut().zip(o.par_iter()).for_each(|(c, &u)| *c = contrib[u as usize]);
        }
        let cv: &[f32] = if old_of_new.is_some() { &gathered } else { &contrib };
        match &csr {
            Some(c) => next.par_iter_mut().enumerate().with_min_len(1 << 12).for_each(|(v, y)| {
                // f32 runs of 256 into an f64: the sum stays exact enough for 1e-9 without
                // an f64 add per edge
                let (mut s, mut acc) = (0f64, 0f32);
                for (j, &u) in c.row(v).iter().enumerate() {
                    acc += cv[u as usize];
                    if j & 255 == 255 {
                        s += acc as f64;
                        acc = 0.0;
                    }
                }
                *y = base + d * (s + acc as f64);
            }),
            None => next.par_iter_mut().enumerate().with_min_len(1 << 12).for_each(|(v, y)| {
                let s: f64 = in_followers(g, &h, v as u32, n).iter().map(|&u| cv[u as usize] as f64).sum();
                *y = base + d * s;
            }),
        }
        // f32 contributions lose a little mass a pass; put it back so the ranks stay a distribution
        let total: f64 = next.par_iter().sum();
        next.par_iter_mut().for_each(|y| *y /= total);
        l1 = next.par_iter().zip(r.par_iter()).map(|(a, b)| (a - b).abs()).sum();
        std::mem::swap(&mut r, &mut next);
        let secs = ti.elapsed().as_secs_f64();
        eprintln!("iter {it}: l1 {l1:.3e} {secs:.2}s");
        iters.push(json!({"iter": it, "l1": l1, "secs": secs}));
        if l1 < a.tol {
            converged = true;
            break;
        }
    }
    let iter_s = t_iter.elapsed().as_secs_f64();
    let capped_sources = if cap.is_finite() {
        r.par_iter().zip(outdeg.par_iter()).filter(|(&x, &o)| o > 0 && (x / o as f64) as f32 > cap).count()
    } else {
        0
    };
    let report = json!({
        "source": a.source, "before_us": a.before_us, "max_pass": a.max_pass, "renumber": a.renumber,
        "damping": d, "tol": a.tol, "ids": n, "edges": edges, "dangling": dangling,
        "export_secs": export_s, "export_peak_rss_gb": export_rss, "renumber_secs": renumber_s,
        "iterations": iters.len(), "converged": converged, "l1": l1, "iterate_secs": iter_s,
        "secs_per_iter": iter_s / iters.len().max(1) as f64, "capped_sources": capped_sources, "iters": iters,
    });
    Ok(Ranks { iterations: iters.len(), r, indeg, converged, l1, report })
}

/// Hard-links the snapshot's current checkpoint (its manifest, directory and shard files) into
/// `to`. A checkpoint or compaction that lands meanwhile can delete a file the manifest named,
/// or replace the directory: then it starts over from the new manifest.
pub fn link_checkpoint(snapshot: &Path, to: &Path) -> Result<()> {
    for attempt in 0.. {
        if to.exists() {
            std::fs::remove_dir_all(to).with_context(|| format!("clear {}", to.display()))?;
        }
        std::fs::create_dir_all(to)?;
        let raw = std::fs::read(snapshot.join(MANIFEST)).with_context(|| format!("read {}", snapshot.display()))?;
        let m: Manifest = serde_json::from_slice(&raw)?;
        let mut names: Vec<&str> = vec![DIR_FILE];
        names.extend(m.shards.values().flat_map(|s| s.files()));
        let linked = names.iter().try_for_each(|f| std::fs::hard_link(snapshot.join(f), to.join(f)));
        match linked {
            Ok(()) => {
                std::fs::write(to.join(MANIFEST), &raw)?;
                match marco_core::store::Store::open(to) {
                    Ok(_) => return Ok(()),
                    Err(e) if attempt < 5 => eprintln!("link: {e:#}, again"),
                    Err(e) => return Err(e),
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && attempt < 5 => eprintln!("link: {e}, again"),
            Err(e) => return Err(e).context("hard-link the checkpoint (same filesystem?)"),
        }
    }
    unreachable!()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

/// The rows of the ranks file: every id in the directory, its DID, rank and followers.
pub fn rows(g: &Graph, rk: &Ranks) -> (Vec<Row>, f64, f64) {
    let (q, lo, hi) = quantise(&rk.r);
    let rows = (0..rk.r.len() as u32)
        .into_par_iter()
        .filter_map(|i| {
            let did = g.did_of(i)?;
            Some(Row {
                did,
                rank: rk.r[i as usize] as f32,
                rank16: q[i as usize],
                followers: rk.indeg[i as usize],
                flags: if g.hidden_id(i) { HIDDEN } else { 0 },
            })
        })
        .collect();
    (rows, lo, hi)
}

/// Keeps the newest `keep` ranks files in `dir` (by name: the timestamps sort).
fn prune(dir: &Path, keep: usize) -> Result<Vec<String>> {
    let mut names: Vec<String> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| n.starts_with("ranks-") && n.ends_with(".ranks"))
        .collect();
    names.sort();
    let drop = names.len().saturating_sub(keep.max(1));
    for n in &names[..drop] {
        std::fs::remove_file(dir.join(n))?;
    }
    Ok(names[..drop].to_vec())
}

pub fn run(a: &Args) -> Result<Value> {
    let t0 = Instant::now();
    let snap = match &a.link {
        Some(l) => {
            link_checkpoint(&a.snapshot, l)?;
            l.clone()
        }
        None => a.snapshot.clone(),
    };
    let r = run_on(a, &snap, t0);
    if let Some(l) = &a.link {
        let _ = std::fs::remove_dir_all(l);
    }
    r
}

fn run_on(a: &Args, snap: &Path, t0: Instant) -> Result<Value> {
    let g = Graph::open(snap)?;
    let n = g.n_ids();
    let open_s = t0.elapsed().as_secs_f64();
    let cpu0 = sys::cpu_secs();
    sys::reset_peak_rss();
    let latest = a.out_dir.as_ref().map(|d| d.join("latest.ranks"));
    let warm_path = match (&a.warm, &latest) {
        (Some(p), _) => Some(p.clone()),
        (None, Some(l)) if !a.cold && l.exists() => Some(l.clone()),
        _ => None,
    };
    let (warm, warm_hit) = match &warm_path {
        Some(p) => {
            let (r, hit) = warm_start(&g, n, p)?;
            (Some(r), hit)
        }
        None => (None, 0),
    };
    let rk = compute(&g, a, warm)?;
    let mut files = json!({});
    let out = match (&a.out, &a.out_dir) {
        (Some(p), _) => Some(p.clone()),
        (None, Some(d)) => {
            std::fs::create_dir_all(d)?;
            let ts = chrono_like_utc(now_ms());
            Some(d.join(format!("ranks-{ts}.ranks")))
        }
        _ => None,
    };
    if let Some(p) = &out {
        let t = Instant::now();
        let (mut rows, lo, hi) = rows(&g, &rk);
        let m = g.manifest().cloned().unwrap_or_default();
        let h = Header {
            created_ms: now_ms(),
            build: m.build,
            checkpoint: m.checkpoint,
            iterations: rk.iterations as u32,
            converged: rk.converged,
            damping: a.damping,
            max_pass: a.max_pass.max(0.0),
            l1: rk.l1,
            ln_lo: lo,
            ln_hi: hi,
        };
        ranks::write(p, &h, &mut rows)?;
        let bytes = std::fs::metadata(p)?.len();
        let mut pruned = Vec::new();
        if let (Some(d), Some(l)) = (&a.out_dir, &latest) {
            let tmp = d.join("latest.ranks.tmp");
            let _ = std::fs::remove_file(&tmp);
            std::os::unix::fs::symlink(p.file_name().unwrap(), &tmp)?;
            std::fs::rename(&tmp, l)?;
            pruned = prune(d, a.keep)?;
        }
        files = json!({"ranks": p, "rows": rows.len(), "bytes": bytes, "write_secs": t.elapsed().as_secs_f64(),
            "u16_ln_lo": lo, "u16_ln_hi": hi, "u16_max_rel_err": ((hi - lo) / 65535.0 / 2.0).exp() - 1.0, "pruned": pruned});
    }
    let mut top: Vec<u32> = (0..n).collect();
    top.par_sort_unstable_by(|&x, &y| rk.r[y as usize].total_cmp(&rk.r[x as usize]));
    let top20: Vec<Value> = top[..20.min(top.len())]
        .iter()
        .map(|&v| json!({"id": v, "rank": rk.r[v as usize], "followers": rk.indeg[v as usize], "did": g.did_of(v)}))
        .collect();
    let mut rep = rk.report;
    rep["open_secs"] = json!(open_s);
    rep["warm"] = json!(warm_path);
    rep["warm_matched"] = json!(warm_hit);
    rep["files"] = files;
    rep["top20"] = json!(top20);
    rep["rank_sum"] = json!(rk.r.par_iter().sum::<f64>());
    rep["cpu_secs"] = json!(sys::cpu_secs() - cpu0);
    rep["peak_rss_gb"] = json!(sys::peak_rss_gb());
    rep["total_secs"] = json!(t0.elapsed().as_secs_f64());
    rep["threads"] = json!(rayon::current_num_threads());
    Ok(rep)
}

/// `YYYYMMDDTHHMMSSZ` for a Unix time in ms (civil-from-days, no calendar crate).
fn chrono_like_utc(ms: i64) -> String {
    let s = ms.div_euclid(1000);
    let (days, sod) = (s.div_euclid(86400), s.rem_euclid(86400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + (month <= 2) as i64;
    format!("{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z", sod / 3600, sod % 3600 / 60, sod % 60)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use marco_core::{Relations, Status};

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        a: Args,
    }

    fn args(extra: &[&str]) -> Args {
        let mut v = vec!["pr"];
        if !extra.contains(&"--snapshot") {
            v.extend(["--snapshot", "unused"]);
        }
        v.extend_from_slice(extra);
        Cli::parse_from(v).a
    }

    fn did(i: u32) -> String {
        let alphabet = b"abcdefghijklmnopqrstuvwxyz234567";
        let mut v = (i as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let s: String = (0..24)
            .map(|_| {
                v ^= v >> 29;
                v = v.wrapping_mul(0xbf58_476d_1ce4_e5b9);
                alphabet[(v >> 59) as usize] as char
            })
            .collect();
        format!("did:plc:{s}")
    }

    /// A small graph: actor 0 is a hub everyone follows that follows actors 1..=3; the rest
    /// follow pseudo-random others, with a self-follow and a repeat record thrown in.
    fn edges(n: u32) -> Vec<(u32, u32)> {
        let mut e = Vec::new();
        let mut x = 0x2545_f491_4f6c_dd1du64;
        for a in 1..n {
            e.push((a, 0));
            for _ in 0..(a % 7) {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                e.push((a, (x % n as u64) as u32));
            }
        }
        e.extend([(0, 1), (0, 2), (0, 3), (5, 5), (6, 7), (6, 7)]);
        e
    }

    fn snapshot(dir: &Path, n: u32, hide: Option<u32>) -> PathBuf {
        let mut g = Graph::new(Relations::bsky_default());
        for (k, (a, s)) in edges(n).into_iter().enumerate() {
            let tid = vlatproto::tid::Tid::from_parts(1_704_067_200_000_000 + k as u64 * 1_000_000, 0).to_string();
            g.add(&format!("at://{}/app.bsky.graph.follow/{tid}", did(a)), &did(s)).unwrap();
        }
        if let Some(h) = hide {
            g.set_status(&did(h), Some(Status::Takendown), 1).unwrap();
        }
        let p = dir.join("snap");
        g.write_snapshot(&p).unwrap();
        p
    }

    /// The textbook power iteration in f64 on the edge list, with the same cap and dangling rule.
    fn reference(n: u32, e: &[(u32, u32)], d: f64, max_pass: f64, ids: &dyn Fn(u32) -> u32) -> Vec<f64> {
        let mut out: Vec<Vec<u32>> = vec![Vec::new(); n as usize];
        for &(a, s) in e {
            if a != s {
                out[ids(a) as usize].push(ids(s));
            }
        }
        for o in &mut out {
            o.sort_unstable();
            o.dedup();
        }
        let nf = n as f64;
        let cap = if max_pass > 0.0 { max_pass / nf } else { f64::INFINITY };
        let mut r = vec![1.0 / nf; n as usize];
        for _ in 0..500 {
            let mut next = vec![0.0; n as usize];
            let mut held = 0.0;
            for (u, o) in out.iter().enumerate() {
                if o.is_empty() {
                    held += r[u];
                    continue;
                }
                let c = (r[u] / o.len() as f64).min(cap);
                held += r[u] - c * o.len() as f64;
                for &s in o {
                    next[s as usize] += d * c;
                }
            }
            for x in &mut next {
                *x += (1.0 - d) / nf + d * held / nf;
            }
            r = next;
        }
        r
    }

    fn run_graph(snap: &Path, extra: &[&str]) -> (Graph, Ranks) {
        let g = Graph::open(snap).unwrap();
        let rk = compute(&g, &args(extra), None).unwrap();
        (g, rk)
    }

    #[test]
    fn matches_the_reference_every_source() {
        let dir = tempfile::tempdir().unwrap();
        let n = 300;
        let snap = snapshot(dir.path(), n, None);
        let g = Graph::open(&snap).unwrap();
        let id = |i: u32| g.id_of(&did(i)).unwrap();
        for cap in ["0", "3"] {
            let want = reference(n, &edges(n), 0.85, cap.parse().unwrap(), &id);
            for (src, ren) in [("csr-inset", "true"), ("csr-inset", "false"), ("csr-out", "true"), ("inset", "false")] {
                let rk =
                    compute(&g, &args(&["--source", src, "--renumber", ren, "--max-pass", cap, "--tol", "1e-6"]), None)
                        .unwrap();
                assert!(rk.converged, "{src}");
                let err: f64 = rk.r.iter().zip(&want).map(|(a, b)| (a - b).abs()).sum();
                assert!(err < 2e-5, "{src} renumber={ren} cap={cap}: L1 {err}");
                assert!((rk.r.iter().sum::<f64>() - 1.0).abs() < 1e-9);
            }
        }
    }

    #[test]
    fn the_cap_holds_back_the_hub() {
        let dir = tempfile::tempdir().unwrap();
        let snap = snapshot(dir.path(), 300, None);
        let (g, plain) = run_graph(&snap, &["--max-pass", "0"]);
        let capped = compute(&g, &args(&["--max-pass", "3"]), None).unwrap();
        let hub_follow = g.id_of(&did(2)).unwrap() as usize;
        assert!(
            capped.r[hub_follow] < plain.r[hub_follow] * 0.8,
            "{} vs {}",
            capped.r[hub_follow],
            plain.r[hub_follow]
        );
        assert!(capped.report["capped_sources"].as_u64().unwrap() >= 1);
        assert_eq!(plain.report["capped_sources"], 0);
    }

    #[test]
    fn hidden_accounts_pass_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let snap = snapshot(dir.path(), 200, Some(0));
        for src in ["csr-inset", "csr-out", "inset"] {
            let (g, rk) = run_graph(&snap, &["--source", src]);
            let hub = g.id_of(&did(0)).unwrap() as usize;
            assert_eq!(rk.indeg[hub], 0, "{src}");
            let (rows, _, _) = rows(&g, &rk);
            let row = rows.iter().find(|r| r.did == did(0)).unwrap();
            assert_eq!(row.flags, HIDDEN);
        }
    }

    #[test]
    fn warm_start_from_the_last_file_stops_sooner() {
        let dir = tempfile::tempdir().unwrap();
        let snap = snapshot(dir.path(), 400, None);
        let out = dir.path().join("daily");
        let o = out.to_str().unwrap();
        let s = snap.to_str().unwrap();
        let cold = run(&args(&["--snapshot", s, "--out-dir", o, "--tol", "1e-6"])).unwrap();
        let warm = run(&args(&["--snapshot", s, "--out-dir", o, "--tol", "1e-6"])).unwrap();
        assert_eq!(cold["warm"], Value::Null);
        assert!(warm["warm"].as_str().unwrap().ends_with("latest.ranks"));
        assert_eq!(warm["warm_matched"], json!(400));
        let (ci, wi) = (cold["iterations"].as_u64().unwrap(), warm["iterations"].as_u64().unwrap());
        assert!(wi * 3 < ci, "warm {wi} cold {ci}");
        let (h, rows) = ranks::read(&out.join("latest.ranks")).unwrap();
        assert!(h.converged);
        assert_eq!(rows.len(), 400);
        assert_eq!(h.max_pass, 1000.0);
        let followers0 = rows.iter().find(|r| r.did == did(0)).unwrap().followers;
        assert_eq!(followers0, 399);
        let top = rows.iter().max_by_key(|r| r.rank16).unwrap();
        assert_eq!(top.rank16, 65535);
        for _ in 0..3 {
            std::thread::sleep(std::time::Duration::from_millis(1100));
            run(&args(&["--snapshot", s, "--out-dir", o, "--keep", "2"])).unwrap();
        }
        let files = std::fs::read_dir(&out)
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().starts_with("ranks-"))
            .count();
        assert_eq!(files, 2);
        assert!(ranks::read(&out.join("latest.ranks")).is_ok());
    }

    #[test]
    fn before_us_cuts_the_graph() {
        let dir = tempfile::tempdir().unwrap();
        let snap = snapshot(dir.path(), 200, None);
        let all = edges(200).len() as u64;
        // edge k's TID is k seconds after 2024-01-01
        let cut = (1_704_067_200_000_000u64 + 100 * 1_000_000).to_string();
        let (_, rk) = run_graph(&snap, &["--source", "csr-out", "--before-us", &cut]);
        let kept = rk.report["edges"].as_u64().unwrap();
        assert!(kept <= 100 && kept > 80, "{kept} of {all}");
        let g = Graph::open(&snap).unwrap();
        assert!(compute(&g, &args(&["--before-us", &cut]), None).is_err());
    }

    #[test]
    fn links_a_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let snap = snapshot(dir.path(), 100, None);
        let l = dir.path().join("link");
        link_checkpoint(&snap, &l).unwrap();
        let g = Graph::open(&l).unwrap();
        assert_eq!(g.n_ids(), 100);
        std::fs::write(snap.join("manifest.json.tmp"), b"x").unwrap();
        link_checkpoint(&snap, &l).unwrap();
        assert!(!l.join("manifest.json.tmp").exists());
    }

    #[test]
    fn utc_names() {
        assert_eq!(chrono_like_utc(0), "19700101T000000Z");
        assert_eq!(chrono_like_utc(1_794_628_800_000), "20261114T040000Z");
    }
}
