//! PageRank over the follow graph of a snapshot: a prototype that measures
//! what regenerating the ranks costs.
//!
//! The graph is exported once into an in-edge CSR (u32 sources per subject),
//! either from the in sets (one pass) or from the out logs (two passes, and
//! the only way to keep just the records whose TID rkey is older than
//! `--before-us`, which stands in for last week's graph). Iterations are
//! pull-based: every subject sums `rank / out-degree` over its followers, so
//! each iteration writes each rank once with no atomics. Dangling mass (actors
//! who follow nobody) and the teleport are spread uniformly.
//!
//! `--source inset` skips the CSR and iterates over the in sets directly, to
//! show what one iteration costs without the export.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use serde_json::{json, Value};

use marco_core::rkey::NON_TID;
use marco_core::Graph;

use crate::sys;

const FOLLOW: u8 = 0;

#[derive(clap::Args)]
pub struct Args {
    #[arg(long)]
    snapshot: PathBuf,
    /// csr-inset: CSR from the in sets; csr-out: CSR from the out logs (honours --before-us);
    /// inset: no CSR, iterate over the in sets.
    #[arg(long, default_value = "csr-inset")]
    source: String,
    /// Keep only follow records whose TID rkey is older than this (microseconds); needs csr-out.
    #[arg(long)]
    before_us: Option<u64>,
    #[arg(long, default_value_t = 0.85)]
    damping: f64,
    /// Stop when the L1 change of the rank vector (summing to 1) is below this.
    #[arg(long, default_value_t = 1e-9)]
    tol: f64,
    #[arg(long, default_value_t = 300)]
    max_iter: usize,
    /// Start from these ranks (an f32 per id, as --out writes) instead of uniform.
    #[arg(long)]
    warm: Option<PathBuf>,
    /// Write the ranks here as f32 per id, plus <out>.u16 (log-quantised) and <out>.indeg (u32 followers).
    #[arg(long)]
    out: Option<PathBuf>,
    /// Cap what one follower passes along one edge at this many average ranks (1/N each); the excess
    /// goes to the teleport. Off by default: plain PageRank.
    #[arg(long)]
    max_pass: Option<f64>,
    /// Converged ranks (an f32 per id, from a tight run): each iteration reports how many of their top 100,000
    /// would get a different u16 and the Kendall tau of their order.
    #[arg(long)]
    track: Option<PathBuf>,
    /// Renumber sources by out-degree (and sort each in-list) before iterating: the locality test.
    #[arg(long)]
    relabel: bool,
    /// Compare each tolerance's ranks with this run's final ones (top-K overlap, rank error).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    checkpoints: bool,
}

struct Csr {
    off: Vec<u64>,
    src: Vec<u32>,
}

fn csr_from_inset(g: &Graph, n: u32) -> Result<Csr> {
    let chunk = 1u32 << 14;
    let parts: Vec<(Vec<u32>, Vec<u32>)> = (0..n.div_ceil(chunk))
        .into_par_iter()
        .map(|c| {
            let lo = c * chunk;
            let hi = (lo + chunk).min(n);
            let mut lens = Vec::with_capacity((hi - lo) as usize);
            let mut src = Vec::new();
            for s in lo..hi {
                let v = g.read(|cx| g.in_set(cx, FOLLOW, s)).expect("in_set");
                let before = src.len();
                src.extend(v.into_iter().filter(|&a| a != s && a < n));
                lens.push((src.len() - before) as u32);
            }
            (lens, src)
        })
        .collect();
    let total: usize = parts.iter().map(|p| p.1.len()).sum();
    let mut off = Vec::with_capacity(n as usize + 1);
    let mut src = Vec::with_capacity(total);
    off.push(0u64);
    for (lens, s) in parts {
        let mut o = *off.last().unwrap();
        for l in lens {
            o += l as u64;
            off.push(o);
        }
        src.extend_from_slice(&s);
    }
    Ok(Csr { off, src })
}

fn keep(key: u64, before_us: Option<u64>) -> bool {
    match before_us {
        None => true,
        Some(b) => key & NON_TID != 0 || (key >> 10) < b,
    }
}

fn out_subjects(g: &Graph, u: u32, n: u32, before_us: Option<u64>) -> Vec<u32> {
    let mut v: Vec<u32> = g
        .read(|cx| g.out_all(cx, FOLLOW, u))
        .expect("out_all")
        .into_iter()
        .filter(|&(k, s)| s != u && s < n && keep(k, before_us))
        .map(|(_, s)| s)
        .collect();
    v.sort_unstable();
    v.dedup();
    v
}

fn csr_from_out(g: &Graph, n: u32, before_us: Option<u64>) -> Result<Csr> {
    let indeg: Vec<AtomicU32> = (0..n).map(|_| AtomicU32::new(0)).collect();
    (0..n).into_par_iter().with_min_len(4096).for_each(|u| {
        for s in out_subjects(g, u, n, before_us) {
            indeg[s as usize].fetch_add(1, Ordering::Relaxed);
        }
    });
    let mut off = Vec::with_capacity(n as usize + 1);
    off.push(0u64);
    for d in &indeg {
        let o = *off.last().unwrap() + d.load(Ordering::Relaxed) as u64;
        off.push(o);
    }
    let total = *off.last().unwrap() as usize;
    let src: Vec<AtomicU32> = (0..total).map(|_| AtomicU32::new(0)).collect();
    let cur: Vec<AtomicU32> = (0..n).map(|_| AtomicU32::new(0)).collect();
    (0..n).into_par_iter().with_min_len(4096).for_each(|u| {
        for s in out_subjects(g, u, n, before_us) {
            let i = cur[s as usize].fetch_add(1, Ordering::Relaxed) as u64;
            src[(off[s as usize] + i) as usize].store(u, Ordering::Relaxed);
        }
    });
    drop(cur);
    drop(indeg);
    // AtomicU32 has u32's layout
    let src: Vec<u32> = src.into_iter().map(|a| a.into_inner()).collect();
    Ok(Csr { off, src })
}

fn outdeg_from(csr: &Csr, n: u32) -> Vec<u32> {
    let d: Vec<AtomicU32> = (0..n).map(|_| AtomicU32::new(0)).collect();
    csr.src.par_chunks(1 << 20).for_each(|c| {
        for &u in c {
            d[u as usize].fetch_add(1, Ordering::Relaxed);
        }
    });
    d.into_iter().map(|a| a.into_inner()).collect()
}

fn outdeg_inset(g: &Graph, n: u32) -> Vec<u32> {
    let d: Vec<AtomicU32> = (0..n).map(|_| AtomicU32::new(0)).collect();
    (0..n).into_par_iter().with_min_len(4096).for_each(|s| {
        for a in g.read(|cx| g.in_set(cx, FOLLOW, s)).expect("in_set") {
            if a != s && a < n {
                d[a as usize].fetch_add(1, Ordering::Relaxed);
            }
        }
    });
    d.into_iter().map(|a| a.into_inner()).collect()
}

fn read_f32(path: &PathBuf) -> Result<Vec<f32>> {
    let b = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if b.len() % 4 != 0 {
        bail!("{}: not a whole number of f32s", path.display());
    }
    Ok(b.as_chunks::<4>().0.iter().map(|c| f32::from_le_bytes(*c)).collect())
}

fn write_le<T: Copy>(path: &PathBuf, v: &[T], f: impl Fn(T) -> Vec<u8>) -> Result<()> {
    let mut b = Vec::with_capacity(std::mem::size_of_val(v));
    for &x in v {
        b.extend(f(x));
    }
    std::fs::write(path, b).with_context(|| format!("write {}", path.display()))
}

/// Log-scale u16: 0 is the smallest rank, 65535 the largest.
fn quantise(r: &[f64]) -> (Vec<u16>, f64, f64) {
    let lo = r.iter().copied().filter(|&x| x > 0.0).fold(f64::MAX, f64::min).ln();
    let hi = r.iter().copied().fold(0.0, f64::max).ln();
    let q = r
        .par_iter()
        .map(|&x| (((x.max(1e-300).ln() - lo) / (hi - lo)).clamp(0.0, 1.0) * 65535.0).round() as u16)
        .collect();
    (q, lo, hi)
}

fn top_ids(r: &[f64], k: usize) -> Vec<u32> {
    let mut idx: Vec<u32> = (0..r.len() as u32).collect();
    idx.par_sort_unstable_by(|&a, &b| r[b as usize].total_cmp(&r[a as usize]));
    idx.truncate(k);
    idx
}

/// Kendall tau between `v`'s order and the descending order it should be in (ties count as agreeing).
fn kendall_desc(v: &[f64]) -> f64 {
    fn count(v: &mut [f64], buf: &mut Vec<f64>) -> u64 {
        if v.len() < 2 {
            return 0;
        }
        let m = v.len() / 2;
        let mut inv = count(&mut v[..m], buf) + count(&mut v[m..], buf);
        buf.clear();
        let (mut i, mut j) = (0, m);
        while i < m && j < v.len() {
            // descending: a later element larger than an earlier one is a discordant pair
            if v[i] >= v[j] {
                buf.push(v[i]);
                i += 1;
            } else {
                buf.push(v[j]);
                inv += (m - i) as u64;
                j += 1;
            }
        }
        buf.extend_from_slice(&v[i..m]);
        buf.extend_from_slice(&v[j..]);
        v.copy_from_slice(buf);
        inv
    }
    let n = v.len() as f64;
    let mut w = v.to_vec();
    let inv = count(&mut w, &mut Vec::with_capacity(v.len())) as f64;
    1.0 - 4.0 * inv / (n * (n - 1.0))
}

pub fn run(a: &Args) -> Result<Value> {
    let t0 = Instant::now();
    let g = Graph::open(&a.snapshot)?;
    let n = g.base_n();
    let open_s = t0.elapsed().as_secs_f64();
    let cpu0 = sys::cpu_secs();
    sys::reset_peak_rss();

    let t = Instant::now();
    let (csr, outdeg) = match a.source.as_str() {
        "csr-inset" => {
            if a.before_us.is_some() {
                bail!("--before-us needs --source csr-out");
            }
            let c = csr_from_inset(&g, n)?;
            let d = outdeg_from(&c, n);
            (Some(c), d)
        }
        "csr-out" => {
            let c = csr_from_out(&g, n, a.before_us)?;
            let d = outdeg_from(&c, n);
            (Some(c), d)
        }
        "inset" => (None, outdeg_inset(&g, n)),
        s => bail!("unknown --source {s}"),
    };
    let export_s = t.elapsed().as_secs_f64();
    let export_cpu = sys::cpu_secs() - cpu0;
    let edges: u64 = outdeg.iter().map(|&d| d as u64).sum();
    let dangling = outdeg.iter().filter(|&&d| d == 0).count();
    let export_rss = sys::peak_rss_gb();
    eprintln!("{export_s:.1}s export: {n} ids, {edges} edges, {dangling} dangling, peak RSS {export_rss:.1} GB");

    let nf = n as f64;
    let mut r: Vec<f64> = match &a.warm {
        None => vec![1.0 / nf; n as usize],
        Some(p) => {
            let w = read_f32(p)?;
            let mut r: Vec<f64> = (0..n as usize).map(|i| w.get(i).map_or(1.0 / nf, |&x| x as f64)).collect();
            let s: f64 = r.par_iter().sum();
            r.par_iter_mut().for_each(|x| *x /= s);
            r
        }
    };
    let inv: Vec<f32> = outdeg.par_iter().map(|&d| if d == 0 { 0.0 } else { 1.0 / d as f32 }).collect();
    let mut contrib = vec![0f32; n as usize];
    // the locality test: sources renumbered by out-degree, hottest first, each in-list sorted
    let mut csr = csr;
    let mut relabel_s = 0.0;
    let old_of_new: Option<Vec<u32>> = match (&mut csr, a.relabel) {
        (Some(c), true) => {
            let t = Instant::now();
            let mut order: Vec<u32> = (0..n).collect();
            order.par_sort_unstable_by_key(|&u| std::cmp::Reverse(outdeg[u as usize]));
            let mut new_of_old = vec![0u32; n as usize];
            for (i, &u) in order.iter().enumerate() {
                new_of_old[u as usize] = i as u32;
            }
            c.src.par_iter_mut().for_each(|s| *s = new_of_old[*s as usize]);
            let off = &c.off;
            let mut rows: Vec<&mut [u32]> = Vec::with_capacity(n as usize);
            let mut rest: &mut [u32] = &mut c.src;
            for v in 0..n as usize {
                let (row, tail) = rest.split_at_mut((off[v + 1] - off[v]) as usize);
                rows.push(row);
                rest = tail;
            }
            rows.par_iter_mut().for_each(|r| r.sort_unstable());
            relabel_s = t.elapsed().as_secs_f64();
            Some(order)
        }
        _ => None,
    };
    let mut contrib_new = if old_of_new.is_some() { vec![0f32; n as usize] } else { Vec::new() };
    // what --track compares against
    let track = match &a.track {
        None => None,
        Some(p) => {
            let fin: Vec<f64> = read_f32(p)?.into_iter().map(|x| x as f64).collect();
            let lo = fin.iter().copied().filter(|&x| x > 0.0).fold(f64::MAX, f64::min).ln();
            let hi = fin.iter().copied().fold(0.0, f64::max).ln();
            let top = top_ids(&fin, 100_000);
            let q = |x: f64| (((x.max(1e-300).ln() - lo) / (hi - lo)).clamp(0.0, 1.0) * 65535.0).round() as u16;
            let fq: Vec<u16> = top.iter().map(|&v| q(fin[v as usize])).collect();
            Some((top, fq, lo, hi))
        }
    };
    let mut next = vec![0f64; n as usize];
    let d = a.damping;
    let mut iters = Vec::new();
    let mut marks: Vec<(f64, usize, Vec<f32>)> = Vec::new();
    let mut want = [1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8].into_iter().peekable();
    let t_iter = Instant::now();
    let mut converged = false;
    for it in 0..a.max_iter {
        let ti = Instant::now();
        let cap = a.max_pass.map_or(f32::INFINITY, |c| (c / nf) as f32);
        contrib
            .par_iter_mut()
            .zip(r.par_iter())
            .zip(inv.par_iter())
            .for_each(|((c, &x), &i)| *c = ((x as f32) * i).min(cap));
        // rank that isn't passed on: dangling actors' whole rank, and what the cap holds back
        let dmass: f64 = r
            .par_iter()
            .zip(outdeg.par_iter())
            .zip(contrib.par_iter())
            .map(|((&x, &o), &c)| if o == 0 { x } else { (x - c as f64 * o as f64).max(0.0) })
            .sum();
        let base = (1.0 - d) / nf + d * dmass / nf;
        if let Some(o) = &old_of_new {
            contrib_new.par_iter_mut().zip(o.par_iter()).for_each(|(c, &u)| *c = contrib[u as usize]);
        }
        let cv: &[f32] = if old_of_new.is_some() { &contrib_new } else { &contrib };
        match &csr {
            Some(c) => {
                next.par_iter_mut().enumerate().with_min_len(1 << 12).for_each(|(v, y)| {
                    let (lo, hi) = (c.off[v] as usize, c.off[v + 1] as usize);
                    let mut s = 0f64;
                    let mut acc = 0f32;
                    for (j, &u) in c.src[lo..hi].iter().enumerate() {
                        acc += cv[u as usize];
                        if j & 255 == 255 {
                            s += acc as f64;
                            acc = 0.0;
                        }
                    }
                    *y = base + d * (s + acc as f64);
                });
            }
            None => {
                next.par_iter_mut().enumerate().with_min_len(1 << 12).for_each(|(v, y)| {
                    let mut s = 0f64;
                    for u in g.read(|cx| g.in_set(cx, FOLLOW, v as u32)).expect("in_set") {
                        if u as usize != v && u < n {
                            s += contrib[u as usize] as f64;
                        }
                    }
                    *y = base + d * s;
                });
            }
        }
        let l1: f64 = next.par_iter().zip(r.par_iter()).map(|(a, b)| (a - b).abs()).sum();
        std::mem::swap(&mut r, &mut next);
        let secs = ti.elapsed().as_secs_f64();
        let mut rec = json!({"iter": it, "l1": l1, "secs": secs});
        if let Some((top, fq, lo, hi)) = &track {
            let q = |x: f64| (((x.max(1e-300).ln() - lo) / (hi - lo)).clamp(0.0, 1.0) * 65535.0).round() as u16;
            let changed = top.iter().zip(fq).filter(|(&v, &f)| q(r[v as usize]) != f).count();
            let vals: Vec<f64> = top.iter().map(|&v| r[v as usize]).collect();
            rec["u16_changed_top100k"] = json!(changed);
            rec["tau_top100k"] = json!(kendall_desc(&vals));
        }
        eprintln!("iter {it}: {rec}");
        iters.push(rec);
        while let Some(&w) = want.peek() {
            if l1 < w {
                if a.checkpoints {
                    marks.push((w, it + 1, r.iter().map(|&x| x as f32).collect()));
                }
                want.next();
            } else {
                break;
            }
        }
        if l1 < a.tol {
            converged = true;
            break;
        }
    }
    let iter_s = t_iter.elapsed().as_secs_f64();
    let n_iter = iters.len();
    let peak = sys::peak_rss_gb();
    let cpu = sys::cpu_secs() - cpu0;

    // how close each tolerance's ranks are to the final ones, where it matters: the top
    let top = top_ids(&r, 1_000_000);
    let mut tol_report = Vec::new();
    for (w, it, m) in &marks {
        let mf: Vec<f64> = m.iter().map(|&x| x as f64).collect();
        let mt = top_ids(&mf, 1_000_000);
        let mut out = json!({"l1_below": w, "iterations": it});
        for k in [100usize, 10_000, 1_000_000] {
            let a: std::collections::HashSet<u32> = top[..k].iter().copied().collect();
            let o = mt[..k].iter().filter(|x| a.contains(x)).count();
            out[format!("top{k}_overlap")] = json!(o as f64 / k as f64);
        }
        let max_rel = top[..1_000_000]
            .iter()
            .map(|&v| ((mf[v as usize] - r[v as usize]) / r[v as usize]).abs())
            .fold(0.0, f64::max);
        out["top1m_max_rel_err"] = json!(max_rel);
        tol_report.push(out);
    }

    let mut files = json!({});
    if let Some(p) = &a.out {
        let rf: Vec<f32> = r.iter().map(|&x| x as f32).collect();
        write_le(p, &rf, |x| x.to_le_bytes().to_vec())?;
        let (q, lo, hi) = quantise(&r);
        let qp = p.with_extension("u16");
        write_le(&qp, &q, |x| x.to_le_bytes().to_vec())?;
        let indeg: Vec<u32> = match &csr {
            Some(c) => c.off.windows(2).map(|w| (w[1] - w[0]) as u32).collect(),
            None => vec![],
        };
        let ip = p.with_extension("indeg");
        write_le(&ip, &indeg, |x| x.to_le_bytes().to_vec())?;
        // worst relative error of the u16 form on the top million
        let step = (hi - lo) / 65535.0;
        files = json!({"f32": p, "u16": qp, "indeg": ip, "u16_ln_lo": lo, "u16_ln_hi": hi,
            "u16_max_rel_err": (step / 2.0).exp() - 1.0, "bytes_f32": rf.len() * 4, "bytes_u16": q.len() * 2});
    }

    let sum: f64 = r.par_iter().sum();
    let top20: Vec<Value> = top[..20]
        .iter()
        .map(|&v| json!({"id": v, "rank": r[v as usize], "followers": csr.as_ref().map(|c| c.off[v as usize + 1] - c.off[v as usize]), "did": g.did_of(v)}))
        .collect();
    Ok(json!({
        "source": a.source, "before_us": a.before_us, "max_pass": a.max_pass, "relabel": a.relabel, "relabel_secs": relabel_s,
        "capped_sources": a.max_pass.map(|c| r.iter().zip(&outdeg).filter(|(&x, &o)| o > 0 && x / o as f64 > c / nf).count()), "warm": a.warm, "damping": d, "tol": a.tol,
        "ids": n, "edges": edges, "dangling": dangling,
        "open_secs": open_s, "export_secs": export_s, "export_cpu_secs": export_cpu, "export_peak_rss_gb": export_rss,
        "iterations": n_iter, "converged": converged, "iterate_secs": iter_s, "secs_per_iter": iter_s / n_iter.max(1) as f64,
        "total_secs": export_s + iter_s, "cpu_secs": cpu, "peak_rss_gb": peak, "threads": rayon::current_num_threads(),
        "rank_sum": sum, "tolerances": tol_report, "files": files, "top20": top20, "iters": iters,
    }))
}

#[cfg(test)]
mod tests {
    #[test]
    fn kendall() {
        assert_eq!(super::kendall_desc(&[4.0, 3.0, 2.0, 1.0]), 1.0);
        assert_eq!(super::kendall_desc(&[1.0, 2.0, 3.0, 4.0]), -1.0);
        assert!((super::kendall_desc(&[4.0, 2.0, 3.0, 1.0]) - (1.0 - 4.0 / 12.0)).abs() < 1e-12);
    }
}
