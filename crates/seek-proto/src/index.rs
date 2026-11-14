//! The prefix index: every key of every actor, sorted, front-coded in blocks
//! of 16, with a posting (actor id and match tier) per key. Prefixes that cover
//! more than `heavy` postings keep a precomputed top list, so a one- or
//! two-letter query reads at most that list; any other prefix scans its range.

use std::collections::{HashMap, HashSet};

use rayon::prelude::*;

use crate::fold::{fold_into, word_starts};

pub const HANDLE: u32 = 0;
pub const NAME: u32 = 1;
pub const HSEG: u32 = 2;
pub const NSEG: u32 = 3;
pub const MAXK: usize = 32;
const BLOCK: usize = 16;
pub const TOP: usize = 128;
const ID_MASK: u32 = (1 << 30) - 1;

#[inline]
pub fn actor(p: u32) -> u32 {
    p & ID_MASK
}
#[inline]
pub fn tier(p: u32) -> u32 {
    p >> 30
}

/// Ranking knobs, in the rank's u16 units (log-scale: about 4,400 a factor of e).
#[derive(Clone, Copy)]
pub struct Weights {
    pub tier: [i32; 4],
}

impl Default for Weights {
    fn default() -> Self {
        Weights { tier: [0, 0, -4400, -4400] }
    }
}

#[inline]
pub fn score(rank: &[u16], w: &Weights, p: u32) -> i32 {
    rank[actor(p) as usize] as i32 + w.tier[tier(p) as usize]
}

fn cut(s: &str) -> &str {
    if s.len() <= MAXK {
        return s;
    }
    let mut e = MAXK;
    while !s.is_char_boundary(e) {
        e -= 1;
    }
    s[..e].trim_end()
}

/// An actor's keys: the folded handle, its label suffixes inside the part the
/// user picked (not a hosting suffix like `bsky.social`, not the TLD), the
/// folded display name and its word suffixes.
pub fn keys(handle: &str, name: &str, hosting: &HashSet<String>, buf: &mut String, out: &mut Vec<(String, u32)>) {
    out.clear();
    let mut starts = Vec::new();
    if !handle.is_empty() && handle != "handle.invalid" {
        fold_into(handle, buf);
        if !buf.is_empty() {
            let labels: Vec<&str> = handle.split('.').collect();
            let own = if labels.len() > 2 && hosting.contains(&labels[labels.len() - 2..].join(".")) {
                labels.len() - 2
            } else {
                labels.len().saturating_sub(1).max(1)
            };
            let mut own_f = String::new();
            fold_into(&labels[..own].join("."), &mut own_f);
            out.push((cut(buf).to_string(), HANDLE));
            word_starts(buf, 3, &mut starts);
            for &s in &starts {
                if s < own_f.len() {
                    out.push((cut(&buf[s..]).to_string(), HSEG));
                }
            }
        }
    }
    if !name.is_empty() {
        fold_into(name, buf);
        if !buf.is_empty() {
            out.push((cut(buf).to_string(), NAME));
            word_starts(buf, 4, &mut starts);
            for &s in &starts {
                out.push((cut(&buf[s..]).to_string(), NSEG));
            }
        }
    }
    out.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    out.dedup_by(|b, a| a.0 == b.0);
}

pub struct Index {
    heads: Vec<u64>,
    blob: Vec<u8>,
    post: Vec<u32>,
    heavy: HashMap<Box<[u8]>, Box<[u32]>>,
    pub heavy_min: usize,
}

#[derive(Debug, Clone)]
pub struct Sizes {
    pub entries: usize,
    pub blob: usize,
    pub heads: usize,
    pub post: usize,
    pub heavy_prefixes: usize,
    pub heavy_bytes: usize,
}

struct E {
    off: u64,
    len: u8,
    post: u32,
}

/// Builds the index over `n` actors whose keys `keys_of(id, out)` gives.
pub fn build(
    n: u32,
    keys_of: impl Fn(u32, &mut String, &mut Vec<(String, u32)>) + Sync,
    rank: &[u16],
    w: &Weights,
    heavy_min: usize,
) -> Index {
    let chunk = 1u32 << 16;
    let parts: Vec<(Vec<u8>, Vec<E>)> = (0..n.div_ceil(chunk))
        .into_par_iter()
        .map(|c| {
            let mut arena = Vec::new();
            let mut es = Vec::new();
            let mut buf = String::new();
            let mut ks = Vec::new();
            for id in c * chunk..((c + 1) * chunk).min(n) {
                keys_of(id, &mut buf, &mut ks);
                for (k, t) in &ks {
                    es.push(E { off: arena.len() as u64, len: k.len() as u8, post: id | t << 30 });
                    arena.extend_from_slice(k.as_bytes());
                }
            }
            (arena, es)
        })
        .collect();
    let mut arena = Vec::with_capacity(parts.iter().map(|p| p.0.len()).sum());
    let mut es = Vec::with_capacity(parts.iter().map(|p| p.1.len()).sum());
    for (a, e) in parts {
        let base = arena.len() as u64;
        arena.extend_from_slice(&a);
        es.extend(e.into_iter().map(|x| E { off: x.off + base, ..x }));
    }
    let key = |e: &E| &arena[e.off as usize..e.off as usize + e.len as usize];
    // equal keys: exact handles first, so the exact-match scan stops at the first other tier
    es.par_sort_unstable_by(|a, b| {
        key(a)
            .cmp(key(b))
            .then((tier(a.post) != HANDLE).cmp(&(tier(b.post) != HANDLE)))
            .then(score(rank, w, b.post).cmp(&score(rank, w, a.post)))
    });

    // heavy prefixes, level by level inside the previous level's heavy runs
    let mut heavy: HashMap<Box<[u8]>, Box<[u32]>> = HashMap::new();
    let mut runs: Vec<(usize, usize)> = vec![(0, es.len())];
    for l in 1..=MAXK {
        #[allow(clippy::type_complexity)]
        let next: Vec<(Vec<(usize, usize)>, Vec<(Box<[u8]>, Box<[u32]>)>)> = runs
            .par_iter()
            .map(|&(lo, hi)| {
                let mut sub = Vec::new();
                let mut lists = Vec::new();
                let mut i = lo;
                while i < hi && (es[i].len as usize) < l {
                    i += 1;
                }
                while i < hi {
                    let p = &key(&es[i])[..l];
                    let mut j = i + 1;
                    while j < hi && key(&es[j]).len() >= l && &key(&es[j])[..l] == p {
                        j += 1;
                    }
                    if j - i > heavy_min {
                        sub.push((i, j));
                        let boundary = p[l - 1] != b' ' && std::str::from_utf8(p).is_ok();
                        if boundary {
                            lists.push((
                                p.to_vec().into_boxed_slice(),
                                top_of(es[i..j].iter().map(|e| e.post), rank, w),
                            ));
                        }
                    }
                    i = j;
                }
                (sub, lists)
            })
            .collect();
        runs = Vec::new();
        for (s, ls) in next {
            runs.extend(s);
            heavy.extend(ls);
        }
        if runs.is_empty() {
            break;
        }
    }

    // front-coded blob
    let mut heads = Vec::with_capacity(es.len() / BLOCK + 1);
    let mut blob = Vec::with_capacity(arena.len() / 2);
    let mut post = Vec::with_capacity(es.len());
    let mut prev: &[u8] = &[];
    for (i, e) in es.iter().enumerate() {
        let k = key(e);
        let shared = if i % BLOCK == 0 {
            heads.push(blob.len() as u64);
            0
        } else {
            k.iter().zip(prev).take_while(|(a, b)| a == b).count()
        };
        blob.push(shared as u8);
        blob.push((k.len() - shared) as u8);
        blob.extend_from_slice(&k[shared..]);
        post.push(e.post);
        prev = k;
    }
    Index { heads, blob, post, heavy, heavy_min }
}

/// The top actors of a run of postings, each once, best first.
pub fn top_of(posts: impl Iterator<Item = u32>, rank: &[u16], w: &Weights) -> Box<[u32]> {
    let mut v: Vec<(i32, u32)> = posts.map(|p| (score(rank, w, p), p)).collect();
    let k = (TOP * 8).min(v.len());
    if v.len() > k {
        v.select_nth_unstable_by(k - 1, |a, b| b.0.cmp(&a.0));
        v.truncate(k);
    }
    v.sort_unstable_by_key(|x| std::cmp::Reverse(x.0));
    let mut seen = HashSet::with_capacity(TOP);
    v.into_iter().filter(|&(_, p)| seen.insert(actor(p))).take(TOP).map(|(_, p)| p).collect()
}

impl Index {
    pub fn sizes(&self) -> Sizes {
        Sizes {
            entries: self.post.len(),
            blob: self.blob.len(),
            heads: self.heads.len() * 8,
            post: self.post.len() * 4,
            heavy_prefixes: self.heavy.len(),
            heavy_bytes: self.heavy.iter().map(|(k, v)| k.len() + v.len() * 4 + 32).sum(),
        }
    }

    /// Calls `f(index, key)` for entries from block `b` on until it returns false.
    fn walk(&self, b: usize, mut f: impl FnMut(usize, &[u8]) -> bool) {
        let mut key = [0u8; 256];
        let mut o = self.heads[b] as usize;
        let mut i = b * BLOCK;
        while i < self.post.len() {
            let shared = self.blob[o] as usize;
            let len = self.blob[o + 1] as usize;
            key[shared..shared + len].copy_from_slice(&self.blob[o + 2..o + 2 + len]);
            let kl = shared + len;
            o += 2 + len;
            if !f(i, &key[..kl]) {
                return;
            }
            i += 1;
        }
    }

    /// The first entry whose key is >= q.
    fn lower(&self, q: &[u8]) -> usize {
        // blocks whose head < q; the answer is in the last of them, or right after it
        let b = self.heads.partition_point(|&o| {
            let len = self.blob[o as usize + 1] as usize;
            &self.blob[o as usize + 2..o as usize + 2 + len] < q
        });
        if b == 0 {
            return 0;
        }
        let end = (b * BLOCK).min(self.post.len());
        let mut at = end;
        self.walk(b - 1, |i, k| {
            if i >= end {
                return false;
            }
            if k >= q {
                at = i;
                return false;
            }
            true
        });
        at
    }

    /// The entries whose key starts with `q`.
    pub fn range(&self, q: &[u8]) -> (usize, usize) {
        let lo = self.lower(q);
        let mut s = q.to_vec();
        while let Some(&last) = s.last() {
            if last == 0xFF {
                s.pop();
            } else {
                *s.last_mut().unwrap() += 1;
                break;
            }
        }
        let hi = if s.is_empty() { self.post.len() } else { self.lower(&s) };
        (lo, hi)
    }

    /// Exact handle matches of `q`: the entries at the start of its range whose key is `q`.
    pub fn exact_handles(&self, q: &[u8], lo: usize, hi: usize, out: &mut Vec<u32>) {
        if lo >= hi {
            return;
        }
        let b = lo / BLOCK;
        let mut seen = 0;
        self.walk(b, |i, k| {
            if i < lo {
                return true;
            }
            if i >= hi || k != q || tier(self.post[i]) != HANDLE || seen > 64 {
                return false;
            }
            seen += 1;
            out.push(self.post[i]);
            true
        });
    }

    /// The best `k` actors for prefix `q` (folded), exact handle matches first,
    /// skipping those `skip` says. Returns (postings, entries scanned, served from a top list).
    pub fn search(
        &self,
        q: &[u8],
        k: usize,
        rank: &[u16],
        w: &Weights,
        skip: impl Fn(u32) -> bool,
        out: &mut Vec<u32>,
    ) -> (usize, bool) {
        out.clear();
        let q = if q.len() > MAXK { &q[..MAXK] } else { q };
        let (lo, hi) = self.range(q);
        if lo >= hi {
            return (0, false);
        }
        self.exact_handles(q, lo, hi, out);
        out.retain(|&p| !skip(actor(p)));
        if hi - lo > self.heavy_min {
            if let Some(l) = self.heavy.get(q) {
                for &p in l.iter() {
                    if out.len() >= k {
                        break;
                    }
                    if !skip(actor(p)) && !out.iter().any(|&o| actor(o) == actor(p)) {
                        out.push(p);
                    }
                }
                out.truncate(k);
                return (l.len(), true);
            }
        }
        let mut v: Vec<(i32, u32)> =
            self.post[lo..hi].iter().filter(|&&p| !skip(actor(p))).map(|&p| (score(rank, w, p), p)).collect();
        let want = (k * 4).min(v.len());
        if want > 0 && v.len() > want {
            v.select_nth_unstable_by(want - 1, |a, b| b.0.cmp(&a.0));
            v.truncate(want);
        }
        v.sort_unstable_by_key(|x| std::cmp::Reverse(x.0));
        for (_, p) in v {
            if out.len() >= k {
                break;
            }
            if !out.iter().any(|&o| actor(o) == actor(p)) {
                out.push(p);
            }
        }
        (hi - lo, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_prefixes() {
        let hosting: HashSet<String> = ["bsky.social".to_string()].into();
        let people = [
            ("alice.bsky.social", "Alice Smith"),
            ("mary-jane.bsky.social", "Mary Jane Doe"),
            ("jay.example.com", "Jay"),
            ("bob.bsky.social", "José Bob"),
            ("yamada.bsky.social", "山田太郎"),
        ];
        let rank: Vec<u16> = vec![100, 200, 300, 400, 500];
        let w = Weights::default();
        for heavy in [0, 1000] {
            let idx = build(
                people.len() as u32,
                |id, buf, out| keys(people[id as usize].0, people[id as usize].1, &hosting, buf, out),
                &rank,
                &w,
                heavy,
            );
            let mut out = Vec::new();
            let mut q = |s: &str| {
                idx.search(crate::fold::fold(s).as_bytes(), 10, &rank, &w, |_| false, &mut out);
                out.iter().map(|&p| actor(p)).collect::<Vec<_>>()
            };
            assert_eq!(q("ali"), vec![0]);
            assert_eq!(q("jane d"), vec![1]);
            assert_eq!(q("mary-j"), vec![1]);
            assert_eq!(q("jose"), vec![3]);
            assert_eq!(q("太郎"), vec![4]);
            assert_eq!(q("example"), vec![2]);
            assert!(q("bsky").is_empty());
            assert_eq!(q("j"), vec![3, 2, 1]);
            assert_eq!(q("jay.example.com"), vec![2]);
            assert!(q("zz").is_empty());
        }
    }
}
