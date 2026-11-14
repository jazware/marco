//! The base index: every key of every actor, sorted and front-coded in blocks
//! of 16, a posting (actor id and tier) per key, top lists for the prefixes
//! that cover more than `heavy_min` postings, a forward table from actor to
//! its postings, the rank per actor and the DID table. [`build`] writes it as
//! an artifact; [`Index`] reads one in place.
//!
//! A sorted array does what a weighted FST does here: front-coding gets most
//! of the prefix sharing, a lookup is a binary search over block heads and
//! one block's scan, and building it is a sort. The top lists stand in for
//! the FST's max-weight outputs, and they're small because few prefixes are
//! heavy.

use std::path::Path;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::artifact::{Artifact, Writer};
use crate::did::{self, DidKey, PACKED};
use crate::keys::{self, Hosting, Key, HANDLE, MAXK};

pub const BLOCK: usize = 16;
/// Above twice the API's largest page plus its filter margin: a heavy prefix always answers from its list.
pub const TOP: usize = 256;
pub const HEAVY_MIN: usize = 2048;
const ID_BITS: u32 = 30;
const ID_MASK: u32 = (1 << ID_BITS) - 1;

/// One unit of rank is about 1/4,400 of a factor of e (the ranks file's u16 spans ~15 nats), so a
/// nat is this many.
pub const NAT: i32 = 4400;
/// Tier weights in rank units: a later word or label is worth a factor of e less.
pub const TIER_W: [i32; 4] = [0, 0, -NAT, -NAT];

#[inline]
pub fn actor(p: u32) -> u32 {
    p & ID_MASK
}
#[inline]
pub fn tier(p: u32) -> u8 {
    (p >> ID_BITS) as u8
}
#[inline]
pub fn posting(id: u32, tier: u8) -> u32 {
    id | (tier as u32) << ID_BITS
}
/// A handle's fingerprint, kept per actor so an exact match is the handle itself, not anything
/// that folds the same (`jane-doe.example` for `jane.doe.example`). 0 is no handle.
pub fn handle_hash(handle: &str) -> u32 {
    if handle.is_empty() {
        return 0;
    }
    (xxhash_rust::xxh3::xxh3_64(handle.to_ascii_lowercase().as_bytes()) as u32) | 1
}

#[inline]
pub fn base_score(rank: u16, p: u32) -> i32 {
    rank as i32 + TIER_W[tier(p) as usize]
}

/// The artifact's JSON section: what built it and what it's consistent with.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Meta {
    /// Unique per build: the builder's UTC time and cursor.
    pub version: String,
    pub created_ms: i64,
    /// Jetstream's `time_us` this build includes every change up to: a pod replays from here.
    pub cursor_us: u64,
    /// PLC /export's `createdAt` cursor the build's handles are current to.
    #[serde(default)]
    pub plc_cursor: String,
    pub actors: u32,
    pub plc_actors: u32,
    pub keys: u64,
    pub hosting: Vec<String>,
    pub heavy_min: u32,
    /// The ranks file baked in: its creation time and marco checkpoint, or none.
    #[serde(default)]
    pub ranks_created_ms: i64,
    #[serde(default)]
    pub ranks_checkpoint: u64,
    /// Which actors the builder indexes: "profiles" (anyone with a profile record) or "all".
    #[serde(default)]
    pub index_set: String,
    #[serde(default)]
    pub handles_verified: u64,
    #[serde(default)]
    pub build_secs: f64,
}

/// One actor going into a build, in DID order. `handle` is empty unless verified.
pub struct ActorIn<'a> {
    pub did: &'a DidKey,
    pub handle: &'a str,
    pub name: &'a str,
    pub rank: u16,
    pub hidden: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct BuildStats {
    pub actors: u64,
    pub keys: u64,
    pub blob_bytes: u64,
    pub heads_bytes: u64,
    pub post_bytes: u64,
    pub top_prefixes: u64,
    pub top_bytes: u64,
    pub fwd_bytes: u64,
    pub did_bytes: u64,
    pub file_bytes: u64,
    pub secs: f64,
}

type Runs = Vec<(usize, usize)>;
type Tops = Vec<(Vec<u8>, Vec<u32>)>;

struct E {
    off: u64,
    len: u8,
    post: u32,
}

/// Builds the artifact at `path` from `actors` (sorted by DID, each once).
pub fn build(path: &Path, actors: &[ActorIn<'_>], hosting: &Hosting, meta: &mut Meta) -> Result<BuildStats> {
    let t = std::time::Instant::now();
    let n = actors.len();
    if n as u64 > ID_MASK as u64 {
        bail!("{n} actors: more than postings can address");
    }
    if actors.par_windows(2).any(|w| w[0].did >= w[1].did) {
        bail!("actors must be sorted by DID, each once");
    }
    let rank: Vec<u16> = actors.par_iter().map(|a| a.rank).collect();

    // keys, then one sort: equal keys put exact handles first, then by score
    let chunk = 1usize << 16;
    let parts: Vec<(Vec<u8>, Vec<E>)> = actors
        .par_chunks(chunk)
        .enumerate()
        .map(|(c, xs)| {
            let mut arena = Vec::new();
            let mut es = Vec::new();
            let (mut buf, mut ks) = (String::new(), Vec::<Key>::new());
            for (i, a) in xs.iter().enumerate() {
                let id = (c * chunk + i) as u32;
                keys::keys(a.handle, a.name, hosting, &mut buf, &mut ks);
                for (k, t) in &ks {
                    es.push(E { off: arena.len() as u64, len: k.len() as u8, post: posting(id, *t) });
                    arena.extend_from_slice(k);
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
    let score = |p: u32| base_score(rank[actor(p) as usize], p);
    es.par_sort_unstable_by(|a, b| {
        key(a)
            .cmp(key(b))
            .then((tier(a.post) != HANDLE).cmp(&(tier(b.post) != HANDLE)))
            .then(score(b.post).cmp(&score(a.post)))
            .then(a.post.cmp(&b.post))
    });

    // heavy prefixes, level by level inside the last level's heavy runs; a list only where the
    // prefix ends on a character boundary that isn't a space (queries never end in one)
    let heavy_min = HEAVY_MIN;
    let mut tops: Vec<(Vec<u8>, Vec<u32>)> = Vec::new();
    let mut runs: Vec<(usize, usize)> = vec![(0, es.len())];
    for l in 1..=MAXK {
        let next: Vec<(Runs, Tops)> = runs
            .par_iter()
            .map(|&(lo, hi)| {
                let (mut sub, mut lists) = (Vec::new(), Vec::new());
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
                        if p[l - 1] != b' ' && std::str::from_utf8(p).is_ok() {
                            lists.push((p.to_vec(), top_of(es[i..j].iter().map(|e| e.post), &score)));
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
            tops.extend(ls);
        }
        if runs.is_empty() {
            break;
        }
    }
    tops.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let mut w = Writer::create(path)?;
    // the DID table: did:plc packed, then the rest as strings
    let n_plc = actors.iter().take_while(|a| matches!(a.did, DidKey::Plc(_))).count();
    w.begin("did.plc")?;
    let mut buf = Vec::with_capacity(PACKED << 16);
    for a in &actors[..n_plc] {
        if let DidKey::Plc(v) = a.did {
            buf.extend_from_slice(&did::plc_to_bytes(*v));
        }
        if buf.len() >= PACKED << 16 {
            w.put(&buf)?;
            buf.clear();
        }
    }
    w.put(&buf)?;
    let (bits, starts) = did::radix(actors[..n_plc].iter().map(|a| match a.did {
        DidKey::Plc(v) => *v,
        DidKey::Other(_) => unreachable!(),
    }));
    w.section("did.radix", &starts)?;
    let mut ooff = vec![0u32];
    let mut obytes = Vec::new();
    for a in &actors[n_plc..] {
        if let DidKey::Other(s) = a.did {
            obytes.extend_from_slice(s.as_bytes());
        }
        ooff.push(obytes.len() as u32);
    }
    w.section("did.oth.off", &ooff)?;
    w.section("did.oth", &obytes)?;
    w.section("rank", &rank)?;
    let mut hidden = vec![0u64; n.div_ceil(64).max(1)];
    for (i, a) in actors.iter().enumerate() {
        if a.hidden {
            hidden[i / 64] |= 1 << (i % 64);
        }
    }
    w.section("hidden", &hidden)?;
    let hh: Vec<u32> = actors.par_iter().map(|a| handle_hash(a.handle)).collect();
    w.section("hhash", &hh)?;

    // front-coded keys, block heads, postings
    let mut heads = Vec::with_capacity(es.len() / BLOCK + 1);
    let mut post = Vec::with_capacity(es.len());
    w.begin("ix.blob")?;
    let (mut at, mut out) = (0u64, Vec::with_capacity(1 << 20));
    let mut prev: &[u8] = &[];
    for (i, e) in es.iter().enumerate() {
        let k = key(e);
        let shared = if i % BLOCK == 0 {
            heads.push(at + out.len() as u64);
            0
        } else {
            k.iter().zip(prev).take_while(|(a, b)| a == b).count()
        };
        out.push(shared as u8);
        out.push((k.len() - shared) as u8);
        out.extend_from_slice(&k[shared..]);
        post.push(e.post);
        prev = k;
        if out.len() >= 1 << 20 {
            at += out.len() as u64;
            w.put(&out)?;
            out.clear();
        }
    }
    at += out.len() as u64;
    w.put(&out)?;
    let blob_bytes = at;
    drop(arena);
    w.section("ix.heads", &heads)?;
    w.section("ix.post", &post)?;

    let (mut poff, mut pref, mut loff, mut list) = (vec![0u32], Vec::new(), vec![0u32], Vec::new());
    for (p, l) in &tops {
        pref.extend_from_slice(p);
        poff.push(pref.len() as u32);
        list.extend_from_slice(l);
        loff.push(list.len() as u32);
    }
    w.section("top.poff", &poff)?;
    w.section("top.pref", &pref)?;
    w.section("top.loff", &loff)?;
    w.section("top.list", &list)?;
    let top_bytes = (poff.len() * 4 + pref.len() + loff.len() * 4 + list.len() * 4) as u64;

    // forward table: each actor's posting positions
    let mut foff = vec![0u32; n + 1];
    for e in &es {
        foff[actor(e.post) as usize + 1] += 1;
    }
    for i in 0..n {
        foff[i + 1] += foff[i];
    }
    let mut fwd = vec![0u32; es.len()];
    let mut cur = foff.clone();
    for (i, e) in es.iter().enumerate() {
        let a = actor(e.post) as usize;
        fwd[cur[a] as usize] = i as u32;
        cur[a] += 1;
    }
    drop(cur);
    w.section("fwd.off", &foff)?;
    w.section("fwd", &fwd)?;

    meta.actors = n as u32;
    meta.plc_actors = n_plc as u32;
    meta.keys = es.len() as u64;
    meta.hosting = hosting.sorted();
    meta.heavy_min = heavy_min as u32;
    meta.build_secs = t.elapsed().as_secs_f64();
    let mut mj = serde_json::to_value(&*meta)?;
    mj["radix_bits"] = serde_json::json!(bits);
    w.section("meta", serde_json::to_vec(&mj)?.as_slice())?;
    let file_bytes = w.finish()?;
    Ok(BuildStats {
        actors: n as u64,
        keys: es.len() as u64,
        blob_bytes,
        heads_bytes: heads.len() as u64 * 8,
        post_bytes: post.len() as u64 * 4,
        top_prefixes: tops.len() as u64,
        top_bytes,
        fwd_bytes: (foff.len() + fwd.len()) as u64 * 4,
        did_bytes: (n_plc * PACKED + obytes.len() + ooff.len() * 4 + starts.len() * 4) as u64,
        file_bytes,
        secs: t.elapsed().as_secs_f64(),
    })
}

/// The best [`TOP`] postings of a run, one per actor.
fn top_of(posts: impl Iterator<Item = u32>, score: &impl Fn(u32) -> i32) -> Vec<u32> {
    let mut v: Vec<(i32, u32)> = posts.map(|p| (score(p), p)).collect();
    let k = (TOP * 8).min(v.len());
    if v.len() > k {
        v.select_nth_unstable_by(k - 1, |a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        v.truncate(k);
    }
    v.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut seen = rustc_hash::FxHashSet::default();
    v.into_iter().filter(|&(_, p)| seen.insert(actor(p))).take(TOP).map(|(_, p)| p).collect()
}

#[derive(Clone, Copy, Default)]
struct R {
    off: usize,
    len: usize,
}

#[derive(Clone, Copy, Default)]
struct Ranges {
    plc: R,
    radix: R,
    ooff: R,
    oth: R,
    rank: R,
    hidden: R,
    hhash: R,
    blob: R,
    heads: R,
    post: R,
    poff: R,
    pref: R,
    loff: R,
    list: R,
    foff: R,
    fwd: R,
}

/// A built index, read in place from its artifact.
pub struct Index {
    art: Artifact,
    r: Ranges,
    pub meta: Meta,
    pub hosting: Hosting,
    n: u32,
    n_plc: u32,
    radix_bits: u32,
}

macro_rules! sl {
    ($self:ident, $f:ident, $t:ty) => {{
        let b = &$self.art.bytes()[$self.r.$f.off..$self.r.$f.off + $self.r.$f.len];
        let s: &[$t] = bytemuck::cast_slice(b);
        s
    }};
}

impl Index {
    pub fn open(path: &Path, verify: bool) -> Result<Index> {
        Index::from_artifact(Artifact::open(path, verify)?)
    }

    pub fn from_artifact(art: Artifact) -> Result<Index> {
        let mj: serde_json::Value = serde_json::from_slice(art.raw("meta")?).context("meta")?;
        let meta: Meta = serde_json::from_value(mj.clone()).context("meta")?;
        let radix_bits = mj["radix_bits"].as_u64().unwrap_or(4) as u32;
        let get = |name: &str, elem: usize| -> Result<R> {
            let s = art.sections.iter().find(|s| s.name == name).with_context(|| format!("no section {name}"))?;
            if s.len % elem != 0 {
                bail!("section {name}: {} bytes", s.len);
            }
            Ok(R { off: s.off, len: s.len })
        };
        let r = Ranges {
            plc: get("did.plc", PACKED)?,
            radix: get("did.radix", 4)?,
            ooff: get("did.oth.off", 4)?,
            oth: get("did.oth", 1)?,
            rank: get("rank", 2)?,
            hidden: get("hidden", 8)?,
            hhash: get("hhash", 4)?,
            blob: get("ix.blob", 1)?,
            heads: get("ix.heads", 8)?,
            post: get("ix.post", 4)?,
            poff: get("top.poff", 4)?,
            pref: get("top.pref", 1)?,
            loff: get("top.loff", 4)?,
            list: get("top.list", 4)?,
            foff: get("fwd.off", 4)?,
            fwd: get("fwd", 4)?,
        };
        let n = meta.actors;
        let n_plc = meta.plc_actors;
        let ix = Index { art, r, hosting: Hosting::new(meta.hosting.iter().cloned()), meta, n, n_plc, radix_bits };
        if ix.ranks().len() != n as usize
            || ix.foff().len() != n as usize + 1
            || r.plc.len != n_plc as usize * PACKED
            || ix.ooff().len() != (n - n_plc) as usize + 1
            || ix.radix().len() != (1usize << radix_bits) + 1
            || ix.post().len() as u64 != ix.meta.keys
            || ix.fwd().len() != ix.post().len()
            || ix.heads().len() != ix.post().len().div_ceil(BLOCK)
        {
            bail!("artifact sections don't agree with its meta");
        }
        ix.validate()?;
        Ok(ix)
    }

    /// Every offset and id in range, once at open, so no read later can index out of bounds.
    fn validate(&self) -> Result<()> {
        let (n, keys) = (self.n as usize, self.post().len());
        let mono = |v: &[u32], last: usize, what: &str| -> Result<()> {
            if v.first() != Some(&0) || v.windows(2).any(|w| w[0] > w[1]) || *v.last().unwrap() as usize != last {
                bail!("{what} offsets aren't monotone up to {last}");
            }
            Ok(())
        };
        if self.radix_bits > 28 {
            bail!("radix bits {}", self.radix_bits);
        }
        mono(self.radix(), self.n_plc as usize, "did.radix")?;
        mono(self.ooff(), sl!(self, oth, u8).len(), "did.oth")?;
        mono(self.foff(), keys, "fwd")?;
        if self.post().par_iter().any(|&p| actor(p) as usize >= n) {
            bail!("a posting names an actor past the table");
        }
        if self.fwd().par_iter().any(|&f| f as usize >= keys) {
            bail!("the forward table names a posting past the index");
        }
        let (heads, blob) = (self.heads(), self.blob());
        if heads.windows(2).any(|w| w[0] >= w[1]) || heads.last().is_some_and(|&h| h as usize >= blob.len()) {
            bail!("block heads out of order or past the keys");
        }
        // every block's keys decode inside the blob, and each starts from its head
        let bad = (0..heads.len()).into_par_iter().any(|b| {
            let end = heads.get(b + 1).map_or(blob.len(), |&h| h as usize);
            let (mut o, mut prev) = (heads[b] as usize, 0usize);
            for i in 0..BLOCK.min(keys - b * BLOCK) {
                if o + 2 > end {
                    return true;
                }
                let (shared, len) = (blob[o] as usize, blob[o + 1] as usize);
                if (i == 0 && shared != 0) || shared > prev || shared + len > 255 || o + 2 + len > end {
                    return true;
                }
                prev = shared + len;
                o += 2 + len;
            }
            false
        });
        if bad {
            bail!("a key block doesn't decode");
        }
        let (poff, loff) = (sl!(self, poff, u32), sl!(self, loff, u32));
        mono(poff, sl!(self, pref, u8).len(), "top.pref")?;
        mono(loff, sl!(self, list, u32).len(), "top.list")?;
        if poff.len() != loff.len() || sl!(self, list, u32).iter().any(|&p| actor(p) as usize >= n) {
            bail!("top lists don't agree");
        }
        if sl!(self, hhash, u32).len() != n || sl!(self, hidden, u64).len() < n.div_ceil(64) {
            bail!("per-actor sections don't cover the actors");
        }
        Ok(())
    }

    /// The fingerprint of the actor's handle, 0 for none.
    #[inline]
    pub fn handle_hash(&self, id: u32) -> u32 {
        sl!(self, hhash, u32)[id as usize]
    }

    pub fn artifact(&self) -> &Artifact {
        &self.art
    }
    pub fn len(&self) -> u32 {
        self.n
    }
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }
    fn radix(&self) -> &[u32] {
        sl!(self, radix, u32)
    }
    fn ooff(&self) -> &[u32] {
        sl!(self, ooff, u32)
    }
    pub fn ranks(&self) -> &[u16] {
        sl!(self, rank, u16)
    }
    fn heads(&self) -> &[u64] {
        sl!(self, heads, u64)
    }
    fn blob(&self) -> &[u8] {
        sl!(self, blob, u8)
    }
    pub fn post(&self) -> &[u32] {
        sl!(self, post, u32)
    }
    fn foff(&self) -> &[u32] {
        sl!(self, foff, u32)
    }
    fn fwd(&self) -> &[u32] {
        sl!(self, fwd, u32)
    }

    #[inline]
    pub fn rank(&self, id: u32) -> u16 {
        self.ranks()[id as usize]
    }

    #[inline]
    pub fn hidden(&self, id: u32) -> bool {
        let h: &[u64] = sl!(self, hidden, u64);
        h[id as usize / 64] >> (id % 64) & 1 == 1
    }

    pub fn did(&self, id: u32) -> Option<String> {
        if id < self.n_plc {
            let b: &[u8] = sl!(self, plc, u8);
            let o = id as usize * PACKED;
            Some(did::unpack_plc(did::plc_from_bytes(&b[o..o + PACKED])))
        } else if id < self.n {
            let i = (id - self.n_plc) as usize;
            let off = self.ooff();
            let b: &[u8] = sl!(self, oth, u8);
            Some(String::from_utf8_lossy(&b[off[i] as usize..off[i + 1] as usize]).into_owned())
        } else {
            None
        }
    }

    pub fn id_of(&self, d: &str) -> Option<u32> {
        match did::pack_plc(d) {
            Some(v) => self.id_of_plc(v),
            None => {
                let off = self.ooff();
                let b: &[u8] = sl!(self, oth, u8);
                let m = (self.n - self.n_plc) as usize;
                let at = |i: usize| &b[off[i] as usize..off[i + 1] as usize];
                let (mut lo, mut hi) = (0, m);
                while lo < hi {
                    let mid = (lo + hi) / 2;
                    match at(mid).cmp(d.as_bytes()) {
                        std::cmp::Ordering::Less => lo = mid + 1,
                        std::cmp::Ordering::Greater => hi = mid,
                        std::cmp::Ordering::Equal => return Some(self.n_plc + mid as u32),
                    }
                }
                None
            }
        }
    }

    pub fn id_of_key(&self, k: &DidKey) -> Option<u32> {
        match k {
            DidKey::Plc(v) => self.id_of_plc(*v),
            DidKey::Other(s) => self.id_of(s),
        }
    }

    fn id_of_plc(&self, v: u128) -> Option<u32> {
        let b: &[u8] = sl!(self, plc, u8);
        let top = (v >> (120 - self.radix_bits)) as usize;
        let rx = self.radix();
        let (mut lo, mut hi) = (rx[top] as usize, rx[top + 1] as usize);
        let at = |i: usize| did::plc_from_bytes(&b[i * PACKED..]);
        while lo < hi {
            let m = (lo + hi) / 2;
            match at(m).cmp(&v) {
                std::cmp::Ordering::Less => lo = m + 1,
                std::cmp::Ordering::Greater => hi = m,
                std::cmp::Ordering::Equal => return Some(m as u32),
            }
        }
        None
    }

    /// Calls `f(position, key)` for postings from block `b` on, until it returns false.
    fn walk(&self, b: usize, mut f: impl FnMut(usize, &[u8]) -> bool) {
        let (blob, post) = (self.blob(), self.post());
        let mut key = [0u8; 256];
        let mut o = self.heads()[b] as usize;
        let mut i = b * BLOCK;
        while i < post.len() {
            let shared = blob[o] as usize;
            let len = blob[o + 1] as usize;
            key[shared..shared + len].copy_from_slice(&blob[o + 2..o + 2 + len]);
            o += 2 + len;
            if !f(i, &key[..shared + len]) {
                return;
            }
            i += 1;
        }
    }

    /// The key at posting position `pos`, into `out`; returns its length.
    pub fn key_at(&self, pos: usize, out: &mut [u8; 256]) -> usize {
        let mut len = 0;
        self.walk(pos / BLOCK, |i, k| {
            if i == pos {
                out[..k.len()].copy_from_slice(k);
                len = k.len();
                return false;
            }
            true
        });
        len
    }

    /// The first position whose key is >= q.
    fn lower(&self, q: &[u8]) -> usize {
        let (blob, heads, n) = (self.blob(), self.heads(), self.post().len());
        let b = heads.partition_point(|&o| {
            let len = blob[o as usize + 1] as usize;
            &blob[o as usize + 2..o as usize + 2 + len] < q
        });
        if b == 0 {
            return 0;
        }
        let end = (b * BLOCK).min(n);
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

    /// The positions whose key starts with `q`.
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
        let hi = if s.is_empty() { self.post().len() } else { self.lower(&s) };
        (lo, hi.max(lo))
    }

    /// Exact handle matches of `q`: the postings at the start of its range whose key is `q`.
    pub fn exact_handles(&self, q: &[u8], lo: usize, hi: usize, mut f: impl FnMut(u32)) {
        if lo >= hi {
            return;
        }
        let post = self.post();
        let mut seen = 0;
        self.walk(lo / BLOCK, |i, k| {
            if i < lo {
                return true;
            }
            if i >= hi || k != q || tier(post[i]) != HANDLE || seen >= 256 {
                return false;
            }
            seen += 1;
            f(post[i]);
            true
        });
    }

    /// The precomputed best postings of a heavy prefix.
    pub fn top(&self, q: &[u8]) -> Option<&[u32]> {
        let (poff, pref, loff, list): (&[u32], &[u8], &[u32], &[u32]) =
            (sl!(self, poff, u32), sl!(self, pref, u8), sl!(self, loff, u32), sl!(self, list, u32));
        let m = poff.len().checked_sub(1)?;
        let at = |i: usize| &pref[poff[i] as usize..poff[i + 1] as usize];
        let (mut lo, mut hi) = (0, m);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match at(mid).cmp(q) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(&list[loff[mid] as usize..loff[mid + 1] as usize]),
            }
        }
        None
    }

    /// Calls `f(key, tier)` for each of the actor's keys.
    pub fn keys_of(&self, id: u32, mut f: impl FnMut(&[u8], u8)) {
        let (foff, fwd, post) = (self.foff(), self.fwd(), self.post());
        let mut buf = [0u8; 256];
        for &pos in &fwd[foff[id as usize] as usize..foff[id as usize + 1] as usize] {
            let l = self.key_at(pos as usize, &mut buf);
            f(&buf[..l], tier(post[pos as usize]));
        }
    }

    pub fn key_count(&self, id: u32) -> usize {
        let foff = self.foff();
        (foff[id as usize + 1] - foff[id as usize]) as usize
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::fold::fold;

    pub struct Person {
        pub did: String,
        pub handle: &'static str,
        pub name: &'static str,
        pub rank: u16,
    }

    pub fn did_n(i: u32) -> String {
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

    pub fn people() -> Vec<Person> {
        let p = |i: u32, handle, name, rank| Person { did: did_n(i), handle, name, rank };
        vec![
            p(0, "alice.bsky.social", "Alice Smith", 100),
            p(1, "mary-jane.bsky.social", "Mary Jane Doe", 200),
            p(2, "jay.example.com", "Jay", 300),
            p(3, "bob.bsky.social", "José Bob", 400),
            p(4, "yamada.bsky.social", "山田太郎", 500),
            Person { did: "did:web:jay.example.org".into(), handle: "jaybird.example.org", name: "", rank: 50 },
        ]
    }

    /// Builds `people` (any order) into an index in memory.
    pub fn build_people(people: &[Person], hidden: &[&str]) -> Index {
        let mut ps: Vec<(DidKey, &Person)> = people.iter().map(|p| (did::key_of(&p.did), p)).collect();
        ps.sort_by(|a, b| a.0.cmp(&b.0));
        let ins: Vec<ActorIn> = ps
            .iter()
            .map(|(k, p)| ActorIn {
                did: k,
                handle: p.handle,
                name: p.name,
                rank: p.rank,
                hidden: hidden.contains(&p.handle),
            })
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.seek");
        let mut meta = Meta { version: "t".into(), cursor_us: 42, ..Default::default() };
        build(&path, &ins, &Hosting::new(["bsky.social".to_string()]), &mut meta).unwrap();
        let b = std::fs::read(&path).unwrap();
        Index::from_artifact(Artifact::from_bytes(&b, true).unwrap()).unwrap()
    }

    fn handle_of(ix: &Index, ps: &[Person], p: u32) -> &'static str {
        let d = ix.did(actor(p)).unwrap();
        ps.iter().find(|x| x.did == d).unwrap().handle
    }

    #[test]
    fn ranges_tops_forward_and_dids() {
        let ps = people();
        let ix = build_people(&ps, &[]);
        assert_eq!(ix.meta.cursor_us, 42);
        assert_eq!(ix.len(), 6);
        for p in &ps {
            let id = ix.id_of(&p.did).unwrap();
            assert_eq!(ix.did(id).unwrap(), p.did);
            assert_eq!(ix.rank(id), p.rank);
        }
        assert!(ix.id_of("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").is_none());
        assert!(ix.id_of("did:web:nope.example").is_none());
        let range_handles = |q: &str| -> Vec<&str> {
            let f = fold(q);
            let (lo, hi) = ix.range(f.as_bytes());
            let mut v: Vec<&str> = ix.post()[lo..hi].iter().map(|&p| handle_of(&ix, &ps, p)).collect();
            v.dedup();
            v
        };
        assert_eq!(range_handles("ali"), vec!["alice.bsky.social"]);
        assert_eq!(range_handles("jane d"), vec!["mary-jane.bsky.social"]);
        assert_eq!(range_handles("太郎"), vec!["yamada.bsky.social"]);
        assert_eq!(range_handles("example"), vec!["jay.example.com", "jaybird.example.org"]);
        assert!(range_handles("bsky").is_empty());
        assert!(range_handles("zz").is_empty());
        let jay = fold("jay.example.com");
        let (lo, hi) = ix.range(jay.as_bytes());
        let mut ex = Vec::new();
        ix.exact_handles(jay.as_bytes(), lo, hi, |p| ex.push(handle_of(&ix, &ps, p)));
        assert_eq!(ex, vec!["jay.example.com"]);
        assert!(ix.top(b"a").is_none());
        // the forward table gives each actor its keys back
        let id = ix.id_of(&ps[1].did).unwrap();
        let mut ks = Vec::new();
        ix.keys_of(id, |k, t| ks.push((String::from_utf8(k.to_vec()).unwrap(), t)));
        ks.sort();
        assert_eq!(ks.len(), 5);
        assert!(ks.contains(&("mary jane bsky social".to_string(), HANDLE)));
    }

    #[test]
    fn open_refuses_out_of_range_contents() {
        let ps = people();
        let ix = build_people(&ps, &[]);
        let sec = ix.artifact().sections.iter().find(|s| s.name == "ix.post").unwrap().clone();
        let mut b = ix.artifact().bytes().to_vec();
        b[sec.off..sec.off + 4].copy_from_slice(&(1_000_000u32).to_le_bytes());
        // the section's sum no longer matches; skip it to reach the content check
        let a = Artifact::from_bytes(&b, false).unwrap();
        assert!(Index::from_artifact(a).err().unwrap().to_string().contains("posting"));
        assert_eq!(ix.handle_hash(ix.id_of(&ps[0].did).unwrap()), handle_hash("Alice.bsky.social"));
    }

    #[test]
    fn heavy_prefixes_get_top_lists() {
        let mut ps = Vec::new();
        for i in 0..3000u32 {
            let h: &'static str = Box::leak(format!("j{i}.bsky.social").into_boxed_str());
            ps.push(Person { did: did_n(i), handle: h, name: "", rank: (i % 1000) as u16 });
        }
        let ix = build_people(&ps, &[]);
        let t = ix.top(b"j").unwrap();
        assert_eq!(t.len(), TOP);
        let ranks: Vec<u16> = t.iter().map(|&p| ix.rank(actor(p))).collect();
        assert!(ranks.windows(2).all(|w| w[0] >= w[1]));
        assert_eq!(ranks[0], 999);
        assert!(ix.top(b"j1").is_none());
    }
}
