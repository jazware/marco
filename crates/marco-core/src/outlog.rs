//! Out logs: an actor's records in one relation, sorted by rkey (time order).
//!
//! ```text
//! varint n
//! varint dups        n less the distinct subjects (records repeating a subject)
//! nb > 1:            skip     nb x u64   first rkey of each block of 128
//!                    offsets  nb x u32   block start, from the first block
//! n >= subj_index:   bysubj   n x u32    positions sorted by (subject, position)
//! blocks:            subjects k x u32, then (nb == 1 only) the first rkey as u64,
//!                    then k - 1 rkey deltas as varints
//! ```
//!
//! Subjects are a plain u32 column so any position reads in one load, and a
//! block's subjects can be scanned without decoding rkeys. Only long lists
//! pay for `bysubj`; shorter ones scan their subjects.

use std::ops::Range;

use anyhow::Result;

use crate::format6::{Packer, Space};
use crate::read::{need_each, Blocks, Pending, ReadCx, Use, R};
use crate::varint::{self, u32_at, u64_at};

pub const BLOCK: usize = 128;

/// Bytes by part, summed over the lists an encoder wrote.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OutBytes {
    pub header: u64,
    pub skip: u64,
    pub bysubj: u64,
    pub subjects: u64,
    pub rkeys: u64,
}

impl OutBytes {
    pub fn add(&mut self, o: &OutBytes) {
        self.header += o.header;
        self.skip += o.skip;
        self.bysubj += o.bysubj;
        self.subjects += o.subjects;
        self.rkeys += o.rkeys;
    }

    /// Takes `o` away, as when a list it counted is replaced.
    pub fn sub(&mut self, o: &OutBytes) {
        self.header = self.header.saturating_sub(o.header);
        self.skip = self.skip.saturating_sub(o.skip);
        self.bysubj = self.bysubj.saturating_sub(o.bysubj);
        self.subjects = self.subjects.saturating_sub(o.subjects);
        self.rkeys = self.rkeys.saturating_sub(o.rkeys);
    }

    pub fn total(&self) -> u64 {
        self.header + self.skip + self.bysubj + self.subjects + self.rkeys
    }
}

/// `edges` sorted by rkey, rkeys unique.
pub fn encode(edges: &[(u64, u32)], subj_index: usize, out: &mut Vec<u8>, bytes: &mut OutBytes) {
    let n = edges.len();
    debug_assert!(n > 0 && edges.windows(2).all(|w| w[0].0 < w[1].0));
    let at = out.len();
    varint::put(out, n as u64);
    varint::put(out, (n - distinct(edges)) as u64);
    bytes.header += (out.len() - at) as u64;
    let nb = n.div_ceil(BLOCK);
    let offs_at = if nb > 1 {
        for b in 0..nb {
            out.extend_from_slice(&edges[b * BLOCK].0.to_le_bytes());
        }
        let o = out.len();
        out.resize(o + 4 * nb, 0);
        bytes.skip += 12 * nb as u64;
        o
    } else {
        0
    };
    if n >= subj_index {
        let mut pos: Vec<u32> = (0..n as u32).collect();
        pos.sort_unstable_by_key(|&p| (edges[p as usize].1, p));
        for p in pos {
            out.extend_from_slice(&p.to_le_bytes());
        }
        bytes.bysubj += 4 * n as u64;
    }
    let start = out.len();
    for (b, chunk) in edges.chunks(BLOCK).enumerate() {
        if nb > 1 {
            let off = (out.len() - start) as u32;
            out[offs_at + 4 * b..offs_at + 4 * b + 4].copy_from_slice(&off.to_le_bytes());
        }
        for &(_, s) in chunk {
            out.extend_from_slice(&s.to_le_bytes());
        }
        bytes.subjects += 4 * chunk.len() as u64;
        let r0 = out.len();
        if nb == 1 {
            out.extend_from_slice(&chunk[0].0.to_le_bytes());
        }
        for w in chunk.windows(2) {
            varint::put(out, w[1].0 - w[0].0);
        }
        bytes.rkeys += (out.len() - r0) as u64;
    }
}

fn distinct(edges: &[(u64, u32)]) -> usize {
    if edges.len() < 2 {
        return edges.len();
    }
    let mut s: Vec<u32> = edges.iter().map(|e| e.1).collect();
    s.sort_unstable();
    s.dedup();
    s.len()
}

/// An encoded list's record count and bytes by part, from its header.
pub fn bytes_of(b: &[u8], subj_index: usize) -> (usize, OutBytes) {
    let mut pos = 0;
    let n = varint::get(b, &mut pos) as usize;
    if n == 0 {
        return (0, OutBytes { header: pos as u64, ..Default::default() });
    }
    let _dups = varint::get(b, &mut pos);
    let nb = n.div_ceil(BLOCK);
    let mut o = OutBytes { header: pos as u64, subjects: 4 * n as u64, ..Default::default() };
    if nb > 1 {
        o.skip = 12 * nb as u64;
    }
    if n >= subj_index {
        o.bysubj = 4 * n as u64;
    }
    o.rkeys = b.len() as u64 - o.header - o.skip - o.bysubj - o.subjects;
    (n, o)
}

/// The bytes the list [`encode`] wrote at the start of `b` takes.
pub fn encoded_len(b: &[u8], subj_index: usize) -> usize {
    let mut pos = 0;
    let n = varint::get(b, &mut pos) as usize;
    if n == 0 {
        return pos;
    }
    varint::get(b, &mut pos);
    let nb = n.div_ceil(BLOCK);
    let offs = pos + 8 * nb;
    if nb > 1 {
        pos += 12 * nb;
    }
    if n >= subj_index {
        pos += 4 * n;
    }
    let (at, k) =
        if nb == 1 { (pos, n) } else { (pos + u32_at(b, offs + 4 * (nb - 1)) as usize, n - (nb - 1) * BLOCK) };
    let keys = at + 4 * k + if nb == 1 { 8 } else { 0 };
    keys + varint::len_of(&b[keys..], k - 1)
}

#[derive(Clone, Copy)]
pub struct OutList<'a> {
    n: usize,
    dups: usize,
    nb: usize,
    skip: &'a [u8],
    offs: &'a [u8],
    bysubj: Option<&'a [u8]>,
    blocks: &'a [u8],
}

impl<'a> OutList<'a> {
    pub fn parse(bytes: &'a [u8], subj_index: usize) -> OutList<'a> {
        let mut pos = 0;
        let n = varint::get(bytes, &mut pos) as usize;
        // a list merged empty is the one byte n = 0
        let dups = if n == 0 { 0 } else { (varint::get(bytes, &mut pos) as usize).min(n) };
        let nb = n.div_ceil(BLOCK);
        let (skip, offs) = if nb > 1 {
            let s = &bytes[pos..pos + 8 * nb];
            let o = &bytes[pos + 8 * nb..pos + 12 * nb];
            pos += 12 * nb;
            (s, o)
        } else {
            (&bytes[..0], &bytes[..0])
        };
        let bysubj = (n >= subj_index).then(|| {
            let s = &bytes[pos..pos + 4 * n];
            pos += 4 * n;
            s
        });
        OutList { n, dups, nb, skip, offs, bysubj, blocks: &bytes[pos..] }
    }

    pub fn len(&self) -> usize {
        self.n
    }

    /// Distinct subjects.
    pub fn distinct(&self) -> usize {
        self.n - self.dups
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    fn block_len(&self, b: usize) -> usize {
        (self.n - b * BLOCK).min(BLOCK)
    }

    fn block_start(&self, b: usize) -> usize {
        if self.nb == 1 {
            0
        } else {
            u32_at(self.offs, 4 * b) as usize
        }
    }

    fn first_key(&self, b: usize) -> u64 {
        if self.nb == 1 {
            u64_at(self.blocks, 4 * self.n)
        } else {
            u64_at(self.skip, 8 * b)
        }
    }

    pub fn subject(&self, pos: usize) -> u32 {
        let b = pos / BLOCK;
        u32_at(self.blocks, self.block_start(b) + 4 * (pos % BLOCK))
    }

    /// The rkeys of block `b`, in order.
    pub fn block_keys(&self, b: usize, out: &mut [u64; BLOCK]) -> usize {
        let k = self.block_len(b);
        let start = self.block_start(b) + 4 * k;
        let (mut key, mut pos) =
            if self.nb == 1 { (u64_at(self.blocks, start), start + 8) } else { (self.first_key(b), start) };
        out[0] = key;
        for o in out.iter_mut().take(k).skip(1) {
            key += varint::get(self.blocks, &mut pos);
            *o = key;
        }
        k
    }

    pub fn block_subjects(&self, b: usize) -> &'a [u8] {
        let s = self.block_start(b);
        &self.blocks[s..s + 4 * self.block_len(b)]
    }

    pub fn key_at(&self, pos: usize) -> u64 {
        let mut keys = [0u64; BLOCK];
        self.block_keys(pos / BLOCK, &mut keys);
        keys[pos % BLOCK]
    }

    /// The position of `key`, if present.
    pub fn find_key(&self, key: u64) -> Option<usize> {
        if self.n == 0 || key < self.first_key(0) {
            return None;
        }
        let (mut lo, mut hi) = (0, self.nb);
        while hi - lo > 1 {
            let mid = (lo + hi) / 2;
            if self.first_key(mid) <= key {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        let mut keys = [0u64; BLOCK];
        let k = self.block_keys(lo, &mut keys);
        keys[..k].binary_search(&key).ok().map(|i| lo * BLOCK + i)
    }

    /// Positions holding `subject`, newest first, until `f` returns false.
    pub fn for_subject(&self, subject: u32, mut f: impl FnMut(usize) -> bool) {
        if let Some(idx) = self.bysubj {
            let at = |i: usize| u32_at(idx, 4 * i) as usize;
            let (mut lo, mut hi) = (0, self.n);
            while lo < hi {
                let mid = (lo + hi) / 2;
                if self.subject(at(mid)) <= subject {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            let mut i = lo;
            while i > 0 && self.subject(at(i - 1)) == subject {
                if !f(at(i - 1)) {
                    return;
                }
                i -= 1;
            }
            return;
        }
        for b in (0..self.nb).rev() {
            let subj = self.block_subjects(b);
            let hit = subj.as_chunks::<4>().0.iter().fold(false, |acc, c| acc | (u32::from_le_bytes(*c) == subject));
            if !hit {
                continue;
            }
            for i in (0..subj.len() / 4).rev() {
                if u32_at(subj, 4 * i) == subject && !f(b * BLOCK + i) {
                    return;
                }
            }
        }
    }

    /// Positions of records naming `subject`, oldest first, until `f`
    /// returns false: the subject's run in `bysubj` read forward, or the
    /// blocks scanned from the first.
    pub fn for_subject_asc(&self, subject: u32, mut f: impl FnMut(usize) -> bool) {
        if let Some(idx) = self.bysubj {
            let at = |i: usize| u32_at(idx, 4 * i) as usize;
            let (mut lo, mut hi) = (0, self.n);
            while lo < hi {
                let mid = (lo + hi) / 2;
                if self.subject(at(mid)) < subject {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            let mut i = lo;
            while i < self.n && self.subject(at(i)) == subject {
                if !f(at(i)) {
                    return;
                }
                i += 1;
            }
            return;
        }
        for b in 0..self.nb {
            let subj = self.block_subjects(b);
            for i in 0..subj.len() / 4 {
                if u32_at(subj, 4 * i) == subject && !f(b * BLOCK + i) {
                    return;
                }
            }
        }
    }

    /// Records with rkey below `before` (all if None), newest first, until
    /// `f` returns false.
    pub fn for_each_desc(&self, before: Option<u64>, mut f: impl FnMut(u64, u32) -> bool) {
        let mut keys = [0u64; BLOCK];
        for b in (0..self.nb).rev() {
            if before.is_some_and(|c| self.first_key(b) >= c) {
                continue;
            }
            let k = self.block_keys(b, &mut keys);
            let subj = self.block_subjects(b);
            for i in (0..k).rev() {
                if before.is_some_and(|c| keys[i] >= c) {
                    continue;
                }
                if !f(keys[i], u32_at(subj, 4 * i)) {
                    return;
                }
            }
        }
    }

    pub fn to_vec(&self) -> Vec<(u64, u32)> {
        let mut v = Vec::with_capacity(self.n);
        let mut keys = [0u64; BLOCK];
        for b in 0..self.nb {
            let k = self.block_keys(b, &mut keys);
            let subj = self.block_subjects(b);
            for (i, &key) in keys.iter().enumerate().take(k) {
                v.push((key, u32_at(subj, 4 * i)));
            }
        }
        v
    }

    /// Distinct subjects, sorted.
    pub fn subjects_sorted(&self, out: &mut Vec<u32>) {
        out.reserve(self.n - self.dups);
        if let Some(by) = self.bysubj {
            for c in by.as_chunks::<4>().0 {
                let s = self.subject(u32::from_le_bytes(*c) as usize);
                if out.last() != Some(&s) {
                    out.push(s);
                }
            }
            return;
        }
        for b in 0..self.nb {
            for c in self.block_subjects(b).as_chunks::<4>().0 {
                out.push(u32::from_le_bytes(*c));
            }
        }
        out.sort_unstable();
        out.dedup();
    }
}

// ---------------------------------------------------------------- FORMAT 6

/// The columns a big list's blocks go into (format6.rs).
pub struct SplitCols<'a> {
    pub subj: &'a mut Packer,
    pub rkey: &'a mut Packer,
    pub bysubj: &'a mut Packer,
}

/// The spaces of a relation's columns in one shard.
#[derive(Clone, Copy)]
pub struct OutSpaces<'a> {
    pub subj: &'a Space,
    pub rkey: &'a Space,
    pub bysubj: &'a Space,
}

const HEAD_FIXED: usize = 12;
const HEAD_BLOCK: usize = 16;
const HEAD_FENCE: usize = 8;

fn col_at(at: u64, col: &str) -> Result<u32> {
    u32::try_from(at)
        .map_err(|_| anyhow::anyhow!("an out.{col} column passes 4 GiB in one shard: build with more shards"))
}

/// Writes a big list (more than [`crate::format6::WHOLE`] records): its
/// blocks into the columns, its head onto `head`.
///
/// ```text
/// head:  u32 n, u32 dups, u32 nbs (by-subject blocks; 0 below subj_index)
///        nb  x (u64 first rkey, u32 subject block at, u32 rkey block at)
///        nbs x (u32 first subject, u32 by-subject block at)
///        nbs x u8 (by-subject block count - 1)
/// subj:  per block, its k subjects as u32
/// rkey:  per block, its k - 1 rkey deltas as varints (the first rkey is in the head)
/// bysubj: per block of up to 128 positions sorted by (subject, position):
///        the positions as u32, then their subjects as one inset block
///        (Elias-Fano from the block's first subject, which is in the head;
///        repeats allowed)
/// ```
///
/// Subject and rkey blocks are positional, 128 records each. By-subject
/// blocks are filled to 128 by a fresh write and hold
/// [`crate::format6::MIN_FILL`] to 128 after a patch
/// ([`crate::format6::relayout`]), so an append rewrites the one it lands in.
///
/// `edges` sorted by rkey, rkeys unique.
pub fn encode_split(
    edges: &[(u64, u32)],
    subj_index: usize,
    cols: SplitCols,
    head: &mut Vec<u8>,
    bytes: &mut OutBytes,
) -> Result<()> {
    encode_split_over(edges, subj_index, cols, head, bytes, None).map(|_| ())
}

/// The version of a big list a patch replaces: its records and its head as
/// the shard's pinned head section holds it.
#[derive(Clone, Copy)]
pub struct OldSplit<'a> {
    pub edges: &'a [(u64, u32)],
    pub head: &'a [u8],
}

/// Blocks an encoder took from the old version instead of writing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Reused {
    pub blocks: u64,
    pub bysubj: u64,
    /// Block positions written: a subject block with its rkey block, or a
    /// by-subject block.
    pub written: u64,
}

/// The bytes of a big list's head that starts `head`: None if it's cut short.
pub fn split_head_len(head: &[u8]) -> Option<usize> {
    let h = head.get(..HEAD_FIXED)?;
    let n = u32_at(h, 0) as usize;
    let len = HEAD_FIXED + HEAD_BLOCK * n.div_ceil(BLOCK) + (HEAD_FENCE + 1) * u32_at(h, 8) as usize;
    (head.len() >= len).then_some(len)
}

/// Where the blocks of the big list whose head starts `head` are: `f(col,
/// at)` for each block that takes bytes, col 0 the subject column, 1 rkeys,
/// 2 by-subject. False if the head is cut short.
pub fn split_blocks(head: &[u8], mut f: impl FnMut(usize, u64)) -> bool {
    if split_head_len(head).is_none() {
        return false;
    }
    let n = u32_at(head, 0) as usize;
    let nbs = u32_at(head, 8) as usize;
    let nb = n.div_ceil(BLOCK);
    for b in 0..nb {
        let e = HEAD_FIXED + HEAD_BLOCK * b;
        f(0, u32_at(head, e + 8) as u64);
        if (n - b * BLOCK).min(BLOCK) > 1 {
            f(1, u32_at(head, e + 12) as u64);
        }
    }
    for j in 0..nbs {
        f(2, u32_at(head, HEAD_FIXED + HEAD_BLOCK * nb + HEAD_FENCE * j + 4) as u64);
    }
    true
}

/// (subject, position) for every record, sorted: the by-subject order.
fn by_subject(edges: &[(u64, u32)]) -> Vec<(u32, u32)> {
    let mut v: Vec<(u32, u32)> = edges.iter().enumerate().map(|(p, e)| (e.1, p as u32)).collect();
    v.sort_unstable();
    v
}

/// [`encode_split`] over `old`, the list's version in the shard's spaces:
/// a block whose records are the old block's at the same index keeps the old
/// block, and only its head entry is written. A block's bytes depend only on
/// its records (a subject or rkey block on its 128 records, a by-subject
/// block on its 128 (subject, position) pairs), so equal records mean equal
/// bytes. `bytes` counts every block, kept or not, as a full write would.
pub fn encode_split_over(
    edges: &[(u64, u32)],
    subj_index: usize,
    cols: SplitCols,
    head: &mut Vec<u8>,
    bytes: &mut OutBytes,
    old: Option<OldSplit>,
) -> Result<Reused> {
    let n = edges.len();
    debug_assert!(n > crate::format6::WHOLE && edges.windows(2).all(|w| w[0].0 < w[1].0));
    let old = match old {
        Some(o) => {
            let len = split_head_len(o.head).ok_or_else(|| anyhow::anyhow!("an old head cut short"))?;
            if u32_at(o.head, 0) as usize != o.edges.len() {
                anyhow::bail!("an old head of {} records over {} edges", u32_at(o.head, 0), o.edges.len());
            }
            Some((o, &o.head[..len]))
        }
        None => None,
    };
    let nb = n.div_ceil(BLOCK);
    let nbs = if n >= subj_index { nb } else { 0 };
    let mut reused = Reused::default();
    let h0 = head.len();
    head.extend_from_slice(&(n as u32).to_le_bytes());
    head.extend_from_slice(&((n - distinct(edges)) as u32).to_le_bytes());
    head.extend_from_slice(&(nbs as u32).to_le_bytes());
    let mut blk = Vec::with_capacity(4 * BLOCK);
    let mut keys = Vec::with_capacity(10 * BLOCK);
    let old_chunks: Vec<&[(u64, u32)]> = old.map_or_else(Vec::new, |(o, _)| o.edges.chunks(BLOCK).collect());
    for (b, chunk) in edges.chunks(BLOCK).enumerate() {
        if let Some((_, oh)) = old.filter(|_| old_chunks.get(b) == Some(&chunk)) {
            head.extend_from_slice(&oh[HEAD_FIXED + HEAD_BLOCK * b..HEAD_FIXED + HEAD_BLOCK * (b + 1)]);
            reused.blocks += 1;
            continue;
        }
        blk.clear();
        for &(_, s) in chunk {
            blk.extend_from_slice(&s.to_le_bytes());
        }
        keys.clear();
        for w in chunk.windows(2) {
            varint::put(&mut keys, w[1].0 - w[0].0);
        }
        reused.written += 1;
        let sat = col_at(cols.subj.put(&blk), "subj")?;
        let rat = col_at(cols.rkey.put(&keys), "rkey")?;
        head.extend_from_slice(&chunk[0].0.to_le_bytes());
        head.extend_from_slice(&sat.to_le_bytes());
        head.extend_from_slice(&rat.to_le_bytes());
    }
    if nbs > 0 {
        use crate::format6::Piece;
        let pairs = by_subject(edges);
        let pieces = match old {
            Some((o, oh)) if u32_at(oh, 8) > 0 => {
                let onb = o.edges.len().div_ceil(BLOCK);
                let onbs = u32_at(oh, 8) as usize;
                let f0 = HEAD_FIXED + HEAD_BLOCK * onb;
                let counts = &oh[f0 + HEAD_FENCE * onbs..f0 + (HEAD_FENCE + 1) * onbs];
                let lens: Vec<usize> = counts.iter().map(|&c| c as usize + 1).collect();
                if lens.iter().sum::<usize>() != o.edges.len() {
                    anyhow::bail!("an old by-subject index that doesn't hold its {} records", o.edges.len());
                }
                let old_pairs = by_subject(o.edges);
                crate::format6::relayout(&old_pairs, &lens, &pairs)
                    .into_iter()
                    .map(|p| (p, Some((&oh[f0..f0 + HEAD_FENCE * onbs], counts))))
                    .collect::<Vec<_>>()
            }
            _ => {
                (0..pairs.len()).step_by(BLOCK).map(|c| (Piece::Write(c..(c + BLOCK).min(pairs.len())), None)).collect()
            }
        };
        let f0 = head.len();
        head[h0 + 8..h0 + 12].copy_from_slice(&(pieces.len() as u32).to_le_bytes());
        let mut counts = Vec::with_capacity(pieces.len());
        let mut subj = Vec::with_capacity(BLOCK);
        for (p, o) in pieces {
            match (p, o) {
                (Piece::Keep(j), Some((fences, ocounts))) => {
                    head.extend_from_slice(&fences[HEAD_FENCE * j..HEAD_FENCE * (j + 1)]);
                    counts.push(ocounts[j]);
                    reused.bysubj += 1;
                }
                (Piece::Write(r), _) => {
                    let chunk = &pairs[r];
                    blk.clear();
                    subj.clear();
                    for &(s, p) in chunk {
                        blk.extend_from_slice(&p.to_le_bytes());
                        subj.push(s);
                    }
                    crate::inset::encode_block(&subj, &mut blk);
                    reused.written += 1;
                    let at = col_at(cols.bysubj.put(&blk), "bysubj")?;
                    head.extend_from_slice(&subj[0].to_le_bytes());
                    head.extend_from_slice(&at.to_le_bytes());
                    counts.push((chunk.len() - 1) as u8);
                }
                (Piece::Keep(_), None) => unreachable!("a kept block needs an old head"),
            }
        }
        debug_assert_eq!(head.len() - f0, HEAD_FENCE * counts.len());
        head.extend_from_slice(&counts);
    }
    bytes.add(&split_bytes(edges, subj_index));
    Ok(reused)
}

/// What [`encode_split`] counts in `bytes` for `edges`, without writing.
pub fn split_bytes(edges: &[(u64, u32)], subj_index: usize) -> OutBytes {
    let n = edges.len();
    let nb = n.div_ceil(BLOCK);
    let nbs = if n >= subj_index { nb } else { 0 };
    let mut o = OutBytes {
        header: (HEAD_FIXED + HEAD_BLOCK * nb + (HEAD_FENCE + 1) * nbs) as u64,
        subjects: 4 * n as u64,
        ..Default::default()
    };
    let mut keys = Vec::with_capacity(10 * BLOCK);
    for chunk in edges.chunks(BLOCK) {
        keys.clear();
        for w in chunk.windows(2) {
            varint::put(&mut keys, w[1].0 - w[0].0);
        }
        o.rkeys += keys.len() as u64;
    }
    if nbs > 0 {
        let mut blk = Vec::with_capacity(4 * BLOCK);
        let mut subj = Vec::with_capacity(BLOCK);
        for chunk in by_subject(edges).chunks(BLOCK) {
            blk.clear();
            subj.clear();
            subj.extend(chunk.iter().map(|&(s, _)| s));
            crate::inset::encode_block(&subj, &mut blk);
            o.bysubj += (4 * chunk.len() + blk.len()) as u64;
        }
    }
    o
}

/// A big list read through the page store: its head is pinned, its blocks
/// are paged. Every method first registers every page it will read
/// ([`Blocks::need`]), so a pass that misses names all of them, and only
/// then decodes.
#[derive(Clone, Copy)]
pub struct SplitOut<'g> {
    n: usize,
    dups: usize,
    nb: usize,
    nbs: usize,
    blocks: &'g [u8],
    fences: &'g [u8],
    /// Per by-subject block, its count less one.
    counts: &'g [u8],
    sp: OutSpaces<'g>,
}

/// A big list's subject column as [`Blocks`].
pub struct SubjBlocks<'s, 'g>(&'s SplitOut<'g>);
/// A big list's rkey column as [`Blocks`]: block b is its k - 1 deltas, to
/// the end of its page.
pub struct RkeyBlocks<'s, 'g>(&'s SplitOut<'g>);
/// A big list's by-subject blocks as [`Blocks`], each to the end of its page.
pub struct BySubjBlocks<'s, 'g>(&'s SplitOut<'g>);

// Each column's fetch reads a block without counting it as used, for
// need(); block() is what decodes call, and counts it.

impl<'g> SubjBlocks<'_, 'g> {
    fn fetch(&self, cx: &ReadCx<'g>, b: usize) -> R<&'g [u8]> {
        let l = self.0;
        l.sp.subj.slice(cx, l.subj_at(b) as u64, 4 * l.block_len(b))
    }
}

impl<'g> Blocks<'g> for SubjBlocks<'_, 'g> {
    fn block(&self, cx: &ReadCx<'g>, b: usize) -> R<&'g [u8]> {
        let t = self.fetch(cx, b)?;
        cx.used(t, self.0.sp.subj.kind(), Use::Whole);
        Ok(t)
    }

    fn need(&self, cx: &ReadCx<'g>, bs: Range<usize>) -> R<()> {
        need_each(bs, |b| self.fetch(cx, b).is_ok())
    }
}

impl<'g> RkeyBlocks<'_, 'g> {
    fn fetch(&self, cx: &ReadCx<'g>, b: usize) -> R<&'g [u8]> {
        let l = self.0;
        if l.block_len(b) == 1 {
            return Ok(&[]);
        }
        l.sp.rkey.tail(cx, l.rkey_at(b) as u64)
    }
}

impl<'g> Blocks<'g> for RkeyBlocks<'_, 'g> {
    fn block(&self, cx: &ReadCx<'g>, b: usize) -> R<&'g [u8]> {
        let t = self.fetch(cx, b)?;
        cx.used(t, self.0.sp.rkey.kind(), Use::Varints(self.0.block_len(b) as u32 - 1));
        Ok(t)
    }

    fn need(&self, cx: &ReadCx<'g>, bs: Range<usize>) -> R<()> {
        need_each(bs, |b| self.fetch(cx, b).is_ok())
    }
}

impl<'g> BySubjBlocks<'_, 'g> {
    fn fetch(&self, cx: &ReadCx<'g>, j: usize) -> R<&'g [u8]> {
        let l = self.0;
        l.sp.bysubj.tail(cx, l.by_at(j) as u64)
    }
}

impl<'g> Blocks<'g> for BySubjBlocks<'_, 'g> {
    fn block(&self, cx: &ReadCx<'g>, j: usize) -> R<&'g [u8]> {
        let t = self.fetch(cx, j)?;
        cx.used(t, self.0.sp.bysubj.kind(), Use::BySubj(self.0.counts[j] as u32 + 1));
        Ok(t)
    }

    fn need(&self, cx: &ReadCx<'g>, js: Range<usize>) -> R<()> {
        need_each(js, |j| self.fetch(cx, j).is_ok())
    }
}

impl<'g> SplitOut<'g> {
    /// The big list whose head starts `head`, its blocks in `sp`. None if
    /// the head is cut short.
    pub fn parse(head: &'g [u8], sp: OutSpaces<'g>) -> Option<SplitOut<'g>> {
        let h = head.get(..HEAD_FIXED)?;
        let n = u32_at(h, 0) as usize;
        let dups = (u32_at(h, 4) as usize).min(n);
        let nbs = u32_at(h, 8) as usize;
        let nb = n.div_ceil(BLOCK);
        let blocks = head.get(HEAD_FIXED..HEAD_FIXED + HEAD_BLOCK * nb)?;
        let f0 = HEAD_FIXED + HEAD_BLOCK * nb;
        let fences = head.get(f0..f0 + HEAD_FENCE * nbs)?;
        let counts = head.get(f0 + HEAD_FENCE * nbs..f0 + (HEAD_FENCE + 1) * nbs)?;
        Some(SplitOut { n, dups, nb, nbs, blocks, fences, counts, sp })
    }

    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    pub fn distinct(&self) -> usize {
        self.n - self.dups
    }

    /// Blocks of [`BLOCK`] records.
    pub fn blocks(&self) -> usize {
        self.nb
    }

    pub fn has_bysubj(&self) -> bool {
        self.nbs > 0
    }

    pub fn subjects(&self) -> SubjBlocks<'_, 'g> {
        SubjBlocks(self)
    }

    pub fn rkeys(&self) -> RkeyBlocks<'_, 'g> {
        RkeyBlocks(self)
    }

    pub fn by_subject(&self) -> BySubjBlocks<'_, 'g> {
        BySubjBlocks(self)
    }

    fn block_len(&self, b: usize) -> usize {
        (self.n - b * BLOCK).min(BLOCK)
    }

    pub fn first_key(&self, b: usize) -> u64 {
        u64_at(self.blocks, HEAD_BLOCK * b)
    }

    fn subj_at(&self, b: usize) -> u32 {
        u32_at(self.blocks, HEAD_BLOCK * b + 8)
    }

    fn rkey_at(&self, b: usize) -> u32 {
        u32_at(self.blocks, HEAD_BLOCK * b + 12)
    }

    fn fence(&self, j: usize) -> u32 {
        u32_at(self.fences, HEAD_FENCE * j)
    }

    fn by_at(&self, j: usize) -> u32 {
        u32_at(self.fences, HEAD_FENCE * j + 4)
    }

    /// Decodes block b's rkeys from its bytes.
    fn keys_of(&self, b: usize, bytes: &[u8], out: &mut [u64; BLOCK]) -> usize {
        let k = self.block_len(b);
        let mut key = self.first_key(b);
        out[0] = key;
        let mut pos = 0;
        for o in out.iter_mut().take(k).skip(1) {
            key += varint::get(bytes, &mut pos);
            *o = key;
        }
        k
    }

    /// By-subject block j: its positions (u32s) and subjects, decoded.
    fn by_block(&self, cx: &ReadCx<'g>, j: usize, subj: &mut [u32; BLOCK]) -> R<(&'g [u8], usize)> {
        let k = self.counts[j] as usize + 1;
        let b = self.by_subject().block(cx, j)?;
        crate::inset::decode_block(&b[4 * k..], self.fence(j), k, subj);
        Ok((&b[..4 * k], k))
    }

    /// By-subject block j: its positions (u32s), and which of its entries
    /// hold `subject`, found without decoding the block.
    fn by_find(&self, cx: &ReadCx<'g>, j: usize, subject: u32) -> R<(&'g [u8], Range<usize>)> {
        let k = self.counts[j] as usize + 1;
        let b = self.by_subject().block(cx, j)?;
        Ok((&b[..4 * k], crate::inset::block_find(&b[4 * k..], self.fence(j), k, subject)))
    }

    pub fn subject(&self, cx: &ReadCx<'g>, pos: usize) -> R<u32> {
        let b = self.subjects().block(cx, pos / BLOCK)?;
        Ok(u32_at(b, 4 * (pos % BLOCK)))
    }

    pub fn block_subjects(&self, cx: &ReadCx<'g>, b: usize) -> R<&'g [u8]> {
        self.subjects().block(cx, b)
    }

    pub fn block_keys(&self, cx: &ReadCx<'g>, b: usize, out: &mut [u64; BLOCK]) -> R<usize> {
        let bytes = self.rkeys().block(cx, b)?;
        Ok(self.keys_of(b, bytes, out))
    }

    pub fn key_at(&self, cx: &ReadCx<'g>, pos: usize) -> R<u64> {
        let mut keys = [0u64; BLOCK];
        self.block_keys(cx, pos / BLOCK, &mut keys)?;
        Ok(keys[pos % BLOCK])
    }

    pub fn find_key(&self, cx: &ReadCx<'g>, key: u64) -> R<Option<usize>> {
        if self.n == 0 || key < self.first_key(0) {
            return Ok(None);
        }
        let b = partition(self.nb, |b| self.first_key(b) <= key) - 1;
        if self.first_key(b) == key {
            return Ok(Some(b * BLOCK));
        }
        let mut keys = [0u64; BLOCK];
        let k = self.block_keys(cx, b, &mut keys)?;
        Ok(keys[..k].binary_search(&key).ok().map(|i| b * BLOCK + i))
    }

    /// The by-subject blocks that may hold `subject`.
    fn by_range(&self, subject: u32) -> Range<usize> {
        let lo = partition(self.nbs, |j| self.fence(j) < subject);
        let hi = partition(self.nbs, |j| self.fence(j) <= subject);
        lo.saturating_sub(1)..hi
    }

    /// Positions holding `subject`, newest first, until `f` returns false.
    ///
    /// Reads the by-subject blocks as the walk reaches them: a delete asks
    /// this per record whether another names the same subject, and every item
    /// of a list names that list, so needing the whole range first would make
    /// an account's deletion quadratic in its lists' sizes. The first missing
    /// block names the rest of the range, so a cold walk is still one round.
    pub fn for_subject(&self, cx: &ReadCx<'g>, subject: u32, mut f: impl FnMut(usize) -> bool) -> R<()> {
        if self.nbs > 0 {
            let js = self.by_range(subject);
            for j in js.clone().rev() {
                let (pos, hits) = match self.by_find(cx, j, subject) {
                    Ok(found) => found,
                    Err(Pending) => {
                        let _ = self.by_subject().need(cx, js.start..j);
                        return Err(Pending);
                    }
                };
                for i in hits.rev() {
                    if !f(u32_at(pos, 4 * i) as usize) {
                        return Ok(());
                    }
                }
            }
            return Ok(());
        }
        self.subjects().need(cx, 0..self.nb)?;
        for b in (0..self.nb).rev() {
            let s = self.subjects().block(cx, b)?;
            for i in (0..s.len() / 4).rev() {
                if u32_at(s, 4 * i) == subject && !f(b * BLOCK + i) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Positions holding `subject`, oldest first, until `f` returns false.
    pub fn for_subject_asc(&self, cx: &ReadCx<'g>, subject: u32, mut f: impl FnMut(usize) -> bool) -> R<()> {
        if self.nbs > 0 {
            let js = self.by_range(subject);
            self.by_subject().need(cx, js.clone())?;
            for j in js {
                let (pos, hits) = self.by_find(cx, j, subject)?;
                for i in hits {
                    if !f(u32_at(pos, 4 * i) as usize) {
                        return Ok(());
                    }
                }
            }
            return Ok(());
        }
        self.subjects().need(cx, 0..self.nb)?;
        for b in 0..self.nb {
            let s = self.subjects().block(cx, b)?;
            for i in 0..s.len() / 4 {
                if u32_at(s, 4 * i) == subject && !f(b * BLOCK + i) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Records with rkey below `before` (all if None), newest first, until
    /// `f` returns false. `want` bounds how many `f` takes: the blocks
    /// holding that many are fetched in one round, and a caller that takes
    /// more reads on past them.
    pub fn for_each_desc(
        &self,
        cx: &ReadCx<'g>,
        before: Option<u64>,
        want: usize,
        mut f: impl FnMut(u64, u32) -> bool,
    ) -> R<()> {
        let end = match before {
            None => self.nb,
            Some(c) => partition(self.nb, |b| self.first_key(b) < c),
        };
        let span = want.saturating_add(BLOCK - 1) / BLOCK + 1;
        let start = end.saturating_sub(span);
        both(self.subjects().need(cx, start..end), self.rkeys().need(cx, start..end))?;
        let mut keys = [0u64; BLOCK];
        for b in (0..end).rev() {
            let k = self.block_keys(cx, b, &mut keys)?;
            let subj = self.subjects().block(cx, b)?;
            for i in (0..k).rev() {
                if before.is_some_and(|c| keys[i] >= c) {
                    continue;
                }
                if !f(keys[i], u32_at(subj, 4 * i)) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub fn to_vec(&self, cx: &ReadCx<'g>) -> R<Vec<(u64, u32)>> {
        both(self.subjects().need(cx, 0..self.nb), self.rkeys().need(cx, 0..self.nb))?;
        let mut v = Vec::with_capacity(self.n);
        let mut keys = [0u64; BLOCK];
        for b in 0..self.nb {
            let k = self.block_keys(cx, b, &mut keys)?;
            let subj = self.subjects().block(cx, b)?;
            for (i, &key) in keys.iter().enumerate().take(k) {
                v.push((key, u32_at(subj, 4 * i)));
            }
        }
        Ok(v)
    }

    /// Distinct subjects, sorted.
    pub fn subjects_sorted(&self, cx: &ReadCx<'g>, out: &mut Vec<u32>) -> R<()> {
        if self.nbs > 0 {
            self.by_subject().need(cx, 0..self.nbs)?;
            out.reserve(self.n - self.dups);
            let mut subj = [0u32; BLOCK];
            for j in 0..self.nbs {
                let (_, k) = self.by_block(cx, j, &mut subj)?;
                for &s in &subj[..k] {
                    if out.last() != Some(&s) {
                        out.push(s);
                    }
                }
            }
            return Ok(());
        }
        self.subjects().need(cx, 0..self.nb)?;
        out.reserve(self.n);
        for b in 0..self.nb {
            for c in self.subjects().block(cx, b)?.as_chunks::<4>().0 {
                out.push(u32::from_le_bytes(*c));
            }
        }
        out.sort_unstable();
        out.dedup();
        Ok(())
    }
}

/// Both columns' misses registered before either fails the pass.
fn both(a: R<()>, b: R<()>) -> R<()> {
    a.and(b)
}

/// The first i in 0..n where `pred` turns false (it must be true, then false).
fn partition(n: usize, pred: impl Fn(usize) -> bool) -> usize {
    let (mut lo, mut hi) = (0, n);
    while lo < hi {
        let mid = (lo + hi) / 2;
        if pred(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// An out list in a FORMAT 6 shard: whole (a list of at most
/// [`crate::format6::WHOLE`] records, in [`OutList`]'s bytes, the one page that
/// holds it already read) or split into a pinned head and paged blocks. An
/// override built from deltas is a `Whole` of any length.
#[derive(Clone, Copy)]
pub enum PagedOut<'g> {
    Whole(OutList<'g>),
    Split(SplitOut<'g>),
}

impl<'g> PagedOut<'g> {
    /// The list an index entry names: a small one is read from its page
    /// now, a big one only has its head parsed.
    pub fn open(
        cx: &ReadCx<'g>,
        e: crate::format6::Entry,
        small: &'g Space,
        head: &'g [u8],
        sp: OutSpaces<'g>,
        subj_index: usize,
    ) -> R<PagedOut<'g>> {
        match e {
            crate::format6::Entry::Small(at) => {
                let t = small.tail(cx, at as u64)?;
                cx.used(t, small.kind(), Use::OutList(subj_index));
                Ok(PagedOut::Whole(OutList::parse(t, subj_index)))
            }
            crate::format6::Entry::Head(at) => {
                let h = head.get(at as usize..).ok_or(Pending)?;
                SplitOut::parse(h, sp).map(PagedOut::Split).ok_or(Pending)
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            PagedOut::Whole(l) => l.len(),
            PagedOut::Split(l) => l.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn distinct(&self) -> usize {
        match self {
            PagedOut::Whole(l) => l.distinct(),
            PagedOut::Split(l) => l.distinct(),
        }
    }

    pub fn blocks(&self) -> usize {
        self.len().div_ceil(BLOCK)
    }

    pub fn subject(&self, cx: &ReadCx<'g>, pos: usize) -> R<u32> {
        match self {
            PagedOut::Whole(l) => Ok(l.subject(pos)),
            PagedOut::Split(l) => l.subject(cx, pos),
        }
    }

    pub fn block_subjects(&self, cx: &ReadCx<'g>, b: usize) -> R<&'g [u8]> {
        match self {
            PagedOut::Whole(l) => Ok(l.block_subjects(b)),
            PagedOut::Split(l) => l.block_subjects(cx, b),
        }
    }

    pub fn block_keys(&self, cx: &ReadCx<'g>, b: usize, out: &mut [u64; BLOCK]) -> R<usize> {
        match self {
            PagedOut::Whole(l) => Ok(l.block_keys(b, out)),
            PagedOut::Split(l) => l.block_keys(cx, b, out),
        }
    }

    pub fn key_at(&self, cx: &ReadCx<'g>, pos: usize) -> R<u64> {
        match self {
            PagedOut::Whole(l) => Ok(l.key_at(pos)),
            PagedOut::Split(l) => l.key_at(cx, pos),
        }
    }

    pub fn find_key(&self, cx: &ReadCx<'g>, key: u64) -> R<Option<usize>> {
        match self {
            PagedOut::Whole(l) => Ok(l.find_key(key)),
            PagedOut::Split(l) => l.find_key(cx, key),
        }
    }

    pub fn for_subject(&self, cx: &ReadCx<'g>, subject: u32, f: impl FnMut(usize) -> bool) -> R<()> {
        match self {
            PagedOut::Whole(l) => {
                l.for_subject(subject, f);
                Ok(())
            }
            PagedOut::Split(l) => l.for_subject(cx, subject, f),
        }
    }

    pub fn for_subject_asc(&self, cx: &ReadCx<'g>, subject: u32, f: impl FnMut(usize) -> bool) -> R<()> {
        match self {
            PagedOut::Whole(l) => {
                l.for_subject_asc(subject, f);
                Ok(())
            }
            PagedOut::Split(l) => l.for_subject_asc(cx, subject, f),
        }
    }

    pub fn for_each_desc(
        &self,
        cx: &ReadCx<'g>,
        before: Option<u64>,
        want: usize,
        f: impl FnMut(u64, u32) -> bool,
    ) -> R<()> {
        match self {
            PagedOut::Whole(l) => {
                l.for_each_desc(before, f);
                Ok(())
            }
            PagedOut::Split(l) => l.for_each_desc(cx, before, want, f),
        }
    }

    pub fn to_vec(&self, cx: &ReadCx<'g>) -> R<Vec<(u64, u32)>> {
        match self {
            PagedOut::Whole(l) => Ok(l.to_vec()),
            PagedOut::Split(l) => l.to_vec(cx),
        }
    }

    pub fn subjects_sorted(&self, cx: &ReadCx<'g>, out: &mut Vec<u32>) -> R<()> {
        match self {
            PagedOut::Whole(l) => {
                l.subjects_sorted(out);
                Ok(())
            }
            PagedOut::Split(l) => l.subjects_sorted(cx, out),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(edges: &[(u64, u32)], subj_index: usize) {
        let mut enc = Vec::new();
        let mut bytes = OutBytes::default();
        encode(edges, subj_index, &mut enc, &mut bytes);
        assert_eq!(bytes.total(), enc.len() as u64);
        assert_eq!(bytes_of(&enc, subj_index), (edges.len(), bytes));
        let mut padded = enc.clone();
        padded.extend_from_slice(&[0x80, 0x81, 7, 0, 3]);
        assert_eq!(encoded_len(&padded, subj_index), enc.len());
        let l = OutList::parse(&enc, subj_index);
        assert_eq!(l.to_vec(), edges);
        let mut want: Vec<u32> = edges.iter().map(|e| e.1).collect();
        want.sort_unstable();
        want.dedup();
        assert_eq!(l.distinct(), want.len());
        let mut got = Vec::new();
        l.subjects_sorted(&mut got);
        assert_eq!(got, want);
        for (i, &(k, s)) in edges.iter().enumerate() {
            assert_eq!(l.find_key(k), Some(i));
            assert_eq!(l.key_at(i), k);
            assert_eq!(l.subject(i), s);
            assert_eq!(l.find_key(k + 1).is_some(), edges.iter().any(|e| e.0 == k + 1));
            let mut got = Vec::new();
            l.for_subject(s, |p| {
                got.push(p);
                true
            });
            let want: Vec<usize> = (0..edges.len()).rev().filter(|&p| edges[p].1 == s).collect();
            assert_eq!(got, want);
            let mut asc = Vec::new();
            l.for_subject_asc(s, |p| {
                asc.push(p);
                true
            });
            assert_eq!(asc, want.iter().rev().copied().collect::<Vec<_>>());
        }
        let cut = edges[edges.len() / 2].0;
        let mut desc = Vec::new();
        l.for_each_desc(Some(cut), |k, s| {
            desc.push((k, s));
            true
        });
        let want: Vec<(u64, u32)> = edges.iter().rev().filter(|e| e.0 < cut).copied().collect();
        assert_eq!(desc, want);
    }

    #[test]
    fn shapes() {
        let tid = |i: u64| 0x0d00_0000_0000_0000u64 + i * 1_234_567;
        for n in [1usize, 2, 127, 128, 129, 300, 1000] {
            let edges: Vec<(u64, u32)> = (0..n as u64).map(|i| (tid(i), (i * 7919 % 97) as u32)).collect();
            check(&edges, usize::MAX);
            check(&edges, 2);
        }
        check(&[(1, 5), (super::super::rkey::NON_TID | 3, 5)], 1);
    }
}
