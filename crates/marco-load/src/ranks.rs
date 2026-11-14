//! The ranks file `marco-load pagerank` writes: one row per actor in the
//! snapshot's directory, keyed by DID so a reader with ids of its own (seek)
//! can merge it. Little-endian throughout.
//!
//! ```text
//! header   128 bytes: magic "mrcranks", version, then the fields of `Header`, zero-padded
//! dids     n_plc × 15 bytes: did:plc packed as marco packs it, ascending
//!          then n − n_plc × (u16 length, bytes): every other DID, ascending by string
//! rank     n × f32: the PageRank, summing to 1 over the directory
//! rank16   n × u16: ln(rank) scaled between ln_lo (0) and ln_hi (65535)
//! followers n × u32: distinct followers that pass rank (self-follows and hidden accounts left out)
//! flags    n × u8: bit 0 hidden (taken down or deleted in marco)
//! trailer  xxh3-64 of every byte before it, then the magic again
//! ```
//!
//! A version bump is a new layout; a reader refuses versions it doesn't know.

use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};
use marco_core::did::{pack_plc, plc_from_bytes, plc_to_bytes, unpack_plc, PACKED_LEN};
use xxhash_rust::xxh3::Xxh3;

pub const MAGIC: &[u8; 8] = b"mrcranks";
pub const VERSION: u32 = 1;
pub const HEADER_LEN: usize = 128;
pub const HIDDEN: u8 = 1;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Header {
    pub created_ms: i64,
    /// The snapshot's directory build and checkpoint, so a reader can tell two runs' inputs apart.
    pub build: u64,
    pub checkpoint: u64,
    pub iterations: u32,
    pub converged: bool,
    pub damping: f64,
    /// 0 when the pass wasn't capped.
    pub max_pass: f64,
    pub l1: f64,
    pub ln_lo: f64,
    pub ln_hi: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub did: String,
    pub rank: f32,
    pub rank16: u16,
    pub followers: u32,
    pub flags: u8,
}

/// Sort key: did:plc first in packed order (which is string order), then the rest by string.
fn key(did: &str) -> (u8, u128, &str) {
    match pack_plc(did) {
        Some(v) => (0, v, ""),
        None => (1, 0, did),
    }
}

/// Writes `rows` (any order) to `path` through a temp file and a rename.
pub fn write(path: &Path, h: &Header, rows: &mut [Row]) -> Result<()> {
    rows.sort_unstable_by(|a, b| key(&a.did).cmp(&key(&b.did)));
    if rows.windows(2).any(|w| w[0].did == w[1].did) {
        bail!("ranks rows repeat a DID");
    }
    let n_plc = rows.iter().take_while(|r| pack_plc(&r.did).is_some()).count();
    let tmp = path.with_extension("tmp");
    let f = std::fs::File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
    let mut w = Hashing { w: BufWriter::with_capacity(1 << 20, f), h: Xxh3::new() };
    let mut head = Vec::with_capacity(HEADER_LEN);
    head.extend_from_slice(MAGIC);
    head.extend_from_slice(&VERSION.to_le_bytes());
    head.extend_from_slice(&(HEADER_LEN as u32).to_le_bytes());
    head.extend_from_slice(&(rows.len() as u64).to_le_bytes());
    head.extend_from_slice(&(n_plc as u64).to_le_bytes());
    head.extend_from_slice(&h.created_ms.to_le_bytes());
    head.extend_from_slice(&h.build.to_le_bytes());
    head.extend_from_slice(&h.checkpoint.to_le_bytes());
    head.extend_from_slice(&h.iterations.to_le_bytes());
    head.extend_from_slice(&(h.converged as u32).to_le_bytes());
    for x in [h.damping, h.max_pass, h.l1, h.ln_lo, h.ln_hi] {
        head.extend_from_slice(&x.to_le_bytes());
    }
    head.resize(HEADER_LEN, 0);
    w.put(&head)?;
    for r in &rows[..n_plc] {
        w.put(&plc_to_bytes(pack_plc(&r.did).unwrap()))?;
    }
    for r in &rows[n_plc..] {
        if r.did.len() > u16::MAX as usize {
            bail!("DID too long: {} bytes", r.did.len());
        }
        w.put(&(r.did.len() as u16).to_le_bytes())?;
        w.put(r.did.as_bytes())?;
    }
    for r in rows.iter() {
        w.put(&r.rank.to_le_bytes())?;
    }
    for r in rows.iter() {
        w.put(&r.rank16.to_le_bytes())?;
    }
    for r in rows.iter() {
        w.put(&r.followers.to_le_bytes())?;
    }
    let flags: Vec<u8> = rows.iter().map(|r| r.flags).collect();
    w.put(&flags)?;
    let sum = w.h.digest();
    let mut out = w.w.into_inner().map_err(|e| e.into_error())?;
    out.write_all(&sum.to_le_bytes())?;
    out.write_all(MAGIC)?;
    out.sync_all()?;
    drop(out);
    std::fs::rename(&tmp, path).with_context(|| format!("rename to {}", path.display()))?;
    Ok(())
}

struct Hashing<W: Write> {
    w: W,
    h: Xxh3,
}

impl<W: Write> Hashing<W> {
    fn put(&mut self, b: &[u8]) -> Result<()> {
        self.h.update(b);
        self.w.write_all(b)?;
        Ok(())
    }
}

/// Reads and checks a whole ranks file: rows in file order.
pub fn read(path: &Path) -> Result<(Header, Vec<Row>)> {
    let b = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    parse(&b).with_context(|| format!("ranks file {}", path.display()))
}

pub fn parse(b: &[u8]) -> Result<(Header, Vec<Row>)> {
    if b.len() < HEADER_LEN + 16 || &b[..8] != MAGIC || &b[b.len() - 8..] != MAGIC {
        bail!("not a ranks file");
    }
    let u32_at = |o: usize| u32::from_le_bytes(b[o..o + 4].try_into().unwrap());
    let u64_at = |o: usize| u64::from_le_bytes(b[o..o + 8].try_into().unwrap());
    let f64_at = |o: usize| f64::from_le_bytes(b[o..o + 8].try_into().unwrap());
    let version = u32_at(8);
    if version != VERSION {
        bail!("version {version}, this build reads {VERSION}");
    }
    let body = &b[..b.len() - 16];
    if xxhash_rust::xxh3::xxh3_64(body) != u64_at(b.len() - 16) {
        bail!("checksum mismatch");
    }
    let hl = u32_at(12) as usize;
    let n = u64_at(16) as usize;
    let n_plc = u64_at(24) as usize;
    if hl < 112 || n_plc > n || body.len() < hl {
        bail!("bad header");
    }
    let h = Header {
        created_ms: u64_at(32) as i64,
        build: u64_at(40),
        checkpoint: u64_at(48),
        iterations: u32_at(56),
        converged: u32_at(60) != 0,
        damping: f64_at(64),
        max_pass: f64_at(72),
        l1: f64_at(80),
        ln_lo: f64_at(88),
        ln_hi: f64_at(96),
    };
    let mut o = hl;
    let need = |o: usize, k: usize| -> Result<()> {
        if o + k > body.len() {
            bail!("truncated");
        }
        Ok(())
    };
    let mut dids = Vec::with_capacity(n);
    need(o, n_plc * PACKED_LEN)?;
    for i in 0..n_plc {
        dids.push(unpack_plc(plc_from_bytes(&b[o + i * PACKED_LEN..])));
    }
    o += n_plc * PACKED_LEN;
    for _ in n_plc..n {
        need(o, 2)?;
        let l = u16::from_le_bytes([b[o], b[o + 1]]) as usize;
        need(o + 2, l)?;
        dids.push(std::str::from_utf8(&b[o + 2..o + 2 + l]).context("DID not UTF-8")?.to_string());
        o += 2 + l;
    }
    need(o, n * 11)?;
    if o + n * 11 != body.len() {
        bail!("{} bytes past the columns", body.len() - o - n * 11);
    }
    let (r0, r16, f0, fl) = (o, o + n * 4, o + n * 6, o + n * 10);
    let rows = dids
        .into_iter()
        .enumerate()
        .map(|(i, did)| Row {
            did,
            rank: f32::from_le_bytes(b[r0 + i * 4..r0 + i * 4 + 4].try_into().unwrap()),
            rank16: u16::from_le_bytes([b[r16 + i * 2], b[r16 + i * 2 + 1]]),
            followers: u32::from_le_bytes(b[f0 + i * 4..f0 + i * 4 + 4].try_into().unwrap()),
            flags: b[fl + i],
        })
        .collect();
    Ok((h, rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows() -> Vec<Row> {
        let mk = |did: &str, rank: f32, followers: u32, flags: u8| Row {
            did: did.into(),
            rank,
            rank16: (rank * 1000.0) as u16,
            followers,
            flags,
        };
        vec![
            mk("did:web:example.com", 0.1, 3, 0),
            mk("did:plc:z72i7hdynmk6r22z27h6tvur", 0.5, 30, 0),
            mk("did:plc:ewvi7nxzyoun6zhxrhs64oiz", 0.3, 9, HIDDEN),
            mk("did:key:zQ3sh", 0.1, 0, 0),
        ]
    }

    #[test]
    fn round_trip_sorted_by_did() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("r.ranks");
        let h = Header {
            created_ms: 7,
            build: 9,
            iterations: 15,
            converged: true,
            damping: 0.85,
            ln_lo: -20.0,
            ln_hi: -2.0,
            ..Default::default()
        };
        write(&p, &h, &mut rows()).unwrap();
        let (h2, got) = read(&p).unwrap();
        assert_eq!(h2, h);
        let order: Vec<&str> = got.iter().map(|r| r.did.as_str()).collect();
        assert_eq!(
            order,
            [
                "did:plc:ewvi7nxzyoun6zhxrhs64oiz",
                "did:plc:z72i7hdynmk6r22z27h6tvur",
                "did:key:zQ3sh",
                "did:web:example.com"
            ]
        );
        assert_eq!(got[0].flags, HIDDEN);
        assert_eq!(got[1].followers, 30);
        assert_eq!(got[3].rank, 0.1);
    }

    #[test]
    fn refuses_damage() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("r.ranks");
        write(&p, &Header::default(), &mut rows()).unwrap();
        let good = std::fs::read(&p).unwrap();
        let mut flipped = good.clone();
        flipped[HEADER_LEN + 3] ^= 1;
        assert!(parse(&flipped).unwrap_err().to_string().contains("checksum"));
        assert!(parse(&good[..good.len() - 1]).is_err());
        let mut v2 = good.clone();
        v2[8] = 2;
        assert!(parse(&v2).unwrap_err().to_string().contains("version"));
        let mut dup = rows();
        dup.push(dup[0].clone());
        assert!(write(&p, &Header::default(), &mut dup).is_err());
    }
}
