//! Reads the ranks file `marco-load pagerank` writes (its layout is in
//! marco-load's README). Rows come in seek's DID order, so a build merges
//! them with its actors in one pass.

use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::did::{plc_from_bytes, DidKey, PACKED};

pub const MAGIC: &[u8; 8] = b"mrcranks";
pub const VERSION: u32 = 1;
pub const HIDDEN: u8 = 1;
const HEADER_LEN: usize = 128;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Header {
    pub rows: usize,
    pub plc_rows: usize,
    pub created_ms: i64,
    pub build: u64,
    pub checkpoint: u64,
    pub iterations: u32,
    pub converged: bool,
    pub max_pass: f64,
    pub l1: f64,
}

pub struct RanksFile {
    map: memmap2::Mmap,
    pub header: Header,
    dids: usize,
    cols: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub did: DidKey,
    pub rank16: u16,
    pub followers: u32,
    pub hidden: bool,
}

impl RanksFile {
    pub fn open(path: &Path) -> Result<RanksFile> {
        let f = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
        // SAFETY: ranks files are written to a temp name and renamed into place, never rewritten.
        let map = unsafe { memmap2::Mmap::map(&f)? };
        RanksFile::check(map).with_context(|| format!("ranks file {}", path.display()))
    }

    fn check(map: memmap2::Mmap) -> Result<RanksFile> {
        let b: &[u8] = &map;
        if b.len() < HEADER_LEN + 16 || &b[..8] != MAGIC || &b[b.len() - 8..] != MAGIC {
            bail!("not a ranks file");
        }
        let u32_at = |o: usize| u32::from_le_bytes(b[o..o + 4].try_into().unwrap());
        let u64_at = |o: usize| u64::from_le_bytes(b[o..o + 8].try_into().unwrap());
        let f64_at = |o: usize| f64::from_le_bytes(b[o..o + 8].try_into().unwrap());
        if u32_at(8) != VERSION {
            bail!("version {}, this build reads {VERSION}", u32_at(8));
        }
        let body = &b[..b.len() - 16];
        if xxhash_rust::xxh3::xxh3_64(body) != u64_at(b.len() - 16) {
            bail!("checksum mismatch");
        }
        let hl = u32_at(12) as usize;
        let (n, n_plc) = (u64_at(16) as usize, u64_at(24) as usize);
        let header = Header {
            rows: n,
            plc_rows: n_plc,
            created_ms: u64_at(32) as i64,
            build: u64_at(40),
            checkpoint: u64_at(48),
            iterations: u32_at(56),
            converged: u32_at(60) != 0,
            max_pass: f64_at(72),
            l1: f64_at(80),
        };
        let cols = n.checked_mul(11);
        let dids = n_plc.checked_mul(PACKED).and_then(|d| d.checked_add(hl));
        let (Some(cols), Some(mut o)) = (cols, dids) else { bail!("bad header") };
        if n_plc > n || hl < 104 || o > body.len() {
            bail!("bad header");
        }
        for _ in n_plc..n {
            if o + 2 > body.len() {
                bail!("truncated");
            }
            o += 2 + u16::from_le_bytes([b[o], b[o + 1]]) as usize;
            if o > body.len() {
                bail!("truncated");
            }
        }
        if o.checked_add(cols) != Some(body.len()) {
            bail!("columns don't fit the file");
        }
        Ok(RanksFile { map, header, dids: hl, cols: o })
    }

    /// The rows in file order, which is seek's DID order.
    pub fn rows(&self) -> impl Iterator<Item = Row> + '_ {
        let b: &[u8] = &self.map;
        let (n, n_plc) = (self.header.rows, self.header.plc_rows);
        let (r16, fo, fl) = (self.cols + n * 4, self.cols + n * 6, self.cols + n * 10);
        let mut o = self.dids;
        (0..n).map(move |i| {
            let did = if i < n_plc {
                let v = plc_from_bytes(&b[o..o + PACKED]);
                o += PACKED;
                DidKey::Plc(v)
            } else {
                let l = u16::from_le_bytes([b[o], b[o + 1]]) as usize;
                let s = String::from_utf8_lossy(&b[o + 2..o + 2 + l]).into();
                o += 2 + l;
                DidKey::Other(s)
            };
            Row {
                did,
                rank16: u16::from_le_bytes([b[r16 + i * 2], b[r16 + i * 2 + 1]]),
                followers: u32::from_le_bytes(b[fo + i * 4..fo + i * 4 + 4].try_into().unwrap()),
                hidden: b[fl + i] & HIDDEN != 0,
            }
        })
    }
}

/// Writes a ranks file in marco-load's layout, for tests and for ranking by other means.
pub fn write(path: &Path, h: &Header, rows: &mut [Row]) -> Result<()> {
    rows.sort_by(|a, b| a.did.cmp(&b.did));
    let n_plc = rows.iter().take_while(|r| matches!(r.did, DidKey::Plc(_))).count();
    let mut b = Vec::new();
    b.extend_from_slice(MAGIC);
    b.extend_from_slice(&VERSION.to_le_bytes());
    b.extend_from_slice(&(HEADER_LEN as u32).to_le_bytes());
    b.extend_from_slice(&(rows.len() as u64).to_le_bytes());
    b.extend_from_slice(&(n_plc as u64).to_le_bytes());
    b.extend_from_slice(&h.created_ms.to_le_bytes());
    b.extend_from_slice(&h.build.to_le_bytes());
    b.extend_from_slice(&h.checkpoint.to_le_bytes());
    b.extend_from_slice(&h.iterations.to_le_bytes());
    b.extend_from_slice(&(h.converged as u32).to_le_bytes());
    for x in [0.85, h.max_pass, h.l1, 0.0, 0.0f64] {
        b.extend_from_slice(&x.to_le_bytes());
    }
    b.resize(HEADER_LEN, 0);
    for r in rows.iter() {
        match &r.did {
            DidKey::Plc(v) => b.extend_from_slice(&crate::did::plc_to_bytes(*v)),
            DidKey::Other(s) => {
                b.extend_from_slice(&(s.len() as u16).to_le_bytes());
                b.extend_from_slice(s.as_bytes());
            }
        }
    }
    for r in rows.iter() {
        b.extend_from_slice(&(r.rank16 as f32 / 65535.0).to_le_bytes());
    }
    for r in rows.iter() {
        b.extend_from_slice(&r.rank16.to_le_bytes());
    }
    for r in rows.iter() {
        b.extend_from_slice(&r.followers.to_le_bytes());
    }
    b.extend(rows.iter().map(|r| r.hidden as u8));
    let sum = xxhash_rust::xxh3::xxh3_64(&b);
    b.extend_from_slice(&sum.to_le_bytes());
    b.extend_from_slice(MAGIC);
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, &b)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::did::key_of;

    /// Written by marco-load's ranks.rs from its round-trip test's rows, so the two stay in step.
    const MARCO_WRITTEN: &[u8] = include_bytes!("../testdata/marco.ranks");

    #[test]
    fn reads_what_marco_load_writes() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("m.ranks");
        std::fs::write(&p, MARCO_WRITTEN).unwrap();
        let f = RanksFile::open(&p).unwrap();
        assert_eq!(f.header.rows, 4);
        assert_eq!(f.header.checkpoint, 0);
        assert!(f.header.converged);
        let rows: Vec<Row> = f.rows().collect();
        let dids: Vec<String> = rows.iter().map(|r| r.did.to_did()).collect();
        assert_eq!(
            dids,
            [
                "did:plc:ewvi7nxzyoun6zhxrhs64oiz",
                "did:plc:z72i7hdynmk6r22z27h6tvur",
                "did:key:zQ3sh",
                "did:web:example.com"
            ]
        );
        assert!(rows[0].hidden);
        assert_eq!(rows[1].followers, 30);
        assert_eq!(rows[1].rank16, 500);
    }

    #[test]
    fn writes_what_it_reads() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("w.ranks");
        let mut rows = vec![
            Row { did: key_of("did:web:a.example"), rank16: 9, followers: 1, hidden: false },
            Row { did: key_of("did:plc:z72i7hdynmk6r22z27h6tvur"), rank16: 7, followers: 2, hidden: true },
        ];
        write(&p, &Header { checkpoint: 5, ..Default::default() }, &mut rows).unwrap();
        let f = RanksFile::open(&p).unwrap();
        assert_eq!(f.header.checkpoint, 5);
        assert_eq!(f.rows().collect::<Vec<_>>(), rows);
        let mut b = std::fs::read(&p).unwrap();
        b[130] ^= 1;
        std::fs::write(&p, &b).unwrap();
        assert!(RanksFile::open(&p).is_err());
    }
}
