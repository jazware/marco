//! The artifact's container: named sections, each 64-byte aligned and
//! checksummed, behind a 4 KiB header. A pod maps the file and reads every
//! section in place; nothing is copied or parsed but the small JSON meta.
//!
//! ```text
//! 0     magic "seekidx\0", version u32, section count u32
//! 16    count × (name [u8; 16], offset u64, length u64, xxh3-64 u64)
//! 4088  xxh3-64 of bytes 0..4088
//! 4096  sections
//! ```

use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};
use xxhash_rust::xxh3::{xxh3_64, Xxh3};

pub const MAGIC: &[u8; 8] = b"seekidx\0";
pub const VERSION: u32 = 1;
const HEADER: usize = 4096;
const ENTRY: usize = 40;
const MAX_SECTIONS: usize = (HEADER - 16 - 8) / ENTRY;
const ALIGN: u64 = 64;

pub struct Writer {
    w: BufWriter<File>,
    at: u64,
    entries: Vec<([u8; 16], u64, u64, u64)>,
    cur: Option<([u8; 16], u64, Xxh3)>,
}

fn name16(name: &str) -> Result<[u8; 16]> {
    if name.is_empty() || name.len() > 16 {
        bail!("section name {name:?}");
    }
    let mut n = [0u8; 16];
    n[..name.len()].copy_from_slice(name.as_bytes());
    Ok(n)
}

impl Writer {
    pub fn create(path: &Path) -> Result<Writer> {
        let f = File::create(path).with_context(|| format!("create {}", path.display()))?;
        let mut w = BufWriter::with_capacity(4 << 20, f);
        w.write_all(&[0u8; HEADER])?;
        Ok(Writer { w, at: HEADER as u64, entries: Vec::new(), cur: None })
    }

    /// Starts a section; [`Writer::put`] appends to it until the next begin or finish.
    pub fn begin(&mut self, name: &str) -> Result<()> {
        self.end()?;
        if self.entries.len() >= MAX_SECTIONS {
            bail!("too many sections");
        }
        let pad = (ALIGN - self.at % ALIGN) % ALIGN;
        self.w.write_all(&vec![0u8; pad as usize])?;
        self.at += pad;
        self.cur = Some((name16(name)?, self.at, Xxh3::new()));
        Ok(())
    }

    pub fn put(&mut self, b: &[u8]) -> Result<()> {
        let Some((_, _, h)) = &mut self.cur else { bail!("put outside a section") };
        h.update(b);
        self.w.write_all(b)?;
        self.at += b.len() as u64;
        Ok(())
    }

    pub fn put_pod<T: bytemuck::Pod>(&mut self, v: &[T]) -> Result<()> {
        self.put(bytemuck::cast_slice(v))
    }

    /// A whole section at once.
    pub fn section<T: bytemuck::Pod>(&mut self, name: &str, v: &[T]) -> Result<()> {
        self.begin(name)?;
        self.put_pod(v)
    }

    fn end(&mut self) -> Result<()> {
        if let Some((n, off, h)) = self.cur.take() {
            self.entries.push((n, off, self.at - off, h.digest()));
        }
        Ok(())
    }

    /// Writes the header and syncs. Returns the file's length.
    pub fn finish(mut self) -> Result<u64> {
        self.end()?;
        let mut head = vec![0u8; HEADER];
        head[..8].copy_from_slice(MAGIC);
        head[8..12].copy_from_slice(&VERSION.to_le_bytes());
        head[12..16].copy_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (i, (n, off, len, sum)) in self.entries.iter().enumerate() {
            let o = 16 + i * ENTRY;
            head[o..o + 16].copy_from_slice(n);
            head[o + 16..o + 24].copy_from_slice(&off.to_le_bytes());
            head[o + 24..o + 32].copy_from_slice(&len.to_le_bytes());
            head[o + 32..o + 40].copy_from_slice(&sum.to_le_bytes());
        }
        let hs = xxh3_64(&head[..HEADER - 8]);
        head[HEADER - 8..].copy_from_slice(&hs.to_le_bytes());
        let mut f = self.w.into_inner().map_err(|e| e.into_error())?;
        f.seek(SeekFrom::Start(0))?;
        f.write_all(&head)?;
        f.sync_all()?;
        Ok(self.at)
    }
}

/// The bytes an artifact lives in: a read-only map of its file, or memory (tests, small builds).
pub enum Buf {
    Map(memmap2::Mmap),
    Owned(Vec<u64>, usize),
}

impl Buf {
    pub fn bytes(&self) -> &[u8] {
        match self {
            Buf::Map(m) => m,
            Buf::Owned(v, len) => &bytemuck::cast_slice(v)[..*len],
        }
    }
}

#[derive(Clone, Debug)]
pub struct Section {
    pub name: String,
    pub off: usize,
    pub len: usize,
    pub sum: u64,
}

pub struct Artifact {
    buf: Buf,
    pub sections: Vec<Section>,
}

impl Artifact {
    /// Maps `path`. `verify` checks every section's sum (the whole file, ~0.15 s a GB); the
    /// header's is always checked.
    pub fn open(path: &Path, verify: bool) -> Result<Artifact> {
        let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
        // SAFETY: the file is ours and never written once published: a pod downloads to a new
        // name and renames it into place, and deletes old ones only after their last reader.
        let m = unsafe { memmap2::Mmap::map(&f)? };
        Artifact::from_buf(Buf::Map(m), verify).with_context(|| format!("artifact {}", path.display()))
    }

    pub fn from_bytes(b: &[u8], verify: bool) -> Result<Artifact> {
        let mut v = vec![0u64; b.len().div_ceil(8)];
        bytemuck::cast_slice_mut::<u64, u8>(&mut v)[..b.len()].copy_from_slice(b);
        Artifact::from_buf(Buf::Owned(v, b.len()), verify)
    }

    fn from_buf(buf: Buf, verify: bool) -> Result<Artifact> {
        let b = buf.bytes();
        if b.len() < HEADER || &b[..8] != MAGIC {
            bail!("not a seek artifact");
        }
        let version = u32::from_le_bytes(b[8..12].try_into().unwrap());
        if version != VERSION {
            bail!("artifact version {version}, this build reads {VERSION}");
        }
        if xxh3_64(&b[..HEADER - 8]) != u64::from_le_bytes(b[HEADER - 8..HEADER].try_into().unwrap()) {
            bail!("header checksum mismatch");
        }
        let count = u32::from_le_bytes(b[12..16].try_into().unwrap()) as usize;
        if count > MAX_SECTIONS {
            bail!("bad section count");
        }
        let mut sections = Vec::with_capacity(count);
        for i in 0..count {
            let o = 16 + i * ENTRY;
            let name = String::from_utf8_lossy(&b[o..o + 16]).trim_end_matches('\0').to_string();
            let off = u64::from_le_bytes(b[o + 16..o + 24].try_into().unwrap()) as usize;
            let len = u64::from_le_bytes(b[o + 24..o + 32].try_into().unwrap()) as usize;
            let sum = u64::from_le_bytes(b[o + 32..o + 40].try_into().unwrap());
            if !off.is_multiple_of(ALIGN as usize) || off.checked_add(len).is_none_or(|e| e > b.len()) {
                bail!("section {name} out of bounds");
            }
            sections.push(Section { name, off, len, sum });
        }
        let a = Artifact { buf, sections };
        if verify {
            a.verify()?;
        }
        Ok(a)
    }

    pub fn verify(&self) -> Result<()> {
        use rayon::prelude::*;
        let b = self.buf.bytes();
        self.sections.par_iter().try_for_each(|s| {
            if xxh3_64(&b[s.off..s.off + s.len]) != s.sum {
                bail!("section {} checksum mismatch", s.name);
            }
            Ok(())
        })
    }

    pub fn len(&self) -> usize {
        self.buf.bytes().len()
    }

    pub fn bytes(&self) -> &[u8] {
        self.buf.bytes()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn raw(&self, name: &str) -> Result<&[u8]> {
        let s = self.sections.iter().find(|s| s.name == name).with_context(|| format!("no section {name}"))?;
        Ok(&self.buf.bytes()[s.off..s.off + s.len])
    }

    pub fn typed<T: bytemuck::Pod>(&self, name: &str) -> Result<&[T]> {
        bytemuck::try_cast_slice(self.raw(name)?).map_err(|e| anyhow::anyhow!("section {name}: {e:?}"))
    }

    /// Tells the kernel the map's pages may go: a pod calls it on the artifact it swapped out, so
    /// the old pages leave as soon as the last query on them ends.
    pub fn release(&self) {
        if let Buf::Map(m) = &self.buf {
            // SAFETY: a read-only map of a file nobody writes: dropped pages read back the same
            unsafe {
                let _ = m.unchecked_advise(memmap2::UncheckedAdvice::DontNeed);
            }
        }
    }

    /// Pre-faults the map so the first queries after a swap don't page in from disk.
    pub fn prefetch(&self) {
        if let Buf::Map(m) = &self.buf {
            let _ = m.advise(memmap2::Advice::WillNeed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sections_round_trip_and_checksums() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.seek");
        let mut w = Writer::create(&p).unwrap();
        w.section("meta", b"{\"x\":1}").unwrap();
        w.section("nums", &[1u32, 2, 3]).unwrap();
        w.begin("big").unwrap();
        w.put_pod(&[7u64; 100]).unwrap();
        w.put_pod(&[8u64; 100]).unwrap();
        let len = w.finish().unwrap();
        assert_eq!(std::fs::metadata(&p).unwrap().len(), len);
        let a = Artifact::open(&p, true).unwrap();
        assert_eq!(a.raw("meta").unwrap(), b"{\"x\":1}");
        assert_eq!(a.typed::<u32>("nums").unwrap(), &[1, 2, 3]);
        assert_eq!(a.typed::<u64>("big").unwrap()[150], 8);
        assert!(a.raw("nope").is_err());

        let mut b = std::fs::read(&p).unwrap();
        let s = a.sections.iter().find(|s| s.name == "big").unwrap().off;
        b[s + 5] ^= 1;
        assert!(Artifact::from_bytes(&b, false).is_ok());
        assert!(Artifact::from_bytes(&b, true).err().unwrap().to_string().contains("big"));
        b[20] ^= 1;
        assert!(Artifact::from_bytes(&b, false).err().unwrap().to_string().contains("header"));
        assert!(Artifact::from_bytes(&b[..100], false).is_err());
    }
}
