//! scan-scylla's state: each bucket file's committed length (see
//! crate::spill) and each range's position. After a crash every range goes
//! on from its position.

use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub use crate::spill::{bucket_of, Spill, BUCKETS};

pub const STATE: &str = "scan-state.json";
pub const PLAN: &str = "scan-plan.json";

/// How far a range has committed: pieces before `piece` whole, and in
/// `piece` every token up to `after`.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Pos {
    pub piece: u32,
    pub after: Option<i64>,
    pub done: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct State {
    /// When the first run began (µs): every row read is at least this new.
    pub started_us: i64,
    pub finished_us: Option<i64>,
    /// Scanning time over every run.
    pub scan_secs: f64,
    pub runs: u32,
    pub pos: Vec<Pos>,
    /// Per table, per bucket: committed bytes.
    pub lens: Vec<Vec<u64>>,
    /// Per table: committed rows, their bytes as rows, and rows skipped for a null column.
    pub rows: Vec<u64>,
    pub row_bytes: Vec<u64>,
    pub null_rows: Vec<u64>,
    /// Per table: list items skipped because their list is in another repo.
    #[serde(default)]
    pub cross_repo_rows: Vec<u64>,
    /// Per record table: records the relation doesn't keep (`when`), and
    /// records that don't parse or aren't an edge.
    #[serde(default)]
    pub unmatched_rows: Vec<u64>,
    #[serde(default)]
    pub bad_rows: Vec<u64>,
    pub commits: u64,
}

impl State {
    pub fn new(ranges: usize, tables: usize, started_us: i64) -> State {
        State {
            started_us,
            pos: vec![Pos::default(); ranges],
            lens: vec![vec![0; BUCKETS]; tables],
            rows: vec![0; tables],
            row_bytes: vec![0; tables],
            null_rows: vec![0; tables],
            cross_repo_rows: vec![0; tables],
            unmatched_rows: vec![0; tables],
            bad_rows: vec![0; tables],
            ..Default::default()
        }
    }

    pub fn read(dir: &Path) -> Result<Option<State>> {
        let p = dir.join(STATE);
        match std::fs::read(&p) {
            Ok(b) => Ok(Some(serde_json::from_slice(&b).with_context(|| format!("parse {}", p.display()))?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("read {}", p.display())),
        }
    }

    pub fn write(&self, dir: &Path) -> Result<()> {
        marco_core::store::write_atomic(dir, STATE, &serde_json::to_vec(self)?, true)
    }

    pub fn spill_bytes(&self) -> u64 {
        self.lens.iter().flatten().sum()
    }
}
