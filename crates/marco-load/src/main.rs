//! marco-load: the bulk loader and the P0 measurements.
//!
//! ```text
//! marco-load build  --input DIR --out FILE     export files -> snapshot
//! marco-load open   --snapshot FILE            open time and first queries (drop caches first for cold)
//! marco-load bench  --snapshot FILE            read latencies and throughput
//! marco-load bench-lists --snapshot FILE      list sizes, lookups, set ops and listitem writes
//! marco-load verify --snapshot FILE            checksums, every list decoded, a sampled export checked
//! marco-load replay --snapshot FILE --deletes DIR   delete-by-URI replay through the mutable path
//! marco-load diff   --a SNAP --b SNAP              two snapshots compared record by record
//! marco-load pagerank --snapshot DIR [--link DIR] --out-dir DIR   PageRank over the follows, a ranks file (daily: warm from the last)
//! marco-load mirror --url ADMIN --dir DIR      a local copy of a running marco's checkpoint
//! marco-load scan-scylla --config TOML --out DIR [--snapshot SNAP]   Scylla tables -> build input (-> snapshot)
//! marco-load fill-scylla --config TOML --input DIR   build input -> Scylla tables (tests, benchmarks)
//! marco-load scan-hubble --out DIR [--host URL] [--snapshot SNAP]   a sync-API host's repos -> build input (-> snapshot)
//! marco-load scan-jetstream --out DIR [--host URL] [--snapshot SNAP]   a Jetstream v2 archive -> build input (-> snapshot)
//! ```
//!
//! The build's input format is in this crate's README.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

mod bench;
mod build;
#[cfg(feature = "scylla")]
mod cql;
mod diff;
#[cfg(feature = "hubble")]
mod hubble;
#[cfg(feature = "jetstream")]
mod jetstream;
mod lists_bench;
mod metrics;
#[cfg(feature = "hubble")]
mod mirror;
mod pagerank;
mod ranks;
mod rows;
#[cfg(any(feature = "scylla", feature = "hubble"))]
mod spill;
mod stats;
mod sys;
mod verify;

marco_ops::jemalloc!();

#[derive(Parser)]
struct Cli {
    /// Serve /metrics and /debug/pprof/* here while running (keep it on loopback).
    #[arg(long, global = true)]
    ops: Option<String>,
    /// Where to write the JSON report (it also goes to stdout).
    #[arg(long, global = true)]
    report: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Build {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        out: PathBuf,
        /// Spill directory for the in pass (default: beside the output).
        #[arg(long)]
        tmp: Option<PathBuf>,
        /// Relations config (TOML); default follow, block and listitem.
        #[arg(long)]
        relations: Option<PathBuf>,
        #[arg(long, default_value_t = marco_core::graph::DEFAULT_SUBJ_INDEX)]
        subj_index: usize,
        #[arg(long, default_value_t = marco_core::store::DEFAULT_SHARDS)]
        shards: usize,
        /// Spill files per shard and relation for the in pass; each is sorted in memory alone.
        #[arg(long, default_value_t = 4)]
        sub_buckets: usize,
        /// Write the keys whose latest op is a delete here (for `replay`), as
        /// (repo, rkey, subject of the last create or "", Int64 time_us).
        #[arg(long)]
        deletes_out: Option<PathBuf>,
        /// Only deletes at or after this time_us (default: the archive crawl's start).
        #[arg(long, default_value_t = 1_771_484_880_000_000)]
        deletes_since_us: i64,
        /// How far back the sources in the input's cursors.json replay: an older
        /// Jetstream cursor is refused (Jetstream keeps about 36 h).
        #[arg(long, default_value_t = 36.0)]
        retention_hours: f64,
        /// Build even if a cursor is past the retention.
        #[arg(long)]
        allow_gap: bool,
        /// Lay each shard's small lists out hottest first by these weights (`did<TAB>weight`): out lists by
        /// viewers, then subjects; in sets by subjects, then viewers. Off by default.
        #[arg(long, num_args = 2, value_names = ["VIEWERS", "SUBJECTS"])]
        heat: Option<Vec<PathBuf>>,
    },
    Open {
        #[arg(long)]
        snapshot: PathBuf,
    },
    Bench(bench::Args),
    BenchLists(lists_bench::Args),
    Verify(verify::VerifyArgs),
    Replay(verify::ReplayArgs),
    Diff(diff::Args),
    /// PageRank over the follow graph: the capped pass to L1 < 1e-5, warm from the last run, a DID-keyed ranks file.
    Pagerank(pagerank::Args),
    /// Keep a local copy of a running marco's current checkpoint (its admin listener's checkpoint export).
    #[cfg(feature = "hubble")]
    Mirror(mirror::Args),
    /// Scan Scylla tables into build input (see the README), then optionally build.
    #[cfg(feature = "scylla")]
    ScanScylla(Box<cql::scan::Args>),
    /// Load build input into Scylla tables as current state (tests, benchmarks).
    #[cfg(feature = "scylla")]
    FillScylla(cql::fill::Args),
    /// Fetch every repo a sync-API host lists (Hubble by default) into build input, then optionally build.
    #[cfg(feature = "hubble")]
    ScanHubble(Box<hubble::Args>),
    /// Replay a Jetstream v2 instance's sealed archive of the relations into build input, then optionally build.
    #[cfg(feature = "jetstream")]
    ScanJetstream(Box<jetstream::Args>),
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    // a build holds every export file's reader, a spill file per shard, sub-bucket and relation,
    // and a writer per shard at once: past a shell's usual soft limit of 1024
    eprintln!("open-file limit {}", sys::raise_nofile());
    if let Some(addr) = &cli.ops {
        let a = marco_ops::serve(addr, None)?;
        eprintln!("ops on http://{a}");
    }
    let report: serde_json::Value = match cli.cmd {
        Cmd::Build {
            input,
            out,
            tmp,
            relations,
            subj_index,
            shards,
            sub_buckets,
            deletes_out,
            deletes_since_us,
            retention_hours,
            allow_gap,
            heat,
        } => {
            let rels = match relations {
                Some(p) => marco_core::Relations::parse_toml(&std::fs::read_to_string(p)?)?,
                None => marco_core::Relations::bsky_default(),
            };
            let tmp = tmp.unwrap_or_else(|| out.with_extension("spill"));
            let deletes = deletes_out.map(|dir| build::DeletesOut { dir, since_us: deletes_since_us });
            let retention = std::time::Duration::from_secs_f64(retention_hours.max(0.0) * 3600.0);
            let cursors = build::read_cursors(&input, retention, allow_gap)?;
            let o = build::Opts {
                input,
                out,
                tmp: tmp.clone(),
                rels,
                subj_index,
                shards,
                sub_buckets,
                deletes,
                cursors,
                heat: heat.map(|h| (h[0].clone(), h[1].clone())),
            };
            let r = build::build(&o)?;
            let _ = std::fs::remove_dir(&tmp);
            serde_json::to_value(r)?
        }
        Cmd::Open { snapshot } => bench::open(&snapshot)?,
        Cmd::Bench(a) => bench::run(&a)?,
        Cmd::BenchLists(a) => lists_bench::run(&a)?,
        Cmd::Verify(a) => verify::verify(&a)?,
        Cmd::Replay(a) => verify::replay(&a)?,
        Cmd::Diff(a) => diff::run(&a)?,
        Cmd::Pagerank(a) => pagerank::run(&a)?,
        #[cfg(feature = "hubble")]
        Cmd::Mirror(a) => mirror::run(&a)?,
        #[cfg(feature = "scylla")]
        Cmd::ScanScylla(a) => cql::scan::run(&a)?,
        #[cfg(feature = "scylla")]
        Cmd::FillScylla(a) => cql::fill::run(&a)?,
        #[cfg(feature = "hubble")]
        Cmd::ScanHubble(a) => hubble::run(&a)?,
        #[cfg(feature = "jetstream")]
        Cmd::ScanJetstream(a) => jetstream::run(&a)?,
    };
    let s = serde_json::to_string_pretty(&report)?;
    if let Some(p) = cli.report {
        std::fs::write(p, &s)?;
    }
    println!("{s}");
    Ok(())
}
