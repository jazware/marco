//! `marco-load fill-scylla`: build input into the config's tables, as
//! current state (each key's latest op, deletes dropped), for tests and
//! benchmarks of scan-scylla. Rows go in as unlogged batches of one actor's
//! records at a time (one partition when the actor is the partition key),
//! many in flight.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use scylla::client::session::Session;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use super::config::{ident, Config, Table};
use crate::rows::Rows;
use crate::sys;

#[derive(clap::Args)]
pub struct Args {
    #[arg(long)]
    pub config: PathBuf,
    /// Build input: <relation>/*.rb.zst op rows.
    #[arg(long)]
    pub input: PathBuf,
    /// Relations config (TOML) the tables' relations come from; default the built-in one.
    #[arg(long)]
    pub relations: Option<PathBuf>,
    /// Create the keyspace and tables first, keyed ((actor), rkey) or ((actor), subject).
    /// A list relation's table is keyed ((actor), rkey) either way.
    #[arg(long, value_parser = ["actor-rkey", "actor-subject"])]
    pub create: Option<String>,
    #[arg(long, default_value_t = 1)]
    pub replication_factor: u32,
    /// With --create: tablets on or off (default: the cluster's default).
    #[arg(long)]
    pub tablets: Option<bool>,
    /// Writes in flight.
    #[arg(long, default_value_t = 1024)]
    pub concurrency: usize,
    /// Rows per unlogged batch (one actor's); 1 for single inserts.
    #[arg(long, default_value_t = 32)]
    pub batch: usize,
    /// Input files read and reduced at once.
    #[arg(long, default_value_t = 4)]
    pub files_at_once: usize,
    /// Only files whose names sort in from..=to (like p-22, p-2z): a subset of whole actors.
    #[arg(long)]
    pub from: Option<String>,
    #[arg(long)]
    pub to: Option<String>,
}

/// A row's columns: actor, rkey, subject, and the list's URI (empty
/// outside a list relation).
type Rec = [String; 4];

/// A file's current state, by actor then rkey. A list item is written
/// whatever repo its list is in: the scan is what drops the others.
fn reduce(f: &Path, listed: bool) -> Result<Vec<Rec>> {
    let mut rows = Rows::open(f)?;
    let mut cols = vec![Vec::new(); if listed { 4 } else { 3 }];
    let mut ops: Vec<(String, String, i64, u8, String, String)> = Vec::new();
    while rows.next(&mut cols)? {
        let us = rows.i64()?;
        let op = rows.u8()?;
        let utf8 = |b: &Vec<u8>| String::from_utf8(b.clone()).ok();
        let (Some(a), Some(k), Some(s)) = (utf8(&cols[0]), utf8(&cols[1]), utf8(&cols[2])) else { continue };
        let Some(l) = cols.get(3).map_or(Some(String::new()), utf8) else { continue };
        ops.push((a, k, us, op, s, l));
    }
    // the build's rule: the latest op per key wins, a delete winning a tie
    ops.sort_unstable_by(|x, y| (&x.0, &x.1, x.2, x.3).cmp(&(&y.0, &y.1, y.2, y.3)));
    let mut out = Vec::with_capacity(ops.len());
    let mut i = 0;
    while i < ops.len() {
        let mut j = i;
        while j + 1 < ops.len() && ops[j + 1].0 == ops[i].0 && ops[j + 1].1 == ops[i].1 {
            j += 1;
        }
        let last = &mut ops[j];
        if last.3 != 3 && !last.4.is_empty() && (!listed || !last.5.is_empty()) {
            let t = std::mem::take;
            out.push([t(&mut last.0), t(&mut last.1), t(&mut last.4), t(&mut last.5)]);
        }
        i = j + 1;
    }
    Ok(out)
}

async fn create(session: &Session, cfg: &Config, a: &Args, key: &str) -> Result<()> {
    let tablets = match a.tablets {
        Some(t) => format!(" AND tablets = {{'enabled': {t}}}"),
        None => String::new(),
    };
    let q = format!(
        "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {}}}{tablets}",
        ident(&cfg.keyspace),
        a.replication_factor
    );
    session.query_unpaged(q.as_str(), ()).await.with_context(|| q.clone())?;
    for t in &cfg.tables {
        let (actor, rkey, subject) = (ident(&t.actor), ident(&t.rkey), ident(&t.subject));
        let pk = if key == "actor-rkey" || t.list.is_some() {
            format!("(({actor}), {rkey})")
        } else {
            format!("(({actor}), {subject})")
        };
        let list = t.list.as_deref().map_or(String::new(), |l| format!(", {} text", ident(l)));
        let q = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} ({actor} text, {rkey} text, {subject} text{list}, PRIMARY KEY {pk})",
            ident(&cfg.keyspace),
            ident(&t.name)
        );
        session.query_unpaged(q.as_str(), ()).await.with_context(|| q.clone())?;
    }
    session.await_schema_agreement().await?;
    Ok(())
}

struct Counters {
    rows: AtomicU64,
    batches: AtomicU64,
    retries: AtomicU64,
}

async fn write(
    session: &Session,
    ins: &PreparedStatement,
    batches: &[Batch],
    rows: &[Rec],
    cols: usize,
    c: &Counters,
) -> Result<()> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let r = if rows.len() == 1 {
            session.execute_unpaged(ins, &rows[0][..cols]).await.map(|_| ())
        } else {
            let vals: Vec<&[String]> = rows.iter().map(|r| &r[..cols]).collect();
            session.batch(&batches[rows.len()], vals).await.map(|_| ())
        };
        match r {
            Ok(()) => {
                c.rows.fetch_add(rows.len() as u64, Relaxed);
                c.batches.fetch_add(1, Relaxed);
                return Ok(());
            }
            Err(e) if attempt < 10 => {
                c.retries.fetch_add(1, Relaxed);
                if attempt == 1 {
                    eprintln!("fill-scylla: retrying a write: {e}");
                }
                tokio::time::sleep(Duration::from_millis(100 << attempt.min(7))).await;
            }
            Err(e) => bail!("write failed after {attempt} attempts: {e}"),
        }
    }
}

async fn fill_table(
    session: Arc<Session>,
    cfg: &Config,
    t: &Table,
    files: Vec<PathBuf>,
    a: &Args,
    c: Arc<Counters>,
) -> Result<()> {
    let names: Vec<String> = [&t.actor, &t.rkey, &t.subject].into_iter().chain(&t.list).map(|c| ident(c)).collect();
    let cols = names.len();
    let listed = t.list.is_some();
    let q = format!(
        "INSERT INTO {}.{} ({}) VALUES ({})",
        ident(&cfg.keyspace),
        ident(&t.name),
        names.join(", "),
        vec!["?"; cols].join(", ")
    );
    let mut ins = session.prepare(q.as_str()).await.with_context(|| q.clone())?;
    ins.set_consistency(cfg.consistency()?);
    ins.set_is_idempotent(true);
    ins.set_request_timeout(Some(cfg.timeout()));
    let ins = Arc::new(ins);
    let max = a.batch.max(1);
    let batches: Arc<Vec<Batch>> = Arc::new(
        (0..=max)
            .map(|n| {
                let mut b = Batch::new(BatchType::Unlogged);
                for _ in 0..n {
                    b.append_statement(ins.as_ref().clone());
                }
                b.set_consistency(ins.get_consistency().unwrap_or(scylla::frame::types::Consistency::One));
                b.set_is_idempotent(true);
                b
            })
            .collect(),
    );
    let sem = Arc::new(Semaphore::new(a.concurrency.max(1)));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Rec>>(a.files_at_once.max(1));
    let reader = tokio::task::spawn_blocking({
        let files_at_once = a.files_at_once.max(1);
        move || -> Result<()> {
            use rayon::prelude::*;
            for group in files.chunks(files_at_once) {
                let reduced: Vec<Vec<Rec>> = group.par_iter().map(|f| reduce(f, listed)).collect::<Result<_>>()?;
                for r in reduced {
                    if tx.blocking_send(r).is_err() {
                        return Ok(());
                    }
                }
            }
            Ok(())
        }
    });
    let mut tasks = tokio::task::JoinSet::new();
    while let Some(rows) = rx.recv().await {
        let rows = Arc::new(rows);
        let mut i = 0;
        while i < rows.len() {
            let mut j = i + 1;
            while j < rows.len() && j - i < max && rows[j][0] == rows[i][0] {
                j += 1;
            }
            let permit = sem.clone().acquire_owned().await?;
            let (s, ins, b, r, c) = (session.clone(), ins.clone(), batches.clone(), rows.clone(), c.clone());
            tasks.spawn(async move {
                let out = write(&s, &ins, &b, &r[i..j], cols, &c).await;
                drop(permit);
                out
            });
            while let Some(done) = tasks.try_join_next() {
                done??;
            }
            i = j;
        }
    }
    while let Some(done) = tasks.join_next().await {
        done??;
    }
    reader.await??;
    Ok(())
}

pub fn run(a: &Args) -> Result<Value> {
    let mut cfg = Config::read(&a.config)?;
    let relations = match &a.relations {
        Some(p) => marco_core::Relations::parse_toml(&std::fs::read_to_string(p)?)?,
        None => marco_core::Relations::bsky_default(),
    };
    cfg.resolve(&relations)?;
    if let Some(t) = cfg.tables.iter().find(|t| t.record.is_some()) {
        bail!("table {:?} holds records, which build input doesn't: fill-scylla fills column tables only", t.name);
    }
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    rt.block_on(async {
        let session = Arc::new(cfg.connect().await?);
        if let Some(k) = &a.create {
            create(&session, &cfg, a, k).await?;
        }
        let c = Arc::new(Counters { rows: AtomicU64::new(0), batches: AtomicU64::new(0), retries: AtomicU64::new(0) });
        let t0 = Instant::now();
        let cpu0 = sys::cpu_secs();
        let progress = {
            let c = c.clone();
            tokio::spawn(async move {
                let mut last = 0u64;
                loop {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    let n = c.rows.load(Relaxed);
                    eprintln!(
                        "fill: {:.1}M rows ({:.0}k/s), {} retries, {:.0} s",
                        n as f64 / 1e6,
                        (n - last) as f64 / 10.0 / 1e3,
                        c.retries.load(Relaxed),
                        t0.elapsed().as_secs_f64()
                    );
                    last = n;
                }
            })
        };
        let mut per = Vec::new();
        for t in &cfg.tables {
            let d = a.input.join(&t.relation);
            let mut files: Vec<PathBuf> = std::fs::read_dir(&d)
                .with_context(|| format!("read {}", d.display()))?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.to_string_lossy().ends_with(".rb.zst"))
                .filter(|p| {
                    let n = p.file_name().unwrap().to_string_lossy();
                    let n = n.trim_end_matches(".rb.zst");
                    a.from.as_deref().is_none_or(|f| n >= f) && a.to.as_deref().is_none_or(|x| n <= x)
                })
                .collect();
            files.sort();
            let r0 = c.rows.load(Relaxed);
            let t1 = Instant::now();
            let n_files = files.len();
            fill_table(session.clone(), &cfg, t, files, a, c.clone()).await?;
            let rows = c.rows.load(Relaxed) - r0;
            let secs = t1.elapsed().as_secs_f64();
            eprintln!("{}: {rows} rows from {n_files} files in {secs:.0} s ({:.0}k/s)", t.name, rows as f64 / secs / 1e3);
            per.push(json!({"table": t.name, "relation": t.relation, "files": n_files, "rows": rows, "secs": secs, "rows_per_s": rows as f64 / secs}));
        }
        progress.abort();
        let secs = t0.elapsed().as_secs_f64();
        let rows = c.rows.load(Relaxed);
        let cpu = sys::cpu_secs() - cpu0;
        Ok(json!({
            "tables": per,
            "rows": rows,
            "batches": c.batches.load(Relaxed),
            "retries": c.retries.load(Relaxed),
            "secs": secs,
            "rows_per_s": rows as f64 / secs,
            "concurrency": a.concurrency,
            "batch": a.batch,
            "client_cpu_secs": cpu,
            "client_cores_busy": cpu / secs,
            "peak_rss_gb": sys::peak_rss_gb(),
        }))
    })
}
