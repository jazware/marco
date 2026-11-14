//! The builder: one long-running process that holds every actor's names,
//! follows Jetstream into them, verifies handles, and every interval builds
//! the artifact pods serve, with the newest ranks baked in. It keeps its
//! names state on its own volume, so a restart replays Jetstream from the
//! state's cursor instead of exporting again.
//!
//! A cycle: drain the events read since the last one (Jetstream keeps
//! reading meanwhile, into a queue), pull new ranks if marco published some,
//! build the artifact at the state's cursor, write it to the artifact
//! directory (newest few plus `latest.seek`, which `/artifact/latest`
//! serves), upload it when a bucket is configured, queue the next handles to
//! verify, and every few cycles save the state.

use std::collections::BTreeMap;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use seek_core::did::{key_of, DidKey};
use seek_core::index::{self, ActorIn, Meta};
use seek_core::keys::Hosting;
use seek_core::ranks::RanksFile;
use serde::Deserialize;
use tokio::sync::mpsc;
use xxhash_rust::xxh3::Xxh3;

use crate::jetstream::Ev;
use crate::metrics::*;
use crate::store::{Bucket, Latest};
use crate::verify::{Outcome, Queue};

#[derive(Clone, Debug, PartialEq)]
pub struct Actor {
    pub did: DidKey,
    /// The handle the DID claims (PLC, Jetstream), verified or not.
    pub handle: Option<Box<str>>,
    /// When the claim was last verified both ways, 0 if it isn't.
    pub verified_us: i64,
    pub name: Option<Box<str>>,
    pub profile: bool,
    pub hidden: bool,
}

impl Actor {
    pub fn new(did: DidKey) -> Actor {
        Actor { did, handle: None, verified_us: 0, name: None, profile: false, hidden: false }
    }
}

/// Each actor's u16 rank and hidden flag from a ranks file: one merge, both in DID order.
pub fn ranks_for<'a>(
    actors: impl Iterator<Item = &'a Actor>,
    rf: Option<&RanksFile>,
    n: usize,
) -> (Vec<u16>, Vec<bool>) {
    let mut rank = vec![0u16; n];
    let mut hidden = vec![false; n];
    let Some(rf) = rf else { return (rank, hidden) };
    let mut it = rf.rows().peekable();
    for (i, a) in actors.enumerate() {
        while it.peek().is_some_and(|r| r.did < a.did) {
            it.next();
        }
        if let Some(r) = it.peek().filter(|r| r.did == a.did) {
            rank[i] = r.rank16;
            hidden[i] = r.hidden;
        }
    }
    (rank, hidden)
}

/// Every actor the builder knows, sorted by DID, plus those added since the last merge.
#[derive(Default)]
pub struct State {
    pub actors: Vec<Actor>,
    pub added: BTreeMap<DidKey, Actor>,
    pub cursor_us: u64,
    pub plc_cursor: String,
}

const STATE_MAGIC: &[u8; 8] = b"seekstat";
const STATE_VERSION: u32 = 1;

impl State {
    pub fn get_mut(&mut self, did: &str) -> &mut Actor {
        let k = key_of(did);
        match self.actors.binary_search_by(|a| a.did.cmp(&k)) {
            Ok(i) => &mut self.actors[i],
            Err(_) => self.added.entry(k.clone()).or_insert_with(|| Actor::new(k)),
        }
    }

    pub fn get(&self, did: &str) -> Option<&Actor> {
        let k = key_of(did);
        match self.actors.binary_search_by(|a| a.did.cmp(&k)) {
            Ok(i) => Some(&self.actors[i]),
            Err(_) => self.added.get(&k),
        }
    }

    pub fn merge_added(&mut self) {
        if self.added.is_empty() {
            return;
        }
        let added = std::mem::take(&mut self.added);
        self.actors.extend(added.into_values());
        self.actors.par_sort_unstable_by(|a, b| a.did.cmp(&b.did));
    }

    pub fn len(&self) -> usize {
        self.actors.len() + self.added.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// One change from Jetstream. Returns the DID to verify, if its claim changed.
    pub fn apply(&mut self, ev: &Ev) -> Option<String> {
        let t = ev.time_us();
        let out = match ev {
            Ev::Profile { did, name, deleted, .. } => {
                let a = self.get_mut(did);
                a.profile = !deleted;
                a.name = name.as_deref().filter(|n| !n.trim().is_empty()).map(Into::into);
                None
            }
            Ev::Account { did, active, .. } => {
                if self.get(did).is_some() {
                    self.get_mut(did).hidden = !active;
                }
                None
            }
            Ev::Identity { did, handle, .. } => {
                let h = handle.as_deref().map(str::to_ascii_lowercase);
                let a = self.get_mut(did);
                if a.handle.as_deref() != h.as_deref() || a.verified_us == 0 {
                    a.handle = h.map(Into::into);
                    a.verified_us = 0;
                    Some(did.clone())
                } else {
                    None
                }
            }
            Ev::Follow { .. } | Ev::Gap { .. } => None,
        };
        self.cursor_us = self.cursor_us.max(t);
        out
    }

    pub fn verified(&mut self, did: &str, o: &Outcome) {
        let a = self.get_mut(did);
        match o {
            Outcome::Valid(h) => {
                a.handle = Some(h.as_str().into());
                a.verified_us = now_us() as i64;
            }
            Outcome::Invalid => a.verified_us = 0,
            Outcome::Unknown => {}
        }
    }

    pub fn save(&mut self, path: &Path) -> Result<u64> {
        self.merge_added();
        let tmp = path.with_extension("tmp");
        let f = std::fs::File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        let mut z = zstd::Encoder::new(BufWriter::with_capacity(4 << 20, f), 1)?;
        let mut h = Xxh3::new();
        let mut put = |z: &mut zstd::Encoder<'_, BufWriter<std::fs::File>>, b: &[u8]| -> Result<()> {
            h.update(b);
            z.write_all(b)?;
            Ok(())
        };
        put(&mut z, STATE_MAGIC)?;
        put(&mut z, &STATE_VERSION.to_le_bytes())?;
        put(&mut z, &self.cursor_us.to_le_bytes())?;
        put(&mut z, &(self.plc_cursor.len() as u16).to_le_bytes())?;
        put(&mut z, self.plc_cursor.as_bytes())?;
        put(&mut z, &(self.actors.len() as u64).to_le_bytes())?;
        let mut row = Vec::with_capacity(256);
        for a in &self.actors {
            row.clear();
            match &a.did {
                DidKey::Plc(v) => {
                    row.push(0);
                    row.extend_from_slice(&seek_core::did::plc_to_bytes(*v));
                }
                DidKey::Other(s) => {
                    row.push(1);
                    row.extend_from_slice(&(s.len() as u16).to_le_bytes());
                    row.extend_from_slice(s.as_bytes());
                }
            }
            row.push(a.profile as u8 | (a.hidden as u8) << 1);
            row.extend_from_slice(&a.verified_us.to_le_bytes());
            for s in [&a.handle, &a.name] {
                let s = s.as_deref().unwrap_or("");
                row.extend_from_slice(&(s.len() as u16).to_le_bytes());
                row.extend_from_slice(s.as_bytes());
            }
            put(&mut z, &row)?;
        }
        let sum = h.digest();
        z.write_all(&sum.to_le_bytes())?;
        let w = z.finish()?;
        let f = w.into_inner().map_err(|e| e.into_error())?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
        Ok(std::fs::metadata(path)?.len())
    }

    pub fn load(path: &Path) -> Result<State> {
        let f = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
        let mut z = BufReader::with_capacity(4 << 20, zstd::Decoder::new(f)?);
        let mut h = Xxh3::new();
        let mut take = |z: &mut dyn Read, n: usize| -> Result<Vec<u8>> {
            let mut b = vec![0u8; n];
            z.read_exact(&mut b).context("state file truncated")?;
            h.update(&b);
            Ok(b)
        };
        if take(&mut z, 8)? != STATE_MAGIC {
            bail!("{}: not a seek state file", path.display());
        }
        let v = u32::from_le_bytes(take(&mut z, 4)?.try_into().unwrap());
        if v != STATE_VERSION {
            bail!("state version {v}, this build reads {STATE_VERSION}");
        }
        let cursor_us = u64::from_le_bytes(take(&mut z, 8)?.try_into().unwrap());
        let l = u16::from_le_bytes(take(&mut z, 2)?.try_into().unwrap()) as usize;
        let plc_cursor = String::from_utf8(take(&mut z, l)?)?;
        let n = u64::from_le_bytes(take(&mut z, 8)?.try_into().unwrap()) as usize;
        let mut actors = Vec::with_capacity(n);
        let str_of = |b: Vec<u8>| -> Option<Box<str>> { (!b.is_empty()).then(|| String::from_utf8_lossy(&b).into()) };
        for _ in 0..n {
            let kind = take(&mut z, 1)?[0];
            let did = if kind == 0 {
                DidKey::Plc(seek_core::did::plc_from_bytes(&take(&mut z, 15)?))
            } else {
                let l = u16::from_le_bytes(take(&mut z, 2)?.try_into().unwrap()) as usize;
                DidKey::Other(String::from_utf8_lossy(&take(&mut z, l)?).into())
            };
            let flags = take(&mut z, 1)?[0];
            let verified_us = i64::from_le_bytes(take(&mut z, 8)?.try_into().unwrap());
            let l = u16::from_le_bytes(take(&mut z, 2)?.try_into().unwrap()) as usize;
            let handle = str_of(take(&mut z, l)?);
            let l = u16::from_le_bytes(take(&mut z, 2)?.try_into().unwrap()) as usize;
            let name = str_of(take(&mut z, l)?);
            actors.push(Actor { did, handle, verified_us, name, profile: flags & 1 != 0, hidden: flags & 2 != 0 });
        }
        let want = h.digest();
        let mut s = [0u8; 8];
        z.read_exact(&mut s)?;
        if u64::from_le_bytes(s) != want {
            bail!("{}: state checksum mismatch", path.display());
        }
        Ok(State { actors, added: BTreeMap::new(), cursor_us, plc_cursor })
    }

    /// The first state, from the archive's export: JSON lines (zstd) of
    /// `{"did", "handle", "verified_us", "name", "profile"}` (internal/seek/scripts/bootstrap.sql).
    pub fn bootstrap(path: &Path, cursor_us: u64) -> Result<State> {
        #[derive(Deserialize)]
        struct Row {
            did: String,
            #[serde(default)]
            handle: String,
            #[serde(default)]
            verified_us: i64,
            #[serde(default)]
            name: String,
            #[serde(default)]
            profile: serde_json::Value,
        }
        let f = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
        let r = BufReader::with_capacity(4 << 20, zstd::Decoder::new(f)?);
        let mut lines = std::io::BufRead::lines(r);
        let mut actors = Vec::new();
        loop {
            let batch: Vec<String> = lines.by_ref().take(1 << 20).collect::<std::io::Result<_>>()?;
            if batch.is_empty() {
                break;
            }
            let rows: Vec<Actor> = batch
                .par_iter()
                .filter_map(|l| {
                    let r: Row = serde_json::from_str(l).ok()?;
                    if !r.did.starts_with("did:") {
                        return None;
                    }
                    let handle = r.handle.to_ascii_lowercase();
                    let handle = (!handle.is_empty() && seek_core::did::valid_handle(&handle)).then(|| handle.into());
                    let profile = matches!(r.profile, serde_json::Value::Bool(true)) || r.profile.as_u64() == Some(1);
                    Some(Actor {
                        did: key_of(&r.did),
                        verified_us: if handle.is_some() { r.verified_us } else { 0 },
                        handle,
                        name: (!r.name.trim().is_empty()).then(|| r.name.as_str().into()),
                        profile,
                        hidden: false,
                    })
                })
                .collect();
            actors.extend(rows);
        }
        actors.par_sort_unstable_by(|a, b| a.did.cmp(&b.did));
        actors.dedup_by(|b, a| a.did == b.did);
        Ok(State { actors, added: BTreeMap::new(), cursor_us, plc_cursor: String::new() })
    }
}

/// Who gets indexed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum IndexSet {
    /// Anyone with a profile record.
    Profiles,
    /// Every DID the builder knows (PLC's whole directory, with a handle or a profile).
    All,
}

pub struct BuildOut {
    pub path: PathBuf,
    pub meta: Meta,
    pub stats: index::BuildStats,
}

/// Builds the artifact for `st` with `ranks` baked in, at `dir/<version>.seek`.
pub fn build(st: &mut State, ranks: Option<&RanksFile>, set: IndexSet, dir: &Path) -> Result<BuildOut> {
    st.merge_added();
    let t = Instant::now();
    let take = |a: &Actor| match set {
        IndexSet::Profiles => a.profile,
        IndexSet::All => a.profile || a.handle.is_some(),
    };
    let chosen: Vec<&Actor> = st.actors.iter().filter(|a| take(a)).collect();
    let (rank, hidden) = ranks_for(chosen.iter().copied(), ranks, chosen.len());
    STEP_SECONDS.with(&["ranks"]).since(t);
    STEP_LAST.with_label_values(&["ranks"]).set(t.elapsed().as_secs_f64());
    let verified = |a: &Actor| a.verified_us > 0 && a.handle.is_some();
    let hosting =
        Hosting::from_handles(chosen.iter().filter(|a| verified(a)).filter_map(|a| a.handle.as_deref()), 1000);
    let ins: Vec<ActorIn> = chosen
        .iter()
        .enumerate()
        .map(|(i, a)| ActorIn {
            did: &a.did,
            handle: if verified(a) { a.handle.as_deref().unwrap_or("") } else { "" },
            name: a.name.as_deref().unwrap_or(""),
            rank: rank[i],
            hidden: a.hidden || hidden[i],
        })
        .collect();
    let created_ms = (now_secs() * 1e3) as i64;
    let version = format!("{}{:03}-{}", utc_name(created_ms), created_ms.rem_euclid(1000), st.cursor_us);
    let mut meta = Meta {
        version: version.clone(),
        created_ms,
        cursor_us: st.cursor_us,
        plc_cursor: st.plc_cursor.clone(),
        index_set: format!("{set:?}").to_lowercase(),
        handles_verified: chosen.iter().filter(|a| verified(a)).count() as u64,
        ranks_created_ms: ranks.map_or(0, |r| r.header.created_ms),
        ranks_checkpoint: ranks.map_or(0, |r| r.header.checkpoint),
        ..Default::default()
    };
    let path = dir.join(format!("{version}.seek"));
    let tmp = dir.join(format!("{version}.seek.tmp"));
    let tb = Instant::now();
    let stats = index::build(&tmp, &ins, &hosting, &mut meta)?;
    std::fs::rename(&tmp, &path)?;
    STEP_SECONDS.with(&["build"]).since(tb);
    STEP_LAST.with_label_values(&["build"]).set(tb.elapsed().as_secs_f64());
    for (s, v) in [
        ("known", st.actors.len()),
        ("indexed", chosen.len()),
        ("handle_verified", meta.handles_verified as usize),
        ("handle_unverified", chosen.iter().filter(|a| a.handle.is_some() && !verified(a)).count()),
        ("hidden", ins.iter().filter(|a| a.hidden).count()),
    ] {
        BUILDER_ACTORS.with_label_values(&[s]).set(v as i64);
    }
    Ok(BuildOut { path, meta, stats })
}

/// `YYYYMMDDTHHMMSSZ`.
pub fn utc_name(ms: i64) -> String {
    let s = ms.div_euclid(1000);
    let (days, sod) = (s.div_euclid(86400), s.rem_euclid(86400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + (month <= 2) as i64;
    format!("{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z", sod / 3600, sod % 3600 / 60, sod % 60)
}

/// Points `dir/latest.seek` at `path` and keeps the newest `keep` artifacts.
pub fn publish_local(dir: &Path, path: &Path, keep: usize) -> Result<()> {
    let tmp = dir.join("latest.seek.tmp");
    let _ = std::fs::remove_file(&tmp);
    std::os::unix::fs::symlink(path.file_name().unwrap(), &tmp)?;
    std::fs::rename(&tmp, dir.join("latest.seek"))?;
    let mut names: Vec<String> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| n.ends_with(".seek") && n != "latest.seek")
        .collect();
    names.sort();
    for n in &names[..names.len().saturating_sub(keep.max(1))] {
        std::fs::remove_file(dir.join(n))?;
    }
    Ok(())
}

/// Where ranks come from: a local ranks file or directory (with latest.ranks), or marco's admin
/// endpoint (`GET /admin/ranks/latest`, the admin token, an ETag).
pub enum RanksSource {
    Path(PathBuf),
    Http { url: String, token: Option<String>, client: reqwest::Client },
}

impl RanksSource {
    pub fn parse(s: &str, token_file: Option<&Path>) -> Result<RanksSource> {
        if s.starts_with("http://") || s.starts_with("https://") {
            let token = token_file.map(std::fs::read_to_string).transpose()?.map(|t| t.trim().to_string());
            Ok(RanksSource::Http { url: s.into(), token, client: vlatproto::http::base("seek-builder").build()? })
        } else {
            Ok(RanksSource::Path(s.into()))
        }
    }

    /// A newer ranks file than `have` (its ETag or path), at a local path.
    pub async fn poll(&self, have: Option<&str>, dir: &Path) -> Result<Option<(PathBuf, String)>> {
        match self {
            RanksSource::Path(p) => {
                let p = if p.is_dir() { p.join("latest.ranks") } else { p.clone() };
                if !p.exists() {
                    return Ok(None);
                }
                let real = std::fs::canonicalize(&p)?;
                let tag = format!("{}:{}", real.display(), std::fs::metadata(&real)?.len());
                Ok((Some(tag.as_str()) != have).then_some((real, tag)))
            }
            RanksSource::Http { url, token, client } => {
                let mut req = client.get(url.as_str());
                if let Some(t) = token {
                    req = req.bearer_auth(t);
                }
                if let Some(h) = have {
                    req = req.header(http::header::IF_NONE_MATCH, h);
                }
                let resp = req.send().await?;
                if resp.status() == reqwest::StatusCode::NOT_MODIFIED || resp.status() == reqwest::StatusCode::NOT_FOUND
                {
                    return Ok(None);
                }
                if !resp.status().is_success() {
                    bail!("{url}: {}", resp.status());
                }
                let tag =
                    resp.headers().get(http::header::ETAG).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
                let to = dir.join("ranks.ranks");
                let tmp = dir.join("ranks.ranks.part");
                std::fs::write(&tmp, resp.bytes().await?)?;
                RanksFile::open(&tmp)?;
                std::fs::rename(&tmp, &to)?;
                Ok(Some((to, tag)))
            }
        }
    }
}

pub struct Config {
    pub dir: PathBuf,
    pub interval: Duration,
    pub keep: usize,
    pub set: IndexSet,
    pub state_every: u32,
    pub verify_per_cycle: usize,
    pub recheck: Duration,
    pub bucket: Option<Bucket>,
    pub ranks: Option<RanksSource>,
}

/// What `/artifact/latest` serves, updated after each cycle.
#[derive(Default)]
pub struct Published {
    pub latest: Option<(PathBuf, Latest)>,
    pub last_error: Option<String>,
}

/// The cycle loop. `js` brings Jetstream's events, `verified` the verifier's outcomes.
pub async fn run(
    cfg: Config,
    mut st: State,
    mut js: mpsc::Receiver<Ev>,
    mut verified: mpsc::Receiver<(String, Outcome)>,
    queue: Queue,
    published: Arc<Mutex<Published>>,
) -> Result<()> {
    let state_path = cfg.dir.join("state.seekstate");
    let art_dir = cfg.dir.join("artifacts");
    std::fs::create_dir_all(&art_dir)?;
    let mut ranks: Option<(RanksFile, String)> = None;
    let mut cycle = 0u32;
    let mut next = tokio::time::Instant::now();
    loop {
        // between cycles: apply events and verifications as they come
        loop {
            tokio::select! {
                Some(ev) = js.recv() => {
                    if let Ev::Gap { asked_us, first_us } = ev {
                        tracing::error!(asked_us, first_us, "Jetstream replay started past the state's cursor: changes between were missed");
                    }
                    if let Some(d) = st.apply(&ev) { queue.push(&d); }
                }
                Some((d, o)) = verified.recv() => st.verified(&d, &o),
                _ = tokio::time::sleep_until(next) => break,
            }
        }
        next += cfg.interval;
        cycle += 1;
        let t0 = Instant::now();
        let r = cycle_once(&cfg, &mut st, &mut ranks, &art_dir, &state_path, cycle, &queue).await;
        match r {
            Ok(latest) => {
                CYCLES.with_label_values(&["ok"]).inc();
                LAST_SUCCESS.set(now_secs());
                tracing::info!(version = %latest.1.version, secs = t0.elapsed().as_secs_f64(), "cycle done");
                let mut p = published.lock().unwrap();
                p.latest = Some(latest);
                p.last_error = None;
            }
            Err(e) => {
                CYCLES.with_label_values(&["error"]).inc();
                tracing::error!(error = %format!("{e:#}"), "cycle failed");
                published.lock().unwrap().last_error = Some(format!("{e:#}"));
            }
        }
        if next < tokio::time::Instant::now() {
            next = tokio::time::Instant::now() + cfg.interval;
        }
    }
}

async fn cycle_once(
    cfg: &Config,
    st: &mut State,
    ranks: &mut Option<(RanksFile, String)>,
    art_dir: &Path,
    state_path: &Path,
    cycle: u32,
    queue: &Queue,
) -> Result<(PathBuf, Latest)> {
    if let Some(src) = &cfg.ranks {
        let t = Instant::now();
        match src.poll(ranks.as_ref().map(|r| r.1.as_str()), &cfg.dir).await {
            Ok(Some((p, tag))) => {
                let rf = RanksFile::open(&p)?;
                PR_PASSES.set(rf.header.iterations as i64);
                PR_L1.set(rf.header.l1);
                tracing::info!(rows = rf.header.rows, checkpoint = rf.header.checkpoint, "new ranks");
                *ranks = Some((rf, tag));
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(error = %format!("{e:#}"), "ranks unreadable: keeping the last"),
        }
        STEP_LAST.with_label_values(&["pagerank"]).set(t.elapsed().as_secs_f64());
    }
    if let Some((r, _)) = ranks {
        CHECKPOINT_AGE.set(now_secs() - r.header.created_ms as f64 / 1e3);
    }
    let out = tokio::task::block_in_place(|| build(st, ranks.as_ref().map(|r| &r.0), cfg.set, art_dir))?;
    let bytes = out.stats.file_bytes;
    ARTIFACT_BYTES.set(bytes as i64);
    publish_local(art_dir, &out.path, cfg.keep)?;
    let latest = Latest {
        version: out.meta.version.clone(),
        key: format!("{}.seek", out.meta.version),
        bytes,
        cursor_us: out.meta.cursor_us,
        created_ms: out.meta.created_ms,
    };
    if let Some(b) = &cfg.bucket {
        let t = Instant::now();
        b.publish(&out.path, &latest).await?;
        b.prune(cfg.keep).await?;
        UPLOAD_SECONDS.set(t.elapsed().as_secs_f64());
        STEP_SECONDS.with(&["upload"]).since(t);
        STEP_LAST.with_label_values(&["upload"]).set(t.elapsed().as_secs_f64());
    }
    // handles to verify: the highest-ranked unverified claims first, then the oldest checks
    let t = Instant::now();
    if queue.len() < cfg.verify_per_cycle {
        let (rank, _) = ranks_for(st.actors.iter(), ranks.as_ref().map(|r| &r.0), st.actors.len());
        let recheck = now_us() as i64 - cfg.recheck.as_micros() as i64;
        let mut todo: Vec<(usize, bool)> = st
            .actors
            .iter()
            .enumerate()
            .filter(|(_, a)| a.handle.is_some() && !a.hidden && (a.verified_us == 0 || a.verified_us < recheck))
            .map(|(i, a)| (i, a.verified_us == 0))
            .collect();
        todo.sort_by_key(|&(i, new)| (!new, std::cmp::Reverse(rank[i]), st.actors[i].verified_us));
        for (i, _) in todo.into_iter().take(cfg.verify_per_cycle - queue.len()) {
            queue.push(&st.actors[i].did.to_did());
        }
    }
    STEP_SECONDS.with(&["verify"]).since(t);
    STEP_LAST.with_label_values(&["verify"]).set(t.elapsed().as_secs_f64());
    if cycle == 1 || cycle.is_multiple_of(cfg.state_every.max(1)) {
        let t = Instant::now();
        let n = tokio::task::block_in_place(|| st.save(state_path))?;
        STEP_SECONDS.with(&["state"]).since(t);
        STEP_LAST.with_label_values(&["state"]).set(t.elapsed().as_secs_f64());
        tracing::info!(bytes = n, actors = st.actors.len(), "state saved");
    }
    Ok((art_dir.join(&latest.key), latest))
}

/// One cycle and exit: the benchmark's builder step. Prints a JSON report of its steps.
pub async fn build_once(cfg: Config, mut st: State) -> Result<()> {
    let art_dir = cfg.dir.join("artifacts");
    std::fs::create_dir_all(&art_dir)?;
    let mut ranks = None;
    let (tx, _rx) = mpsc::channel(1);
    let queue = Queue::start(Arc::new(crate::verify::Verifier::new("http://127.0.0.1:1", 1.0, 1.0, true)), 0, tx);
    let t = Instant::now();
    let (_, latest) =
        cycle_once(&cfg, &mut st, &mut ranks, &art_dir, &cfg.dir.join("state.seekstate"), 1, &queue).await?;
    let steps: serde_json::Map<String, serde_json::Value> = BUILDER_STEPS
        .iter()
        .map(|s| (s.to_string(), serde_json::json!(STEP_LAST.with_label_values(&[s]).get())))
        .collect();
    println!(
        "{}",
        serde_json::to_string_pretty(
            &serde_json::json!({"latest": latest, "cycle_secs": t.elapsed().as_secs_f64(), "steps": steps, "queued_to_verify": queue.len()})
        )?
    );
    Ok(())
}

/// The builder's HTTP routes: the newest artifact, /healthz, /readyz.
pub fn router(published: Arc<Mutex<Published>>) -> axum::Router {
    use axum::extract::State as S;
    use axum::response::IntoResponse;
    async fn latest(S(p): S<Arc<Mutex<Published>>>, headers: http::HeaderMap) -> axum::response::Response {
        let Some((path, l)) = p.lock().unwrap().latest.clone() else {
            return (http::StatusCode::SERVICE_UNAVAILABLE, "no artifact yet\n").into_response();
        };
        let etag = format!("\"{}\"", l.version);
        if headers.get(http::header::IF_NONE_MATCH).and_then(|v| v.to_str().ok()) == Some(etag.as_str()) {
            return (http::StatusCode::NOT_MODIFIED, [(http::header::ETAG, etag)]).into_response();
        }
        let f = match tokio::fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) => return (http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        };
        let body = axum::body::Body::from_stream(tokio_util_stream(f));
        (
            [
                (http::header::ETAG, etag),
                (http::header::CONTENT_LENGTH, l.bytes.to_string()),
                (http::header::CONTENT_TYPE, "application/octet-stream".into()),
                (http::HeaderName::from_static("x-seek-version"), l.version.clone()),
                (http::HeaderName::from_static("x-seek-cursor"), l.cursor_us.to_string()),
            ],
            body,
        )
            .into_response()
    }
    async fn ready(S(p): S<Arc<Mutex<Published>>>) -> (http::StatusCode, String) {
        let p = p.lock().unwrap();
        match (&p.latest, &p.last_error) {
            (Some(_), None) => (http::StatusCode::OK, "ready\n".into()),
            (_, Some(e)) => (http::StatusCode::SERVICE_UNAVAILABLE, format!("last cycle failed: {e}\n")),
            (None, None) => (http::StatusCode::SERVICE_UNAVAILABLE, "no artifact yet\n".into()),
        }
    }
    axum::Router::new()
        .route("/artifact/latest", axum::routing::get(latest))
        .route("/readyz", axum::routing::get(ready))
        .route("/healthz", axum::routing::get(|| async { "ok\n" }))
        .with_state(published)
}

fn tokio_util_stream(mut f: tokio::fs::File) -> impl futures::Stream<Item = std::io::Result<bytes::Bytes>> {
    async_stream(move |tx| async move {
        let mut buf = vec![0u8; 1 << 20];
        loop {
            match tokio::io::AsyncReadExt::read(&mut f, &mut buf).await {
                Ok(0) => return,
                Ok(n) => {
                    if tx.send(Ok(bytes::Bytes::copy_from_slice(&buf[..n]))).await.is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            }
        }
    })
}

fn async_stream<F, Fut>(f: F) -> impl futures::Stream<Item = std::io::Result<bytes::Bytes>>
where
    F: FnOnce(mpsc::Sender<std::io::Result<bytes::Bytes>>) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let (tx, rx) = mpsc::channel(4);
    tokio::spawn(f(tx));
    futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|x| (x, rx)) })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st() -> State {
        let mut s = State::default();
        for (d, h, v, n, p) in [
            ("did:plc:z72i7hdynmk6r22z27h6tvur", "bsky.app", 1, "Bluesky", true),
            ("did:plc:ewvi7nxzyoun6zhxrhs64oiz", "atproto.com", 0, "AT Protocol", true),
            ("did:web:example.com", "example.com", 1, "", false),
        ] {
            let a = s.get_mut(d);
            a.handle = Some(h.into());
            a.verified_us = v;
            a.name = (!n.is_empty()).then(|| n.into());
            a.profile = p;
        }
        s.cursor_us = 99;
        s.merge_added();
        s
    }

    #[test]
    fn state_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("s");
        let mut s = st();
        s.plc_cursor = "2026-11-14T00:00:00Z".into();
        s.save(&p).unwrap();
        let l = State::load(&p).unwrap();
        assert_eq!(l.actors, s.actors);
        assert_eq!((l.cursor_us, l.plc_cursor.as_str()), (99, "2026-11-14T00:00:00Z"));
        let mut b = std::fs::read(&p).unwrap();
        let n = b.len();
        b.truncate(n - 3);
        std::fs::write(&p, &b).unwrap();
        assert!(State::load(&p).is_err());
    }

    #[test]
    fn events_change_the_state() {
        let mut s = st();
        let d = "did:plc:z72i7hdynmk6r22z27h6tvur".to_string();
        assert!(s.apply(&Ev::Identity { did: d.clone(), time_us: 120, handle: Some("bsky.app".into()) }).is_none());
        assert_eq!(
            s.apply(&Ev::Identity { did: d.clone(), time_us: 130, handle: Some("Other.example".into()) }),
            Some(d.clone())
        );
        assert_eq!(s.get(&d).unwrap().verified_us, 0);
        assert_eq!(s.get(&d).unwrap().handle.as_deref(), Some("other.example"));
        s.verified(&d, &Outcome::Valid("other.example".into()));
        assert!(s.get(&d).unwrap().verified_us > 0);
        s.apply(&Ev::Account { did: d.clone(), time_us: 140, active: false });
        assert!(s.get(&d).unwrap().hidden);
        s.apply(&Ev::Profile {
            did: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into(),
            time_us: 150,
            name: Some("New".into()),
            deleted: false,
        });
        assert_eq!(s.added.len(), 1);
        assert_eq!(s.cursor_us, 150);
        s.merge_added();
        assert_eq!(s.actors.len(), 4);
        assert!(s.actors.windows(2).all(|w| w[0].did < w[1].did));
    }

    #[test]
    fn builds_with_ranks_and_only_verified_handles() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = st();
        let rp = dir.path().join("r.ranks");
        let mut rows = vec![seek_core::ranks::Row {
            did: key_of("did:plc:ewvi7nxzyoun6zhxrhs64oiz"),
            rank16: 777,
            followers: 3,
            hidden: false,
        }];
        seek_core::ranks::write(&rp, &Default::default(), &mut rows).unwrap();
        let rf = RanksFile::open(&rp).unwrap();
        let out = build(&mut s, Some(&rf), IndexSet::Profiles, dir.path()).unwrap();
        assert_eq!(out.meta.actors, 2, "profiles only");
        assert_eq!(out.meta.cursor_us, 99);
        let ix = seek_core::index::Index::open(&out.path, true).unwrap();
        let id = ix.id_of("did:plc:ewvi7nxzyoun6zhxrhs64oiz").unwrap();
        assert_eq!(ix.rank(id), 777);
        let ov = seek_core::overlay::Overlay::new(&ix);
        let p = seek_core::query::Params::default();
        // atproto.com's claim isn't verified: findable by its name, not its handle
        assert!(seek_core::query::search(&ix, &ov, None, "atproto.com", 10, &p).is_empty());
        assert_eq!(seek_core::query::search(&ix, &ov, None, "AT Prot", 10, &p).len(), 1);
        assert_eq!(seek_core::query::search(&ix, &ov, None, "bsky.app", 10, &p).len(), 1);
        std::thread::sleep(std::time::Duration::from_millis(5));
        let all = build(&mut s, None, IndexSet::All, dir.path()).unwrap();
        assert_eq!(all.meta.actors, 3);
        publish_local(dir.path(), &all.path, 1).unwrap();
        assert!(dir.path().join("latest.seek").exists());
        assert!(!out.path.exists(), "pruned");
    }

    #[test]
    fn bootstraps_from_json_lines() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("b.jsonl.zst");
        let lines = [
            r#"{"did":"did:plc:z72i7hdynmk6r22z27h6tvur","handle":"bsky.app","verified_us":5,"name":"Bluesky","profile":true}"#,
            r#"{"did":"did:plc:ewvi7nxzyoun6zhxrhs64oiz","handle":"Not Valid","verified_us":5,"name":"","profile":1}"#,
            r#"{"did":"did:plc:z72i7hdynmk6r22z27h6tvur","handle":"bsky.app","verified_us":5,"name":"Bluesky","profile":true}"#,
            "garbage",
        ]
        .join("\n");
        std::fs::write(&p, zstd::encode_all(lines.as_bytes(), 1).unwrap()).unwrap();
        let s = State::bootstrap(&p, 7).unwrap();
        assert_eq!(s.actors.len(), 2);
        let a = s.get("did:plc:ewvi7nxzyoun6zhxrhs64oiz").unwrap();
        assert!(a.handle.is_none() && a.verified_us == 0 && a.profile);
        assert_eq!(s.cursor_us, 7);
    }
}
