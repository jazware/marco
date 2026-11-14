//! marco.v1.Admin: what the console shows and the operator's actions.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use marco_core::read::{ReadCx, R};
use marco_core::relation::parse_uri;
use marco_core::rkey::{is_tid, key_of};
use marco_core::{Dir, Graph, RelId, Stored};
use marco_proto::connect::marco::v1 as svc;
use marco_proto::v1::*;

use crate::app::{App, JobState, Status, Verify};
use crate::auth::Caller;
use crate::graph_svc::{bad, dir_of, dir_pb, rel_id, run_admin as run};

pub struct AdminApi(pub Arc<App>);

/// How far a list is scanned for answers that need its whole length
/// (an actor's first record, a record's position).
const SCAN_CAP: u64 = 100_000;

fn stored(s: Stored) -> String {
    match s {
        Stored::Base => "base",
        Stored::Delta => "delta",
        Stored::Merged => "merged",
    }
    .into()
}

/// The Cache panel's numbers: the budget by part, lookups by structure,
/// errors, and what checkpoints and compactions did.
fn cache_pb(app: &App) -> Cache {
    use marco_core::read::{Found, Structure};
    let b = crate::metrics::store_bytes(app);
    let st = app.engine.stats();
    let structures = Structure::ALL
        .iter()
        .filter(|&&s| s != Structure::Other)
        .map(|&s| {
            let l = &st.lookups[s as usize];
            CacheStructure {
                name: s.name().into(),
                lookups: l.iter().sum(),
                misses: l[Found::Miss as usize],
                ..Default::default()
            }
        })
        .collect();
    Cache {
        budget_bytes: b.budget,
        pinned_dir_bytes: b.pinned_dir,
        pinned_index_bytes: b.pinned_index,
        pinned_heads_bytes: b.pinned_heads,
        pinned_tables_bytes: b.pinned_tables,
        main_bytes: b.cache_main,
        small_bytes: b.cache_small,
        retired_bytes: b.retired,
        pins_bytes: b.pinned_frames,
        free_bytes: b.free,
        borrowed_bytes: b.borrowed,
        garbage_bytes: b.garbage,
        structures,
        bad_pages: st.bad_pages,
        checksum_errors: st.checksum,
        eio_errors: st.eio,
        short_errors: st.short,
        eio_retried: st.eio_retried,
        failing_pages: app.engine.failing_pages(marco_core::store::engine::STORM_WINDOW) as u32,
        miss_threads: app.cfg.miss_threads as u32,
        compactions_24h: app.compactions_since(crate::app::now_ms() - 86_400_000),
        resident: app.resident.load(Ordering::Relaxed),
        written_through: st.written_through,
        write_through_skipped: st.write_through_skipped,
        ..Default::default()
    }
}

fn job_pb(j: &JobState) -> Job {
    Job {
        kind: j.kind.into(),
        running: j.running,
        started_ms: j.started_ms,
        finished_ms: j.finished_ms,
        error: j.error.clone(),
        done: j.done,
        total: j.total,
        ..Default::default()
    }
}

fn created_us(key: u64) -> i64 {
    if is_tid(key) {
        (key >> 10) as i64
    } else {
        0
    }
}

/// Delta entries and tombstones over every list.
pub fn delta_totals(g: &Graph) -> (u64, u64) {
    let mut d = 0u64;
    let mut t = 0u64;
    for r in 0..g.relations().len() as RelId {
        for dir in [Dir::Out, Dir::In] {
            g.for_each_delta(r, dir, |_, a, x| d += (a + x) as u64);
        }
        t += g.tombstones(r) as u64;
    }
    (d, t)
}

struct ShardAcc {
    delta: u64,
    delta_lists: u32,
    merged: u32,
    merged_bytes: u64,
    tombs: u32,
}

/// Per shard: delta entries, merged lists and tombstones, by the shard that
/// holds the list (an id added since the snapshot counts in the last).
fn shard_accs(g: &Graph) -> Vec<ShardAcc> {
    let n = g.n_shards().max(1);
    let w = g.shard_width().max(1);
    let at = |id: u32| ((id / w) as usize).min(n - 1);
    let mut v: Vec<ShardAcc> =
        (0..n).map(|_| ShardAcc { delta: 0, delta_lists: 0, merged: 0, merged_bytes: 0, tombs: 0 }).collect();
    for r in 0..g.relations().len() as RelId {
        for dir in [Dir::Out, Dir::In] {
            g.for_each_delta(r, dir, |id, a, d| {
                let x = &mut v[at(id)];
                x.delta += (a + d) as u64;
                x.delta_lists += 1;
            });
            g.for_each_merged(r, dir, |id, b| {
                let x = &mut v[at(id)];
                x.merged += 1;
                x.merged_bytes += b as u64;
            });
        }
        for (a, _) in g.tombstone_keys(r) {
            v[at(a)].tombs += 1;
        }
    }
    v
}

fn shard_state(app: &App, dirty: bool) -> &'static str {
    let job = app.job();
    if job.running && job.kind == "checkpoint" && dirty {
        "writing"
    } else if dirty {
        "dirty"
    } else {
        "clean"
    }
}

fn durability_pb(app: &App) -> Durability {
    let d = app.durability.lock().unwrap().clone();
    Durability {
        segment_interval_seconds: app.cfg.segment_interval.as_secs_f64(),
        segment: d.segment,
        segment_ms: d.segment_ms,
        segment_write_seconds: d.segment_write_seconds,
        segments: d.segments,
        segment_bytes: d.segment_bytes,
        segment_ops: d.segment_ops,
        pending_ops: d.pending_ops,
        checkpoint: d.checkpoint,
        checkpoint_ms: d.checkpoint_ms,
        checkpoint_ops: app.cfg.checkpoint_ops,
        checkpoint_age_seconds: app.cfg.checkpoint_age.as_secs_f64(),
        checkpoint_seconds: d.checkpoint_seconds,
        checkpoint_bytes: d.checkpoint_bytes,
        checkpoint_shards: d.checkpoint_shards,
        replayed_segments: d.replayed_segments,
        replayed_ops: d.replayed_ops,
        replay_seconds: d.replay_seconds,
        segment_error: d.segment_error,
        checkpoint_error: d.checkpoint_error,
        checkpoint_failures: d.checkpoint_failures,
        checkpoint_patch_bytes: d.checkpoint_patch_bytes,
        checkpoint_pinned_bytes: d.checkpoint_pinned_bytes,
        ..Default::default()
    }
}

fn status_pb(s: &str) -> Result<Option<Status>, ConnectError> {
    match s {
        "active" => Ok(None),
        "deleted" => Ok(Some(Status::Deleted)),
        "takendown" => Ok(Some(Status::Takendown)),
        _ => Err(bad("status must be active, deleted or takendown")),
    }
}

fn dist_pb(d: &crate::liststats::Dist, threshold: u64) -> ListDist {
    ListDist {
        log2_counts: d.log2(),
        lists: d.lists(),
        p50: d.quantile(0.5),
        p90: d.quantile(0.9),
        p99: d.quantile(0.99),
        p999: d.quantile(0.999),
        max: d.max(),
        over_threshold: d.at_least(threshold),
        ..Default::default()
    }
}

impl AdminApi {
    fn writable(&self) -> Result<(), ConnectError> {
        if self.0.cfg.read_only {
            return Err(ConnectError::failed_precondition("this marco is --read-only"));
        }
        Ok(())
    }
}

#[allow(refining_impl_trait)]
impl svc::Admin for AdminApi {
    async fn get_session(
        &self,
        ctx: RequestContext,
        _: ServiceRequest<'_, GetSessionRequest>,
    ) -> ServiceResult<GetSessionResponse> {
        let (auth, operator) = match ctx.extensions().get::<Caller>() {
            Some(Caller::Token) => ("token", String::new()),
            Some(Caller::Proxy(l)) => ("proxy", l.clone()),
            Some(Caller::Export) | None => ("", String::new()),
        };
        Response::ok(GetSessionResponse { auth: auth.into(), operator, ..Default::default() })
    }

    async fn get_overview(
        &self,
        _: RequestContext,
        _: ServiceRequest<'_, GetOverviewRequest>,
    ) -> ServiceResult<GetOverviewResponse> {
        let app = &self.0;
        let g = app.graph();
        let snap = g.snapshot().ok_or_else(|| ConnectError::unavailable("no snapshot"))?;
        let meta = &snap.dir.meta;
        let stats = snap.stats();
        let ratio = g.merge_ratio;
        let dirty = g.dirty_shards().iter().filter(|&&d| d).count() as u32;
        let mut relations = Vec::new();
        let (mut delta_entries, mut base_entries, mut tombstones, mut merged_bytes, mut merged_lists) = (0, 0, 0, 0, 0);
        for (i, rel) in g.relations().list.iter().enumerate() {
            let r = i as RelId;
            let st = stats.get(i).cloned().unwrap_or_default();
            let (mut dout, mut din, mut mb) = (0u64, 0u64, 0u64);
            g.for_each_delta(r, Dir::Out, |_, a, d| dout += (a + d) as u64);
            g.for_each_delta(r, Dir::In, |_, a, d| din += (a + d) as u64);
            for dir in [Dir::Out, Dir::In] {
                g.for_each_merged(r, dir, |_, b| {
                    mb += b as u64;
                    merged_lists += 1;
                });
            }
            let records = g.records(r).0;
            let tombs = g.tombstones(r) as u64;
            delta_entries += dout + din;
            base_entries += st.out_records + st.in_pairs;
            tombstones += tombs;
            merged_bytes += mb;
            relations.push(RelationTotals {
                name: rel.name.clone(),
                collection: rel.collection.clone(),
                subject: rel.subject.clone(),
                records,
                snapshot_records: st.out_records,
                snapshot_pairs: st.in_pairs,
                out_lists: st.out_lists,
                in_lists: st.in_lists,
                non_tid: st.non_tid,
                delta_out: dout,
                delta_in: din,
                tombstones: tombs,
                merged_bytes: mb,
                ..Default::default()
            });
        }
        let p = crate::sys::proc_self();
        let (ready, ready_reason) = app.readiness();
        let resp = GetOverviewResponse {
            version: crate::VERSION.into(),
            commit: crate::COMMIT.into(),
            started_ms: app.started_ms,
            read_only: app.cfg.read_only,
            snapshot: SnapshotInfo {
                path: app.cfg.snapshot.display().to_string(),
                build: format!("{:016x}", meta.build),
                format: meta.format,
                shards: g.n_shards() as u32,
                size_bytes: snap.bytes(),
                written_ms: snap.manifest.written_ms,
                open_seconds: app.open_seconds,
                subj_index: meta.subj_index,
                files: snap.files().count() as u32,
                ..Default::default()
            }
            .into(),
            directory: DirectoryInfo {
                base: meta.n_ids as u64,
                live: (g.n_ids() - meta.n_ids) as u64,
                ..Default::default()
            }
            .into(),
            relations,
            process: ProcessInfo {
                rss_bytes: p.rss_bytes,
                mem_total_bytes: crate::sys::mem_total(),
                cpu_seconds: p.cpu_seconds,
                threads: p.threads,
                ..Default::default()
            }
            .into(),
            trace_ratio: marco_core::obs::trace_ratio(),
            job: job_pb(&app.job()).into(),
            merge_ratio: ratio,
            dirty_shards: dirty,
            delta_entries,
            base_entries,
            tombstones,
            merged_bytes,
            merged_lists,
            write_cursor: app.latest_cursor(crate::app::API_SOURCE).unwrap_or_default(),
            write_seq: app.write_seq.load(Ordering::Relaxed),
            api_listen: app.cfg.api_listen.clone(),
            admin_listen: app.cfg.admin_listen.clone(),
            durability: durability_pb(app).into(),
            ready,
            ready_reason,
            reads_before_ready: app.reads_before_ready.load(Ordering::Relaxed),
            cache: cache_pb(app).into(),
            ..Default::default()
        };
        Response::ok(resp)
    }

    async fn get_series(
        &self,
        _: RequestContext,
        req: ServiceRequest<'_, GetSeriesRequest>,
    ) -> ServiceResult<GetSeriesResponse> {
        let w = match req.window_seconds {
            0 => 3600,
            x => x.min(86_400),
        };
        let (step_ms, t_ms, series) = self.0.series.lock().unwrap().window(w as u64);
        let series = series
            .into_iter()
            .map(|(k, values)| Series {
                metric: k.metric.into(),
                op: k.op,
                rel: k.rel,
                outcome: k.outcome,
                dir: k.dir,
                rpc: k.rpc,
                values,
                ..Default::default()
            })
            .collect();
        Response::ok(GetSeriesResponse { step_ms, t_ms, series, ..Default::default() })
    }

    async fn list_recent_writes(
        &self,
        _: RequestContext,
        req: ServiceRequest<'_, ListRecentWritesRequest>,
    ) -> ServiceResult<ListRecentWritesResponse> {
        let lim = if req.limit == 0 { 50 } else { req.limit.min(256) } as usize;
        let ring = self.0.recent.lock().unwrap();
        let writes = ring
            .iter()
            .rev()
            .filter(|w| w.seq > req.after)
            .take(lim)
            .map(|w| RecentWrite {
                seq: w.seq,
                at_ms: w.at_ms,
                rel: w.rel.clone(),
                add: w.add,
                actor: w.actor.clone(),
                subject: w.subject.clone(),
                outcome: w.outcome.into(),
                uri: w.uri.clone(),
                ..Default::default()
            })
            .collect();
        Response::ok(ListRecentWritesResponse {
            writes,
            last_seq: self.0.write_seq.load(Ordering::Relaxed),
            ..Default::default()
        })
    }

    async fn list_shards(
        &self,
        _: RequestContext,
        _: ServiceRequest<'_, ListShardsRequest>,
    ) -> ServiceResult<ListShardsResponse> {
        let app = &self.0;
        let g = app.graph();
        let snap = g.snapshot().ok_or_else(|| ConnectError::unavailable("no snapshot"))?;
        let accs = shard_accs(&g);
        let dirty = g.dirty_shards();
        let verify = app.verify.lock().unwrap().clone();
        let vword = |i: usize| verify.get(i).cloned().unwrap_or_default();
        // verify slots follow the files: the directory, then each shard file in order
        let mut file_slot = 0;
        let w = g.shard_width();
        let shards = accs
            .iter()
            .enumerate()
            .map(|(k, a)| {
                let f = snap.shards.get(k).and_then(|f| f.as_deref());
                let base: u64 = f.map_or(0, |f| f.meta.stats.iter().map(|s| s.out_records + s.in_pairs).sum());
                // a shard's verdict is its first file's that isn't ok
                let v = match f {
                    Some(f) => {
                        let vs: Vec<Verify> = (0..f.parts.len()).map(|i| vword(file_slot + 1 + i)).collect();
                        file_slot += f.parts.len();
                        vs.iter().find(|v| !matches!(v, Verify::Ok)).cloned().unwrap_or(Verify::Ok)
                    }
                    None => Verify::Unverified,
                };
                let lo = k as u32 * w;
                Shard {
                    shard: k as u32,
                    lo,
                    hi: f.map_or(lo, |f| f.meta.hi),
                    size_bytes: f.map_or(0, |f| f.len()),
                    written_ms: f.map_or(0, |f| f.meta.written_ms),
                    file: f.map_or_else(String::new, |f| f.name.clone()),
                    checkpoint: f.map_or(0, |f| f.meta.checkpoint),
                    base_entries: base,
                    delta_entries: a.delta,
                    delta_lists: a.delta_lists,
                    merged_lists: a.merged,
                    merged_bytes: a.merged_bytes,
                    tombstones: a.tombs,
                    state: shard_state(app, dirty.get(k).copied().unwrap_or(true)).into(),
                    verify: v.word().into(),
                    verify_error: match v {
                        Verify::Mismatch(e) => e,
                        _ => String::new(),
                    },
                    ..Default::default()
                }
            })
            .collect();
        Response::ok(ListShardsResponse {
            shards,
            merge_ratio: g.merge_ratio,
            job: job_pb(&app.job()).into(),
            directory_bytes: snap.dir.file.len(),
            directory_verify: vword(0).word().into(),
            ..Default::default()
        })
    }

    async fn get_storage(
        &self,
        ctx: RequestContext,
        req: ServiceRequest<'_, GetStorageRequest>,
    ) -> ServiceResult<GetStorageResponse> {
        let by_size = match req.by {
            "" | "ratio" => false,
            "size" => true,
            _ => return Err(bad("by must be ratio or size")),
        };
        let g = self.0.graph();
        let snap = g.snapshot().ok_or_else(|| ConnectError::unavailable("no snapshot"))?;
        let mut secs: std::collections::BTreeMap<String, (u64, u32)> = Default::default();
        for (_, f) in snap.files() {
            for s in f.sections() {
                let e = secs.entry(s.name.clone()).or_default();
                e.0 += s.len();
                e.1 += 1;
            }
        }
        let rel_of = |name: &str| -> String {
            name.strip_prefix('r')
                .and_then(|r| r.split('.').next())
                .and_then(|n| n.parse::<usize>().ok())
                .and_then(|i| g.relations().list.get(i))
                .map_or_else(|| "directory".to_string(), |r| r.name.clone())
        };
        let sections = secs
            .into_iter()
            .map(|(name, (b, files))| Section { rel: rel_of(&name), name, size_bytes: b, files, ..Default::default() })
            .collect();
        let mut changed: Vec<(RelId, Dir, u32, u64, u64)> = Vec::new();
        for r in 0..g.relations().len() as RelId {
            for dir in [Dir::Out, Dir::In] {
                g.for_each_delta(r, dir, |id, a, d| changed.push((r, dir, id, a as u64, d as u64)));
            }
        }
        drop(g);
        let lists: Vec<(RelId, Dir, u32)> = changed.iter().map(|&(r, dir, id, _, _)| (r, dir, id)).collect();
        let now = run(&self.0, &ctx, "Admin/GetStorage", move |g, cx| {
            lists
                .iter()
                .map(|&(r, dir, id)| match dir {
                    Dir::Out => g.out_count(cx, r, id),
                    Dir::In => g.in_count(cx, r, id),
                })
                .collect::<R<Vec<u64>>>()
        })
        .await?;
        let g = self.0.graph();
        // (changed / base, changed, the list, its base length)
        let mut lists: Vec<_> = changed
            .into_iter()
            .zip(now)
            .map(|(l @ (_, _, _, a, d), now)| {
                let base = (now + d).saturating_sub(a);
                ((a + d) as f64 / base.max(1) as f64, a + d, l, base)
            })
            .collect();
        let delta_lists = lists.len() as u32;
        if by_size {
            lists.sort_unstable_by_key(|x| std::cmp::Reverse(x.1));
        } else {
            lists.sort_unstable_by(|x, y| y.0.total_cmp(&x.0).then(y.1.cmp(&x.1)));
        }
        let deltas = lists
            .iter()
            .take(20)
            .map(|&(_, _, (r, dir, id, a, d), base)| DeltaList {
                did: g.did_of(id).unwrap_or_default(),
                rel: g.relations().get(r).name.clone(),
                dir: dir_pb(dir),
                base,
                adds: a,
                dels: d,
                ..Default::default()
            })
            .collect();
        Response::ok(GetStorageResponse { sections, deltas, delta_lists, ..Default::default() })
    }

    async fn list_relations(
        &self,
        _: RequestContext,
        _: ServiceRequest<'_, ListRelationsRequest>,
    ) -> ServiceResult<ListRelationsResponse> {
        let g = self.0.graph();
        let stats = g.snapshot().map(|s| s.stats()).unwrap_or_default();
        let relations = g
            .relations()
            .list
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let st = stats.get(i).cloned().unwrap_or_default();
                RelationInfo {
                    name: r.name.clone(),
                    collection: r.collection.clone(),
                    subject: r.subject.clone(),
                    records: st.out_records,
                    pairs: st.in_pairs,
                    duplicates: st.out_records.saturating_sub(st.in_pairs),
                    non_tid: st.non_tid,
                    list: r.list.clone().unwrap_or_default(),
                    owner_of: r.owner_of.clone().unwrap_or_default(),
                    ..Default::default()
                }
            })
            .collect();
        // the config as written: companions are made, not configured
        let user = marco_core::Relations {
            list: g.relations().list.iter().filter(|r| r.owner_of.is_none()).cloned().collect(),
        };
        let config = toml::to_string(&user).unwrap_or_default();
        Response::ok(ListRelationsResponse { relations, config, ..Default::default() })
    }

    async fn get_list_stats(
        &self,
        _: RequestContext,
        req: ServiceRequest<'_, GetListStatsRequest>,
    ) -> ServiceResult<GetListStatsResponse> {
        let (rel, build) = {
            let g = self.0.graph();
            let rel = rel_id(&g, req.rel)?;
            (rel, g.snapshot().map_or(0, |s| s.manifest.checkpoint ^ s.dir.meta.build))
        };
        match self.0.list_stats.get(build, self.0.cfg.snapshot.clone(), self.0.engine.clone()) {
            crate::liststats::Answer::Computing(progress) => {
                Response::ok(GetListStatsResponse { ready: false, progress, ..Default::default() })
            }
            crate::liststats::Answer::Ready(st) => {
                let r = &st.rels[rel as usize];
                let t = st.threshold as u64;
                Response::ok(GetListStatsResponse {
                    ready: true,
                    progress: 1.0,
                    computed_ms: st.computed_ms,
                    out: dist_pb(&r.out, t).into(),
                    r#in: dist_pb(&r.inn, t).into(),
                    big: r
                        .big
                        .iter()
                        .map(|(len, did, out)| BigList {
                            did: did.clone(),
                            dir: dir_pb(if *out { Dir::Out } else { Dir::In }),
                            length: *len,
                            ..Default::default()
                        })
                        .collect(),
                    threshold: st.threshold,
                    ..Default::default()
                })
            }
        }
    }

    async fn list_sources(
        &self,
        _: RequestContext,
        _: ServiceRequest<'_, ListSourcesRequest>,
    ) -> ServiceResult<ListSourcesResponse> {
        let now = crate::app::now_ms();
        let rates = self.0.source_rates();
        let sources = self
            .0
            .sources
            .lock()
            .unwrap()
            .iter()
            .map(|(name, s)| {
                let r = s.report.clone().unwrap_or_default();
                Source {
                    name: name.clone(),
                    url: r.url,
                    state: if r.state.is_empty() {
                        if now - s.last_ms < 60_000 { "writing" } else { "idle" }.into()
                    } else {
                        r.state
                    },
                    cursor: s.cursor.clone(),
                    cursor_kind: r.cursor_kind,
                    lag_seconds: r.lag_seconds,
                    ops_per_second: rates.get(name).copied().unwrap_or(0.0),
                    errors: r.errors,
                    reconnects: r.reconnects,
                    durable_cursor: s.durable.clone(),
                    expected: s.expected,
                    caught_up: s.caught_up,
                    last_ms: s.last_ms,
                    ops: s.ops,
                    ..Default::default()
                }
            })
            .collect();
        Response::ok(ListSourcesResponse { sources, ..Default::default() })
    }

    async fn get_actor(
        &self,
        ctx: RequestContext,
        req: ServiceRequest<'_, GetActorRequest>,
    ) -> ServiceResult<GetActorResponse> {
        let did = req.actor.trim().to_string();
        let list = did.starts_with("at://");
        if marco_core::did::classify(&did).is_none() && !(list && parse_uri(&did).is_some()) {
            return Err(bad(format!("{did:?} isn't a DID or a list's AT URI")));
        }
        let g = self.0.graph();
        let status = if list && g.hidden(&did) {
            "owner hidden".to_string()
        } else {
            Status::name(self.0.status_of(&did)).to_string()
        };
        let Some(id) = g.id_of(&did) else {
            return Response::ok(GetActorResponse { did, status, list, ..Default::default() });
        };
        let w = g.shard_width().max(1);
        drop(g);
        let rel = move |g: &Graph, cx: &ReadCx<'_>, r: RelId| -> R<ActorRelation> {
            let out = g.out_count(cx, r, id)?;
            let inn = g.in_count(cx, r, id)?;
            let mutual = if out.min(inn) == 0 || out.min(inn) > 1_000_000 {
                0
            } else {
                g.intersect(
                    cx,
                    marco_core::ListRef { rel: r, dir: Dir::Out, actor: id },
                    marco_core::ListRef { rel: r, dir: Dir::In, actor: id },
                    usize::MAX,
                )?
                .len() as u64
            };
            let first_us = if out > 0 && out <= SCAN_CAP {
                g.out_page(cx, r, id, out as usize, None)?.last().map_or(0, |&(k, _)| created_us(k))
            } else {
                0
            };
            Ok(ActorRelation {
                rel: g.relations().get(r).name.clone(),
                out,
                r#in: inn,
                mutual,
                first_us,
                out_stored: stored(g.list_stored(r, Dir::Out, id)),
                in_stored: stored(g.list_stored(r, Dir::In, id)),
                out_subjects: if out == 0 { 0 } else { g.out_distinct(cx, r, id)? },
                ..Default::default()
            })
        };
        let rels = run(&self.0, &ctx, "Admin/GetActor", move |g, cx| {
            let mut pend = Ok(());
            let rels: Vec<Option<ActorRelation>> = (0..g.relations().len() as RelId)
                .map(|r| crate::graph_svc::item(&mut pend, rel(g, cx, r).map(Some)))
                .collect();
            pend?;
            Ok(rels.into_iter().flatten().collect::<Vec<_>>())
        })
        .await?;
        Response::ok(GetActorResponse { did, known: true, id, shard: id / w, status, rels, list, ..Default::default() })
    }

    async fn list_edges(
        &self,
        ctx: RequestContext,
        req: ServiceRequest<'_, ListEdgesRequest>,
    ) -> ServiceResult<ListEdgesResponse> {
        let req = req.to_owned_message();
        let g = self.0.graph();
        let rel = rel_id(&g, &req.rel)?;
        let dir = dir_of(&req.dir)?;
        let lim = if req.limit == 0 { 50 } else { req.limit.min(500) } as usize;
        let Some(id) = g.id_of(&req.actor) else { return Response::ok(ListEdgesResponse::default()) };
        let after = match (dir, req.cursor.as_str()) {
            (Dir::In, c) if !c.is_empty() => Some(g.id_of(c).ok_or_else(|| bad("cursor isn't a DID in this graph"))?),
            _ => None,
        };
        drop(g);
        let (edges, cursor, total) = run(&self.0, &ctx, "Admin/ListEdges", move |g, cx| {
            let mut edges = Vec::new();
            let mut cursor = String::new();
            let total = match dir {
                Dir::Out => {
                    let before = (!req.cursor.is_empty()).then(|| key_of(&req.cursor).0);
                    let page = g.out_page(cx, rel, id, lim, before)?;
                    if page.len() == lim {
                        cursor = page.last().and_then(|&(k, _)| g.rkey_string(rel, id, k)).unwrap_or_default();
                    }
                    for (k, s) in page {
                        edges.push(Edge {
                            did: g.did_of(s).unwrap_or_default(),
                            id: s,
                            rkey: g.rkey_string(rel, id, k).unwrap_or_default(),
                            uri: g.uri(rel, id, k).unwrap_or_default(),
                            created_us: created_us(k),
                            stored: stored(g.out_stored(rel, id, k)),
                            ..Default::default()
                        });
                    }
                    g.out_count(cx, rel, id)?
                }
                Dir::In => {
                    let page = g.in_page(cx, rel, id, lim, after)?;
                    if page.len() == lim {
                        cursor = page.last().and_then(|&x| g.did_of(x)).unwrap_or_default();
                    }
                    let mut pend = Ok(());
                    let keys: Vec<Option<u64>> = page
                        .iter()
                        .map(|&x| crate::graph_svc::item(&mut pend, g.out_find_subject(cx, rel, x, id)))
                        .collect();
                    pend?;
                    for (x, k) in page.into_iter().zip(keys) {
                        edges.push(Edge {
                            did: g.did_of(x).unwrap_or_default(),
                            id: x,
                            rkey: k.and_then(|k| g.rkey_string(rel, x, k)).unwrap_or_default(),
                            uri: k.and_then(|k| g.uri(rel, x, k)).unwrap_or_default(),
                            created_us: k.map_or(0, created_us),
                            stored: k.map_or_else(String::new, |k| stored(g.out_stored(rel, x, k))),
                            ..Default::default()
                        });
                    }
                    g.in_count(cx, rel, id)?
                }
            };
            Ok((edges, cursor, total))
        })
        .await?;
        Response::ok(ListEdgesResponse { edges, cursor, total, ..Default::default() })
    }

    async fn resolve_uri(
        &self,
        ctx: RequestContext,
        req: ServiceRequest<'_, ResolveUriRequest>,
    ) -> ServiceResult<ResolveUriResponse> {
        let uri = req.uri.trim();
        let Some((did, coll, rkey)) = parse_uri(uri) else { return Err(bad(format!("{uri:?} isn't a record URI"))) };
        let g = self.0.graph();
        let Some(rel) = g.relations().by_collection(coll) else { return Err(bad(format!("no relation for {coll}"))) };
        let (key, side) = key_of(rkey);
        let mut r = ResolveUriResponse {
            rel: g.relations().get(rel).name.clone(),
            collection: coll.into(),
            actor: did.into(),
            rkey: rkey.into(),
            tid: !side,
            created_us: created_us(key),
            ..Default::default()
        };
        let w = g.shard_width().max(1);
        let Some(owner) = g.id_of(did) else { return Response::ok(r) };
        r.actor_id = owner;
        r.shard = owner / w;
        let base = r;
        drop(g);
        let r = run(&self.0, &ctx, "Admin/ResolveUri", move |g, cx| {
            let mut r = base.clone();
            // a list relation's record is its owner's; its edge is in the list's log
            let rec = g.relations().owner_rel(rel).unwrap_or(rel);
            r.out_count = g.out_count(cx, rec, owner)?;
            r.tombstoned = g.is_tombstoned(rec, owner, key);
            let a = if rec == rel {
                owner
            } else {
                let Some(l) = g.out_get(cx, rec, owner, key)? else { return Ok(r) };
                r.list = g.did_of(l).unwrap_or_default();
                l
            };
            let Some(s) = g.out_get(cx, rel, a, key)? else { return Ok(r) };
            r.subject = g.did_of(s).unwrap_or_default();
            r.subject_id = s;
            r.subject_shard = s / w;
            r.stored = stored(g.out_stored(rel, a, key));
            let n = g.out_count(cx, rel, a)?;
            if n <= SCAN_CAP {
                let page = g.out_page(cx, rel, a, n as usize, None)?;
                r.position = page.iter().position(|e| e.0 == key).map_or(0, |p| p as u64 + 1);
            }
            r.pair_records = g.records_naming(cx, rel, a, s)? as u64;
            r.in_contains = g.in_contains(cx, rel, s, a)?;
            r.subject_in_count = g.in_count(cx, rel, s)?;
            r.subject_in_stored = stored(g.list_stored(rel, Dir::In, s));
            Ok(r)
        })
        .await?;
        Response::ok(r)
    }

    async fn get_settings(
        &self,
        _: RequestContext,
        _: ServiceRequest<'_, GetSettingsRequest>,
    ) -> ServiceResult<GetSettingsResponse> {
        let flags = self
            .0
            .cfg
            .flags
            .iter()
            .map(|f| Flag {
                name: f.name.clone(),
                value: f.value.clone(),
                source: f.source.clone(),
                ..Default::default()
            })
            .collect();
        let format = self.0.graph().snapshot().map_or(0, |s| s.dir.meta.format);
        let features = ["metrics", "jemalloc", "cpu-profile"].map(String::from).to_vec();
        Response::ok(GetSettingsResponse {
            flags,
            build: BuildInfo {
                version: crate::VERSION.into(),
                commit: crate::COMMIT.into(),
                format,
                features,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
    }

    async fn checkpoint(
        &self,
        _: RequestContext,
        _: ServiceRequest<'_, CheckpointRequest>,
    ) -> ServiceResult<CheckpointResponse> {
        self.writable()?;
        let j = self.0.start_checkpoint().map_err(ConnectError::failed_precondition)?;
        Response::ok(CheckpointResponse { job: job_pb(&j).into(), ..Default::default() })
    }

    async fn verify_shards(
        &self,
        _: RequestContext,
        _: ServiceRequest<'_, VerifyShardsRequest>,
    ) -> ServiceResult<VerifyShardsResponse> {
        let j = self.0.start_verify().map_err(ConnectError::failed_precondition)?;
        Response::ok(VerifyShardsResponse { job: job_pb(&j).into(), ..Default::default() })
    }

    async fn set_trace_ratio(
        &self,
        _: RequestContext,
        req: ServiceRequest<'_, SetTraceRatioRequest>,
    ) -> ServiceResult<SetTraceRatioResponse> {
        if !(0.0..=1.0).contains(&req.ratio) {
            return Err(bad("ratio must be 0 to 1"));
        }
        marco_core::obs::set_trace_ratio(req.ratio);
        Response::ok(SetTraceRatioResponse { ratio: marco_core::obs::trace_ratio(), ..Default::default() })
    }

    async fn set_actor_status(
        &self,
        _: RequestContext,
        req: ServiceRequest<'_, SetActorStatusRequest>,
    ) -> ServiceResult<SetActorStatusResponse> {
        let st = status_pb(req.status)?;
        self.writable()?;
        let did = req.did.trim().to_string();
        let removed = self.0.set_status(&did, st).await.map_err(bad)?;
        Response::ok(SetActorStatusResponse { did, status: Status::name(st).into(), removed, ..Default::default() })
    }
}
