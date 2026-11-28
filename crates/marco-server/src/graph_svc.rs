//! marco.v1.Graph over the served graph.

use std::sync::Arc;

use connectrpc::{ConnectError, InboundStream, RequestContext, Response, ServiceRequest, ServiceResult};
use futures::StreamExt;
use marco_core::graph::VIA_MAX_LISTS;
use marco_core::read::{Hold, ReadCx, Source, StorageError, R};
use marco_core::relation::parse_uri;
use marco_core::relationship::{Derive, Relationship as Rship, RelationshipSet};
use marco_core::{
    AmongPlan, Dir, Graph, ListRef, PairQuery, RelId, SetOp, ViaCuts, ViaHit, ViaLists, ViaQuery, COUNT_EXACT,
};
use marco_proto::connect::marco::v1 as svc;
use marco_proto::v1::__buffa::oneof::{via, write_op, write_result};
use marco_proto::v1::*;

use crate::app::{App, Op, Outcome, MAX_BATCH};
use crate::pool::PoolError;

/// The Graph service; `gate` holds reads until marco is ready (the API
/// listener; the console's copy on the admin listener answers regardless).
pub struct GraphApi(pub Arc<App>, pub bool);

pub fn bad(m: impl Into<String>) -> ConnectError {
    ConnectError::invalid_argument(m)
}

pub fn rel_id(g: &Graph, name: &str) -> Result<RelId, ConnectError> {
    if name.is_empty() {
        return Err(bad("rel is required"));
    }
    g.relations().by_name(name).ok_or_else(|| bad(format!("no relation {name:?}")))
}

/// The named relations; none names every one a caller configured.
pub fn rel_ids(g: &Graph, names: &[String]) -> Result<Vec<RelId>, ConnectError> {
    if names.is_empty() {
        return Ok(g.relations().user());
    }
    names.iter().map(|n| rel_id(g, n)).collect()
}

pub fn dir_of(d: &buffa::EnumValue<marco_proto::v1::Dir>) -> Result<Dir, ConnectError> {
    match d.as_known() {
        Some(marco_proto::v1::Dir::DIR_OUT) => Ok(Dir::Out),
        Some(marco_proto::v1::Dir::DIR_IN) => Ok(Dir::In),
        _ => Err(bad("dir must be DIR_OUT or DIR_IN")),
    }
}

pub fn dir_pb(d: Dir) -> buffa::EnumValue<marco_proto::v1::Dir> {
    match d {
        Dir::Out => marco_proto::v1::Dir::DIR_OUT.into(),
        Dir::In => marco_proto::v1::Dir::DIR_IN.into(),
    }
}

/// Pairs a BlockPairs call takes: a thread's hydration is under 100, so
/// this only stops a runaway caller.
pub const MAX_BLOCK_PAIRS: usize = 1000;

/// Actors an EdgesAmong request takes by default (`--edges-among-max-actors`).
pub const EDGES_AMONG_MAX: usize = 10_000;

/// EdgesAmong requests of more actors than this run on the miss pool from
/// the start: a set of 500 popular accounts takes ~25 ms, too long to hold
/// a worker.
pub const EDGES_AMONG_ON_WORKER: usize = 100;

/// How long an EdgesAmong request may run, at most: its client's deadline
/// if sooner.
pub const EDGES_AMONG_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

/// Vias a Relationships call takes: a profile's viewer state needs three.
pub const MAX_VIAS: usize = 10;

/// A via's `members` and `kind`, resolved: the list relation, and the
/// relation whose live records say which lists count.
fn members_kind(
    g: &Graph,
    members: &str,
    kind: &str,
    e: &impl Fn(String) -> ConnectError,
) -> Result<(RelId, Option<RelId>), ConnectError> {
    let rels = g.relations();
    let rel = |n: &str| rels.by_name(n).ok_or_else(|| e(format!("no relation {n:?}")));
    if members.is_empty() {
        return Err(e("members is required".into()));
    }
    let m = rel(members)?;
    if !rels.is_list(m) {
        return Err(e(format!("members {members:?} isn't a list relation")));
    }
    let k = match kind {
        "" => None,
        k => {
            let r = rel(k)?;
            if !rels.is_uri_subject(r) {
                return Err(e(format!("kind {k:?}'s subjects aren't records")));
            }
            Some(r)
        }
    };
    Ok((m, k))
}

/// A relation whose subjects are records, which a via reads a subscriber's
/// lists from.
fn lists_rel(g: &Graph, name: &str, e: &impl Fn(String) -> ConnectError) -> Result<RelId, ConnectError> {
    if name.is_empty() {
        return Err(e("rel is required".into()));
    }
    let r = g.relations().by_name(name).ok_or_else(|| e(format!("no relation {name:?}")))?;
    if !g.relations().is_uri_subject(r) {
        return Err(e(format!("rel {name:?}'s subjects aren't records: give it a subject_collection")));
    }
    Ok(r)
}

/// A Relationships via, its names resolved and checked. The caller's lists
/// stay URIs until a read resolves them against the graph it reads.
#[derive(Clone, Debug)]
pub struct ViaArgs {
    from: ViaFrom,
    members: RelId,
    kind: Option<RelId>,
    dir: Dir,
}

#[derive(Clone, Debug)]
enum ViaFrom {
    Rel(RelId),
    Lists(Vec<String>),
}

impl ViaArgs {
    /// Via `i` of a request.
    pub fn of(g: &Graph, v: &Via, i: usize) -> Result<ViaArgs, ConnectError> {
        let e = |m: String| bad(format!("via {i}: {m}"));
        let (members, kind) = members_kind(g, &v.members, &v.kind, &e)?;
        let dir = match v.dir.as_known() {
            Some(marco_proto::v1::Dir::DIR_OUT) => Dir::Out,
            Some(marco_proto::v1::Dir::DIR_IN) => Dir::In,
            _ => return Err(e("dir must be DIR_OUT or DIR_IN".into())),
        };
        let from = match &v.source {
            None => return Err(e("rel or lists is required".into())),
            Some(via::Source::Rel(r)) => ViaFrom::Rel(lists_rel(g, r, &e)?),
            Some(via::Source::Lists(l)) => {
                if dir != Dir::Out {
                    return Err(e("lists are the actor's own: DIR_OUT only".into()));
                }
                if l.uris.len() > VIA_MAX_LISTS {
                    return Err(e(format!("at most {VIA_MAX_LISTS} lists")));
                }
                if let Some(u) = l.uris.iter().find(|u| parse_uri(u).is_none()) {
                    return Err(e(format!("{u:?} isn't a record URI")));
                }
                ViaFrom::Lists(l.uris.clone())
            }
        };
        Ok(ViaArgs { from, members, kind, dir })
    }

    /// The via as the graph reads it: a caller's lists marco doesn't know
    /// hold nobody.
    pub fn query(&self, g: &Graph) -> ViaQuery {
        let lists = match &self.from {
            ViaFrom::Rel(r) => ViaLists::Rel(*r),
            ViaFrom::Lists(l) => {
                let mut ids: Vec<u32> = l.iter().filter_map(|u| g.id_of(u)).collect();
                ids.sort_unstable();
                ids.dedup();
                ViaLists::Given(ids)
            }
        };
        ViaQuery { lists, members: self.members, kind: self.kind, dir: self.dir }
    }

    /// The relation a drop by this via names (Filter's `Dropped.rel`): its
    /// rel, or for a caller's lists the list relation.
    pub fn rel(&self) -> RelId {
        match self.from {
            ViaFrom::Rel(r) => r,
            ViaFrom::Lists(_) => self.members,
        }
    }

    pub fn dir(&self) -> Dir {
        self.dir
    }
}

/// A request's vias.
pub fn vias_of(g: &Graph, vias: &[Via]) -> Result<Vec<ViaArgs>, ConnectError> {
    if vias.len() > MAX_VIAS {
        return Err(bad(format!("at most {MAX_VIAS} vias")));
    }
    vias.iter().enumerate().map(|(i, v)| ViaArgs::of(g, v, i)).collect()
}

/// A BlockPairs via, its names resolved and checked.
pub fn pair_via_of(g: &Graph, v: &PairVia) -> Result<PairQuery, ConnectError> {
    let e = |m: String| bad(format!("via: {m}"));
    let rel = lists_rel(g, &v.rel, &e)?;
    let (members, kind) = members_kind(g, &v.members, &v.kind, &e)?;
    Ok(PairQuery { rel, members, kind })
}

/// A list's URI, or empty.
fn list_uri(g: &Graph, l: Option<u32>) -> String {
    l.and_then(|l| g.did_of(l)).unwrap_or_default()
}

fn limit(l: u32, def: u32, max: u32) -> usize {
    (if l == 0 { def } else { l.min(max) }) as usize
}

fn ops_of(ops: &[WriteOp]) -> Result<Vec<Op>, ConnectError> {
    if ops.len() > MAX_BATCH {
        return Err(bad(format!("at most {MAX_BATCH} ops a message")));
    }
    ops.iter()
        .enumerate()
        .map(|(i, o)| match &o.op {
            Some(write_op::Op::Add(a)) => Ok(Op::Add {
                uri: a.uri.clone(),
                subject: a.subject.clone(),
                list: (!a.list.is_empty()).then(|| a.list.clone()),
                time_us: a.time_us,
            }),
            Some(write_op::Op::Delete(d)) => Ok(Op::Delete { uri: d.uri.clone(), time_us: d.time_us }),
            None => Err(bad(format!("op {i} is neither add nor delete"))),
        })
        .collect()
}

fn result_pb(o: Outcome) -> WriteResult {
    let outcome = match o {
        Outcome::Added => write_result::Outcome::Added(true),
        Outcome::Exists => write_result::Outcome::Exists(true),
        Outcome::Deleted => write_result::Outcome::Deleted(true),
        Outcome::Removed(s) => write_result::Outcome::Removed(s),
        Outcome::NotFound => write_result::Outcome::NotFound(true),
        Outcome::Error(e) => write_result::Outcome::Error(e),
    };
    WriteResult { outcome: Some(outcome), ..Default::default() }
}

impl GraphApi {
    fn writable(&self) -> Result<(), ConnectError> {
        if self.0.cfg.read_only {
            return Err(ConnectError::failed_precondition("this marco is --read-only"));
        }
        Ok(())
    }

    async fn write_ops(&self, ops: &[WriteOp], cursor: &str) -> Result<Vec<WriteResult>, ConnectError> {
        self.writable()?;
        let ops = ops_of(ops)?;
        App::validate(&self.0.graph(), &ops).map_err(bad)?;
        let out = self.0.apply(&ops, cursor).await.map_err(|e| ConnectError::unavailable(format!("{e:#}")))?;
        Ok(out.into_iter().map(result_pb).collect())
    }

    fn readable(&self) -> Result<(), ConnectError> {
        if self.1 {
            self.0.read_gate().map_err(ConnectError::unavailable)?;
        }
        Ok(())
    }
}

/// A read's error as an RPC's: a page that failed its sum is data loss, a
/// request too big for the cache is resource_exhausted, and one the disk
/// couldn't give is unavailable.
pub fn storage_error(e: StorageError) -> ConnectError {
    match e {
        StorageError::Checksum(_) => ConnectError::data_loss(e.to_string()),
        StorageError::TooBig { .. } => ConnectError::resource_exhausted(e.to_string()),
        _ => ConnectError::unavailable(e.to_string()),
    }
}

/// How long a request that missed waits for a miss pool thread, at most.
pub const BUSY_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// Runs a request's read, response building included, as passes of `f`
/// over the published copy of the graph. The first pass runs on this
/// worker and never waits. A request that misses moves whole to the miss
/// pool, where its passes fetch what they missed in between; each pass
/// loads the published copy afresh, so no request holds a copy while it
/// waits on disk and the writer's swap never waits on one. `rpc` labels
/// the request's pages and rounds histograms. A pass may need at most
/// [`marco_core::read::Engine::page_limit`] pages at once.
pub async fn run<T: Send + 'static>(
    app: &Arc<App>,
    ctx: &RequestContext,
    rpc: &'static str,
    f: impl Fn(&Graph, &ReadCx<'_>) -> R<T> + Send + 'static,
) -> Result<T, ConnectError> {
    run_with(app, ctx, rpc, false, f).await
}

/// [`run`] for the console's reads, which scan whole lists: a pass may need
/// half the cache at once, as a blocking job's may.
pub async fn run_admin<T: Send + 'static>(
    app: &Arc<App>,
    ctx: &RequestContext,
    rpc: &'static str,
    f: impl Fn(&Graph, &ReadCx<'_>) -> R<T> + Send + 'static,
) -> Result<T, ConnectError> {
    run_with(app, ctx, rpc, true, f).await
}

async fn run_with<T: Send + 'static>(
    app: &Arc<App>,
    ctx: &RequestContext,
    rpc: &'static str,
    job: bool,
    f: impl Fn(&Graph, &ReadCx<'_>) -> R<T> + Send + 'static,
) -> Result<T, ConnectError> {
    let first = {
        let g = app.graph();
        let source = if job { Source::Job } else { Source::Request };
        app.engine.pass(&mut Hold::none().source(source), |cx| f(&g, cx))
    };
    match first {
        Ok(t) => {
            observe(rpc, 0, 0);
            Ok(t)
        }
        Err(m) => on_pool(app, ctx, rpc, job, Some(m), f).await,
    }
}

/// [`run`] for a read too long for a worker's pass, such as a big
/// EdgesAmong: every pass runs on the miss pool.
pub async fn run_on_pool<T: Send + 'static>(
    app: &Arc<App>,
    ctx: &RequestContext,
    rpc: &'static str,
    f: impl Fn(&Graph, &ReadCx<'_>) -> R<T> + Send + 'static,
) -> Result<T, ConnectError> {
    on_pool(app, ctx, rpc, false, None, f).await
}

async fn on_pool<T: Send + 'static>(
    app: &Arc<App>,
    ctx: &RequestContext,
    rpc: &'static str,
    job: bool,
    first: Option<marco_core::read::Misses>,
    f: impl Fn(&Graph, &ReadCx<'_>) -> R<T> + Send + 'static,
) -> Result<T, ConnectError> {
    crate::metrics::set_path("pool");
    let limit = if job { app.engine.job_limit() } else { app.engine.page_limit() };
    if let Some(m) = &first {
        if m.needed() > limit {
            return Err(storage_error(StorageError::TooBig { pages: m.needed(), limit }));
        }
    }
    let wait = ctx.time_remaining().map_or(BUSY_WAIT, |r| r.min(BUSY_WAIT));
    let a = app.clone();
    let r = app
        .miss_pool
        .run(wait, move || {
            let mut hold = if job { Hold::job(&a.engine) } else { Hold::request(&a.engine) };
            let r = a.engine.resume_blocking(&mut hold, first, |cx| {
                let g = a.graph();
                f(&g, cx)
            });
            (r, hold.rounds(), hold.fetched())
        })
        .await;
    match r {
        Err(PoolError::Busy) => Err(ConnectError::resource_exhausted("storage busy")),
        Err(PoolError::Panicked) => Err(ConnectError::internal("the read panicked")),
        Ok((r, rounds, pages)) => {
            observe(rpc, rounds, pages);
            r.map_err(storage_error)
        }
    }
}

fn observe(rpc: &str, rounds: u32, pages: u64) {
    crate::metrics::REQUEST_ROUNDS.with_label_values(&[rpc]).observe(rounds as f64);
    crate::metrics::REQUEST_PAGES.with_label_values(&[rpc]).observe(pages as f64);
}

/// Has, its relation resolved: what the handler and the tests run.
pub fn has_(g: &Graph, cx: &ReadCx<'_>, rel: RelId, req: &HasRequest) -> R<HasResponse> {
    let hidden = g.hidden(&req.from) || g.hidden(&req.to);
    let uri = if hidden { None } else { g.has(cx, rel, &req.from, &req.to)? };
    Ok(HasResponse { uri: uri.unwrap_or_default(), ..Default::default() })
}

/// A relation as a direct relationship of its own name: what a request
/// naming relations by id gets.
pub fn direct(g: &Graph, rels: &[RelId]) -> Vec<Rship> {
    rels.iter()
        .map(|&r| Rship {
            name: g.relations().get(r).name.clone(),
            derive: Derive::Direct(r),
            blocks: false,
            default: false,
            private: false,
        })
        .collect()
}

/// Relationships over relations alone, its names resolved, and the via
/// reads the cap cut. `cap` is the most of a subscriber's records a via
/// reads.
pub fn relationships_(
    g: &Graph,
    cx: &ReadCx<'_>,
    rels: &[RelId],
    vias: &[ViaArgs],
    cap: usize,
    req: &RelationshipsRequest,
) -> R<(RelationshipsResponse, ViaCuts)> {
    relationships_set(g, cx, &direct(g, rels), vias, cap, req)
}

/// Relationships, its relationships resolved: a direct one's records come
/// from the direct read, a list one's lists from the vias, read with the
/// request's own in one pass.
pub fn relationships_set(
    g: &Graph,
    cx: &ReadCx<'_>,
    entries: &[Rship],
    vias: &[ViaArgs],
    cap: usize,
    req: &RelationshipsRequest,
) -> R<(RelationshipsResponse, ViaCuts)> {
    let others: Vec<&str> = req.others.iter().map(String::as_str).collect();
    let direct: Vec<RelId> = entries
        .iter()
        .filter_map(|e| match e.derive {
            Derive::Direct(r) => Some(r),
            Derive::List { .. } => None,
        })
        .collect();
    let rows = g.relationships(cx, &req.actor, &others, &direct);
    let actor_hidden = g.hidden(&req.actor);
    // each list relationship's out via, and its in via unless it's private,
    // then the request's
    let mut qs: Vec<ViaQuery> = Vec::new();
    let mut at: Vec<(Option<usize>, Option<usize>)> = Vec::with_capacity(entries.len());
    for e in entries {
        let mut one = |d: Dir| {
            e.derive.via(d).map(|q| {
                qs.push(q);
                qs.len() - 1
            })
        };
        let out = one(Dir::Out);
        let inn = if e.private { None } else { one(Dir::In) };
        at.push((out, inn));
    }
    let own = qs.len();
    qs.extend(vias.iter().map(|v| v.query(g)));
    // a hidden actor or other answers no via, as it has no edges
    let via = if qs.is_empty() {
        Ok((Vec::new(), ViaCuts::default()))
    } else {
        let actor = g.id_of(&req.actor).filter(|_| !actor_hidden);
        let ids: Vec<Option<u32>> = others.iter().map(|o| g.id_of(o).filter(|_| !g.hidden(o))).collect();
        g.relationships_via(cx, actor, &ids, &qs, cap)
    };
    // both asked before either's miss returns, so one pass names both's pages
    let (rows, (via, cuts)) = (rows?, via?);
    let relationships = req
        .others
        .iter()
        .zip(rows)
        .enumerate()
        .map(|(j, (o, per))| {
            let hide = actor_hidden || g.hidden(o);
            let hits: &[ViaHit] = via.get(j).map_or(&[], Vec::as_slice);
            let hit = |i: Option<usize>| i.and_then(|i| hits.get(i)).copied().unwrap_or_default();
            let mut per = per.into_iter();
            let rels = entries
                .iter()
                .zip(&at)
                .map(|(e, &(oi, ii))| match e.derive {
                    Derive::Direct(_) => {
                        let x = per.next().unwrap_or_default();
                        RelationshipEdge {
                            rel: e.name.clone(),
                            out_uri: if hide { String::new() } else { x.out.unwrap_or_default() },
                            in_uri: if hide || e.private { String::new() } else { x.inn.unwrap_or_default() },
                            ..Default::default()
                        }
                    }
                    Derive::List { .. } => {
                        let (x, y) = (hit(oi), hit(ii));
                        RelationshipEdge {
                            rel: e.name.clone(),
                            out_uri: list_uri(g, x.list),
                            in_uri: list_uri(g, y.list),
                            out_truncated: x.truncated,
                            in_truncated: y.truncated,
                            ..Default::default()
                        }
                    }
                })
                .collect();
            Relationship {
                other: o.clone(),
                rels,
                via: hits
                    .get(own..)
                    .unwrap_or_default()
                    .iter()
                    .map(|h| ViaAnswer { list: list_uri(g, h.list), truncated: h.truncated, ..Default::default() })
                    .collect(),
                ..Default::default()
            }
        })
        .collect();
    Ok((RelationshipsResponse { relationships, ..Default::default() }, cuts))
}

/// BlockPairs over relations and at most one via, its names resolved, and
/// the via reads the cap cut.
pub fn block_pairs_(
    g: &Graph,
    cx: &ReadCx<'_>,
    rels: &[RelId],
    via: Option<&PairQuery>,
    cap: usize,
    req: &BlockPairsRequest,
) -> R<(BlockPairsResponse, ViaCuts)> {
    block_pairs_set(g, cx, rels, via.map(std::slice::from_ref).unwrap_or_default(), cap, req)
}

/// BlockPairs: the direct relations' records, and each list check's lists
/// both ways, the first to match answering. Without lists or `uris` the
/// answer is the direct blocks alone.
pub fn block_pairs_set(
    g: &Graph,
    cx: &ReadCx<'_>,
    rels: &[RelId],
    lists: &[PairQuery],
    cap: usize,
    req: &BlockPairsRequest,
) -> R<(BlockPairsResponse, ViaCuts)> {
    let pairs: Vec<(&str, &str)> = req.pairs.iter().map(|p| (p.a.as_str(), p.b.as_str())).collect();
    let res = g.block_pairs(cx, &pairs, rels);
    let hidden = |(a, b): (&str, &str)| g.any_status() && (g.hidden(a) || g.hidden(b));
    if lists.is_empty() && !req.uris {
        let mut res = res?;
        for (r, &p) in res.iter_mut().zip(&pairs) {
            if (r.0 || r.1) && hidden(p) {
                *r = (false, false);
            }
        }
        let resp = BlockPairsResponse {
            a_blocks_b: res.iter().map(|r| r.0).collect(),
            b_blocks_a: res.iter().map(|r| r.1).collect(),
            blocked: res.iter().map(|r| r.0 || r.1).collect(),
            ..Default::default()
        };
        return Ok((resp, ViaCuts::default()));
    }
    // an unknown or hidden DID blocks nobody and is blocked by nobody
    let ids: Vec<Option<(u32, u32)>> =
        pairs.iter().map(|&p| g.id_of(p.0).zip(g.id_of(p.1)).filter(|_| !hidden(p))).collect();
    // every check asked before any miss returns, so one pass names all their pages
    let found: Vec<R<PairLists>> = lists.iter().map(|q| g.block_pairs_via(cx, &ids, q, cap)).collect();
    let res = res?;
    let mut hits = vec![(ViaHit::default(), ViaHit::default()); pairs.len()];
    let mut cuts = ViaCuts::default();
    for f in found {
        let (per, c) = f?;
        cuts.cap += c.cap;
        cuts.deletes += c.deletes;
        for (h, (x, y)) in hits.iter_mut().zip(per) {
            merge(&mut h.0, x);
            merge(&mut h.1, y);
        }
    }
    let direct: Vec<(bool, bool)> =
        res.iter().zip(&ids).map(|(&r, id)| if id.is_some() { r } else { (false, false) }).collect();
    let mut resp = BlockPairsResponse::default();
    if req.uris {
        let mut pend = Ok(());
        for (&(x, y), id) in direct.iter().zip(&ids) {
            let Some((a, b)) = *id else {
                resp.a_blocks_b_uri.push(String::new());
                resp.b_blocks_a_uri.push(String::new());
                continue;
            };
            // a URI costs a lookup only on a hit
            let uri = |hit: bool, from: u32, to: u32, pend: &mut R<()>| {
                if hit {
                    item(pend, g.block_uri(cx, rels, from, to)).unwrap_or_default()
                } else {
                    String::new()
                }
            };
            resp.a_blocks_b_uri.push(uri(x, a, b, &mut pend));
            resp.b_blocks_a_uri.push(uri(y, b, a, &mut pend));
        }
        pend?;
    }
    for (&(x, y), &(lx, ly)) in direct.iter().zip(&hits) {
        resp.a_blocks_b.push(x);
        resp.b_blocks_a.push(y);
        resp.blocked.push(x || y || lx.list.is_some() || ly.list.is_some());
        if !lists.is_empty() {
            resp.a_blocks_b_list.push(list_uri(g, lx.list));
            resp.b_blocks_a_list.push(list_uri(g, ly.list));
            resp.a_blocks_b_truncated.push(lx.truncated);
            resp.b_blocks_a_truncated.push(ly.truncated);
        }
    }
    Ok((resp, cuts))
}

/// A pair check's lists each way per pair, and the reads it cut.
type PairLists = (Vec<(ViaHit, ViaHit)>, ViaCuts);

/// One more check's answer into a pair side's: the first list found
/// stands, and an answer without one is truncated if any check's was.
fn merge(into: &mut ViaHit, x: ViaHit) {
    if into.list.is_none() {
        into.list = x.list;
        into.truncated = x.list.is_none() && (into.truncated || x.truncated);
    }
}

pub fn list_out_(g: &Graph, cx: &ReadCx<'_>, rel: RelId, req: &ListOutRequest) -> R<ListOutResponse> {
    if g.hidden(&req.actor) {
        return Ok(ListOutResponse::default());
    }
    let cursor = (!req.cursor.is_empty()).then_some(req.cursor.as_str());
    let lim = limit(req.limit, 50, 1000);
    let (items, next) = g.out_list_opts(cx, rel, &req.actor, lim, cursor, req.distinct)?;
    let edges = items
        .into_iter()
        .filter(|(_, s)| !g.hidden(s))
        .map(|(uri, subject)| OutEdge { uri, subject, ..Default::default() })
        .collect();
    Ok(ListOutResponse { edges, cursor: next.unwrap_or_default(), ..Default::default() })
}

pub fn list_in_(g: &Graph, cx: &ReadCx<'_>, rel: RelId, req: &ListInRequest) -> R<ListInResponse> {
    if g.hidden(&req.subject) {
        return Ok(ListInResponse::default());
    }
    let cursor = (!req.cursor.is_empty()).then_some(req.cursor.as_str());
    let lim = limit(req.limit, 50, 1000);
    let (actors, next) = g.in_list(cx, rel, &req.subject, lim, cursor)?;
    let actors = actors.into_iter().filter(|a| !g.hidden(a)).collect();
    Ok(ListInResponse { actors, cursor: next.unwrap_or_default(), ..Default::default() })
}

/// `r`'s value, or the default with its Pending kept in `pend`: a handler
/// that reads one list per item carries on past a miss, so one pass names
/// every page the batch misses.
pub fn item<T: Default>(pend: &mut R<()>, r: R<T>) -> T {
    r.unwrap_or_else(|p| {
        *pend = Err(p);
        T::default()
    })
}

pub fn counts_(g: &Graph, cx: &ReadCx<'_>, rels: &[RelId], req: &CountsRequest) -> R<CountsResponse> {
    let mut pend = Ok(());
    let mut counts = Vec::with_capacity(req.actors.len());
    for a in &req.actors {
        let hide = g.hidden(a);
        let mut per = Vec::with_capacity(rels.len());
        for &r in rels {
            let (o, i) = if hide { (0, 0) } else { item(&mut pend, g.counts(cx, r, a)) };
            let subjects = match g.id_of(a) {
                Some(id) if o > 0 => item(&mut pend, g.out_distinct(cx, r, id)),
                _ => 0,
            };
            per.push(RelCounts {
                rel: g.relations().get(r).name.clone(),
                out_records: o,
                in_actors: i,
                out_subjects: subjects,
                ..Default::default()
            });
        }
        counts.push(ActorCounts { actor: a.clone(), rels: per, ..Default::default() });
    }
    pend?;
    Ok(CountsResponse { counts, ..Default::default() })
}

/// An Intersect request with its names resolved, but not its actors.
#[derive(Clone)]
pub struct IntersectArgs {
    pub a: (RelId, Dir, String),
    pub b: (RelId, Dir, String),
    pub op: SetOp,
    pub cursor: String,
    pub lim: usize,
    pub cap: usize,
}

impl IntersectArgs {
    pub fn of(g: &Graph, req: &IntersectRequest) -> Result<IntersectArgs, ConnectError> {
        let side = |s: &ListSide| -> Result<(RelId, Dir, String), ConnectError> {
            Ok((rel_id(g, &s.rel)?, dir_of(&s.dir)?, s.actor.clone()))
        };
        let op = match req.op.as_known() {
            Some(marco_proto::v1::SetOp::SET_OP_AND_NOT) => SetOp::AndNot,
            Some(_) => SetOp::And,
            None => return Err(bad("op must be SET_OP_AND or SET_OP_AND_NOT")),
        };
        // exact unless the caller caps it: the work is one pass over the side the page decodes anyway
        let cap = match (req.count, req.count_limit) {
            (false, _) => 0,
            (true, 0) => COUNT_EXACT,
            (true, l) => limit(l, 0, 10_000),
        };
        Ok(IntersectArgs {
            a: side(&req.a)?,
            b: side(&req.b)?,
            op,
            cursor: req.cursor.clone(),
            lim: limit(req.limit, 100, 10_000),
            cap,
        })
    }
}

/// Intersect; Err(None) is a cursor that isn't in the graph.
pub fn intersect_(g: &Graph, cx: &ReadCx<'_>, q: &IntersectArgs) -> R<Result<IntersectResponse, String>> {
    let side = |(rel, dir, actor): &(RelId, Dir, String)| -> Option<ListRef> {
        if g.hidden(actor) {
            return None;
        }
        g.id_of(actor).map(|actor| ListRef { rel: *rel, dir: *dir, actor })
    };
    let (a, b) = (side(&q.a), side(&q.b));
    let after = match q.cursor.as_str() {
        "" => None,
        c => match g.id_of(c) {
            Some(id) => Some(id),
            None => return Ok(Err("cursor isn't in this graph".into())),
        },
    };
    let (lim, cap, op) = (q.lim, q.cap, q.op);
    let len = |s: Option<ListRef>| match s {
        None => Ok(0),
        Some(s) => match s.dir {
            Dir::Out => g.out_count(cx, s.rel, s.actor),
            Dir::In => g.in_count(cx, s.rel, s.actor),
        },
    };
    let (ids, total) = match (a, b, op) {
        (Some(x), Some(y), _) => g.combine_counted(cx, x, y, op, lim, after, cap)?,
        // a without a missing (or hidden) b is all of a
        (Some(x), None, SetOp::AndNot) => {
            let mut v = match x.dir {
                Dir::Out => g.out_set(cx, x.rel, x.actor)?,
                Dir::In => g.in_set(cx, x.rel, x.actor)?,
            };
            let total = (cap > 0).then(|| (v.len().min(cap) as u64, v.len() > cap));
            v.retain(|&i| after.is_none_or(|a| i > a));
            v.truncate(lim);
            (v, total)
        }
        _ => (Vec::new(), (cap > 0).then_some((0, false))),
    };
    let (total, total_capped) = total.unwrap_or_default();
    let cursor = if ids.len() == lim { ids.last().and_then(|&i| g.did_of(i)) } else { None };
    let actors = ids.into_iter().filter_map(|id| g.did_of(id)).filter(|d| !g.hidden(d)).collect();
    Ok(Ok(IntersectResponse {
        actors,
        a_len: len(a)?,
        b_len: len(b)?,
        cursor: cursor.unwrap_or_default(),
        total,
        total_capped,
        ..Default::default()
    }))
}

/// A Filter request's drops and sets, their names resolved.
#[derive(Clone)]
pub struct FilterArgs {
    pub drops: Vec<(RelId, Dir)>,
    /// (rel, dir, the list's actor)
    pub sets: Vec<(RelId, Dir, String)>,
    pub vias: Vec<ViaArgs>,
}

impl FilterArgs {
    /// A rule's rel can name a relationship by list too: its via in the
    /// rule's dir, as a via rule.
    pub fn of(g: &Graph, set: &RelationshipSet, req: &FilterRequest) -> Result<FilterArgs, ConnectError> {
        let mut drops = Vec::new();
        let mut sets = Vec::new();
        let mut vias = Vec::new();
        for (i, d) in req.drop.iter().enumerate() {
            match (d.member_of.as_option(), d.via.as_option()) {
                (Some(_), Some(_)) => return Err(bad(format!("drop {i}: member_of or via, not both"))),
                (None, Some(_)) if !d.rel.is_empty() => {
                    return Err(bad(format!("drop {i}: via in place of rel and dir, not beside them")))
                }
                (None, Some(v)) => vias.push(ViaArgs::of(g, v, vias.len())?),
                (Some(m), None) => sets.push((rel_id(g, &m.rel)?, dir_of(&m.dir)?, m.actor.clone())),
                (None, None) => match set.by_name(&d.rel).map(|r| (r, r.derive)) {
                    Some((r, Derive::List { subscription, membership, filter })) => {
                        let dir = dir_of(&d.dir)?;
                        if r.private && dir == Dir::In {
                            return Err(bad(format!("drop {i}: {:?} is private: DIR_OUT only", r.name)));
                        }
                        let from = ViaFrom::Rel(subscription);
                        vias.push(ViaArgs { from, members: membership, kind: filter, dir });
                    }
                    _ => drops.push((rel_id(g, &d.rel)?, dir_of(&d.dir)?)),
                },
            }
        }
        if vias.len() > MAX_VIAS {
            return Err(bad(format!("at most {MAX_VIAS} vias")));
        }
        Ok(FilterArgs { drops, sets, vias })
    }
}

/// Filter, its rules resolved, and the via reads the cap cut. `cap` is the
/// most of a subscriber's records a via reads.
pub fn filter_(
    g: &Graph,
    cx: &ReadCx<'_>,
    q: &FilterArgs,
    cap: usize,
    req: &FilterRequest,
) -> R<(FilterResponse, ViaCuts)> {
    let drops = &q.drops;
    // an unknown or hidden list drops nobody
    let sets: Vec<ListRef> = q
        .sets
        .iter()
        .filter_map(|(rel, dir, a)| {
            g.id_of(a).filter(|_| !g.hidden(a)).map(|actor| ListRef { rel: *rel, dir: *dir, actor })
        })
        .collect();
    let viewer = g.id_of(&req.viewer);
    let ids: Vec<Option<u32>> = req.actors.iter().map(|a| g.id_of(a)).collect();
    let known: Vec<u32> = ids.iter().flatten().copied().collect();
    let kept: std::collections::HashSet<u32> = match viewer {
        Some(v) => g.filter_sets(cx, v, &known, drops, &sets)?.into_iter().collect(),
        None => g.filter_sets(cx, u32::MAX, &known, &[], &sets)?.into_iter().collect(),
    };
    // via rules ask about the actors the other rules keep; a viewer that's
    // unknown or hidden subscribes to nothing, and a hidden actor is dropped
    // for its status
    let (vias, cuts) = if q.vias.is_empty() {
        (Vec::new(), ViaCuts::default())
    } else {
        let live = viewer.filter(|_| !g.hidden(&req.viewer));
        let left: Vec<Option<u32>> =
            req.actors.iter().zip(&ids).map(|(a, id)| id.filter(|x| kept.contains(x) && !g.hidden(a))).collect();
        let qs: Vec<ViaQuery> = q.vias.iter().map(|v| v.query(g)).collect();
        g.relationships_via(cx, live, &left, &qs, cap)?
    };
    let mut actors = Vec::new();
    let mut dropped = Vec::new();
    let mut via_truncated = Vec::new();
    let mut pend = Ok(());
    for (j, (a, id)) in req.actors.iter().zip(&ids).enumerate() {
        if g.hidden(a) {
            dropped.push(Dropped { actor: a.clone(), rel: "status".into(), ..Default::default() });
            continue;
        }
        match id {
            Some(x) if !kept.contains(x) => {
                // every reason's lookup, so one pass names every dropped actor's pages
                let mut why = Vec::new();
                if let Some(v) = viewer {
                    for &(r, d) in drops {
                        let yes = match d {
                            Dir::Out => item(&mut pend, g.has_id(cx, r, v, *x)).is_some(),
                            Dir::In => item(&mut pend, g.in_contains(cx, r, v, *x)),
                        };
                        why.push((yes, (r, d)));
                    }
                }
                for s in &sets {
                    why.push((item(&mut pend, g.side_contains(cx, *s, *x)), (s.rel, s.dir)));
                }
                let hit = why.into_iter().find(|w| w.0).map(|w| w.1);
                let (r, d) = hit.unwrap_or((0, Dir::Out));
                dropped.push(Dropped {
                    actor: a.clone(),
                    rel: g.relations().get(r).name.clone(),
                    dir: dir_pb(d),
                    ..Default::default()
                });
            }
            _ => {
                let hits = vias.get(j).map_or(&[][..], Vec::as_slice);
                match hits.iter().zip(&q.vias).find(|(h, _)| h.list.is_some()) {
                    Some((h, v)) => dropped.push(Dropped {
                        actor: a.clone(),
                        rel: g.relations().get(v.rel()).name.clone(),
                        dir: dir_pb(v.dir()),
                        list: list_uri(g, h.list),
                        ..Default::default()
                    }),
                    None => {
                        if hits.iter().any(|h| h.truncated) {
                            via_truncated.push(a.clone());
                        }
                        actors.push(a.clone());
                    }
                }
            }
        }
    }
    pend?;
    Ok((FilterResponse { actors, dropped, via_truncated, ..Default::default() }, cuts))
}

/// An EdgesAmong request with its relations resolved.
#[derive(Clone)]
pub struct AmongArgs {
    pub actors: Vec<String>,
    pub rels: Vec<RelId>,
    pub limit: usize,
    pub plan: AmongPlan,
}

impl AmongArgs {
    pub fn of(g: &Graph, req: &EdgesAmongRequest, max: usize) -> Result<AmongArgs, ConnectError> {
        if req.actors.len() > max {
            return Err(bad(format!("at most {max} actors")));
        }
        let rels = if req.rels.is_empty() {
            g.relations().actor_rels()
        } else {
            req.rels.iter().map(|n| rel_id(g, n)).collect::<Result<Vec<_>, _>>()?
        };
        let rs = g.relations();
        for &r in &rels {
            let name = &rs.get(r).name;
            if rs.is_list(r) || rs.is_owner(r) {
                return Err(bad(format!("{name:?} is a list relation: its actors are lists")));
            }
            if rs.is_uri_subject(r) {
                return Err(bad(format!("{name:?}'s subjects are records, not actors")));
            }
        }
        Ok(AmongArgs {
            actors: req.actors.clone(),
            rels,
            limit: limit(req.limit, 10_000, 50_000),
            plan: AmongPlan::Auto,
        })
    }
}

/// EdgesAmong; None when `deadline` passed first.
pub fn edges_among_(
    g: &Graph,
    cx: &ReadCx<'_>,
    q: &AmongArgs,
    deadline: Option<std::time::Instant>,
) -> R<Option<EdgesAmongResponse>> {
    let mut unknown = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut ids = Vec::with_capacity(q.actors.len());
    let mut did = std::collections::HashMap::with_capacity(q.actors.len());
    let status = g.any_status();
    for a in &q.actors {
        match g.id_of(a) {
            None if seen.insert(a.as_str()) => unknown.push(a.clone()),
            None => {}
            Some(id) if !(status && g.hidden_id(id)) => {
                ids.push(id);
                did.insert(id, a.as_str());
            }
            Some(_) => {}
        }
    }
    ids.sort_unstable();
    ids.dedup();
    let Some(r) = g.edges_among(cx, &ids, &q.rels, q.limit, q.plan, deadline)? else { return Ok(None) };
    let name = |rel: RelId| g.relations().get(rel).name.clone();
    let edges = r
        .edges
        .iter()
        .map(|&(rel, a, s, k)| AmongEdge {
            from: did[&a].to_string(),
            rel: name(rel),
            to: did[&s].to_string(),
            uri: g.uri(rel, a, k).unwrap_or_default(),
            ..Default::default()
        })
        .collect();
    let counts = q
        .rels
        .iter()
        .zip(&r.counts)
        .map(|(&rel, &n)| AmongCount { rel: name(rel), edges: n, ..Default::default() })
        .collect();
    Ok(Some(EdgesAmongResponse { edges, counts, unknown, truncated: r.truncated, ..Default::default() }))
}

/// The relations a BlockPairs request names: block by default, and no
/// list relation or relation whose subjects are records.
pub fn block_pair_rels(g: &Graph, names: &[String]) -> Result<Vec<RelId>, ConnectError> {
    let rels = if names.is_empty() {
        vec![g.relations().by_name("block").ok_or_else(|| bad("no relation \"block\": name rels"))?]
    } else {
        names.iter().map(|n| rel_id(g, n)).collect::<Result<Vec<_>, _>>()?
    };
    if let Some(&r) = rels.iter().find(|&&r| g.relations().is_list(r)) {
        return Err(bad(format!("{:?} is a list relation", g.relations().get(r).name)));
    }
    if let Some(&r) = rels.iter().find(|&&r| g.relations().is_uri_subject(r)) {
        return Err(bad(format!("{:?}'s subjects are records", g.relations().get(r).name)));
    }
    Ok(rels)
}

/// The relationships a Relationships request names, or the default set.
pub fn relationship_entries(g: &Graph, set: &RelationshipSet, names: &[String]) -> Result<Vec<Rship>, ConnectError> {
    if names.is_empty() {
        return Ok(set.defaults());
    }
    set.named(g.relations(), names).map_err(bad)
}

/// What a BlockPairs request checks: its direct relations, and its
/// relationships by list as pair checks. Empty names are the configured
/// relationships that block.
pub fn block_pair_set(
    g: &Graph,
    set: &RelationshipSet,
    names: &[String],
) -> Result<(Vec<RelId>, Vec<PairQuery>), ConnectError> {
    let entries = if names.is_empty() {
        let b = set.blocking();
        if b.is_empty() {
            return Err(bad("no relationship blocks (--relationships): name rels"));
        }
        b
    } else {
        set.named(g.relations(), names).map_err(bad)?
    };
    let mut rels = Vec::new();
    let mut lists = Vec::new();
    for e in &entries {
        if e.private {
            return Err(bad(format!("{:?} is private: either side of a pair would see it", e.name)));
        }
        match e.derive {
            Derive::Direct(r) => rels.push(r),
            d => lists.extend(d.pair()),
        }
    }
    if let Some(&r) = rels.iter().find(|&&r| g.relations().is_list(r)) {
        return Err(bad(format!("{:?} is a list relation", g.relations().get(r).name)));
    }
    if let Some(&r) = rels.iter().find(|&&r| g.relations().is_uri_subject(r)) {
        return Err(bad(format!("{:?}'s subjects are records", g.relations().get(r).name)));
    }
    Ok((rels, lists))
}

#[allow(refining_impl_trait)]
impl svc::Graph for GraphApi {
    async fn has(&self, ctx: RequestContext, req: ServiceRequest<'_, HasRequest>) -> ServiceResult<HasResponse> {
        self.readable()?;
        let req = req.to_owned_message();
        let rel = rel_id(&self.0.graph(), &req.rel)?;
        Response::ok(run(&self.0, &ctx, "Graph/Has", move |g, cx| has_(g, cx, rel, &req)).await?)
    }

    async fn relationships(
        &self,
        ctx: RequestContext,
        req: ServiceRequest<'_, RelationshipsRequest>,
    ) -> ServiceResult<RelationshipsResponse> {
        self.readable()?;
        let req = req.to_owned_message();
        if req.others.len() > 100 {
            return Err(bad("at most 100 others"));
        }
        let (entries, vias) = {
            let g = self.0.graph();
            (relationship_entries(&g, &self.0.relationships, &req.rels)?, vias_of(&g, &req.via)?)
        };
        let cap = self.0.cfg.via_max_records;
        let (resp, cuts) = run(&self.0, &ctx, "Graph/Relationships", move |g, cx| {
            relationships_set(g, cx, &entries, &vias, cap, &req)
        })
        .await?;
        crate::metrics::via_truncated("Graph/Relationships", cuts);
        Response::ok(resp)
    }

    async fn block_pairs(
        &self,
        ctx: RequestContext,
        req: ServiceRequest<'_, BlockPairsRequest>,
    ) -> ServiceResult<BlockPairsResponse> {
        self.readable()?;
        let req = req.to_owned_message();
        if req.pairs.len() > MAX_BLOCK_PAIRS {
            return Err(bad(format!("at most {MAX_BLOCK_PAIRS} pairs")));
        }
        let (rels, lists) = {
            let g = self.0.graph();
            let (rels, mut lists) = block_pair_set(&g, &self.0.relationships, &req.rels)?;
            if let Some(v) = req.via.as_option().map(|v| pair_via_of(&g, v)).transpose()? {
                // a via the relationships already check would read every side twice
                if !lists.contains(&v) {
                    lists.push(v);
                }
            }
            (rels, lists)
        };
        let cap = self.0.cfg.via_max_records;
        let (resp, cuts) =
            run(&self.0, &ctx, "Graph/BlockPairs", move |g, cx| block_pairs_set(g, cx, &rels, &lists, cap, &req))
                .await?;
        crate::metrics::via_truncated("Graph/BlockPairs", cuts);
        Response::ok(resp)
    }

    async fn list_out(
        &self,
        ctx: RequestContext,
        req: ServiceRequest<'_, ListOutRequest>,
    ) -> ServiceResult<ListOutResponse> {
        self.readable()?;
        let req = req.to_owned_message();
        let rel = rel_id(&self.0.graph(), &req.rel)?;
        Response::ok(run(&self.0, &ctx, "Graph/ListOut", move |g, cx| list_out_(g, cx, rel, &req)).await?)
    }

    async fn list_in(
        &self,
        ctx: RequestContext,
        req: ServiceRequest<'_, ListInRequest>,
    ) -> ServiceResult<ListInResponse> {
        self.readable()?;
        let req = req.to_owned_message();
        let rel = rel_id(&self.0.graph(), &req.rel)?;
        Response::ok(run(&self.0, &ctx, "Graph/ListIn", move |g, cx| list_in_(g, cx, rel, &req)).await?)
    }

    async fn counts(
        &self,
        ctx: RequestContext,
        req: ServiceRequest<'_, CountsRequest>,
    ) -> ServiceResult<CountsResponse> {
        self.readable()?;
        let req = req.to_owned_message();
        if req.actors.len() > 100 {
            return Err(bad("at most 100 actors"));
        }
        let rels = rel_ids(&self.0.graph(), &req.rels)?;
        Response::ok(run(&self.0, &ctx, "Graph/Counts", move |g, cx| counts_(g, cx, &rels, &req)).await?)
    }

    async fn intersect(
        &self,
        ctx: RequestContext,
        req: ServiceRequest<'_, IntersectRequest>,
    ) -> ServiceResult<IntersectResponse> {
        self.readable()?;
        let req = req.to_owned_message();
        let q = IntersectArgs::of(&self.0.graph(), &req)?;
        let resp = run(&self.0, &ctx, "Graph/Intersect", move |g, cx| intersect_(g, cx, &q)).await?;
        Response::ok(resp.map_err(bad)?)
    }

    async fn filter(
        &self,
        ctx: RequestContext,
        req: ServiceRequest<'_, FilterRequest>,
    ) -> ServiceResult<FilterResponse> {
        self.readable()?;
        let req = req.to_owned_message();
        if req.actors.len() > 1000 {
            return Err(bad("at most 1000 actors"));
        }
        let q = FilterArgs::of(&self.0.graph(), &self.0.relationships, &req)?;
        let cap = self.0.cfg.via_max_records;
        let (resp, cuts) = run(&self.0, &ctx, "Graph/Filter", move |g, cx| filter_(g, cx, &q, cap, &req)).await?;
        crate::metrics::via_truncated("Graph/Filter", cuts);
        Response::ok(resp)
    }

    async fn edges_among(
        &self,
        ctx: RequestContext,
        req: ServiceRequest<'_, EdgesAmongRequest>,
    ) -> ServiceResult<EdgesAmongResponse> {
        self.readable()?;
        let req = req.to_owned_message();
        let q = AmongArgs::of(&self.0.graph(), &req, self.0.cfg.edges_among_max)?;
        let budget = ctx.time_remaining().map_or(EDGES_AMONG_DEADLINE, |r| r.min(EDGES_AMONG_DEADLINE));
        let deadline = Some(std::time::Instant::now() + budget);
        let f = move |g: &Graph, cx: &ReadCx<'_>| edges_among_(g, cx, &q, deadline);
        let resp = if req.actors.len() > EDGES_AMONG_ON_WORKER {
            run_on_pool(&self.0, &ctx, "Graph/EdgesAmong", f).await?
        } else {
            run(&self.0, &ctx, "Graph/EdgesAmong", f).await?
        };
        resp.map_or_else(|| Err(ConnectError::deadline_exceeded("EdgesAmong ran past its deadline")), Response::ok)
    }

    async fn write(&self, _: RequestContext, mut reqs: InboundStream<WriteRequest>) -> ServiceResult<WriteResponse> {
        self.writable()?;
        let mut results = Vec::new();
        let mut cursor = String::new();
        while let Some(m) = reqs.next().await {
            let m = m?.to_owned_message();
            results.extend(self.write_ops(&m.ops, &m.cursor).await?);
            if !m.cursor.is_empty() {
                cursor = m.cursor;
            }
        }
        Response::ok(WriteResponse { results, cursor, ..Default::default() })
    }

    async fn write_batch(
        &self,
        _: RequestContext,
        req: ServiceRequest<'_, WriteBatchRequest>,
    ) -> ServiceResult<WriteBatchResponse> {
        let req = req.to_owned_message();
        let results = self.write_ops(&req.ops, &req.cursor).await?;
        Response::ok(WriteBatchResponse { results, cursor: req.cursor, ..Default::default() })
    }
}
