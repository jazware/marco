//! The scan plan: each table's token ring cut into ranges that one replica
//! shard owns whole, about `ranges_per_shard` per shard.
//!
//! Tablet tables name each tablet's replicas and their shards in
//! `system.tablets`. Vnode tables give a replica per vnode from the ring;
//! inside a node a token's shard comes from Scylla's sharding function,
//! which cuts the ring into 2^msb_ignore × shards stripes, so a vnode is cut
//! at stripe edges. Either way each replica shard's intervals are then split
//! into groups of equal token width. A range is the unit of work, of
//! routing and of resume.

use std::collections::{BTreeMap, HashMap};

use anyhow::{bail, Context, Result};
use marco_core::Relation;
use scylla::client::session::Session;
use scylla::cluster::{ClusterState, Node};
use scylla::deserialize::value::DeserializeValue;
use scylla::frame::response::result::TableSpec;
use scylla::routing::Token;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::config::{Config, Table};
use super::scan::RecordCol;

/// A token interval (lo, hi]: Scylla's token() never returns i64::MIN, so
/// (i64::MIN, i64::MAX] is the whole ring.
pub type Piece = (i64, i64);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Range {
    pub table: usize,
    /// The replica (host id) and shard that own every piece: a routing hint.
    pub host: Option<String>,
    pub shard: Option<u32>,
    /// Ascending, disjoint.
    pub pieces: Vec<Piece>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Plan {
    pub keyspace: String,
    /// The datacenter whose replicas the ranges are routed to.
    #[serde(default)]
    pub datacenter: Option<String>,
    pub tables: Vec<Table>,
    /// Per table: "tablets" or "vnodes".
    pub modes: Vec<String>,
    /// Per table: the primary key, for the report (e.g. "((actor_did), rkey)").
    pub keys: Vec<String>,
    /// Per table: the partition key's columns, for token().
    pub partition_keys: Vec<Vec<String>>,
    pub clustering_keys: Vec<Vec<String>>,
    /// Replica shards the ranges are spread over.
    pub shards: usize,
    pub nodes: usize,
    pub ranges: Vec<Range>,
    /// Per table, the relation a record table's rows are read by
    /// ([`record_rules`]): the scan keeps edges, not records, so a resume
    /// under another rule would mix the two. Empty when no table holds
    /// records, as in a plan from before record tables.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub record_rules: Vec<Option<Relation>>,
}

/// Per table, the relation (its `when`, subject and collection) a record
/// table's rows are read by, or None for a table read from columns, whose
/// rows the build checks; empty when no table holds records. `defs` is one
/// per table.
pub fn record_rules(tables: &[Table], defs: &[Relation]) -> Vec<Option<Relation>> {
    let rules: Vec<Option<Relation>> =
        tables.iter().zip(defs).map(|(t, d)| t.record.as_ref().map(|_| d.clone())).collect();
    if rules.iter().all(Option::is_none) {
        Vec::new()
    } else {
        rules
    }
}

pub fn width(p: Piece) -> u128 {
    (p.1 as i128 - p.0 as i128) as u128
}

/// Scylla's shard for a token on a node with `n` shards (the driver's Sharder::shard_of).
pub fn shard_of(t: i64, n: u32, msb_ignore: u8) -> u32 {
    let z = ((t as u64).wrapping_add(1 << 63)) << msb_ignore;
    ((z as u128 * n as u128) >> 64) as u32
}

/// `p` cut where the shard changes: (piece, shard) in token order.
pub fn stripes(p: Piece, n: u32, msb_ignore: u8) -> Vec<(Piece, u32)> {
    let mut out = Vec::new();
    let (mut cur, hi) = p;
    let block_bits = 64 - msb_ignore as u32;
    while cur < hi {
        let first = cur + 1;
        let s = shard_of(first, n, msb_ignore);
        let z = (first as i128 + (1i128 << 63)) as u128;
        let block = z >> block_bits;
        // the first y of shard s+1 in this block, back in z units (rounded up)
        let y_next = ((s as u128 + 1) << 64).div_ceil(n as u128);
        let o_next = y_next.div_ceil(1u128 << msb_ignore);
        let z_next = (block << block_bits) + o_next;
        let boundary = z_next as i128 - (1i128 << 63);
        let end = if boundary > hi as i128 { hi } else { (boundary - 1) as i64 };
        match out.last_mut() {
            Some(((_, h), ls)) if *ls == s => *h = end,
            _ => out.push(((cur, end), s)),
        }
        cur = end;
    }
    out
}

/// Splits ascending pieces into at most `k` groups of about equal token width.
pub fn split(pieces: &[Piece], k: usize) -> Vec<Vec<Piece>> {
    let total: u128 = pieces.iter().map(|&p| width(p)).sum();
    if total == 0 || k == 0 {
        return Vec::new();
    }
    let target = total.div_ceil(k as u128).max(1);
    let mut groups: Vec<Vec<Piece>> = vec![Vec::new()];
    let mut acc = 0u128;
    for &p in pieces {
        let mut lo = p.0;
        while lo < p.1 {
            if acc == target && groups.len() < k {
                groups.push(Vec::new());
                acc = 0;
            }
            let room = if groups.len() < k { target - acc } else { u128::MAX };
            let w = width((lo, p.1));
            let take = w.min(room);
            let hi = (lo as i128 + take as i128) as i64;
            groups.last_mut().unwrap().push((lo, hi));
            acc += take;
            lo = hi;
        }
    }
    groups.retain(|g| !g.is_empty());
    groups
}

/// Owner (host id, shard) per interval; `None` shard where the node gives no sharding info.
type Owned = Vec<((uuid::Uuid, Option<u32>), Piece)>;

/// Picks the least-loaded of `replicas` (already filtered to the datacenter) for `p`.
fn pick<'a>(
    replicas: &[(&'a Arc<Node>, Option<u32>)],
    load: &mut HashMap<uuid::Uuid, u128>,
    p: Piece,
) -> Option<(&'a Arc<Node>, Option<u32>)> {
    let best = replicas.iter().min_by_key(|(n, _)| (load.get(&n.host_id).copied().unwrap_or(0), n.host_id))?;
    *load.entry(best.0.host_id).or_default() += width(p);
    Some(*best)
}

fn in_dc(n: &Node, dc: Option<&str>) -> bool {
    dc.is_none() || n.datacenter.as_deref() == dc
}

async fn tablets(session: &Session, cs: &ClusterState, ks: &str, table: &str, dc: Option<&str>) -> Result<Owned> {
    let id: (uuid::Uuid,) = session
        .query_unpaged("SELECT id FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?", (ks, table))
        .await?
        .into_rows_result()?
        .single_row()
        .with_context(|| format!("{ks}.{table}'s id"))?;
    let rows = session
        .query_unpaged("SELECT last_token, replicas FROM system.tablets WHERE table_id = ?", (id.0,))
        .await?
        .into_rows_result()?;
    let mut tabs: Vec<(i64, Vec<(uuid::Uuid, i32)>)> = rows.rows()?.collect::<Result<_, _>>()?;
    tabs.sort_by_key(|t| t.0);
    if tabs.is_empty() {
        bail!("{ks}.{table} is tablet-based but system.tablets has no tablets for it");
    }
    let mut load = HashMap::new();
    let mut out = Vec::new();
    let mut lo = i64::MIN;
    for (last, reps) in tabs {
        let p = (lo, last);
        lo = last;
        let nodes: Vec<(&Arc<Node>, Option<u32>)> = reps
            .iter()
            .filter_map(|(h, s)| cs.get_node_by_host_id(*h).map(|n| (n, Some(*s as u32))))
            .filter(|(n, _)| in_dc(n, dc))
            .collect();
        match pick(&nodes, &mut load, p) {
            Some((n, s)) => out.push(((n.host_id, s), p)),
            None => bail!(
                "tablet ending at {last} has no replica the driver knows{}",
                dc.map_or(String::new(), |d| format!(" in {d}"))
            ),
        }
    }
    if lo != i64::MAX {
        bail!("{ks}.{table}'s tablets end at {lo}, not the ring's end");
    }
    Ok(out)
}

fn vnodes(cs: &ClusterState, ks: &str, table: &str, dc: Option<&str>) -> Result<Owned> {
    let keyspace = cs.get_keyspace(ks).with_context(|| format!("keyspace {ks}"))?;
    let spec = TableSpec::owned(ks.to_string(), table.to_string());
    let locator = cs.replica_locator();
    let ring: Vec<i64> = locator.ring().iter().map(|(t, _)| t.value()).collect();
    if ring.is_empty() {
        bail!("the token ring is empty");
    }
    // (prev, t] belongs to the vnode at t; the wrap piece to the first one
    let mut ivs: Vec<(i64, Piece)> = Vec::new();
    let mut lo = i64::MIN;
    for &t in &ring {
        if t > lo {
            ivs.push((t, (lo, t)));
        }
        lo = t;
    }
    if lo < i64::MAX {
        ivs.push((ring[0], (lo, i64::MAX)));
    }
    let mut load = HashMap::new();
    let mut out = Vec::new();
    for (owner, p) in ivs {
        let reps: Vec<(&Arc<Node>, Option<u32>)> = locator
            .replicas_for_token(Token::new(owner), &keyspace.strategy, dc, &spec)
            .into_iter()
            .map(|(n, _)| (n, None))
            .collect();
        let Some((n, _)) = pick(&reps, &mut load, p) else {
            bail!("no replica for token {owner}{}", dc.map_or(String::new(), |d| format!(" in {d}")));
        };
        match n.sharder() {
            Some(sh) => {
                for (q, s) in stripes(p, sh.nr_shards.get() as u32, sh.msb_ignore) {
                    out.push(((n.host_id, Some(s)), q));
                }
            }
            None => out.push(((n.host_id, None), p)),
        }
    }
    Ok(out)
}

/// The plan for every table in `cfg`, from the cluster's metadata.
pub async fn make(session: &Session, cfg: &Config) -> Result<Plan> {
    session.refresh_metadata().await.context("refresh cluster metadata")?;
    let cs = session.get_cluster_state();
    let keyspace = cs.get_keyspace(&cfg.keyspace).with_context(|| format!("no keyspace {:?}", cfg.keyspace))?;
    let dc = cfg.datacenter.as_deref();
    let mut plan = Plan {
        keyspace: cfg.keyspace.clone(),
        datacenter: cfg.datacenter.clone(),
        tables: cfg.tables.clone(),
        modes: Vec::new(),
        keys: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
        shards: 0,
        nodes: 0,
        ranges: Vec::new(),
        record_rules: Vec::new(),
    };
    let mut shards = std::collections::BTreeSet::new();
    for (ti, t) in cfg.tables.iter().enumerate() {
        let meta = keyspace.tables.get(&t.name).with_context(|| format!("no table {}.{}", cfg.keyspace, t.name))?;
        let cols: Vec<&String> = match &t.record {
            Some(rec) => vec![&t.actor, &t.rkey, rec],
            None => [&t.actor, &t.rkey, &t.subject].into_iter().chain(&t.list).collect(),
        };
        for c in cols {
            if !meta.columns.contains_key(c) {
                let mut have: Vec<&String> = meta.columns.keys().collect();
                have.sort();
                bail!("{}.{} has no column {c:?} (it has {have:?})", cfg.keyspace, t.name);
            }
        }
        if let Some(rec) = &t.record {
            let typ = &meta.columns[rec].typ;
            if RecordCol::type_check(typ).is_err() {
                bail!(
                    "{}.{}'s record column {rec:?} is {typ:?}: a record column is blob or text",
                    cfg.keyspace,
                    t.name
                );
            }
        }
        plan.partition_keys.push(meta.partition_key.clone());
        plan.clustering_keys.push(meta.clustering_key.clone());
        plan.keys.push(format!("(({}), {})", meta.partition_key.join(", "), meta.clustering_key.join(", ")));
        let owned = if keyspace.tablet_based {
            plan.modes.push("tablets".into());
            tablets(session, &cs, &cfg.keyspace, &t.name, dc).await?
        } else {
            plan.modes.push("vnodes".into());
            vnodes(&cs, &cfg.keyspace, &t.name, dc)?
        };
        let total: u128 = owned.iter().map(|(_, p)| width(*p)).sum();
        if total != u64::MAX as u128 {
            bail!("{}.{}: the plan covers {total} tokens, not the whole ring", cfg.keyspace, t.name);
        }
        let mut by: BTreeMap<(uuid::Uuid, Option<u32>), Vec<Piece>> = BTreeMap::new();
        for (k, p) in owned {
            by.entry(k).or_default().push(p);
        }
        for ((host, shard), mut pieces) in by {
            pieces.sort_unstable();
            shards.insert((host, shard));
            for g in split(&pieces, cfg.ranges_per_shard) {
                plan.ranges.push(Range { table: ti, host: Some(host.to_string()), shard, pieces: g });
            }
        }
    }
    plan.shards = shards.len();
    plan.nodes = shards.iter().map(|s| s.0).collect::<std::collections::BTreeSet<_>>().len();
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A plan keeps the rule each record table's rows were read by, and a
    /// changed rule (here `when`) doesn't match it; column tables keep none,
    /// so a plan from before record tables still matches a config without
    /// them.
    #[test]
    fn record_rules_pin_the_rule_a_scan_read_by() {
        let rels = |purpose: &str| {
            marco_core::Relations::parse_toml(&format!(
                "[[relation]]\nname = \"follow\"\ncollection = \"app.bsky.graph.follow\"\nsubject = \"subject\"\n\n\
                 [[relation]]\nname = \"modlist\"\ncollection = \"app.bsky.graph.list\"\nsubject = \"$self\"\n\
                 when = {{ purpose = \"{purpose}\" }}\n"
            ))
            .unwrap()
        };
        let defs = |r: &marco_core::Relations| ["follow", "modlist"].map(|n| r.get(r.by_name(n).unwrap()).clone());
        let table = |relation: &str, record: Option<&str>| Table {
            relation: relation.into(),
            name: format!("{relation}s"),
            actor: "actor_did".into(),
            rkey: "rkey".into(),
            subject: "subject".into(),
            list: None,
            record: record.map(String::from),
        };
        let tables = vec![table("follow", None), table("modlist", Some("raw"))];
        let rules = record_rules(&tables, &defs(&rels("app.bsky.graph.defs#modlist")));
        assert_eq!(rules.len(), 2);
        assert!(rules[0].is_none() && rules[1].as_ref().is_some_and(|r| r.name == "modlist"));
        let other = record_rules(&tables, &defs(&rels("app.bsky.graph.defs#curatelist")));
        assert_ne!(rules, other, "another when");
        let plan = Plan {
            keyspace: "bsky".into(),
            datacenter: None,
            tables,
            modes: vec![],
            keys: vec![],
            partition_keys: vec![],
            clustering_keys: vec![],
            shards: 0,
            nodes: 0,
            ranges: vec![],
            record_rules: rules.clone(),
        };
        let back: Plan = serde_json::from_str(&serde_json::to_string(&plan).unwrap()).unwrap();
        assert_eq!(back.record_rules, rules);
        // column tables alone keep none: a plan without the field reads back as one that matches
        let cols = vec![table("follow", None)];
        assert!(record_rules(&cols, &defs(&rels("app.bsky.graph.defs#modlist"))[..1]).is_empty());
        let old = Plan { tables: cols, record_rules: vec![], ..plan };
        let json = serde_json::to_string(&old).unwrap();
        assert!(!json.contains("record_rules"), "{json}");
        assert_eq!(serde_json::from_str::<Plan>(&json).unwrap(), old);
    }

    #[test]
    fn stripes_follow_the_sharding_function() {
        for (n, ign) in [(1u32, 0u8), (4, 12), (7, 12), (32, 12), (3, 0), (12, 4)] {
            let mut rng = crate::stats::Rng::new(n as u64);
            for _ in 0..20 {
                let a = rng.below(u64::MAX) as i64;
                let b = a.saturating_add((rng.below(1 << 50)) as i64);
                let st = stripes((a, b), n, ign);
                assert_eq!(st.first().unwrap().0 .0, a);
                assert_eq!(st.last().unwrap().0 .1, b);
                for w in st.windows(2) {
                    assert_eq!(w[0].0 .1, w[1].0 .0);
                    assert_ne!(w[0].1, w[1].1, "neighbours on one shard should have merged");
                }
                for ((lo, hi), s) in st {
                    assert_eq!(shard_of(lo + 1, n, ign), s);
                    assert_eq!(shard_of(hi, n, ign), s);
                    if hi < b {
                        assert_ne!(shard_of(hi + 1, n, ign), s);
                    }
                }
            }
        }
        // the whole ring: 2^ign blocks of n stripes each
        assert_eq!(stripes((i64::MIN, i64::MAX), 4, 3).len(), 8 * 4);
    }

    #[test]
    fn shard_of_matches_the_driver() {
        let sh = scylla::routing::Sharder::new(std::num::NonZeroU16::new(7).unwrap(), 12);
        let mut rng = crate::stats::Rng::new(3);
        for _ in 0..10_000 {
            let t = rng.below(u64::MAX) as i64;
            assert_eq!(shard_of(t, 7, 12), sh.shard_of(Token::new(t)));
        }
    }

    #[test]
    fn split_is_even_and_covers() {
        let pieces = vec![(i64::MIN, -100), (-50, 10), (1000, i64::MAX)];
        for k in [1, 2, 3, 8, 100] {
            let g = split(&pieces, k);
            assert!(g.len() <= k && !g.is_empty());
            let flat: Vec<Piece> = g.iter().flatten().copied().collect();
            assert_eq!(flat.iter().map(|&p| width(p)).sum::<u128>(), pieces.iter().map(|&p| width(p)).sum::<u128>());
            for w in flat.windows(2) {
                assert!(w[0].1 <= w[1].0);
            }
            let ws: Vec<u128> = g.iter().map(|x| x.iter().map(|&p| width(p)).sum()).collect();
            let max = *ws.iter().max().unwrap();
            let min = *ws.iter().min().unwrap();
            assert!(max - min <= max / 100 + 2 || g.len() == 1, "{k}: {ws:?}");
        }
        assert_eq!(split(&[(0, 3)], 8).len(), 3);
    }
}
