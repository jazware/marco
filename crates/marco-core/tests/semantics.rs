use std::collections::{BTreeMap, BTreeSet, HashSet};

use marco_core::rkey::key_of;
use marco_core::{Added, Dir, Graph, ListRef, Relations, Removed, SetOp, Status, COUNT_EXACT};
use vlatproto::tid::Tid;

const FOLLOW: u8 = 0;
const BLOCK: u8 = 1;

fn did(i: u32) -> String {
    let alphabet = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut v = (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let s: String = (0..24)
        .map(|_| {
            let c = alphabet[(v & 31) as usize] as char;
            v = v.rotate_right(5) ^ 0x5bd1_e995;
            c
        })
        .collect();
    format!("did:plc:{s}")
}

fn tid(t: u64) -> String {
    Tid::from_parts(1_700_000_000_000_000 + t * 1000, 0).to_string()
}

fn follow(a: &str, rkey: &str) -> String {
    format!("at://{a}/app.bsky.graph.follow/{rkey}")
}

fn block(a: &str, rkey: &str) -> String {
    format!("at://{a}/app.bsky.graph.block/{rkey}")
}

fn graph() -> Graph {
    let mut g = Graph::new(Relations::bsky_default());
    g.shards = 3;
    g
}

/// The graph written to a snapshot and opened again.
fn reopen(g: &Graph) -> Graph {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("g.marco");
    g.write_snapshot(&p).unwrap();
    // overwriting a snapshot in place works too
    g.write_snapshot(&p).unwrap();
    let mut g2 = Graph::open(&p).unwrap();
    g2.snapshot().unwrap().verify_checksums().unwrap();
    g2.shards = g.shards;
    // the open files outlive the directory entries
    drop(dir);
    g2
}

fn removed(s: &str) -> Removed {
    Removed::Subject(s.to_string())
}

#[test]
fn duplicate_records_hold_the_pair_until_the_last_goes() {
    let (a, b) = (did(1), did(2));
    for snapshot_between in [false, true] {
        let mut g = graph();
        assert_eq!(g.add(&follow(&a, &tid(1)), &b).unwrap(), Added::Added);
        assert_eq!(g.add(&follow(&a, &tid(2)), &b).unwrap(), Added::Added);
        assert_eq!(g.add(&follow(&a, &tid(2)), &b).unwrap(), Added::Exists);
        if snapshot_between {
            g = reopen(&g);
        }
        assert_eq!(g.read(|cx| g.counts(cx, FOLLOW, &a)).unwrap(), (2, 0));
        assert_eq!(g.read(|cx| g.counts(cx, FOLLOW, &b)).unwrap(), (0, 1));
        assert_eq!(g.read(|cx| g.has(cx, FOLLOW, &a, &b)).unwrap(), Some(follow(&a, &tid(2))), "newest record");
        assert_eq!(g.delete(&follow(&a, &tid(2))).unwrap(), removed(&b));
        assert_eq!(g.read(|cx| g.has(cx, FOLLOW, &a, &b)).unwrap(), Some(follow(&a, &tid(1))));
        assert_eq!(g.read(|cx| g.counts(cx, FOLLOW, &b)).unwrap(), (0, 1));
        assert_eq!(g.delete(&follow(&a, &tid(1))).unwrap(), removed(&b));
        assert_eq!(g.read(|cx| g.has(cx, FOLLOW, &a, &b)).unwrap(), None);
        assert_eq!(g.read(|cx| g.counts(cx, FOLLOW, &a)).unwrap(), (0, 0));
        assert_eq!(g.read(|cx| g.counts(cx, FOLLOW, &b)).unwrap(), (0, 0));
        assert_eq!(g.read(|cx| g.in_list(cx, FOLLOW, &b, 10, None)).unwrap().0, Vec::<String>::new());
    }
}

#[test]
fn duplicate_split_between_base_and_delta() {
    let (a, b) = (did(1), did(2));
    let mut g = graph();
    g.add(&follow(&a, &tid(1)), &b).unwrap();
    let mut g = reopen(&g);
    assert_eq!(g.add(&follow(&a, &tid(5)), &b).unwrap(), Added::Added);
    assert_eq!(g.delete(&follow(&a, &tid(1))).unwrap(), removed(&b));
    assert_eq!(g.read(|cx| g.has(cx, FOLLOW, &a, &b)).unwrap(), Some(follow(&a, &tid(5))));
    let mut g = reopen(&g);
    assert_eq!(g.delete(&follow(&a, &tid(5))).unwrap(), removed(&b));
    assert_eq!(g.read(|cx| g.has(cx, FOLLOW, &a, &b)).unwrap(), None);
    assert_eq!(g.read(|cx| g.counts(cx, FOLLOW, &b)).unwrap(), (0, 0));
}

#[test]
fn non_tid_rkeys_keep_their_strings() {
    let (a, b, c) = (did(1), did(2), did(3));
    let mut g = graph();
    g.add(&follow(&a, &tid(1)), &b).unwrap();
    g.add(&follow(&a, "m96dsjuf12"), &c).unwrap();
    g.add(&follow(&a, "3lhv48k7p7pza"), &b).unwrap();
    for g in [&mut g.clone_via_snapshot(), &mut g] {
        assert_eq!(g.read(|cx| g.has(cx, FOLLOW, &a, &c)).unwrap(), Some(follow(&a, "m96dsjuf12")));
        let (page, _) = g.read(|cx| g.out_list(cx, FOLLOW, &a, 10, None)).unwrap();
        assert_eq!(page.len(), 3);
        // non-TIDs sort after every TID, so they're the newest
        assert_eq!(page[2], (follow(&a, &tid(1)), b.clone()));
        assert!(page[..2].iter().any(|(u, _)| u.ends_with("/m96dsjuf12")));
        assert_eq!(g.delete(&follow(&a, "3lhv48k7p7pza")).unwrap(), removed(&b));
        assert_eq!(g.read(|cx| g.has(cx, FOLLOW, &a, &b)).unwrap(), Some(follow(&a, &tid(1))));
        assert_eq!(g.delete(&follow(&a, "m96dsjuf12")).unwrap(), removed(&c));
        assert_eq!(g.delete(&follow(&a, "m96dsjuf12")).unwrap(), Removed::NotFound);
        assert_eq!(g.read(|cx| g.has(cx, FOLLOW, &a, &c)).unwrap(), None);
    }
}

trait CloneViaSnapshot {
    fn clone_via_snapshot(&self) -> Graph;
}

impl CloneViaSnapshot for Graph {
    fn clone_via_snapshot(&self) -> Graph {
        reopen(self)
    }
}

#[test]
fn a_delete_holds_against_older_adds_only() {
    let (a, b) = (did(1), did(2));
    for base in [false, true] {
        let mut g = graph();
        g.add(&follow(&did(9), &tid(1)), &b).unwrap();
        // the live tail deletes a record marco never saw, at t=100
        assert_eq!(g.delete_at(&follow(&a, &tid(7)), 100).unwrap(), Removed::NotFound);
        assert_eq!(g.tombstones(FOLLOW), 1);
        if base {
            g = reopen(&g);
            assert_eq!(g.tombstones(FOLLOW), 1, "tombstones persist");
        }
        // a backfill that saw the record before the delete
        assert_eq!(g.add_at(&follow(&a, &tid(7)), &b, 50).unwrap(), Added::Deleted);
        assert_eq!(g.read(|cx| g.has(cx, FOLLOW, &a, &b)).unwrap(), None);
        assert_eq!(g.read(|cx| g.counts(cx, FOLLOW, &b)).unwrap(), (0, 1));
        // another rkey for the same pair is a different record
        assert_eq!(g.add_at(&follow(&a, &tid(8)), &b, 50).unwrap(), Added::Added);
        assert_eq!(g.read(|cx| g.has(cx, FOLLOW, &a, &b)).unwrap(), Some(follow(&a, &tid(8))));
        // a re-create seen after the delete, from any source
        assert_eq!(g.add_at(&follow(&a, &tid(7)), &b, 150).unwrap(), Added::Added);
        assert_eq!(g.tombstones(FOLLOW), 0);
        assert_eq!(g.read(|cx| g.counts(cx, FOLLOW, &a)).unwrap(), (2, 0));
        // a delete of a record it has leaves a tombstone too
        assert_eq!(g.delete_at(&follow(&a, &tid(7)), 200).unwrap(), removed(&b));
        assert_eq!(g.add_at(&follow(&a, &tid(7)), &b, 180).unwrap(), Added::Deleted);
        if base {
            let g2 = reopen(&g);
            assert_eq!(g2.tombstones(FOLLOW), 1);
            assert_eq!(g2.read(|cx| g2.counts(cx, FOLLOW, &a)).unwrap(), (1, 0));
        }
    }
}

#[test]
fn checkpoints_drop_tombstones_past_their_ttl() {
    let mut g = graph();
    g.add(&follow(&did(1), &tid(1)), &did(2)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("g");
    g.write_snapshot(&p).unwrap();
    let mut g = Graph::open(&p).unwrap();
    let now = marco_core::store::now_ms() * 1000;
    let day = 86_400_000_000i64;
    g.delete_at(&follow(&did(3), &tid(1)), now - 10 * day).unwrap();
    g.delete_at(&follow(&did(3), &tid(2)), now - day).unwrap();
    g.tombstone_ttl_us = 7 * day;
    let m = g.write_checkpoint(&p, 1, 0, Default::default()).unwrap().manifest;
    marco_core::store::remove_unlisted(&p, &m, true).unwrap();
    assert_eq!(Graph::open(&p).unwrap().tombstones(FOLLOW), 1);
}

#[test]
fn statuses_persist_and_hide() {
    let (a, b, c) = (did(1), did(2), did(3));
    let mut g = graph();
    g.add(&follow(&a, &tid(1)), &b).unwrap();
    g.add(&follow(&c, &tid(1)), &a).unwrap();
    assert_eq!(g.set_status(&a, Some(Status::Deleted), 10).unwrap(), 1);
    g.set_status(&c, Some(Status::Takendown), 10).unwrap();
    g.set_status("did:web:never.example", Some(Status::Takendown), 10).unwrap();
    let mut g = reopen(&g);
    assert_eq!(g.status_of(&a), Some(Status::Deleted));
    assert_eq!(g.status_of(&c), Some(Status::Takendown));
    assert_eq!(g.status_of("did:web:never.example"), Some(Status::Takendown));
    assert_eq!(g.status_of(&b), None);
    assert_eq!(g.read(|cx| g.counts(cx, FOLLOW, &a)).unwrap().0, 0, "deleted drops its records");
    assert_eq!(g.read(|cx| g.counts(cx, FOLLOW, &c)).unwrap().0, 1, "takendown keeps them");
    g.set_status(&c, None, 20).unwrap();
    let g = reopen(&g);
    assert_eq!(g.status_of(&c), None);
    assert_eq!(g.statuses().len(), 2);
}

#[test]
fn delete_by_uri_returns_the_subject_per_relation() {
    let (a, b, c) = (did(1), did(2), did(3));
    let mut g = graph();
    g.add(&follow(&a, &tid(1)), &b).unwrap();
    g.add(&block(&a, &tid(1)), &c).unwrap();
    let mut g = reopen(&g);
    assert_eq!(g.delete(&block(&a, &tid(1))).unwrap(), removed(&c));
    assert_eq!(g.read(|cx| g.has(cx, FOLLOW, &a, &b)).unwrap(), Some(follow(&a, &tid(1))));
    assert_eq!(g.read(|cx| g.has(cx, BLOCK, &a, &c)).unwrap(), None);
    assert!(g.delete("at://did:plc:x/app.bsky.feed.like/3lab2xyz7qk2c").is_err());
    assert!(g.add(&follow(&a, &tid(2)), "not-a-did").is_err());
}

#[test]
fn cursors_page_through_base_and_delta() {
    let a = did(0);
    let mut g = graph();
    for i in 1..=300u32 {
        g.add(&follow(&a, &tid(i as u64 * 2)), &did(i)).unwrap();
        g.add(&follow(&did(i), &tid(1)), &a).unwrap();
    }
    let mut g = reopen(&g);
    // interleave delta records between base ones, and delete some base ones
    for i in 301..=340u32 {
        g.add(&follow(&a, &tid((i - 300) as u64 * 14 + 1)), &did(i)).unwrap();
        g.add(&follow(&did(i), &tid(1)), &a).unwrap();
    }
    for i in (10..300u32).step_by(37) {
        g.delete(&follow(&a, &tid(i as u64 * 2))).unwrap();
        g.delete(&follow(&did(i), &tid(1))).unwrap();
    }
    for g in [&g.clone_via_snapshot(), &g] {
        let mut uris = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let (page, next) = g.read(|cx| g.out_list(cx, FOLLOW, &a, 7, cursor.as_deref())).unwrap();
            uris.extend(page.into_iter().map(|(u, _)| u));
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        let rkeys: Vec<&str> = uris.iter().map(|u| u.rsplit('/').next().unwrap()).collect();
        let mut sorted = rkeys.clone();
        sorted.sort_unstable_by(|x, y| y.cmp(x));
        assert_eq!(rkeys, sorted, "newest first");
        assert_eq!(rkeys.len() as u64, g.read(|cx| g.counts(cx, FOLLOW, &a)).unwrap().0);
        assert_eq!(rkeys.iter().collect::<HashSet<_>>().len(), rkeys.len());

        let mut followers = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let (page, next) = g.read(|cx| g.in_list(cx, FOLLOW, &a, 11, cursor.as_deref())).unwrap();
            followers.extend(page);
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        let ids: Vec<u32> = followers.iter().map(|d| g.id_of(d).unwrap()).collect();
        assert!(ids.windows(2).all(|w| w[0] < w[1]), "id order");
        assert_eq!(ids.len() as u64, g.read(|cx| g.counts(cx, FOLLOW, &a)).unwrap().1);
        assert_eq!(ids.len(), 300 - 8 + 40);
    }
}

#[test]
fn reads_intersect_filter_relationships() {
    let v = did(0);
    let mut g = graph();
    // v follows 1..=20; 1..=10 follow v back; 5..=15 follow x; v blocks 3; 4 blocks v
    let x = did(99);
    for i in 1..=20 {
        g.add(&follow(&v, &tid(i)), &did(i as u32)).unwrap();
    }
    for i in 1..=10 {
        g.add(&follow(&did(i), &tid(100)), &v).unwrap();
    }
    for i in 5..=15 {
        g.add(&follow(&did(i), &tid(200)), &x).unwrap();
    }
    g.add(&block(&v, &tid(1)), &did(3)).unwrap();
    g.add(&block(&did(4), &tid(1)), &v).unwrap();
    for g in [&g.clone_via_snapshot(), &g] {
        let id = |i: u32| g.id_of(&did(i)).unwrap();
        let vid = g.id_of(&v).unwrap();
        let mutuals = g
            .read(|cx| {
                g.intersect(
                    cx,
                    ListRef { rel: FOLLOW, dir: Dir::Out, actor: vid },
                    ListRef { rel: FOLLOW, dir: Dir::In, actor: vid },
                    100,
                )
            })
            .unwrap();
        let want: BTreeSet<u32> = (1..=10).map(id).collect();
        assert_eq!(mutuals.iter().copied().collect::<BTreeSet<_>>(), want);
        let known = g
            .read(|cx| {
                g.intersect(
                    cx,
                    ListRef { rel: FOLLOW, dir: Dir::In, actor: g.id_of(&x).unwrap() },
                    ListRef { rel: FOLLOW, dir: Dir::Out, actor: vid },
                    100,
                )
            })
            .unwrap();
        assert_eq!(known.iter().copied().collect::<BTreeSet<_>>(), (5..=15).map(id).collect());
        assert_eq!(known.len(), 11);
        let page: Vec<u32> = (1..=6).map(id).collect();
        let kept = g.read(|cx| g.filter_id(cx, vid, &page, &[(BLOCK, Dir::Out), (BLOCK, Dir::In)])).unwrap();
        assert_eq!(kept, [1, 2, 5, 6].map(id).to_vec());
        let others = [did(1), did(3), did(4), did(15), did(50)];
        let others: Vec<&str> = others.iter().map(String::as_str).collect();
        let r = g.read(|cx| g.relationships(cx, &v, &others, &[FOLLOW, BLOCK])).unwrap();
        assert_eq!(r[0][0].out, Some(follow(&v, &tid(1))));
        assert_eq!(r[0][0].inn, Some(follow(&did(1), &tid(100))));
        assert_eq!(r[1][1].out, Some(block(&v, &tid(1))));
        assert_eq!(r[2][1].inn, Some(block(&did(4), &tid(1))));
        assert_eq!(r[3][0].inn, None);
        assert_eq!(r[4], vec![Default::default(), Default::default()]);
    }
}

#[test]
fn ids_survive_snapshots_with_live_dids() {
    let mut g = graph();
    g.add(&follow(&did(5), &tid(1)), &did(6)).unwrap();
    g.add(&follow("did:web:example.com", &tid(1)), &did(6)).unwrap();
    let mut g = reopen(&g);
    // new DIDs after the base get ids in arrival order, past the base's
    g.add(&follow(&did(1), &tid(1)), "did:web:a.example").unwrap();
    let before: Vec<(String, u32)> = [did(5), did(6), did(1), "did:web:example.com".into(), "did:web:a.example".into()]
        .into_iter()
        .map(|d| {
            let id = g.id_of(&d).unwrap();
            (d, id)
        })
        .collect();
    assert_eq!(g.id_of(&did(1)), Some(3));
    let g2 = reopen(&g);
    for (d, id) in &before {
        assert_eq!(g2.id_of(d), Some(*id), "{d}");
        assert_eq!(g2.did_of(*id).as_deref(), Some(d.as_str()));
    }
    assert_eq!(g2.read(|cx| g2.has(cx, FOLLOW, &did(1), "did:web:a.example")).unwrap(), Some(follow(&did(1), &tid(1))));
    let g3 = reopen(&g2);
    assert_eq!(g3.id_of(&did(1)), Some(3));
}

#[derive(Default)]
struct Model {
    records: BTreeMap<(u8, u32, String), u32>,
    /// Tombstones with the time of the delete that left them.
    tomb: BTreeMap<(u8, u32, String), i64>,
    /// Each key's last change: (time, kept through a checkpoint).
    stamp: BTreeMap<(u8, u32, String), (i64, u8, bool)>,
}

/// The rules a key follows, for any value (a subject, or a list and member).
fn model_add<K: Ord + Clone, V: PartialEq + Copy>(
    records: &mut BTreeMap<K, V>,
    tomb: &mut BTreeMap<K, i64>,
    stamp: &mut BTreeMap<K, (i64, u8, bool)>,
    key: K,
    v: V,
    t: i64,
    src: u8,
) -> Added {
    let stale = |stamp: &BTreeMap<K, (i64, u8, bool)>| t != 0 && stamp.get(&key).is_some_and(|s| s.1 != src && t < s.0);
    let mut recreated = false;
    if let Some(&d) = tomb.get(&key) {
        if t < d {
            return Added::Deleted;
        }
        if stale(stamp) {
            return Added::Exists;
        }
        tomb.remove(&key);
        recreated = true;
    }
    if stale(stamp) {
        return Added::Exists;
    }
    let held = records.get(&key).copied();
    if held == Some(v) {
        return Added::Exists;
    }
    records.insert(key.clone(), v);
    let e = stamp.entry(key).or_insert((i64::MIN, src, false));
    e.0 = e.0.max(t);
    e.1 = src;
    e.2 |= recreated || held.is_some();
    Added::Added
}

fn model_delete<K: Ord + Clone, V: Copy>(
    records: &mut BTreeMap<K, V>,
    tomb: &mut BTreeMap<K, i64>,
    stamp: &BTreeMap<K, (i64, u8, bool)>,
    key: K,
    t: i64,
    src: u8,
) -> Option<V> {
    if t != 0 && stamp.get(&key).is_some_and(|s| s.1 != src && s.0 > t) && records.contains_key(&key) {
        return None;
    }
    let e = tomb.entry(key.clone()).or_insert(t);
    *e = (*e).max(t);
    records.remove(&key)
}

/// A checkpoint keeps only the stamps of keys replaced or re-created.
fn model_checkpoint<K: Ord>(stamp: &mut BTreeMap<K, (i64, u8, bool)>) {
    stamp.retain(|_, v| v.2);
}

impl Model {
    fn pair(&self, rel: u8, a: u32, b: u32) -> bool {
        self.records.iter().any(|((r, x, _), s)| *r == rel && *x == a && *s == b)
    }
    fn out_count(&self, rel: u8, a: u32) -> u64 {
        self.records.keys().filter(|(r, x, _)| *r == rel && *x == a).count() as u64
    }
    fn in_count(&self, rel: u8, b: u32) -> u64 {
        self.records
            .iter()
            .filter(|((r, _, _), s)| *r == rel && **s == b)
            .map(|((_, x, _), _)| *x)
            .collect::<BTreeSet<_>>()
            .len() as u64
    }
}

/// Random adds and deletes (duplicates, non-TID rkeys, deletes before
/// creates) against a plain model, through snapshots and list compaction.
#[test]
fn matches_a_model() {
    run_model(marco_core::graph::DEFAULT_SUBJ_INDEX);
}

#[test]
fn matches_a_model_with_subject_indexes() {
    run_model(3);
}

fn run_model(subj_index: usize) {
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    let mut rnd = |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    let rels = [("app.bsky.graph.follow", FOLLOW), ("app.bsky.graph.block", BLOCK)];
    let mut g = graph();
    g.set_subj_index(subj_index);
    g.compact_min = 4;
    let mut m = Model::default();
    let mut actors = 20u64;
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("g");
    let mut ckpt = 0;
    for step in 0..6000 {
        // new DIDs keep arriving, so checkpoints write live DIDs and new shards
        if step % 100 == 0 {
            actors += 1;
        }
        let (coll, rel) = rels[(rnd(5) == 0) as usize];
        let a = rnd(actors) as u32;
        let rkey = match rnd(20) {
            0 => format!("x{}", rnd(30)),
            _ => tid(rnd(120)),
        };
        let uri = format!("at://{}/{coll}/{rkey}", did(a));
        // mostly in order, a quarter from a backfill that saw things up to 300 steps ago
        // the backfill source sees things up to 300 steps late
        let src = (rnd(4) == 0) as u8;
        let t = step as i64 * 10 - if src == 1 { rnd(3000) as i64 } else { 0 };
        g.set_source(["live", "backfill"][src as usize]);
        let key = (rel, a, rkey.clone());
        if rnd(3) == 0 {
            let got = g.delete_at(&uri, t).unwrap();
            match model_delete(&mut m.records, &mut m.tomb, &m.stamp, key, t, src) {
                Some(s) => assert_eq!(got, removed(&did(s)), "step {step}"),
                None => assert_eq!(got, Removed::NotFound, "step {step}"),
            }
        } else {
            let b = rnd(actors) as u32;
            let got = g.add_at(&uri, &did(b), t).unwrap();
            let want = model_add(&mut m.records, &mut m.tomb, &mut m.stamp, key, b, t, src);
            assert_eq!(got, want, "step {step} {uri}");
        }
        if step % 750 == 749 {
            model_checkpoint(&mut m.stamp);
            // a full write, then checkpoints over it that copy unchanged lists
            g = if g.snapshot().is_none() || step % 3000 == 2999 {
                g.write_snapshot(&snap).unwrap();
                Graph::open(&snap).unwrap()
            } else {
                ckpt += 1;
                let m = g.write_checkpoint(&snap, ckpt, 0, Default::default()).unwrap().manifest;
                marco_core::store::remove_unlisted(&snap, &m, true).unwrap();
                let g2 = Graph::open(&snap).unwrap();
                g2.snapshot().unwrap().verify_checksums().unwrap();
                g2
            };
            g.shards = 3;
            g.compact_min = 4;
        }
        if step % 97 == 0 {
            for (_, rel) in rels {
                for x in 0..actors as u32 {
                    let d = did(x);
                    assert_eq!(
                        g.read(|cx| g.counts(cx, rel, &d)).unwrap().0,
                        m.out_count(rel, x),
                        "out {rel} {x} step {step}"
                    );
                    assert_eq!(
                        g.read(|cx| g.counts(cx, rel, &d)).unwrap().1,
                        m.in_count(rel, x),
                        "in {rel} {x} step {step}"
                    );
                    let y = rnd(actors) as u32;
                    assert_eq!(g.read(|cx| g.has(cx, rel, &d, &did(y))).unwrap().is_some(), m.pair(rel, x, y));
                    let (page, _) = g.read(|cx| g.out_list(cx, rel, &d, 1000, None)).unwrap();
                    let mut want: Vec<(u64, String)> = m
                        .records
                        .iter()
                        .filter(|((r, xx, _), _)| *r == rel && *xx == x)
                        .map(|((_, _, k), s)| (key_of(k).0, did(*s)))
                        .collect();
                    want.sort_unstable_by_key(|p| std::cmp::Reverse(p.0));
                    let got: Vec<(u64, String)> =
                        page.into_iter().map(|(u, s)| (key_of(u.rsplit('/').next().unwrap()).0, s)).collect();
                    assert_eq!(got, want, "out list {rel} {x} step {step}");
                    if let Some(xi) = g.id_of(&d) {
                        let others: Vec<u32> = (0..12).filter_map(|_| g.id_of(&did(rnd(actors) as u32))).collect();
                        let r = g.read(|cx| g.relationships_id(cx, xi, &others, &[FOLLOW, BLOCK])).unwrap();
                        for (j, &o) in others.iter().enumerate() {
                            for (ri, rl) in [FOLLOW, BLOCK].into_iter().enumerate() {
                                assert_eq!(
                                    r[j][ri],
                                    (
                                        g.read(|cx| g.has_id(cx, rl, xi, o)).unwrap(),
                                        g.read(|cx| g.has_id(cx, rl, o, xi)).unwrap()
                                    ),
                                    "relationships step {step}"
                                );
                            }
                        }
                    }
                }
            }
            let ds: Vec<String> = (0..12).map(|_| did(rnd(actors) as u32)).collect();
            let pairs: Vec<(&str, &str)> = ds.chunks(2).map(|p| (p[0].as_str(), p[1].as_str())).collect();
            let got = g.read(|cx| g.block_pairs(cx, &pairs, &[BLOCK])).unwrap();
            for (&(a, b), r) in pairs.iter().zip(got) {
                let (x, y) = (g.id_of(a), g.id_of(b));
                let named = |from: Option<u32>, to: Option<u32>| {
                    from.zip(to).is_some_and(|(f, t)| g.read(|cx| g.has_id(cx, BLOCK, f, t)).unwrap().is_some())
                };
                assert_eq!(r, (named(x, y), named(y, x)), "block pairs step {step}");
            }
            check_combine(&g, &m, actors as u32, &mut rnd, step);
        }
    }
}

/// combine_counted against the model's sets: pages under cursors, totals under caps.
fn check_combine(g: &Graph, m: &Model, actors: u32, rnd: &mut impl FnMut(u64) -> u64, step: usize) {
    let (p, q) = (rnd(actors as u64) as u32, rnd(actors as u64) as u32);
    let (Some(pi), Some(qi)) = (g.id_of(&did(p)), g.id_of(&did(q))) else { return };
    let ids = |f: &dyn Fn(u32) -> bool| -> Vec<u32> {
        let mut v: Vec<u32> = (0..actors).filter(|&x| f(x)).filter_map(|x| g.id_of(&did(x))).collect();
        v.sort_unstable();
        v
    };
    let out_p = ids(&|y| m.pair(FOLLOW, p, y));
    let in_q = ids(&|x| m.pair(FOLLOW, x, q));
    let out_q = ids(&|y| m.pair(FOLLOW, q, y));
    let a = ListRef { rel: FOLLOW, dir: Dir::Out, actor: pi };
    for (b, other) in [
        (ListRef { rel: FOLLOW, dir: Dir::In, actor: qi }, &in_q),
        (ListRef { rel: FOLLOW, dir: Dir::Out, actor: qi }, &out_q),
    ] {
        for op in [SetOp::And, SetOp::AndNot] {
            let want: Vec<u32> = out_p.iter().copied().filter(|x| other.contains(x) == (op == SetOp::And)).collect();
            for after in [None, want.first().copied(), want.get(want.len() / 2).copied()] {
                for (limit, cap) in [(2, 0), (2, 1), (5, 3), (100, 1000), (1, 2), (3, COUNT_EXACT)] {
                    let (page, total) = g.read(|cx| g.combine_counted(cx, a, b, op, limit, after, cap)).unwrap();
                    let rest: Vec<u32> =
                        want.iter().copied().filter(|&x| after.is_none_or(|y| x > y)).take(limit).collect();
                    assert_eq!(page, rest, "page {op:?} after {after:?} limit {limit} step {step}");
                    let n = want.len();
                    let t = (cap > 0).then(|| (n.min(cap) as u64, n > cap));
                    assert_eq!(total, t, "total {op:?} cap {cap} step {step}");
                    assert_eq!(page, g.read(|cx| g.combine(cx, a, b, op, limit, after)).unwrap());
                }
            }
        }
    }
}

#[test]
fn checksums_catch_a_flipped_byte() {
    let mut g = graph();
    g.add(&follow(&did(1), &tid(1)), &did(2)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("g.marco");
    g.write_snapshot(&p).unwrap();
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1, "no temp dir left");
    assert_eq!(std::fs::read_dir(&p).unwrap().count(), 5, "directory + 3 shards + manifest");
    let id = Graph::open(&p).unwrap().id_of(&did(1)).unwrap();
    let store = marco_core::store::Store::open(&p).unwrap();
    let k = (id / store.dir.meta.shard_width) as usize;
    let f = p.join(marco_core::store::shard_file(k, 0));
    let sections = store.shards[k].as_ref().unwrap().newest().sections().to_vec();
    drop(store);
    let flip = |name: &str| {
        let sec = sections.iter().find(|s| s.name == name).unwrap();
        let mut b = std::fs::read(&f).unwrap();
        b[sec.off as usize] ^= 1;
        std::fs::write(&f, &b).unwrap();
        b[sec.off as usize] ^= 1;
        b
    };
    let good = flip("r0.out.small");
    let err = open_and_verify(&p);
    assert!(err.contains("r0.out.small page"), "{err}");
    std::fs::write(&f, good).unwrap();
    flip("r0.out.idx");
    let err = open_and_verify(&p);
    assert!(err.contains("section r0.out.idx checksum"), "{err}");
}

/// Why opening the snapshot at `p` and checking it fails: the engine checks
/// pinned sections at open, pages when asked.
fn open_and_verify(p: &std::path::Path) -> String {
    let e = marco_core::store::Store::open(p).and_then(|s| s.verify_checksums()).unwrap_err();
    format!("{e:#}")
}

#[test]
fn shards_must_belong_to_the_snapshot() {
    let mut g = graph();
    g.add(&follow(&did(1), &tid(1)), &did(2)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (dir.path().join("a"), dir.path().join("b"));
    g.write_snapshot(&a).unwrap();
    g.write_snapshot(&b).unwrap();
    std::fs::copy(b.join(marco_core::store::shard_file(1, 0)), a.join(marco_core::store::shard_file(1, 0))).unwrap();
    assert!(Graph::open(&a).is_err());
}

#[test]
fn a_checkpoint_id_is_never_written_twice() {
    let mut g = graph();
    g.add(&follow(&did(1), &tid(1)), &did(2)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("g");
    g.write_snapshot(&p).unwrap();
    let mut g = Graph::open(&p).unwrap();
    g.add(&follow(&did(1), &tid(2)), &did(3)).unwrap();
    let m = g.write_checkpoint(&p, 1, 0, Default::default()).unwrap().manifest;
    let mut g = Graph::open(&p).unwrap();
    g.add(&follow(&did(1), &tid(3)), &did(4)).unwrap();
    let before: Vec<Vec<u8>> =
        m.shards.values().flat_map(|f| f.files()).map(|f| std::fs::read(p.join(f)).unwrap()).collect();
    for id in [0, 1] {
        assert!(g.write_checkpoint(&p, id, 0, Default::default()).is_err(), "id {id}");
    }
    let after: Vec<Vec<u8>> =
        m.shards.values().flat_map(|f| f.files()).map(|f| std::fs::read(p.join(f)).unwrap()).collect();
    assert!(before == after, "the committed files are untouched");
    g.write_checkpoint(&p, 2, 0, Default::default()).unwrap();
}

const LIST: u8 = 2;
const LIST_OWNER: u8 = 3;

fn listitem(a: &str, rkey: &str) -> String {
    format!("at://{a}/app.bsky.graph.listitem/{rkey}")
}

fn list_uri(a: &str, l: u32) -> String {
    format!("at://{a}/app.bsky.graph.list/l{l}")
}

#[test]
fn lists_are_sets_of_members() {
    let (o, x, y, z) = (did(1), did(2), did(3), did(4));
    let (l1, l2) = (list_uri(&o, 1), list_uri(&o, 2));
    for snapshot_between in [false, true] {
        let mut g = graph();
        let add = |g: &mut Graph, rk: u64, l: &str, s: &str| g.add_rec(&listitem(&o, &tid(rk)), s, Some(l), 0).unwrap();
        assert_eq!(add(&mut g, 1, &l1, &x), Added::Added);
        assert_eq!(add(&mut g, 2, &l1, &y), Added::Added);
        assert_eq!(add(&mut g, 3, &l1, &x), Added::Added, "a repeat member is a second record");
        assert_eq!(add(&mut g, 4, &l2, &y), Added::Added);
        assert_eq!(add(&mut g, 5, &l2, &z), Added::Added);
        assert!(g.add_rec(&listitem(&o, &tid(9)), &x, None, 0).is_err(), "a listitem names its list");
        assert!(g.add_rec(&listitem(&x, &tid(9)), &y, Some(&l1), 0).is_err(), "the list is in the owner's repo");
        if snapshot_between {
            g = reopen(&g);
        }
        let id = |g: &Graph, s: &str| g.id_of(s).unwrap();
        assert_eq!(g.read(|cx| g.counts(cx, LIST, &l1)).unwrap(), (3, 0));
        assert_eq!(g.read(|cx| g.out_distinct(cx, LIST, id(&g, &l1))).unwrap(), 2);
        assert_eq!(g.read(|cx| g.counts(cx, LIST, &x)).unwrap(), (0, 1), "x is on one list");
        assert_eq!(g.read(|cx| g.counts(cx, LIST, &y)).unwrap(), (0, 2));
        assert_eq!(g.read(|cx| g.counts(cx, LIST_OWNER, &o)).unwrap(), (5, 0));
        let (members, _) = g.read(|cx| g.out_list(cx, LIST, &l1, 10, None)).unwrap();
        let want: Vec<(String, String)> =
            [(3, &x), (2, &y), (1, &x)].into_iter().map(|(k, s)| (listitem(&o, &tid(k)), s.to_string())).collect();
        assert_eq!(members, want, "newest first, URIs in the owner's repo");
        let (members, _) = g.read(|cx| g.out_list_opts(cx, LIST, &l1, 10, None, true)).unwrap();
        assert_eq!(members, want[1..].to_vec(), "one record per member, its oldest");
        let (lists, _) = g.read(|cx| g.in_list(cx, LIST, &y, 10, None)).unwrap();
        assert_eq!(lists.iter().collect::<BTreeSet<_>>(), [&l1, &l2].into_iter().collect());
        assert_eq!(
            g.read(|cx| g.has(cx, LIST, &l1, &x)).unwrap(),
            Some(listitem(&o, &tid(1))),
            "the oldest, as the distinct page keeps"
        );
        assert_eq!(g.read(|cx| g.has(cx, LIST, &l2, &x)).unwrap(), None);

        let side = |g: &Graph, rel, dir, s: &str| ListRef { rel, dir, actor: id(g, s) };
        let names = |g: &Graph, v: Vec<u32>| v.into_iter().map(|i| g.did_of(i).unwrap()).collect::<BTreeSet<_>>();
        let (s1, s2) = (side(&g, LIST, Dir::Out, &l1), side(&g, LIST, Dir::Out, &l2));
        assert_eq!(
            names(&g, g.read(|cx| g.combine(cx, s1, s2, marco_core::SetOp::And, 10, None)).unwrap()),
            [y.clone()].into()
        );
        assert_eq!(
            names(&g, g.read(|cx| g.combine(cx, s1, s2, marco_core::SetOp::AndNot, 10, None)).unwrap()),
            [x.clone()].into()
        );
        assert_eq!(
            names(&g, g.read(|cx| g.combine(cx, s2, s1, marco_core::SetOp::AndNot, 10, None)).unwrap()),
            [z.clone()].into()
        );
        let both = g.read(|cx| g.combine(cx, s1, s2, marco_core::SetOp::And, 10, None)).unwrap()[0];
        assert!(
            g.read(|cx| g.combine(cx, s1, s2, marco_core::SetOp::And, 10, Some(both))).unwrap().is_empty(),
            "the cursor"
        );
        let ids: Vec<u32> = [&x, &y, &z].iter().map(|d| id(&g, d)).collect();
        assert_eq!(
            names(&g, g.read(|cx| g.filter_sets(cx, id(&g, &o), &ids, &[], &[s1])).unwrap()),
            [z.clone()].into()
        );

        assert_eq!(g.delete(&listitem(&o, &tid(2))).unwrap(), removed(&y), "delete by URI finds the list");
        assert_eq!(g.read(|cx| g.counts(cx, LIST, &y)).unwrap(), (0, 1));
        assert_eq!(g.read(|cx| g.out_distinct(cx, LIST, id(&g, &l1))).unwrap(), 1);
        assert_eq!(g.delete(&listitem(&o, &tid(2))).unwrap(), Removed::NotFound);
        assert_eq!(g.add_rec(&listitem(&o, &tid(6)), &z, Some(&l1), -5).unwrap(), Added::Added);
        g.delete_at(&listitem(&o, &tid(7)), 100).unwrap();
        assert_eq!(g.add_rec(&listitem(&o, &tid(7)), &z, Some(&l1), 50).unwrap(), Added::Deleted, "tombstone");

        g.set_status(&o, Some(Status::Takendown), 0).unwrap();
        assert!(g.hidden(&l1), "a list hides with its owner");
        g.set_status(&o, None, 0).unwrap();
        let g = reopen(&g);
        let mut g = g;
        assert_eq!(g.set_status(&o, Some(Status::Deleted), 200).unwrap(), 5);
        assert_eq!(g.read(|cx| g.counts(cx, LIST, &l1)).unwrap(), (0, 0));
        assert_eq!(g.read(|cx| g.counts(cx, LIST, &y)).unwrap(), (0, 0));
        assert_eq!(g.read(|cx| g.counts(cx, LIST_OWNER, &o)).unwrap(), (0, 0));
    }
}

#[test]
fn lists_match_a_model() {
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut rnd = |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    let mut g = graph();
    g.set_subj_index(8);
    g.compact_min = 4;
    // (owner, rkey) -> (list, member), and tombstones by (owner, rkey)
    let mut recs: BTreeMap<(u32, String), (u32, u32)> = BTreeMap::new();
    let mut tomb: BTreeMap<(u32, String), i64> = BTreeMap::new();
    let mut stamp: BTreeMap<(u32, String), (i64, u8, bool)> = BTreeMap::new();
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("g");
    let mut ckpt = 0;
    let owners = 6u64;
    for step in 0..5000 {
        let o = rnd(owners) as u32;
        let rkey = if rnd(25) == 0 { format!("x{}", rnd(20)) } else { tid(rnd(150)) };
        let uri = listitem(&did(o), &rkey);
        let src = (rnd(4) == 0) as u8;
        let t = step as i64 * 10 - if src == 1 { rnd(2000) as i64 } else { 0 };
        g.set_source(["live", "backfill"][src as usize]);
        let key = (o, rkey.clone());
        if rnd(3) == 0 {
            let got = g.delete_at(&uri, t).unwrap();
            match model_delete(&mut recs, &mut tomb, &stamp, key, t, src) {
                Some((_, m)) => assert_eq!(got, removed(&did(m)), "step {step}"),
                None => assert_eq!(got, Removed::NotFound, "step {step}"),
            }
        } else {
            let (l, m) = (rnd(3) as u32, 10 + rnd(15) as u32);
            let got = g.add_rec(&uri, &did(m), Some(&list_uri(&did(o), l)), t).unwrap();
            let want = model_add(&mut recs, &mut tomb, &mut stamp, key, (l, m), t, src);
            assert_eq!(got, want, "step {step} {uri}");
        }
        if step % 700 == 699 {
            model_checkpoint(&mut stamp);
            g = if g.snapshot().is_none() || step % 2800 == 2799 {
                g.write_snapshot(&snap).unwrap();
                Graph::open(&snap).unwrap()
            } else {
                ckpt += 1;
                let m = g.write_checkpoint(&snap, ckpt, 0, Default::default()).unwrap().manifest;
                marco_core::store::remove_unlisted(&snap, &m, true).unwrap();
                Graph::open(&snap).unwrap()
            };
            g.shards = 3;
            g.compact_min = 4;
        }
        if step % 83 == 0 {
            for o in 0..owners as u32 {
                let n_owner = recs.keys().filter(|k| k.0 == o).count() as u64;
                assert_eq!(g.read(|cx| g.counts(cx, LIST_OWNER, &did(o))).unwrap().0, n_owner, "owner {o} step {step}");
                for l in 0..3 {
                    let lu = list_uri(&did(o), l);
                    let items: Vec<(&String, u32)> =
                        recs.iter().filter(|(k, v)| k.0 == o && v.0 == l).map(|(k, v)| (&k.1, v.1)).collect();
                    let members: BTreeSet<u32> = items.iter().map(|i| i.1).collect();
                    assert_eq!(
                        g.read(|cx| g.counts(cx, LIST, &lu)).unwrap().0,
                        items.len() as u64,
                        "list {o}/{l} step {step}"
                    );
                    let lid = g.id_of(&lu);
                    assert_eq!(
                        lid.map_or(0, |i| g.read(|cx| g.out_distinct(cx, LIST, i)).unwrap()),
                        members.len() as u64,
                        "step {step}"
                    );
                    let (page, _) = g.read(|cx| g.out_list_opts(cx, LIST, &lu, 1000, None, true)).unwrap();
                    assert_eq!(page.iter().map(|p| p.1.clone()).collect::<BTreeSet<_>>().len(), page.len());
                    assert_eq!(page.len(), members.len());
                    for m in 10..25u32 {
                        assert_eq!(
                            g.read(|cx| g.has(cx, LIST, &lu, &did(m))).unwrap().is_some(),
                            members.contains(&m),
                            "step {step}"
                        );
                        if let (Some(li), Some(mi)) = (lid, g.id_of(&did(m))) {
                            let s = ListRef { rel: LIST, dir: Dir::Out, actor: li };
                            assert_eq!(g.read(|cx| g.side_contains(cx, s, mi)).unwrap(), members.contains(&m));
                        }
                    }
                }
            }
        }
    }
}

/// A list merged down to nothing is the one byte n = 0, read back as empty.
#[test]
fn a_list_compacted_to_empty_reads_as_empty() {
    let a = did(1);
    let mut g = graph();
    for k in 0..4 {
        g.add(&follow(&a, &tid(k)), &did(10 + k as u32)).unwrap();
    }
    let mut g = reopen(&g);
    g.compact_min = 4;
    for k in 0..4 {
        g.delete(&follow(&a, &tid(k))).unwrap();
    }
    assert_eq!(g.read(|cx| g.counts(cx, FOLLOW, &a)).unwrap(), (0, 0));
    let back = reopen(&g);
    assert_eq!(back.read(|cx| back.counts(cx, FOLLOW, &a)).unwrap(), (0, 0));
}

#[test]
fn an_update_replaces_the_record() {
    let (a, b, c) = (did(1), did(2), did(3));
    let mut g = graph();
    let u = follow(&a, &tid(1));
    assert_eq!(g.add_at(&u, &b, 1).unwrap(), Added::Added);
    let mut g = reopen(&g);
    assert_eq!(g.add_at(&u, &c, 2).unwrap(), Added::Added, "a new subject under the same rkey");
    assert_eq!(g.add_at(&u, &c, 3).unwrap(), Added::Exists);
    assert_eq!(
        (g.read(|cx| g.has(cx, FOLLOW, &a, &b)).unwrap(), g.read(|cx| g.has(cx, FOLLOW, &a, &c)).unwrap()),
        (None, Some(u.clone()))
    );
    assert_eq!(
        (
            g.read(|cx| g.counts(cx, FOLLOW, &a)).unwrap(),
            g.read(|cx| g.counts(cx, FOLLOW, &b)).unwrap(),
            g.read(|cx| g.counts(cx, FOLLOW, &c)).unwrap()
        ),
        ((1, 0), (0, 0), (0, 1))
    );
    g.delete_at(&u, 10).unwrap();
    assert_eq!(g.add_at(&u, &b, 5).unwrap(), Added::Deleted, "an older update stays deleted");
}

#[test]
fn an_update_moves_a_listitem() {
    let (o, x) = (did(1), did(2));
    let (l1, l2) = (list_uri(&o, 1), list_uri(&o, 2));
    let u = listitem(&o, &tid(1));
    for snapshot_between in [false, true] {
        let mut g = graph();
        assert_eq!(g.add_rec(&u, &x, Some(&l1), 1).unwrap(), Added::Added);
        if snapshot_between {
            g = reopen(&g);
        }
        assert_eq!(g.add_rec(&u, &x, Some(&l2), 2).unwrap(), Added::Added);
        assert_eq!(
            (g.read(|cx| g.has(cx, LIST, &l1, &x)).unwrap(), g.read(|cx| g.has(cx, LIST, &l2, &x)).unwrap()),
            (None, Some(u.clone()))
        );
        assert_eq!(
            (
                g.read(|cx| g.counts(cx, LIST, &l1)).unwrap().0,
                g.read(|cx| g.counts(cx, LIST, &l2)).unwrap().0,
                g.read(|cx| g.counts(cx, LIST, &x)).unwrap().1
            ),
            (0, 1, 1)
        );
        assert_eq!(g.read(|cx| g.counts(cx, LIST_OWNER, &o)).unwrap().0, 1);
        assert_eq!(g.delete(&u).unwrap(), removed(&x), "the delete finds the new list");
        assert_eq!(g.read(|cx| g.counts(cx, LIST, &l2)).unwrap().0, 0);
    }
}

/// The defaults, then `listblock` (actor -> a list's URI) and `modlist` (an
/// owner -> its own list record, while its purpose is a mod list).
fn list_subjects() -> Relations {
    Relations::parse_toml(
        r#"
[[relation]]
name = "follow"
collection = "app.bsky.graph.follow"
subject = "subject"

[[relation]]
name = "block"
collection = "app.bsky.graph.block"
subject = "subject"

[[relation]]
name = "listitem"
collection = "app.bsky.graph.listitem"
subject = "subject"
list = "list"

[[relation]]
name = "listblock"
collection = "app.bsky.graph.listblock"
subject = "subject"
subject_collection = "app.bsky.graph.list"

[[relation]]
name = "modlist"
collection = "app.bsky.graph.list"
subject = "$self"
when = { purpose = "app.bsky.graph.defs#modlist" }
"#,
    )
    .unwrap()
}

const LISTBLOCK: u8 = 4;
const MODLIST: u8 = 5;

fn listblock(a: &str, rkey: &str) -> String {
    format!("at://{a}/app.bsky.graph.listblock/{rkey}")
}

/// A list's URI is the subject of a listblock and of its owner's modlist
/// edge, and the actor of its listitems: one node, one id, through a
/// snapshot too.
#[test]
fn records_as_subjects_share_the_list_nodes() {
    let (o, v, w, t) = (did(1), did(2), did(3), did(4));
    let (l1, l2) = (list_uri(&o, 1), list_uri(&o, 2));
    for snapshot_between in [false, true] {
        let mut g = Graph::new(list_subjects());
        g.shards = 3;
        assert_eq!(g.add_rec(&listitem(&o, &tid(1)), &t, Some(&l1), 1).unwrap(), Added::Added);
        assert_eq!(g.add_at(&listblock(&v, &tid(2)), &l1, 2).unwrap(), Added::Added);
        assert_eq!(g.add_at(&listblock(&v, &tid(3)), &l2, 3).unwrap(), Added::Added);
        assert_eq!(g.add_at(&listblock(&w, &tid(4)), &l1, 4).unwrap(), Added::Added);
        assert_eq!(g.add_at(&l1, &l1, 5).unwrap(), Added::Added, "a mod list's record names itself");
        assert!(g.add_at(&listblock(&v, &tid(9)), &t, 9).is_err(), "a DID isn't a list");
        assert!(g.add_at(&listblock(&v, &tid(9)), &follow(&o, &tid(1)), 9).is_err(), "a record in another collection");
        assert!(g.add_at(&l2, &l1, 9).is_err(), "a $self subject is the record's own URI");
        assert!(g.add_at(&follow(&v, &tid(9)), &l1, 9).is_err(), "a follow's subject is a DID");
        if snapshot_between {
            g = reopen(&g);
        }
        let id = |g: &Graph, s: &str| g.id_of(s).unwrap();
        let blocked_by: BTreeSet<u32> =
            g.read(|cx| g.in_set(cx, LISTBLOCK, id(&g, &l1))).unwrap().into_iter().collect();
        assert_eq!(blocked_by, [id(&g, &v), id(&g, &w)].into());
        let on: Vec<u32> = g.read(|cx| g.in_set(cx, LIST, id(&g, &t))).unwrap();
        assert_eq!(on, [id(&g, &l1)], "the listitem's list is the listblock's subject");
        let (subs, _) = g.read(|cx| g.out_list(cx, LISTBLOCK, &v, 10, None)).unwrap();
        assert_eq!(subs, vec![(listblock(&v, &tid(3)), l2.clone()), (listblock(&v, &tid(2)), l1.clone())]);
        assert_eq!(g.read(|cx| g.has(cx, LISTBLOCK, &v, &l1)).unwrap(), Some(listblock(&v, &tid(2))));
        assert_eq!(g.read(|cx| g.has(cx, MODLIST, &o, &l1)).unwrap(), Some(l1.clone()));
        assert_eq!(g.read(|cx| g.has(cx, MODLIST, &o, &l2)).unwrap(), None);
        assert_eq!(g.read(|cx| g.counts(cx, MODLIST, &l1)).unwrap(), (0, 1));
        let r = g.read(|cx| g.relationships(cx, &v, &[&l1, &l2, &t], &[LISTBLOCK, LIST])).unwrap();
        assert_eq!(r[0][0].out, Some(listblock(&v, &tid(2))), "a batch Has over list URIs");
        assert_eq!(r[1][0].out, Some(listblock(&v, &tid(3))));
        assert_eq!((r[2][0].out.as_deref(), r[0][1].inn.as_deref()), (None, None));

        // a list updated to a curation list is a delete of its modlist edge, and back an add
        assert_eq!(g.delete_at(&l1, 10).unwrap(), removed(&l1));
        assert_eq!(g.read(|cx| g.has(cx, MODLIST, &o, &l1)).unwrap(), None);
        assert_eq!(g.add_at(&l1, &l1, 11).unwrap(), Added::Added, "newer than the tombstone");
        assert_eq!(g.delete(&listblock(&v, &tid(2))).unwrap(), removed(&l1));
        assert_eq!(g.read(|cx| g.counts(cx, LISTBLOCK, &l1)).unwrap(), (0, 1));

        g.set_status(&o, Some(Status::Takendown), 0).unwrap();
        assert!(g.hidden(&l1), "a list subject hides with its owner");
        g.set_status(&o, None, 0).unwrap();
        assert_eq!(g.set_status(&w, Some(Status::Deleted), 20).unwrap(), 1, "an account's listblocks go with it");
        assert_eq!(g.read(|cx| g.counts(cx, LISTBLOCK, &l1)).unwrap(), (0, 0));
    }
}

/// A snapshot names the relation features it needs in its directory meta,
/// and that directory is format 7, which a reader of format 6 alone refuses.
/// A snapshot of the defaults names none and is format 6 with the meta keys
/// it always had. A feature or a format this build doesn't know is refused,
/// saying a newer build is needed.
#[test]
fn a_snapshot_requires_its_relation_features() {
    use marco_core::snapshot::{PagedFooter, PAGED_FORMAT, REQUIRES_FORMAT};
    use marco_core::store::DIR_FILE;
    let dir = tempfile::tempdir().unwrap();
    let footer = |snap: &std::path::Path| PagedFooter::read(&std::fs::read(snap.join(DIR_FILE)).unwrap()).unwrap();
    // the check a reader of format 6 alone makes of the directory meta, word for word
    let format_6_reader = |meta: &serde_json::Value| -> Result<(), String> {
        match meta["format"].as_u64() {
            Some(6) => Ok(()),
            f => Err(format!("snapshot format {}, this build reads 6: rebuild the snapshot", f.unwrap_or(0))),
        }
    };
    let plain = dir.path().join("plain");
    let mut g = graph();
    g.add(&follow(&did(1), &tid(1)), &did(2)).unwrap();
    g.write_snapshot(&plain).unwrap();
    let meta = footer(&plain).meta;
    let mut keys: Vec<&str> = meta.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort_unstable();
    let before = ["build", "format", "layout", "n_ids", "n_other", "n_plc", "relations", "shard_width", "subj_index"];
    assert_eq!(keys, before, "the keys the meta always had, and no requires");
    assert_eq!((meta["format"].as_u64(), PAGED_FORMAT), (Some(6), 6));
    assert_eq!(format_6_reader(&meta), Ok(()));

    let lists = dir.path().join("lists");
    let mut g = Graph::new(list_subjects());
    g.shards = 3;
    let l = list_uri(&did(1), 1);
    g.add_at(&listblock(&did(2), &tid(2)), &l, 2).unwrap();
    g.add_at(&l, &l, 3).unwrap();
    g.write_snapshot(&lists).unwrap();
    let f = footer(&lists);
    assert_eq!(f.meta["requires"], serde_json::json!(["record_subjects", "when"]));
    assert_eq!((f.meta["format"].as_u64(), REQUIRES_FORMAT), (Some(7), 7));
    assert_eq!(f.format, 6, "the file's own format stays 6");
    assert_eq!(
        format_6_reader(&f.meta),
        Err("snapshot format 7, this build reads 6: rebuild the snapshot".into()),
        "a reader of format 6 refuses it"
    );
    let g = Graph::open(&lists).unwrap();
    assert_eq!(g.read(|cx| g.has(cx, MODLIST, &did(1), &l)).unwrap(), Some(l.clone()));
    drop(g);

    // the same directory, its meta edited as a newer build might write it
    let p = lists.join(DIR_FILE);
    let original = std::fs::read(&p).unwrap();
    let rewrite = |edit: &dyn Fn(&mut serde_json::Value)| {
        let mut f = PagedFooter::read(&original).unwrap();
        let end = f.sections.iter().map(|s| s.off + s.len()).max().unwrap();
        edit(&mut f.meta);
        let mut out = original[..end as usize].to_vec();
        out.extend(f.encode(end).unwrap());
        std::fs::write(&p, out).unwrap();
        format!("{:#}", Graph::open(&lists).err().expect("refused"))
    };
    let e = rewrite(&|m| m["requires"] = serde_json::json!(["record_subjects", "when", "someday"]));
    assert!(e.contains("\"someday\"") && e.contains("doesn't know") && e.contains("newer build"), "{e}");
    let e = rewrite(&|m| m["format"] = serde_json::json!(8));
    assert!(e.contains("format 8 is newer than this build reads (6 or 7)") && e.contains("newer build"), "{e}");
    let e = rewrite(&|m| m["format"] = serde_json::json!(5));
    assert!(e.contains("format 5, this build reads 6 or 7: rebuild the snapshot"), "{e}");
}

#[test]
fn a_graph_with_fewer_ids_than_shards_reopens() {
    let mut g = Graph::new(Relations::bsky_default());
    g.add(&follow(&did(1), &tid(1)), &did(2)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("g");
    g.write_snapshot(&p).unwrap();
    let back = Graph::open(&p).unwrap();
    assert_eq!(back.read(|cx| back.counts(cx, FOLLOW, &did(1))).unwrap(), (1, 0));
}

/// Two sources: the live tail saw create x at 10 and update y at 20; a
/// backfill hands over the create at 10 afterwards, through a checkpoint too.
#[test]
fn a_late_older_add_leaves_a_newer_update() {
    let (a, x, y) = (did(1), did(2), did(3));
    let u = follow(&a, &tid(1));
    for snapshot_between in [false, true] {
        let mut g = graph();
        g.add_at(&u, &x, 10).unwrap();
        assert_eq!(g.add_at(&u, &y, 20).unwrap(), Added::Added);
        if snapshot_between {
            g = reopen(&g);
        }
        g.set_source("backfill");
        assert_eq!(g.add_at(&u, &x, 10).unwrap(), Added::Exists, "older than the update");
        assert_eq!(g.read(|cx| g.has(cx, FOLLOW, &a, &y)).unwrap(), Some(u.clone()));
        assert_eq!(g.add_at(&u, &x, 30).unwrap(), Added::Added, "a newer one still lands");
    }
}

/// A delete at 10, a re-create z at 15, then an update y at 12 from a slower
/// source: z holds.
#[test]
fn an_update_older_than_a_re_create_leaves_it() {
    let (a, x, y, z) = (did(1), did(2), did(3), did(4));
    let u = follow(&a, &tid(1));
    for snapshot_between in [false, true] {
        let mut g = graph();
        g.add_at(&u, &x, 1).unwrap();
        g.delete_at(&u, 10).unwrap();
        assert_eq!(g.add_at(&u, &y, 5).unwrap(), Added::Deleted);
        assert_eq!(g.add_at(&u, &z, 15).unwrap(), Added::Added);
        if snapshot_between {
            g = reopen(&g);
        }
        g.set_source("slower");
        assert_eq!(g.add_at(&u, &y, 12).unwrap(), Added::Exists);
        assert_eq!(
            (g.read(|cx| g.has(cx, FOLLOW, &a, &z)).unwrap(), g.read(|cx| g.counts(cx, FOLLOW, &a)).unwrap()),
            (Some(u.clone()), (1, 0))
        );
    }
}

/// The live tail saw a good record at 20; a backfill hands over an earlier
/// malformed create at 10, which ingest maps to a delete. A replaced or
/// re-created key keeps its stamp through a checkpoint; a plain create's
/// lives until the next one.
#[test]
fn a_late_older_delete_leaves_a_newer_record() {
    let (a, x, y) = (did(1), did(2), did(3));
    let u = follow(&a, &tid(1));
    let mut g = graph();
    g.set_source("live");
    g.add_at(&u, &y, 20).unwrap();
    g.set_source("backfill");
    assert_eq!(g.delete_at(&u, 10).unwrap(), Removed::NotFound);
    assert_eq!(g.read(|cx| g.has(cx, FOLLOW, &a, &y)).unwrap(), Some(u.clone()));
    let v = follow(&a, &tid(2));
    g.set_source("live");
    g.add_at(&v, &x, 5).unwrap();
    g.add_at(&v, &y, 20).unwrap();
    let mut g = reopen(&g);
    g.set_source("backfill");
    assert_eq!(g.delete_at(&v, 10).unwrap(), Removed::NotFound, "a replaced key's stamp survives the snapshot");
    assert_eq!(g.delete_at(&v, 20).unwrap(), removed(&y), "a delete wins a tie");
}

#[test]
fn replace_keeps_counts_and_distinct_members_right() {
    let (o, x, y, z) = (did(1), did(2), did(3), did(4));
    let l = list_uri(&o, 1);
    for snapshot_between in [false, true] {
        let mut g = graph();
        g.add_rec(&listitem(&o, &tid(1)), &x, Some(&l), 1).unwrap();
        g.add_rec(&listitem(&o, &tid(2)), &y, Some(&l), 1).unwrap();
        if snapshot_between {
            g = reopen(&g);
        }
        let distinct = |g: &Graph| g.read(|cx| g.out_distinct(cx, LIST, g.id_of(&l).unwrap())).unwrap();
        g.add_rec(&listitem(&o, &tid(2)), &x, Some(&l), 2).unwrap();
        assert_eq!(
            (
                g.read(|cx| g.counts(cx, LIST, &l)).unwrap(),
                distinct(&g),
                g.read(|cx| g.counts(cx, LIST, &x)).unwrap().1,
                g.read(|cx| g.counts(cx, LIST, &y)).unwrap().1
            ),
            ((2, 0), 1, 1, 0)
        );
        assert_eq!(distinct(&reopen(&g)), 1);
        g.add_rec(&listitem(&o, &tid(2)), &z, Some(&l), 3).unwrap();
        assert_eq!(
            (
                g.read(|cx| g.counts(cx, LIST, &l)).unwrap(),
                distinct(&g),
                g.read(|cx| g.counts(cx, LIST, &z)).unwrap().1
            ),
            ((2, 0), 2, 1)
        );
        let (first, _) = g.read(|cx| g.out_list_opts(cx, LIST, &l, 1, None, true)).unwrap();
        let (rest, _) = g.read(|cx| g.out_list_opts(cx, LIST, &l, 1, Some(&tid(2)), true)).unwrap();
        assert_eq!(first.len() + rest.len(), 2);
        assert_eq!(g.delete(&listitem(&o, &tid(2))).unwrap(), removed(&z));
        assert_eq!((distinct(&g), g.records(LIST).0, g.records(LIST_OWNER).0), (1, 1, 1));
        let g3 = reopen(&g);
        assert_eq!((distinct(&g3), g3.records(LIST).0, g3.records(LIST_OWNER).0), (1, 1, 1));
    }
}

/// An account's deletion removes its list items one by one, and each asks
/// whether another of the owner's items still names that list. The walk that
/// answers stops at the first live one, so it must read only the blocks it
/// reaches: reading the list's whole range first makes the deletion quadratic.
#[test]
fn an_owners_deletion_costs_a_few_lookups_per_item_at_any_list_size() {
    for n in [4096u32, 32_768] {
        let o = did(1);
        let l = list_uri(&o, 1);
        let mut g = graph();
        for k in 0..n {
            g.add_rec(&listitem(&o, &tid(k as u64)), &did(10 + k), Some(&l), 0).unwrap();
        }
        let mut g = reopen(&g);
        let lookups = |g: &Graph| g.engine().stats().lookups.iter().flatten().sum::<u64>();
        let before = lookups(&g);
        assert_eq!(g.set_status(&o, Some(Status::Deleted), 1).unwrap(), n as u64);
        let per_item = (lookups(&g) - before) as f64 / n as f64;
        assert!(per_item <= 16.0, "{n} items: {per_item:.1} lookups per item");
        assert_eq!(g.read(|cx| g.counts(cx, LIST, &l)).unwrap(), (0, 0));
        assert_eq!(g.read(|cx| g.counts(cx, LIST, &did(10))).unwrap(), (0, 0));
    }
}

/// Lists as `recs` ((owner, rkey) to (list, member)) has them: totals,
/// owners, lists and members.
fn check_lists(g: &Graph, recs: &BTreeMap<(u32, u64), (u32, u32)>, owners: u32, lists: u32, members: u32, at: &str) {
    let list_members: BTreeSet<(u32, u32, u32)> = recs.iter().map(|(k, v)| (k.0, v.0, v.1)).collect();
    let owner_lists: BTreeSet<(u32, u32)> = recs.iter().map(|(k, v)| (k.0, v.0)).collect();
    let n = recs.len() as u64;
    assert_eq!(g.records(LIST), (n, list_members.len() as u64), "{at}: list totals");
    assert_eq!(g.records(LIST_OWNER), (n, owner_lists.len() as u64), "{at}: owner totals");
    for o in 0..owners {
        let items = recs.keys().filter(|k| k.0 == o).count() as u64;
        assert_eq!(g.read(|cx| g.counts(cx, LIST_OWNER, &did(o))).unwrap(), (items, 0), "{at}: owner {o}");
        let oid = g.id_of(&did(o)).unwrap();
        let distinct = owner_lists.iter().filter(|x| x.0 == o).count() as u64;
        assert_eq!(g.read(|cx| g.out_distinct(cx, LIST_OWNER, oid)).unwrap(), distinct, "{at}: owner {o}'s lists");
        for l in 0..lists {
            let lu = list_uri(&did(o), l);
            let items = recs.iter().filter(|(k, v)| k.0 == o && v.0 == l).count() as u64;
            assert_eq!(g.read(|cx| g.counts(cx, LIST, &lu)).unwrap(), (items, 0), "{at}: list {o}/{l}");
            let owned = u64::from(items > 0);
            assert_eq!(g.read(|cx| g.counts(cx, LIST_OWNER, &lu)).unwrap(), (0, owned), "{at}: list {o}/{l}'s owner");
            let lid = g.id_of(&lu).unwrap();
            let on: BTreeSet<u32> = list_members.iter().filter(|x| x.0 == o && x.1 == l).map(|x| x.2).collect();
            assert_eq!(g.read(|cx| g.out_distinct(cx, LIST, lid)).unwrap(), on.len() as u64, "{at}: list {o}/{l}");
            for m in 10..10 + members {
                let has = g.read(|cx| g.has(cx, LIST, &lu, &did(m))).unwrap().is_some();
                assert_eq!(has, on.contains(&m), "{at}: list {o}/{l} has {m}");
            }
        }
    }
    for m in 10..10 + members {
        let lists_on = list_members.iter().filter(|x| x.2 == m).count() as u64;
        assert_eq!(g.read(|cx| g.counts(cx, LIST, &did(m))).unwrap(), (0, lists_on), "{at}: member {m}");
    }
}

/// An owner's deletion over paged lists (past WHOLE, with by-subject blocks)
/// whose members repeat within a list and across lists, after deletes and
/// adds over the base: the graph reads as a plain model of the records says,
/// with lists merged inside the write and without, and again from a snapshot.
#[test]
fn an_owners_deletion_over_paged_lists_matches_a_model() {
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    let mut rnd = |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    let (owners, lists, members, base) = (3u32, 2u32, 40u32, 600u64);
    let doomed = 1;
    for compact_min in [0, 4] {
        let mut recs: BTreeMap<(u32, u64), (u32, u32)> = BTreeMap::new();
        let mut g = graph();
        g.set_subj_index(8);
        let add = |g: &mut Graph, o: u32, k: u64, (l, m): (u32, u32), t: i64| {
            g.add_rec(&listitem(&did(o), &tid(k)), &did(m), Some(&list_uri(&did(o), l)), t).unwrap()
        };
        for o in 0..owners {
            for k in 0..base {
                let v = (rnd(lists.into()) as u32, 10 + rnd(members.into()) as u32);
                assert_eq!(add(&mut g, o, k, v, 0), Added::Added);
                recs.insert((o, k), v);
            }
        }
        let mut g = reopen(&g);
        g.compact_min = compact_min;
        check_lists(&g, &recs, owners, lists, members, "base");
        for step in 0..400 {
            let o = rnd(owners.into()) as u32;
            if step % 3 == 0 {
                // a new rkey, each once
                let k = base + step;
                let v = (rnd(lists.into()) as u32, 10 + rnd(members.into()) as u32);
                assert_eq!(add(&mut g, o, k, v, 20), Added::Added);
                recs.insert((o, k), v);
            } else {
                let k = rnd(base);
                let got = g.delete_at(&listitem(&did(o), &tid(k)), 10).unwrap();
                let want = recs.remove(&(o, k)).map_or(Removed::NotFound, |(_, m)| removed(&did(m)));
                assert_eq!(got, want, "compact_min {compact_min} step {step}");
            }
        }
        let at = format!("compact_min {compact_min}");
        check_lists(&g, &recs, owners, lists, members, &format!("{at}, deltas"));
        let n = recs.keys().filter(|k| k.0 == doomed).count() as u64;
        assert_eq!(g.set_status(&did(doomed), Some(Status::Deleted), 30).unwrap(), n);
        recs.retain(|k, _| k.0 != doomed);
        // shown again, so the reads count what the deletion left
        g.set_status(&did(doomed), None, 30).unwrap();
        check_lists(&g, &recs, owners, lists, members, &format!("{at}, owner deleted"));
        let g = reopen(&g);
        check_lists(&g, &recs, owners, lists, members, &format!("{at}, reopened"));
    }
}

/// The archive's worst list is 640k records for 119 members: a distinct page
/// and Has stay well under a second on it.
#[test]
fn a_distinct_page_of_a_list_of_repeats_is_quick() {
    let o = did(1);
    let l = list_uri(&o, 1);
    let mut g = graph();
    let (n, members) = (640_000u64, 119u32);
    for k in 0..n {
        let m = did(10 + (k as u32).wrapping_mul(7919) % members);
        g.add_rec(&listitem(&o, &tid(k)), &m, Some(&l), 0).unwrap();
    }
    let g = reopen(&g);
    let t = std::time::Instant::now();
    let (page, next) = g.read(|cx| g.out_list_opts(cx, LIST, &l, 50, None, true)).unwrap();
    let (rest, _) = g.read(|cx| g.out_list_opts(cx, LIST, &l, 100, next.as_deref(), true)).unwrap();
    assert_eq!(page.len() + rest.len(), members as usize);
    assert!(g.read(|cx| g.has(cx, LIST, &l, &did(10))).unwrap().is_some());
    assert!(t.elapsed() < std::time::Duration::from_millis(500), "{:?}", t.elapsed());
}

/// One source, in order: a create stamped 100, then its delete stamped 99 (the
/// source's clock stepped back). The source's own delete applies.
#[test]
fn a_sources_own_delete_applies_behind_its_create_by_clock_skew() {
    let (a, y) = (did(1), did(3));
    let u = follow(&a, &tid(1));
    let mut g = graph();
    g.add_at(&u, &y, 100).unwrap();
    assert_eq!(g.delete_at(&u, 99).unwrap(), removed(&y));
    let v = follow(&a, &tid(2));
    g.add_at(&v, &y, 100).unwrap();
    g.add_at(&v, &did(4), 110).unwrap();
    assert_eq!(g.add_at(&v, &did(5), 105).unwrap(), Added::Added, "its own update, whatever its clock");
}

/// An account's deletion takes every record, even ones stamped after it.
#[test]
fn an_account_deletion_takes_records_stamped_after_it() {
    let (a, y) = (did(1), did(3));
    let mut g = graph();
    g.add_at(&follow(&a, &tid(1)), &y, 100).unwrap();
    g.add_at(&follow(&a, &tid(2)), &y, 50).unwrap();
    g.set_source("relay");
    g.set_status(&a, Some(Status::Deleted), 99).unwrap();
    assert_eq!(
        (g.read(|cx| g.counts(cx, FOLLOW, &a)).unwrap(), g.read(|cx| g.counts(cx, FOLLOW, &y)).unwrap()),
        ((0, 0), (0, 0))
    );
}

#[test]
fn the_stamp_section_is_checksummed() {
    let (a, x, y) = (did(1), did(2), did(3));
    let u = follow(&a, &tid(1));
    let mut g = graph();
    g.add_at(&u, &x, 10).unwrap();
    g.add_at(&u, &y, 20).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s");
    g.write_snapshot(&p).unwrap();
    let g = Graph::open(&p).unwrap();
    let st = g.snapshot().unwrap();
    let (name, r) = st
        .shards
        .iter()
        .flatten()
        .find_map(|f| {
            let r = f.rels[FOLLOW as usize].stamp.clone();
            (!r.is_empty()).then(|| (f.name.clone(), r))
        })
        .expect("a stamp section with the replace");
    st.verify_checksums().unwrap();
    drop(g);
    let fp = p.join(&name);
    let mut b = std::fs::read(&fp).unwrap();
    b[r.off as usize] ^= 1;
    std::fs::write(&fp, &b).unwrap();
    let err = open_and_verify(&p);
    assert!(err.contains("r0.stamp checksum"), "{err}");
}

/// Stamps past the TTL go at a snapshot or checkpoint; newer ones stay. A
/// plain create's stamp isn't written.
#[test]
fn stamps_follow_the_tombstone_ttl_and_checkpoints() {
    let now = marco_core::store::now_ms() * 1000;
    let (a, x, y) = (did(1), did(2), did(3));
    let (old, new, plain) = (follow(&a, &tid(1)), follow(&a, &tid(2)), follow(&a, &tid(3)));
    let mut g = graph();
    g.tombstone_ttl_us = 3_600_000_000;
    g.add_at(&old, &x, now - 7_200_000_000).unwrap();
    g.add_at(&old, &y, now - 7_100_000_000).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s");
    g.write_snapshot(&p).unwrap();
    let mut g = Graph::open(&p).unwrap();
    g.tombstone_ttl_us = 3_600_000_000;
    let id = g.id_of(&a).unwrap();
    let key = |u: &str| key_of(u.rsplit('/').next().unwrap()).0;
    assert_eq!(g.stamp(FOLLOW, id, key(&old)), None, "past the TTL");
    g.add_at(&new, &x, now - 60_000_000).unwrap();
    g.add_at(&new, &y, now - 50_000_000).unwrap();
    g.add_at(&plain, &x, now - 40_000_000).unwrap();
    g.write_checkpoint(&p, 1, 1, BTreeMap::new()).unwrap();
    let g = Graph::open(&p).unwrap();
    g.snapshot().unwrap().verify_checksums().unwrap();
    assert_eq!(g.stamp(FOLLOW, id, key(&new)), Some(now - 50_000_000));
    assert_eq!(g.stamp(FOLLOW, id, key(&plain)), None);
}

/// An actor and which of the others its side holds.
type Keep = (u32, fn(u32) -> bool);

/// combine_counted on lists long enough for AND_NOT to merge in several
/// chunks and for an out side to be probed, sides of both directions on
/// either end, cursors in and out of the result, in the delta and the base.
#[test]
fn combine_counted_on_long_lists() {
    const N: u32 = 6000;
    let mut g = graph();
    let mut edges: BTreeSet<(u32, u32)> = BTreeSet::new();
    let outs: [Keep; 3] = [(0, |_| true), (1, |j| j % 600 != 0), (2, |j| j % 3 == 0)];
    for (a, keep) in outs {
        edges.extend((3..N).filter(|&j| keep(j)).map(|j| (a, j)));
    }
    let ins: [Keep; 3] = [(5000, |j| (10..3000).contains(&j)), (5001, |j| j % 7 == 0), (5002, |j| j % 60 == 0)];
    for (s, keep) in ins {
        edges.extend((3..N).filter(|&j| keep(j) && j != s).map(|j| (j, s)));
    }
    for (k, &(f, t)) in edges.iter().enumerate() {
        g.add(&follow(&did(f), &tid(k as u64)), &did(t)).unwrap();
    }
    let mut sets: Vec<(u32, Dir, Vec<u32>)> = Vec::new();
    for (a, _) in outs {
        sets.push((a, Dir::Out, edges.iter().filter(|e| e.0 == a).map(|e| e.1).collect()));
    }
    for (s, _) in ins {
        sets.push((s, Dir::In, edges.iter().filter(|e| e.1 == s).map(|e| e.0).collect()));
    }
    let mut exact_over_1000 = false;
    for g in [&g.clone_via_snapshot(), &g] {
        let ids = |v: &[u32]| -> Vec<u32> {
            let mut x: Vec<u32> = v.iter().map(|&i| g.id_of(&did(i)).unwrap()).collect();
            x.sort_unstable();
            x
        };
        let sides: Vec<(ListRef, Vec<u32>)> = sets
            .iter()
            .map(|(x, d, v)| (ListRef { rel: FOLLOW, dir: *d, actor: g.id_of(&did(*x)).unwrap() }, ids(v)))
            .collect();
        for (a, av) in &sides {
            for (b, bv) in &sides {
                if a.actor == b.actor {
                    continue;
                }
                for op in [SetOp::And, SetOp::AndNot] {
                    let want: Vec<u32> =
                        av.iter().copied().filter(|x| bv.binary_search(x).is_ok() == (op == SetOp::And)).collect();
                    let mut cursors = vec![None, want.get(want.len() / 3).copied()];
                    // ids that aren't in the result, past its start
                    cursors.extend(
                        [7, 2999, 4321]
                            .iter()
                            .filter_map(|&k| g.id_of(&did(k)))
                            .filter(|c| want.binary_search(c).is_err())
                            .map(Some),
                    );
                    for after in cursors {
                        for (limit, cap) in [
                            (5, 0),
                            (5, 1000),
                            (300, 2000),
                            (2000, 10),
                            (1, COUNT_EXACT),
                            (5, COUNT_EXACT),
                            (300, COUNT_EXACT),
                        ] {
                            let (page, total) =
                                g.read(|cx| g.combine_counted(cx, *a, *b, op, limit, after, cap)).unwrap();
                            let rest: Vec<u32> =
                                want.iter().copied().filter(|&x| after.is_none_or(|y| x > y)).take(limit).collect();
                            assert_eq!(page, rest, "{a:?} {op:?} {b:?} after {after:?} limit {limit}");
                            let n = want.len();
                            assert_eq!(
                                total,
                                (cap > 0).then(|| (n.min(cap) as u64, n > cap)),
                                "{a:?} {op:?} {b:?} cap {cap}"
                            );
                            exact_over_1000 |= cap == COUNT_EXACT && n > 1000 && total == Some((n as u64, false));
                        }
                    }
                }
            }
        }
    }
    assert!(exact_over_1000, "some pair has an exact total past 1,000");
}
