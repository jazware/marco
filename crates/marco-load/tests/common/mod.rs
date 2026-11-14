//! What the scan tests share: synthetic build input, the binary, configs.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

pub fn bin() -> PathBuf {
    // set when cargo builds the test, not when clippy checks it
    match option_env!("CARGO_BIN_EXE_marco-load") {
        Some(p) => PathBuf::from(p),
        // cargo keeps a test's executable in the profile dir (deps/), or under build/ since 1.100,
        // and the binary is in the profile dir either way
        None => std::env::current_exe()
            .unwrap()
            .ancestors()
            .map(|d| d.join("marco-load"))
            .find(|p| p.is_file())
            .expect("marco-load built beside the test"),
    }
}

pub struct Rng(pub u64);

impl Rng {
    pub fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n.max(1)
    }
}

pub fn did(i: u64) -> String {
    if i % 97 == 96 {
        return format!("did:web:host{i}.example");
    }
    let alphabet = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut v = i.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x2545_f491_4f6c_dd1d;
    let s: String = (0..24)
        .map(|_| {
            let c = alphabet[(v & 31) as usize] as char;
            v = v.rotate_right(5).wrapping_mul(0x5bd1_e995) ^ 0x9e37;
            c
        })
        .collect();
    format!("did:plc:{s}")
}

pub fn tid(us: u64, clock: u64) -> String {
    vlatproto::tid::Tid::from_parts(us, clock).to_string()
}

pub type Op = (String, String, String, i64, u8);

/// Ops for one relation, written as build input bucketed by repo DID prefix.
/// Returns the duplicate records (a second live record naming the same subject).
pub fn write_input(dir: &Path, rel: &str, n: u64, actors: u64, seed: u64) -> u64 {
    let mut rng = Rng(seed | 1);
    let mut ops: Vec<Op> = Vec::new();
    for t in 0..n {
        let a = did(rng.below(actors));
        let rkey = match rng.below(200) {
            0 => format!("m{}", rng.below(100_000)),
            _ => tid(1_700_000_000_000_000 + rng.below(1 << 40), rng.below(1024)),
        };
        let (s, op) = match rng.below(20) {
            0..=1 => (String::new(), 3),
            _ => (did(rng.below(actors * 2)), 1),
        };
        ops.push((a, rkey, s, 1_000 + t as i64, op));
    }
    // a second record for an existing pair
    for i in 0..n / 500 {
        let a = did(i % actors);
        ops.push((a.clone(), tid(1_800_000_000_000_000 + i, 1), did(i + 7), 10, 1));
        ops.push((a, tid(1_800_000_000_000_000 + i, 2), did(i + 7), 11, 1));
    }
    let mut latest: BTreeMap<(String, String), (i64, u8, String)> = BTreeMap::new();
    for (a, k, s, us, op) in &ops {
        let e = latest.entry((a.clone(), k.clone())).or_insert((*us, *op, s.clone()));
        if (*us, *op) >= (e.0, e.1) {
            *e = (*us, *op, s.clone());
        }
    }
    let mut pairs: HashMap<(String, String), u64> = HashMap::new();
    for ((a, _), (_, op, s)) in &latest {
        if *op != 3 {
            *pairs.entry((a.clone(), s.clone())).or_default() += 1;
        }
    }
    let dupes = pairs.values().map(|&k| k - 1).sum();
    write_files(dir, rel, ops.iter().map(|(a, k, s, us, op)| (vec![a.as_str(), k, s], *us, *op)));
    dupes
}

/// Rows (string columns, time_us, op) as build input files, bucketed by the
/// first column, the repo.
pub fn write_files<'a>(dir: &Path, rel: &str, rows: impl Iterator<Item = (Vec<&'a str>, i64, u8)>) {
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for (cols, us, op) in rows {
        let plc = cols[0].strip_prefix("did:plc:").filter(|r| r.len() == 24);
        let name = plc.map_or("z-other".to_string(), |r| format!("p-{}", &r[..2]));
        let out = files.entry(name).or_default();
        for x in cols {
            let mut l = x.len() as u64;
            loop {
                let b = (l & 0x7f) as u8;
                l >>= 7;
                out.push(if l > 0 { b | 0x80 } else { b });
                if l == 0 {
                    break;
                }
            }
            out.extend_from_slice(x.as_bytes());
        }
        out.extend_from_slice(&us.to_le_bytes());
        out.push(op);
    }
    std::fs::create_dir_all(dir.join(rel)).unwrap();
    for (name, raw) in files {
        std::fs::write(dir.join(rel).join(format!("{name}.rb.zst")), zstd::encode_all(&raw[..], 1).unwrap()).unwrap();
    }
}

pub fn list_uri(owner: &str, l: &str) -> String {
    format!("at://{owner}/app.bsky.graph.list/{l}")
}

/// What a listitem input holds once reduced to current state.
pub struct ListInput {
    pub owners: Vec<String>,
    /// Every list an item was written to, the other repos' ones included.
    pub lists: BTreeSet<String>,
    /// Live items whose list is in another repo: the scan and the build drop them.
    pub cross_repo: u64,
    /// One owner's list with many members, and its live items.
    pub big: String,
    pub big_records: u64,
    /// A subject only cross-repo items name: on no list after a build.
    pub stray: String,
    /// Subjects to look up the lists of.
    pub subjects: Vec<String>,
}

/// listitem ops: owners with several lists each, deletes, items moved to
/// another list under one rkey, items naming a list in another repo, and
/// one list of `n / 10` members.
pub fn write_list_input(dir: &Path, rel: &str, n: u64, owners: u64, seed: u64) -> ListInput {
    let mut rng = Rng(seed | 1);
    let stray = did(9_000_000);
    // (repo, rkey, subject, list, time_us, op)
    let mut ops: Vec<(String, String, String, String, i64, u8)> = Vec::new();
    let rkey = |rng: &mut Rng| tid(1_700_000_000_000_000 + rng.below(1 << 40), rng.below(1024));
    for t in 0..n {
        let a = did(rng.below(owners));
        let k = rkey(&mut rng);
        let (s, l, op) = match rng.below(40) {
            0..=3 => (String::new(), String::new(), 3),
            // repos past `owners` own no items, so never the item's own
            4 => {
                let s = if rng.below(4) == 0 { stray.clone() } else { did(rng.below(owners * 3)) };
                (s, list_uri(&did(owners + rng.below(owners)), "l0"), 1)
            }
            _ => (did(rng.below(owners * 3)), list_uri(&a, &format!("l{}", rng.below(4))), 1),
        };
        ops.push((a, k, s, l, 1_000 + t as i64, op));
    }
    for i in 0..n / 1000 {
        let a = did(i % owners);
        let k = tid(1_800_000_000_000_000 + i, 3);
        ops.push((a.clone(), k.clone(), did(i), list_uri(&a, "l0"), 10, 1));
        ops.push((a.clone(), k, did(i), list_uri(&a, "l1"), 11, 2));
    }
    let owner = did(0);
    let big = list_uri(&owner, "big");
    for i in 0..n / 10 {
        ops.push((owner.clone(), tid(1_900_000_000_000_000 + i, 0), did(1_000_000 + i), big.clone(), 5, 1));
    }
    let mut latest: BTreeMap<(String, String), (i64, u8, String, String)> = BTreeMap::new();
    for (a, k, s, l, us, op) in &ops {
        let e = latest.entry((a.clone(), k.clone())).or_insert((*us, *op, s.clone(), l.clone()));
        if (*us, *op) >= (e.0, e.1) {
            *e = (*us, *op, s.clone(), l.clone());
        }
    }
    let (mut cross_repo, mut big_records) = (0, 0);
    for ((a, _), (_, op, _, l)) in &latest {
        if *op == 3 {
            continue;
        }
        if !l.starts_with(&format!("at://{a}/")) {
            cross_repo += 1;
        }
        if *l == big {
            big_records += 1;
        }
    }
    write_files(dir, rel, ops.iter().map(|(a, k, s, l, us, op)| (vec![a.as_str(), k, s, l], *us, *op)));
    ListInput {
        owners: (0..owners).map(did).collect(),
        lists: ops.iter().filter(|o| !o.3.is_empty()).map(|o| o.3.clone()).collect(),
        cross_repo,
        big,
        big_records,
        stray,
        subjects: (0..owners * 3).map(did).chain([did(1_000_000), did(1_000_000 + n / 20)]).collect(),
    }
}

/// The rebuilt snapshot answers list queries as the direct build does:
/// each list's members, each owner's items, the lists a DID is on, counts.
pub fn check_lists(direct: &Path, snap: &Path, li: &ListInput) {
    let (a, b) = (marco_core::Graph::open(direct).unwrap(), marco_core::Graph::open(snap).unwrap());
    let ids = |g: &marco_core::Graph| {
        let r = g.relations();
        (r.by_name("listitem").unwrap(), r.by_name("listitem.owner").unwrap())
    };
    let ((la, oa), (lb, ob)) = (ids(&a), ids(&b));
    let all = 10_000_000;
    let mut lists_with_members = 0;
    for l in &li.lists {
        assert_eq!(a.read(|cx| a.counts(cx, la, l)).unwrap(), b.read(|cx| b.counts(cx, lb, l)).unwrap(), "{l}");
        assert_eq!(
            a.read(|cx| a.out_list(cx, la, l, all, None)).unwrap().0,
            b.read(|cx| b.out_list(cx, lb, l, all, None)).unwrap().0,
            "members of {l}"
        );
        lists_with_members += (b.read(|cx| b.counts(cx, lb, l)).unwrap().0 > 0) as u64;
    }
    let mut owners_of_two = 0;
    for o in &li.owners {
        assert_eq!(a.read(|cx| a.counts(cx, oa, o)).unwrap(), b.read(|cx| b.counts(cx, ob, o)).unwrap(), "{o}");
        let items = b.read(|cx| b.out_list(cx, ob, o, all, None)).unwrap().0;
        assert_eq!(a.read(|cx| a.out_list(cx, oa, o, all, None)).unwrap().0, items, "items of {o}");
        owners_of_two += (items.iter().map(|x| &x.1).collect::<BTreeSet<_>>().len() > 1) as u64;
    }
    for s in &li.subjects {
        let (mut x, mut y) = (
            a.read(|cx| a.in_list(cx, la, s, all, None)).unwrap().0,
            b.read(|cx| b.in_list(cx, lb, s, all, None)).unwrap().0,
        );
        x.sort();
        y.sort();
        assert_eq!(x, y, "the lists {s} is on");
    }
    assert_eq!(b.read(|cx| b.counts(cx, lb, &li.big)).unwrap().0, li.big_records);
    assert!(li.big_records > 1000, "{}", li.big_records);
    assert_eq!(
        b.read(|cx| b.in_list(cx, ob, &li.big, all, None)).unwrap().0,
        vec![li.owners[0].clone()],
        "the big list's owner"
    );
    assert_eq!(
        b.read(|cx| b.in_list(cx, lb, &li.stray, all, None)).unwrap().0,
        Vec::<String>::new(),
        "a cross-repo item's subject is on no list"
    );
    assert!(li.cross_repo > 0 && lists_with_members > li.owners.len() as u64 && owners_of_two > 0);
}

pub fn run(args: &[&str]) -> Value {
    let out = Command::new(bin()).args(args).output().unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "marco-load {args:?} failed:\n{err}");
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&out.stdout)))
}

pub fn config(dir: &Path, node: &str, ks: &str) -> PathBuf {
    let p = dir.join(format!("{ks}.toml"));
    std::fs::write(
        &p,
        format!(
            r#"nodes = ["{node}"]
keyspace = "{ks}"
ranges_per_shard = 4
page_size = 1000

[[table]]
relation = "follow"
name = "follows"

[[table]]
relation = "block"
name = "blocks"

[[table]]
relation = "listitem"
name = "list_items"
subject = "subject"
"#
        ),
    )
    .unwrap();
    p
}

pub fn s(p: &Path) -> &str {
    p.to_str().unwrap()
}

pub fn diff(a: &Path, b: &Path) -> Value {
    run(&["diff", "--a", s(a), "--b", s(b)])
}

pub fn rel<'a>(d: &'a Value, name: &str) -> &'a Value {
    d["relations"].as_array().unwrap().iter().find(|r| r["relation"] == name).unwrap()
}

#[cfg(feature = "scylla")]
pub async fn drop_keyspaces(node: &str, names: &[String]) {
    let session = scylla::client::session_builder::SessionBuilder::new().known_node(node).build().await.unwrap();
    for k in names {
        session.query_unpaged(format!("DROP KEYSPACE IF EXISTS {k}"), ()).await.unwrap();
    }
}

/// A relations config of follow, block and listitem, the relations the input has.
pub fn relations(dir: &Path) -> PathBuf {
    let p = dir.join("relations.toml");
    std::fs::write(
        &p,
        "[[relation]]\nname = \"follow\"\ncollection = \"app.bsky.graph.follow\"\nsubject = \"subject\"\n\n\
         [[relation]]\nname = \"block\"\ncollection = \"app.bsky.graph.block\"\nsubject = \"subject\"\n\n\
         [[relation]]\nname = \"listitem\"\ncollection = \"app.bsky.graph.listitem\"\nsubject = \"subject\"\nlist = \"list\"\n",
    )
    .unwrap();
    p
}
