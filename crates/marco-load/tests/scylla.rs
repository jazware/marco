//! scan-scylla against a real Scylla: synthetic ops are built straight into
//! a snapshot, loaded into tables as current state (fill-scylla), scanned
//! back and built again; the two snapshots must hold the same records.
//! Also a scan killed mid-way and resumed, and the ((actor), subject) key.
//! The list items table holds items in several lists, a list of many
//! members, and items naming a list in another repo, which the scan drops.
//! Then list subscriptions and list purposes, from a table with a
//! subscription's list in a column and one with each list's record, where
//! the purpose is.
//!
//! Needs a Scylla: MARCO_SCYLLA=127.0.0.1:9042 cargo test -p marco-load --test scylla -- --ignored
//! MARCO_SCYLLA_OPS sets the ops written per relation (default 300000).
#![cfg(feature = "scylla")]

use std::process::Command;

use serde_json::Value;

mod common;
use common::*;

/// follow, block and listitem, then a list subscription (a list's URI as
/// the subject) and a list's purpose (its own URI, while it's a mod list).
const LIST_SUBJECT_RELATIONS: &str = r#"
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
"#;

/// A list record's JSON, as a table that stores the record keeps it.
fn list_record(purpose: &str) -> String {
    format!(
        r#"{{"$type":"app.bsky.graph.list","purpose":"{purpose}","name":"a list","createdAt":"2024-01-01T00:00:00.000Z"}}"#
    )
}

/// The list subscriptions table is a plain one whose `list` column is the
/// subject: no code of its own, the build checks the subject. The lists
/// table holds each list's record (JSON, a blob) and nothing says its
/// purpose but the record, so its table names a record column. Scanned and
/// built, both match a direct build of the same records, and the lists that
/// aren't mod lists, don't parse or are null are counted apart. A text
/// record column reads the same.
#[test]
#[ignore = "needs MARCO_SCYLLA=host:port"]
fn list_subscriptions_and_purposes_match_the_direct_build() {
    let node = std::env::var("MARCO_SCYLLA").expect("MARCO_SCYLLA=host:port");
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("input");
    write_input(&input, "follow", 20_000, 2_000, 1);
    write_input(&input, "block", 2_000, 400, 2);
    let li = write_list_input(&input, "listitem", 20_000, 200, 3);
    let mut rng = Rng(11);

    // subscriptions to the items' lists and to lists with no items, with
    // deletes and subjects listblock doesn't take: a DID, a record in another collection
    let lists: Vec<String> = li.lists.iter().cloned().chain((0..50).map(|o| list_uri(&did(o), "empty"))).collect();
    let mut subs: Vec<Op> = Vec::new();
    for t in 0..5_000 {
        let a = did(rng.below(1_000));
        let k = tid(1_700_000_000_000_000 + rng.below(1 << 40), rng.below(1024));
        let (s, op) = match rng.below(20) {
            0..=1 => (String::new(), 3),
            2 => (did(rng.below(1_000)), 1),
            3 => (format!("at://{}/app.bsky.graph.follow/3kaaaaaaaaa22", did(rng.below(200))), 1),
            _ => (lists[rng.below(lists.len() as u64) as usize].clone(), 1),
        };
        subs.push((a, k, s, 1_000 + t, op));
    }
    write_files(&input, "listblock", subs.iter().map(|(a, k, s, us, op)| (vec![a.as_str(), k, s], *us, *op)));

    // every owner's lists' records as the lists table holds them, and the
    // mod lists among them as the build's input
    let mut recs: Vec<(String, String, Option<String>)> = Vec::new();
    let mut mods: Vec<Op> = Vec::new();
    let (mut unmatched, mut bad, mut nulls) = (0u64, 0u64, 0u64);
    for (i, o) in li.owners.iter().enumerate() {
        let names = ["l0", "l1", "l2", "l3", "empty"].into_iter().chain((i == 0).then_some("big"));
        for l in names {
            let raw = match rng.below(12) {
                0..=6 => {
                    mods.push((o.clone(), l.into(), list_uri(o, l), 5, 1));
                    Some(list_record("app.bsky.graph.defs#modlist"))
                }
                7 | 8 => Some(list_record("app.bsky.graph.defs#curatelist")),
                9 => Some(list_record("app.bsky.graph.defs#referencelist")),
                10 => Some(r#"{"name":"no purpose"}"#.to_string()),
                _ if rng.below(2) == 0 => Some("{not json".to_string()),
                _ => None,
            };
            match raw.as_deref() {
                None => nulls += 1,
                Some(r) if r.starts_with("{not") => bad += 1,
                Some(r) if !r.contains("#modlist") => unmatched += 1,
                _ => {}
            }
            recs.push((o.clone(), l.to_string(), raw));
        }
    }
    assert!(unmatched > 0 && bad > 0 && nulls > 0 && mods.len() > 100);
    write_files(&input, "modlist", mods.iter().map(|(a, k, s, us, op)| (vec![a.as_str(), k, s], *us, *op)));

    let rels = tmp.path().join("relations.toml");
    std::fs::write(&rels, LIST_SUBJECT_RELATIONS).unwrap();
    let direct = tmp.path().join("direct");
    run(&["build", "--input", s(&input), "--out", s(&direct), "--relations", s(&rels)]);

    let ks = format!("marco_lists_{}", std::process::id());
    let head = format!("nodes = [\"{node}\"]\nkeyspace = \"{ks}\"\nranges_per_shard = 4\npage_size = 1000\n");
    let columns = "[[table]]\nrelation = \"follow\"\nname = \"follows\"\n\n\
                   [[table]]\nrelation = \"block\"\nname = \"blocks\"\n\n\
                   [[table]]\nrelation = \"listitem\"\nname = \"list_items\"\nsubject = \"subject\"\n\n\
                   [[table]]\nrelation = \"listblock\"\nname = \"listblocks\"\nsubject = \"list\"\n\n";
    let config = |name: &str, tables: &str| {
        let p = tmp.path().join(name);
        std::fs::write(&p, format!("{head}\n{tables}")).unwrap();
        p
    };
    let fill_cfg = config("fill.toml", columns);
    let f = run(&[
        "fill-scylla",
        "--config",
        s(&fill_cfg),
        "--input",
        s(&input),
        "--relations",
        s(&rels),
        "--create",
        "actor-rkey",
    ]);
    let lb_rows = f["tables"][3]["rows"].as_u64().unwrap();
    assert!(lb_rows > 3_000, "{f}");
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let session = scylla::client::session_builder::SessionBuilder::new().known_node(&node).build().await.unwrap();
        for (table, typ) in [("lists", "blob"), ("lists_text", "text")] {
            let q = format!(
                "CREATE TABLE {ks}.{table} (actor_did text, rkey text, raw {typ}, PRIMARY KEY ((actor_did), rkey))"
            );
            session.query_unpaged(q, ()).await.unwrap();
        }
        session.await_schema_agreement().await.unwrap();
        let blob =
            session.prepare(format!("INSERT INTO {ks}.lists (actor_did, rkey, raw) VALUES (?, ?, ?)")).await.unwrap();
        let text = session
            .prepare(format!("INSERT INTO {ks}.lists_text (actor_did, rkey, raw) VALUES (?, ?, ?)"))
            .await
            .unwrap();
        for (a, k, raw) in &recs {
            session.execute_unpaged(&blob, (a, k, raw.as_deref().map(str::as_bytes))).await.unwrap();
            session.execute_unpaged(&text, (a, k, raw.as_deref())).await.unwrap();
        }
    });

    // the five tables scanned and built: the same records as the direct build
    let scan_cfg = config(
        "scan.toml",
        &format!("{columns}[[table]]\nrelation = \"modlist\"\nname = \"lists\"\nrecord = \"raw\"\n"),
    );
    let snap = tmp.path().join("snap");
    let r = run(&[
        "scan-scylla",
        "--config",
        s(&scan_cfg),
        "--relations",
        s(&rels),
        "--out",
        s(&tmp.path().join("scan")),
        "--snapshot",
        s(&snap),
    ]);
    let (lb, ml) = (&r["tables"][3], &r["tables"][4]);
    assert_eq!((lb["relation"].as_str(), lb["rows"].as_u64()), (Some("listblock"), Some(lb_rows)), "{lb:#}");
    assert_eq!(ml["relation"], "modlist");
    let counts = |t: &Value| ["rows", "unmatched_rows", "bad_rows", "null_rows"].map(|k| t[k].as_u64().unwrap());
    assert_eq!(counts(ml), [mods.len() as u64, unmatched, bad, nulls], "{ml:#}");
    let d = diff(&direct, &snap);
    assert_eq!(d["equal"], true, "{d:#}");
    for name in ["listblock", "modlist"] {
        assert!(rel(&d, name)["matched"].as_u64().unwrap() > 0, "{name}: {d:#}");
    }
    check_lists(&direct, &snap, &li);
    // a subscription and the list's items meet on one id
    let g = marco_core::Graph::open(&snap).unwrap();
    let [rl, rb, rm] = ["listitem", "listblock", "modlist"].map(|n| g.relations().by_name(n).unwrap());
    let met = li.lists.iter().filter_map(|l| g.id_of(l)).any(|id| {
        let items = g.read(|cx| g.out_count(cx, rl, id)).unwrap();
        let subscribers = g.read(|cx| g.in_count(cx, rb, id)).unwrap();
        let owner = g.read(|cx| g.in_count(cx, rm, id)).unwrap();
        items > 0 && subscribers > 0 && owner > 0
    });
    assert!(met, "a list with items, a subscriber and its purpose");

    // the same records in a text column: the same rows and counts
    let text_cfg = config("text.toml", "[[table]]\nrelation = \"modlist\"\nname = \"lists_text\"\nrecord = \"raw\"\n");
    let t = run(&[
        "scan-scylla",
        "--config",
        s(&text_cfg),
        "--relations",
        s(&rels),
        "--out",
        s(&tmp.path().join("scan-text")),
    ]);
    assert_eq!(counts(&t["tables"][0]), counts(ml), "{t:#}");
    // the scan kept edges by modlist's `when`: a resume under another one is refused
    let curate = tmp.path().join("curate.toml");
    std::fs::write(&curate, std::fs::read_to_string(&rels).unwrap().replace("defs#modlist", "defs#curatelist"))
        .unwrap();
    let scan_text = tmp.path().join("scan-text");
    let again = |relations: &std::path::Path| {
        Command::new(bin())
            .args(["scan-scylla", "--config", s(&text_cfg), "--relations", s(relations), "--out", s(&scan_text)])
            .output()
            .unwrap()
    };
    let out = again(&curate);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success() && err.contains("other relation rules") && err.contains("--fresh"), "{err}");
    let out = again(&rels);
    assert!(out.status.success(), "the same rules resume: {}", String::from_utf8_lossy(&out.stderr));

    rt.block_on(drop_keyspaces(&node, &[ks]));
}

#[test]
#[ignore = "needs MARCO_SCYLLA=host:port"]
fn scan_then_build_matches_the_direct_build() {
    let node = std::env::var("MARCO_SCYLLA").expect("MARCO_SCYLLA=host:port");
    let n: u64 = std::env::var("MARCO_SCYLLA_OPS").ok().and_then(|v| v.parse().ok()).unwrap_or(300_000);
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("input");
    let dupes_f = write_input(&input, "follow", n, n / 40, 1);
    let dupes_b = write_input(&input, "block", n / 10, n / 200, 2);
    let li = write_list_input(&input, "listitem", n / 4, n / 200, 3);
    let direct = tmp.path().join("direct");
    let built = run(&[
        "build",
        "--input",
        s(&input),
        "--out",
        s(&direct),
        "--shards",
        "7",
        "--relations",
        s(&relations(tmp.path())),
    ]);
    let records: u64 = built["rels"].as_array().unwrap().iter().map(|r| r["out_records"].as_u64().unwrap()).sum();
    assert!(records > n / 2, "{built}");

    let pid = std::process::id();
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let mut keyspaces = Vec::new();
    // (primary key, tablets)
    for (i, (key, tablets)) in
        [("actor-rkey", None), ("actor-rkey", Some("false")), ("actor-subject", None)].into_iter().enumerate()
    {
        let ks = format!("marco_test_{pid}_{i}");
        keyspaces.push(ks.clone());
        let cfg = config(tmp.path(), &node, &ks);
        let mut fill =
            vec!["fill-scylla", "--config", s(&cfg), "--input", s(&input), "--create", key, "--concurrency", "256"];
        if let Some(t) = tablets {
            fill.extend(["--tablets", t]);
        }
        let f = run(&fill);
        eprintln!("{ks} {key} tablets={tablets:?}: filled {} rows in {:.1} s", f["rows"], f["secs"].as_f64().unwrap());

        let out = tmp.path().join(format!("scan-{i}"));
        let snap = tmp.path().join(format!("snap-{i}"));
        let before = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64;
        let r = run(&[
            "scan-scylla",
            "--config",
            s(&cfg),
            "--out",
            s(&out),
            "--snapshot",
            s(&snap),
            "--chunk-rows",
            "5000",
            "--checkpoint-secs",
            "1",
        ]);
        let mode = if tablets == Some("false") { "vnodes" } else { r["tables"][0]["mode"].as_str().unwrap() };
        assert_eq!(r["tables"][0]["mode"], mode);
        eprintln!(
            "  scanned {} rows in {:.2} s ({:.0} rows/s), {} ranges, {} replica shards, {} pieces, {}",
            r["rows"],
            r["scan_secs"].as_f64().unwrap(),
            r["run_rows_per_s"].as_f64().unwrap(),
            r["ranges"],
            r["replica_shards"],
            r["tables"][0]["pieces"],
            mode
        );
        assert!(!out.exists(), "--snapshot without --keep-spill removes the scan");
        let collapsed = if key == "actor-rkey" { 0 } else { dupes_f + dupes_b };
        assert_eq!(r["rows"].as_u64().unwrap(), f["rows"].as_u64().unwrap() - collapsed - li.cross_repo);
        let lt = &r["tables"][2];
        assert_eq!(
            (lt["relation"].as_str(), lt["primary_key"].as_str()),
            (Some("listitem"), Some("((actor_did), rkey)"))
        );
        assert_eq!(lt["cross_repo_rows"].as_u64().unwrap(), li.cross_repo, "{lt:#}");
        eprintln!(
            "  list items: {} rows, {} cross-repo dropped, spill {} B, {:.1} B a row",
            lt["rows"],
            lt["cross_repo_rows"],
            lt["spill_bytes"],
            lt["spill_bytes"].as_f64().unwrap() / lt["rows"].as_f64().unwrap()
        );
        // the cursor: the scan's start less the 5 min margin
        let cursor = r["cursor_us"].as_i64().unwrap();
        assert!(
            (before - 300_000_000 - 5_000_000..=before - 300_000_000 + 60_000_000).contains(&cursor),
            "{cursor} vs {before}"
        );
        let d = diff(&direct, &snap);
        assert_eq!(d["b_cursors"]["time_us"], cursor.to_string());
        let (fo, bl) = (rel(&d, "follow"), rel(&d, "block"));
        if key == "actor-rkey" {
            assert_eq!(d["equal"], true, "{d:#}");
        } else {
            // one row per (actor, subject): every extra record of a pair is gone, nothing else
            assert_eq!(fo["only_a"].as_u64().unwrap(), dupes_f, "{fo:#}");
            assert_eq!(bl["only_a"].as_u64().unwrap(), dupes_b, "{bl:#}");
            assert_eq!(fo["only_b"], 0);
            assert_eq!(bl["only_b"], 0);
            assert!(dupes_f > 0);
            for r in ["listitem", "listitem.owner"] {
                let x = rel(&d, r);
                assert_eq!((x["only_a"].as_u64(), x["only_b"].as_u64()), (Some(0), Some(0)), "{x:#}");
            }
        }
        check_lists(&direct, &snap, &li);
    }

    // killed after a few commits, then resumed: the same records, none twice
    let cfg = config(tmp.path(), &node, &keyspaces[0]);
    let out = tmp.path().join("scan-crash");
    let crashed = Command::new(bin())
        .args([
            "scan-scylla",
            "--config",
            s(&cfg),
            "--out",
            s(&out),
            "--chunk-rows",
            "2000",
            "--checkpoint-secs",
            "0.5",
            "--crash-after-commits",
            "40",
            "--in-flight-per-shard",
            "1",
        ])
        .output()
        .unwrap();
    assert!(!crashed.status.success(), "the crash run should abort");
    let st: Value = serde_json::from_slice(&std::fs::read(out.join("scan-state.json")).unwrap()).unwrap();
    let done = st["pos"].as_array().unwrap().iter().filter(|p| p["done"] == true).count();
    let total = st["pos"].as_array().unwrap().len();
    let committed: u64 = st["rows"].as_array().unwrap().iter().map(|r| r.as_u64().unwrap()).sum();
    assert!(committed > 0 && done < total, "the crash should come mid-scan ({committed} rows, {done}/{total} ranges)");
    let r = run(&["scan-scylla", "--config", s(&cfg), "--out", s(&out), "--checkpoint-secs", "1"]);
    assert_eq!(r["runs"], 2);
    let snap = tmp.path().join("snap-crash");
    let rels = out.join("relations.toml");
    run(&["build", "--input", s(&out), "--out", s(&snap), "--relations", s(&rels)]);
    let d = diff(&direct, &snap);
    assert_eq!(d["equal"], true, "{d:#}");
    check_lists(&direct, &snap, &li);

    // a finished scan run again is a no-op; --fresh starts over
    let again = run(&["scan-scylla", "--config", s(&cfg), "--out", s(&out)]);
    assert_eq!(again["already_finished"], true);
    assert_eq!(again["run_rows"], 0);

    rt.block_on(drop_keyspaces(&node, &keyspaces));
}
