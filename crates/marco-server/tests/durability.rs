//! Crashes and restarts: a source replaying from the durable cursor after a
//! kill ends where an uninterrupted run does, through change segments,
//! checkpoints and crashes on either side of a checkpoint's commit.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use marco_core::store::{Manifest, MANIFEST};
use marco_core::{segment, Graph};
use marco_server::app::{App, Config, Op, SourceReport, Status};
use marco_server::synth::{did, list_uri, tid, uri, BLOCK, FOLLOW, LISTITEM};

const N: u32 = 300;
const SRC: &str = "jetstream";

/// The i-th write of a deterministic stream: adds, deletes of earlier adds
/// (some recreated under the same rkey), unknown deletes, new DIDs, statuses,
/// and listitems, whose deletes name only the owner and rkey.
enum W {
    Ops(Vec<Op>),
    Status(String, Option<Status>, i64),
}

fn stream(n: usize) -> Vec<W> {
    let mut v = Vec::new();
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut rnd = |m: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % m.max(1)
    };
    let mut made: Vec<(String, Option<String>)> = Vec::new();
    // event times from a day ago, inside the tombstone TTL
    let t0 = marco_server::app::now_ms() * 1000 - 86_400_000_000;
    for i in 0..n {
        let t = t0 + i as i64 * 1000;
        if rnd(40) == 0 {
            let st = [None, Some(Status::Takendown), Some(Status::Deleted)][rnd(3) as usize];
            v.push(W::Status(did(rnd(N as u64 + 20) as u32), st, t0 + i as i64 * 1000));
            continue;
        }
        let mut ops = Vec::new();
        for _ in 0..1 + rnd(4) {
            let a = if rnd(10) == 0 { format!("did:web:n{}.example", rnd(30)) } else { did(rnd(N as u64) as u32) };
            let r = rnd(12);
            let coll = if r < 2 {
                BLOCK
            } else if r < 4 {
                LISTITEM
            } else {
                FOLLOW
            };
            let list = (coll == LISTITEM).then(|| list_uri(&a, rnd(3) as u32));
            match rnd(10) {
                0..=2 if !made.is_empty() => {
                    let (u, _) = made[rnd(made.len() as u64) as usize].clone();
                    ops.push(Op::Delete { uri: u, time_us: t });
                }
                3 => ops.push(Op::Delete { uri: uri(&a, coll, &tid(50_000_000 + rnd(500), 1)), time_us: t }),
                _ => {
                    let (u, list) = if rnd(12) == 0 && !made.is_empty() {
                        made[rnd(made.len() as u64) as usize].clone()
                    } else {
                        (uri(&a, coll, &tid(40_000_000 + i as u64 * 10 + rnd(10), 1)), list)
                    };
                    made.push((u.clone(), list.clone()));
                    ops.push(Op::Add { uri: u, subject: did(rnd(N as u64 + 10) as u32), list, time_us: t });
                }
            }
        }
        v.push(W::Ops(ops));
    }
    v
}

fn cfg(snap: &Path) -> Config {
    Config { segment_interval: Duration::from_secs(3600), ..Config::new(snap.to_path_buf()) }
}

fn snapshot() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("snap");
    marco_server::synth::write(&snap, N, 4).unwrap();
    (dir, snap)
}

/// Applies writes `from..to` of `s`, the i-th with cursor i+1.
async fn feed(app: &App, s: &[W], from: usize, to: usize) {
    for (i, w) in s.iter().enumerate().take(to).skip(from) {
        let c = (i + 1).to_string();
        match w {
            W::Ops(ops) => {
                app.apply_from(SRC, &c, ops).await.unwrap();
            }
            W::Status(d, st, t) => {
                app.status_from(SRC, &c, d, *st, *t).await.unwrap();
            }
        }
    }
}

/// Resumes after the source's durable cursor, the way a consumer does.
fn resume(app: &App) -> usize {
    app.source_cursor(SRC).map_or(0, |c| c.parse().unwrap())
}

/// Everything a read can see, for every DID the stream names.
fn has(app: &App, rel: marco_core::RelId, a: &str, b: &str) -> Option<String> {
    let g = app.graph();
    g.read(|cx| g.has(cx, rel, a, b)).unwrap()
}

fn dump(g: &Graph) -> Vec<String> {
    let mut dids: Vec<String> = (0..N + 20).map(did).collect();
    dids.extend((0..30).map(|i| format!("did:web:n{i}.example")));
    let mut out = Vec::new();
    let lists: Vec<String> = dids.iter().flat_map(|d| (0..3).map(move |l| list_uri(d, l))).collect();
    for l in &lists {
        let (page, _) = g.read(|cx| g.out_list(cx, 2, l, 100_000, None)).unwrap();
        out.push(format!(
            "{l} members {page:?} counts {:?} distinct {:?}",
            g.read(|cx| g.counts(cx, 2, l)).unwrap(),
            g.id_of(l).map(|i| g.read(|cx| g.out_distinct(cx, 2, i)).unwrap())
        ));
    }
    for d in &dids {
        out.push(format!("{d} status {:?}", g.status_of(d)));
        for rel in [0u8, 1, 2, 3] {
            let (page, _) = g.read(|cx| g.out_list(cx, rel, d, 100_000, None)).unwrap();
            out.push(format!("{d} {rel} out {page:?}"));
            out.push(format!("{d} {rel} counts {:?}", g.read(|cx| g.counts(cx, rel, d)).unwrap()));
            let (inn, _) = g.read(|cx| g.in_list(cx, rel, d, 100_000, None)).unwrap();
            out.push(format!("{d} {rel} in {inn:?}"));
        }
    }
    for rel in [0u8, 1, 2, 3] {
        out.push(format!("{rel} records {:?} tombstones {}", g.records(rel), g.tombstones(rel)));
    }
    out
}

async fn uninterrupted(s: &[W]) -> Vec<String> {
    let (_d, snap) = snapshot();
    let app = App::open(cfg(&snap)).unwrap();
    feed(&app, s, 0, s.len()).await;
    let r = dump(&app.graph());
    app.shutdown().await.unwrap();
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kill_and_a_replay_from_the_durable_cursor_end_where_an_uninterrupted_run_does() {
    let s = stream(1200);
    let want = uninterrupted(&s).await;
    let (_d, snap) = snapshot();
    let app = App::open(cfg(&snap)).unwrap();
    feed(&app, &s, 0, 300).await;
    app.flush().await.unwrap();
    feed(&app, &s, 300, 500).await;
    app.checkpoint().await.unwrap();
    feed(&app, &s, 500, 700).await;
    app.flush().await.unwrap();
    // written after the last segment: a kill -9 loses these, the source replays them
    feed(&app, &s, 700, 760).await;
    drop(app);

    let app = App::open(cfg(&snap)).unwrap();
    assert_eq!(resume(&app), 700);
    assert_eq!(app.durability.lock().unwrap().replayed_segments, 1);
    feed(&app, &s, resume(&app), 1000).await;
    // killed again before any segment: the first restart's replay was lost too
    drop(app);
    let app = App::open(cfg(&snap)).unwrap();
    assert_eq!(resume(&app), 700);
    feed(&app, &s, resume(&app), s.len()).await;
    let got = dump(&app.graph());
    for (a, b) in got.iter().zip(&want) {
        assert_eq!(a, b);
    }
    assert_eq!(got.len(), want.len());

    // a clean stop writes the last segment, so nothing replays
    app.shutdown().await.unwrap();
    let app = App::open(cfg(&snap)).unwrap();
    assert_eq!(resume(&app), s.len());
    assert_eq!(dump(&app.graph()), want);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replaying_more_than_needed_converges() {
    // a consumer that resumes early (its own older cursor) replays writes
    // marco already has: ordered replay of a suffix is idempotent
    let s = stream(600);
    let want = uninterrupted(&s).await;
    let (_d, snap) = snapshot();
    let app = App::open(cfg(&snap)).unwrap();
    feed(&app, &s, 0, 400).await;
    feed(&app, &s, 250, s.len()).await;
    let got = dump(&app.graph());
    let differ: Vec<_> = got.iter().zip(&want).filter(|(a, b)| a != b).collect();
    assert!(differ.is_empty(), "{:?}", &differ[..differ.len().min(3)]);
    app.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_on_either_side_of_a_checkpoints_commit() {
    let s = stream(800);
    let want = uninterrupted(&s).await;
    let (dir, snap) = snapshot();
    let app = App::open(cfg(&snap)).unwrap();
    feed(&app, &s, 0, 400).await;
    app.flush().await.unwrap();
    let before = std::fs::read_dir(&snap).unwrap().map(|e| e.unwrap().file_name()).collect::<Vec<_>>();

    // the checkpoint runs on a copy; its files are then dropped into the
    // real directory as a crash would leave them
    let copy = dir.path().join("copy");
    std::fs::create_dir(&copy).unwrap();
    for e in std::fs::read_dir(&snap).unwrap() {
        let e = e.unwrap();
        std::fs::copy(e.path(), copy.join(e.file_name())).unwrap();
    }
    let ccfg = cfg(&copy);
    let r = marco_server::app::open_replayed(&ccfg, &ccfg.storage.engine().unwrap(), None).unwrap();
    let m = r.graph.write_checkpoint(&copy, 1, r.last, r.cursors.clone()).unwrap().manifest;
    drop(r);
    let new_files: Vec<String> = m
        .shards
        .values()
        .flat_map(|f| f.files())
        .filter(|n| n.ends_with("-00000001.marco"))
        .map(str::to_string)
        .collect();
    assert!(!new_files.is_empty());

    // 1: the shard files were written, the manifest wasn't renamed
    feed(&app, &s, 400, 500).await;
    drop(app);
    for n in &new_files {
        std::fs::copy(copy.join(n), snap.join(n)).unwrap();
    }
    std::fs::write(snap.join("manifest.json.tmp"), b"{half").unwrap();
    let app = App::open(cfg(&snap)).unwrap();
    assert_eq!(Manifest::read(&snap).unwrap().checkpoint, 0);
    let after = std::fs::read_dir(&snap).unwrap().map(|e| e.unwrap().file_name()).collect::<Vec<_>>();
    assert_eq!(after.len(), before.len(), "the uncommitted files are gone");
    assert_eq!(resume(&app), 400);
    feed(&app, &s, resume(&app), 600).await;
    app.flush().await.unwrap();
    drop(app);

    // 2: the manifest was renamed, the old files and segments weren't removed yet
    assert_eq!(marco_server::app::open_replayed(&ccfg, &ccfg.storage.engine().unwrap(), None).unwrap().last, m.segment);
    // the first write's segment (a new source), the flush at 400, then 400..600
    assert_eq!(segment::list(&snap).unwrap().len(), 3);
    // the copy's checkpoint folded the first two; the third is newer
    for n in &new_files {
        std::fs::copy(copy.join(n), snap.join(n)).unwrap();
    }
    std::fs::copy(copy.join(MANIFEST), snap.join(MANIFEST)).unwrap();
    let app = App::open(cfg(&snap)).unwrap();
    assert_eq!(app.graph().manifest().unwrap().checkpoint, 1);
    assert_eq!(segment::list(&snap).unwrap().len(), 1, "the folded segments are removed");
    assert_eq!(resume(&app), 600);
    feed(&app, &s, resume(&app), s.len()).await;
    assert_eq!(dump(&app.graph()), want);
    app.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoints_keep_up_with_writes_and_fold_the_segments() {
    let s = stream(900);
    let want = uninterrupted(&s).await;
    let (_d, snap) = snapshot();
    let mut c = cfg(&snap);
    c.segment_interval = Duration::from_millis(20);
    c.checkpoint_ops = 150;
    let app = App::open(c).unwrap();
    feed(&app, &s, 0, s.len()).await;
    while app.job().running {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(dump(&app.graph()), want, "writes during checkpoints carry over to the new copies");
    let ck = app.durability.lock().unwrap().checkpoint;
    assert!(ck >= 2, "{ck} checkpoints");
    app.shutdown().await.unwrap();
    let app = App::open(cfg(&snap)).unwrap();
    assert_eq!(dump(&app.graph()), want);
    app.graph().snapshot().unwrap().verify_checksums().unwrap();
}

/// Compaction after every checkpoint: each shard with patches is rewritten
/// as one base and swapped into the served copies while writes go on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compactions_swap_shards_in_under_writes() {
    let s = stream(900);
    let want = uninterrupted(&s).await;
    let (_d, snap) = snapshot();
    let mut c = cfg(&snap);
    c.segment_interval = Duration::from_millis(20);
    c.checkpoint_ops = 150;
    c.compaction = marco_core::CompactPolicy { ratio: 1.0, min_garbage: 0 };
    let app = App::open(c).unwrap();
    feed(&app, &s, 0, s.len()).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while app.job().running || app.durability.lock().unwrap().compactions == 0 {
        assert!(std::time::Instant::now() < deadline, "no compaction");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    if app.durability.lock().unwrap().checkpoint < 2 {
        // the writes can all land before a second checkpoint comes due
        app.checkpoint().await.unwrap();
    }
    assert_eq!(dump(&app.graph()), want, "a compacted shard reads as its base and patches did");
    let d = app.durability.lock().unwrap().clone();
    assert!(d.checkpoint >= 2 && d.compaction_error.is_empty(), "{d:?}");
    app.shutdown().await.unwrap();
    let m = Manifest::read(&snap).unwrap();
    let app = App::open(cfg(&snap)).unwrap();
    assert_eq!(dump(&app.graph()), want);
    app.graph().snapshot().unwrap().verify_checksums().unwrap();
    let listed: std::collections::HashSet<String> =
        m.shards.values().flat_map(|f| f.files()).map(str::to_string).collect();
    for e in std::fs::read_dir(&snap).unwrap() {
        let n = e.unwrap().file_name().to_string_lossy().to_string();
        assert!(!n.starts_with("shard-") || listed.contains(&n), "{n} is left over");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_sources_first_cursor_is_durable_on_return() {
    let (_d, snap) = snapshot();
    let app = App::open(cfg(&snap)).unwrap();
    // a first call with ops: its segment is written before they apply
    let u = uri(&did(1), FOLLOW, &tid(70_000_000, 1));
    app.apply_from("relay", "start", &[Op::Add { uri: u.clone(), subject: did(2), list: None, time_us: 0 }])
        .await
        .unwrap();
    assert_eq!(app.source_cursor("relay").as_deref(), Some("start"));
    app.apply_from("relay", "next", &[]).await.unwrap();
    assert_eq!(app.latest_cursor("relay").as_deref(), Some("next"));
    assert_eq!(app.source_cursor("relay").as_deref(), Some("start"), "later cursors ride the next segment");
    drop(app);
    let app = App::open(cfg(&snap)).unwrap();
    assert_eq!(app.source_cursor("relay").as_deref(), Some("start"));
    assert!(has(&app, 0, &did(1), &did(2)).is_some(), "the first call's ops came back with its cursor");
    // the operator's status is durable on return too
    app.set_status(&did(3), Some(Status::Takendown)).await.unwrap();
    drop(app);
    let app = App::open(cfg(&snap)).unwrap();
    assert_eq!(app.status_of(&did(3)), Some(Status::Takendown));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupt_or_missing_segment_refuses_to_start() {
    let (_d, snap) = snapshot();
    let app = App::open(cfg(&snap)).unwrap();
    for i in 0..3 {
        app.apply_from(
            SRC,
            &i.to_string(),
            &[Op::Add { uri: uri(&did(1), FOLLOW, &tid(60_000_000 + i, 1)), subject: did(2), list: None, time_us: 0 }],
        )
        .await
        .unwrap();
        app.flush().await.unwrap();
    }
    drop(app);
    let segs = segment::list(&snap).unwrap();
    assert_eq!(segs.len(), 3);
    let mut b = std::fs::read(&segs[1].1).unwrap();
    let n = b.len();
    b[n - 20] ^= 1;
    std::fs::write(&segs[1].1, &b).unwrap();
    let e = App::open(cfg(&snap)).err().unwrap();
    assert!(format!("{e:#}").contains("checksum"), "{e:#}");
    std::fs::remove_file(&segs[1].1).unwrap();
    let e = App::open(cfg(&snap)).err().unwrap();
    assert!(format!("{e:#}").contains("missing"), "{e:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ready_once_the_expected_sources_catch_up() {
    let (_d, snap) = snapshot();
    let mut c = cfg(&snap);
    c.expected_sources = vec![SRC.into()];
    let app: Arc<App> = App::open(c).unwrap();
    let (ok, why) = app.readiness();
    assert!(!ok && why.contains(SRC), "{why}");
    assert!(app.read_gate().is_err());
    app.report_source(SRC, SourceReport { lag_seconds: 30.0, ..Default::default() });
    assert!(!app.is_ready());
    app.report_source(SRC, SourceReport { lag_seconds: 0.4, ..Default::default() });
    assert!(app.is_ready());
    app.report_source(SRC, SourceReport { lag_seconds: 30.0, ..Default::default() });
    assert!(app.is_ready(), "a later lag doesn't take readiness back");
    assert_eq!(app.reads_before_ready.load(std::sync::atomic::Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replayed_account_deletion_keeps_its_own_time() {
    // the source re-sends an #account deletion and the creates after it, as a
    // consumer does resuming from its durable cursor after a kill
    let (_d, snap) = snapshot();
    let app = App::open(cfg(&snap)).unwrap();
    let t = marco_server::app::now_ms() * 1000 - 60_000_000;
    let a = "did:web:gone.example";
    let later = uri(a, FOLLOW, &tid(71_000_000, 1));
    for _ in 0..2 {
        app.status_from(SRC, "1", a, Some(Status::Deleted), t).await.unwrap();
        app.status_from(SRC, "2", a, None, t + 1_000).await.unwrap();
        app.apply_from(SRC, "3", &[Op::Add { uri: later.clone(), subject: did(2), list: None, time_us: t + 2_000 }])
            .await
            .unwrap();
        assert!(has(&app, 0, a, &did(2)).is_some(), "the create after the deletion stays");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_ids_are_never_reused_and_failures_show() {
    let s = stream(600);
    let want = uninterrupted(&s).await;
    let (_d, snap) = snapshot();
    let app = App::open(cfg(&snap)).unwrap();
    feed(&app, &s, 0, 200).await;
    // checkpoint 1 can't rename its first shard into place: it fails before its commit
    let blocker = snap.join(marco_core::store::shard_file(0, 1));
    std::fs::create_dir(&blocker).unwrap();
    assert!(app.checkpoint().await.is_err());
    let d = app.durability.lock().unwrap().clone();
    assert_eq!((d.checkpoint, d.checkpoint_failures), (0, 1));
    assert!(d.checkpoint_error.starts_with("failed"), "{}", d.checkpoint_error);
    feed(&app, &s, 200, 400).await;
    // checkpoint 2 commits under a new id; the blocker can't be removed after it, which shows but doesn't fail it
    app.checkpoint().await.unwrap();
    let d = app.durability.lock().unwrap().clone();
    assert_eq!(d.checkpoint, 2);
    assert!(d.checkpoint_error.starts_with("cleanup after commit"), "{}", d.checkpoint_error);
    assert_eq!(Manifest::read(&snap).unwrap().checkpoint, 2);
    std::fs::remove_dir(&blocker).unwrap();
    feed(&app, &s, 400, 500).await;
    app.checkpoint().await.unwrap();
    assert_eq!(app.durability.lock().unwrap().checkpoint, 3);
    feed(&app, &s, 500, s.len()).await;
    drop(app);
    let app = App::open(cfg(&snap)).unwrap();
    assert_eq!(resume(&app), 500, "a kill after the last checkpoint replays from its cut");
    feed(&app, &s, resume(&app), s.len()).await;
    assert_eq!(dump(&app.graph()), want);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_checkpoint_slot_always_clears() {
    let (_d, snap) = snapshot();
    let mut c = cfg(&snap);
    c.checkpoint_ops = 20;
    let app = App::open(c).unwrap();
    let s = stream(400);
    for round in 0..20 {
        // a verify holds the slot while an automatic checkpoint starts, then the operator asks for one
        let _ = app.start_verify();
        feed(&app, &s, round * 20, round * 20 + 20).await;
        while app.job().running && app.job().kind == "verify" {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let r = tokio::time::timeout(Duration::from_secs(20), app.checkpoint()).await;
        assert!(r.is_ok(), "round {round}: the slot never cleared");
    }
    // an automatic one may have started since; it clears the slot when it's done
    for _ in 0..2000 {
        if !app.job().running {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the slot never cleared");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_segment_write_shows_and_lands_once_it_can() {
    use std::os::unix::fs::PermissionsExt;
    let (_d, snap) = snapshot();
    let app = App::open(cfg(&snap)).unwrap();
    app.apply_from(SRC, "1", &[]).await.unwrap();
    std::fs::set_permissions(&snap, std::fs::Permissions::from_mode(0o555)).unwrap();
    let u = uri(&did(1), FOLLOW, &tid(72_000_000, 1));
    app.apply_from(SRC, "2", &[Op::Add { uri: u, subject: did(2), list: None, time_us: 0 }]).await.unwrap();
    let flush = {
        let app = app.clone();
        tokio::spawn(async move { app.flush().await })
    };
    let mut seen = String::new();
    for _ in 0..500 {
        seen = app.durability.lock().unwrap().segment_error.clone();
        if !seen.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    std::fs::set_permissions(&snap, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(!seen.is_empty(), "the failing write shows in the durability state");
    flush.await.unwrap().unwrap();
    assert!(app.durability.lock().unwrap().segment_error.is_empty());
    assert_eq!(app.source_cursor(SRC).as_deref(), Some("2"));
}

/// A listitem stream: three owners' lists filled, emptied and refilled, so
/// deletes, re-creates and repeat members cross segments and checkpoints.
fn list_stream(n: usize) -> Vec<W> {
    let t0 = marco_server::app::now_ms() * 1000 - 86_400_000_000;
    let mut v = Vec::new();
    for i in 0..n {
        let o = did((i % 3) as u32);
        let t = t0 + i as i64 * 1000;
        let rk = tid(70_000_000 + (i as u64 % 97), 1);
        let u = uri(&o, LISTITEM, &rk);
        v.push(W::Ops(vec![if i % 4 == 3 {
            Op::Delete { uri: u, time_us: t }
        } else {
            Op::Add {
                uri: u,
                subject: did(10 + (i % 7) as u32),
                list: Some(list_uri(&o, (i % 5) as u32 % 3)),
                time_us: t,
            }
        }]));
        if i == n / 2 {
            v.push(W::Status(did(1), Some(Status::Deleted), t));
        }
    }
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listitems_survive_kills_on_both_sides_of_a_checkpoint() {
    let s = list_stream(900);
    let want = uninterrupted(&s).await;
    let (_d, snap) = snapshot();
    let app = App::open(cfg(&snap)).unwrap();
    feed(&app, &s, 0, 200).await;
    app.flush().await.unwrap();
    feed(&app, &s, 200, 450).await;
    app.checkpoint().await.unwrap();
    feed(&app, &s, 450, 600).await;
    app.flush().await.unwrap();
    feed(&app, &s, 600, 650).await;
    drop(app);
    let app = App::open(cfg(&snap)).unwrap();
    assert_eq!(resume(&app), 600);
    feed(&app, &s, resume(&app), s.len()).await;
    let got = dump(&app.graph());
    for (a, b) in got.iter().zip(&want) {
        assert_eq!(a, b);
    }
    assert_eq!(got.len(), want.len());
}

/// An owner of more list items than the by-subject threshold (4096), in a
/// checkpoint, then the owner's deletion: a restart that replays the deletion
/// from its segment, over the paged lists, ends where a live run does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_big_list_owners_deletion_replays_as_it_applied() {
    let t0 = marco_server::app::now_ms() * 1000 - 86_400_000_000;
    let o = did(1);
    let n = 5000;
    let mut s: Vec<W> = (0..n)
        .map(|i| {
            W::Ops(vec![Op::Add {
                uri: uri(&o, LISTITEM, &tid(70_000_000 + i as u64, 1)),
                subject: did(20 + (i % 40) as u32),
                list: Some(list_uri(&o, (i % 2) as u32)),
                time_us: t0 + i as i64,
            }])
        })
        .collect();
    s.push(W::Status(o.clone(), Some(Status::Deleted), t0 + n as i64 + 1_000));
    let want = uninterrupted(&s).await;
    let (_d, snap) = snapshot();
    let app = App::open(cfg(&snap)).unwrap();
    feed(&app, &s, 0, n).await;
    app.checkpoint().await.unwrap();
    feed(&app, &s, n, s.len()).await;
    app.flush().await.unwrap();
    drop(app);
    let app = App::open(cfg(&snap)).unwrap();
    assert_eq!(resume(&app), s.len());
    assert_eq!(dump(&app.graph()), want);
    let g = app.graph();
    assert_eq!(g.read(|cx| g.counts(cx, 3, &o)).unwrap(), (0, 0), "no item is left");
    for l in 0..2 {
        assert_eq!(g.read(|cx| g.counts(cx, 2, &list_uri(&o, l))).unwrap(), (0, 0), "list {l} is empty");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_listitem_delete_before_its_create_holds_through_a_restart() {
    let (_d, snap) = snapshot();
    let app = App::open(cfg(&snap)).unwrap();
    let o = did(4);
    let u = uri(&o, LISTITEM, &tid(80_000_000, 1));
    let t = marco_server::app::now_ms() * 1000 - 3_600_000_000;
    app.apply_from(SRC, "1", &[Op::Delete { uri: u.clone(), time_us: t }]).await.unwrap();
    app.checkpoint().await.unwrap();
    drop(app);
    let app = App::open(cfg(&snap)).unwrap();
    let add = |time_us| Op::Add { uri: u.clone(), subject: did(9), list: Some(list_uri(&o, 0)), time_us };
    let out = app.apply_from("backfill", "1", &[add(t - 1)]).await.unwrap();
    assert!(matches!(&out[..], [marco_server::app::Outcome::Deleted]), "the owner's tombstone holds");
    let out = app.apply_from(SRC, "2", &[add(t + 1)]).await.unwrap();
    assert!(matches!(&out[..], [marco_server::app::Outcome::Added]), "a later re-create lands");
    app.shutdown().await.unwrap();
    let app = App::open(cfg(&snap)).unwrap();
    assert!(has(&app, 2, &list_uri(&o, 0), &did(9)).is_some());
}

/// A replace's stamp, and its source, survive a kill with or without a
/// checkpoint: another source's older ops still change nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replaces_stamp_survives_a_kill() {
    for checkpoint in [false, true] {
        let (_d, snap) = snapshot();
        let app = App::open(cfg(&snap)).unwrap();
        let a = did(3);
        let u = uri(&a, FOLLOW, &tid(90_000_000, 1));
        let t = marco_server::app::now_ms() * 1000 - 3_600_000_000;
        let add = |s: u32, time_us| Op::Add { uri: u.clone(), subject: did(s), list: None, time_us };
        app.apply_from(SRC, "1", &[add(7, t)]).await.unwrap();
        app.apply_from(SRC, "2", &[add(8, t + 10)]).await.unwrap();
        if checkpoint {
            app.checkpoint().await.unwrap();
        } else {
            app.flush().await.unwrap();
        }
        drop(app);
        let app = App::open(cfg(&snap)).unwrap();
        let out = app.apply_from("backfill", "1", &[Op::Delete { uri: u.clone(), time_us: t + 5 }]).await.unwrap();
        assert!(matches!(&out[..], [marco_server::app::Outcome::NotFound]), "checkpoint {checkpoint}: {out:?}");
        let out = app.apply_from("backfill", "2", &[add(7, t + 5)]).await.unwrap();
        assert!(matches!(&out[..], [marco_server::app::Outcome::Exists]), "checkpoint {checkpoint}: {out:?}");
        assert!(has(&app, 0, &a, &did(8)).is_some());
        app.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bulk_built_snapshots_time_starts_a_new_source_once() {
    let (_d, snap) = snapshot();
    let t = marco_server::app::now_ms() * 1000 - 3_600_000_000;
    let mut m = Manifest::read(&snap).unwrap();
    m.cursors.insert(marco_ingest::SNAPSHOT_TIME.into(), t.to_string());
    m.write(&snap).unwrap();
    let app = App::open(cfg(&snap)).unwrap();
    let src = marco_ingest::Source::new(marco_ingest::Proto::Jetstream, "ws://127.0.0.1:9/subscribe").unwrap();
    let day = Duration::from_secs(86_400);
    let start = marco_server::ingest::start_point(&app, &src, day, false).await.unwrap();
    assert_eq!(start, Some(t as u64));
    // past the source's retention: refused, unless the operator accepts the gap
    let short = Duration::from_secs(1800);
    assert!(marco_server::ingest::start_point(&app, &src, short, false).await.is_err());
    assert_eq!(marco_server::ingest::start_point(&app, &src, short, true).await.unwrap(), Some(t as u64));
    // a firehose resumes only by seq: catching up is Jetstream's job
    let relay = marco_ingest::Source::new(marco_ingest::Proto::Firehose, "ws://127.0.0.1:9").unwrap();
    let e = marco_server::ingest::start_point(&app, &relay, day, false).await.unwrap_err();
    assert!(format!("{e:#}").contains("catch up from Jetstream"), "{e:#}");
    assert_eq!(marco_server::ingest::start_point(&app, &relay, day, true).await.unwrap(), None);
    // the source's first cursor replaces the snapshot's time, durably
    let u = uri(&did(1), FOLLOW, &tid(70_000_000, 1));
    app.apply_from(&src.name, &(t + 5).to_string(), &[Op::Add { uri: u, subject: did(2), list: None, time_us: t }])
        .await
        .unwrap();
    assert_eq!(app.source_cursor(marco_ingest::SNAPSHOT_TIME), None);
    drop(app);
    let app = App::open(cfg(&snap)).unwrap();
    assert_eq!(app.source_cursor(marco_ingest::SNAPSHOT_TIME), None);
    assert_eq!(app.source_cursor(&src.name), Some((t + 5).to_string()));
    assert_eq!(marco_server::ingest::start_point(&app, &src, day, false).await.unwrap(), None);
}
