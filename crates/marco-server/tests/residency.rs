//! Checkpoints and compactions on the engine: a server whose budget holds the
//! whole snapshot never misses across them (their pages are written through
//! into the cache), store memory and RSS stay under the budget at a small
//! and a fits-all one, an older patch's index and heads leave the budget
//! once the newer one serves, and a bad page under a checkpoint fails it
//! cleanly. Its own test binary, so no other test's memory is in the RSS.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use marco_core::read::{Engine, Found, Source};
use marco_core::store::engine::Config as EngineConfig;
use marco_core::store::io::{default_device, Fault, Times};
use marco_core::store::{shard_file, Storage};
use marco_core::{CompactPolicy, Graph};
use marco_server::app::{App, Config, Op};
use marco_server::synth::{did, tid, uri, FOLLOW};

const N: u32 = 3000;
/// One test at a time, so the RSS one counts only its own memory.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
/// Actors in the memory test's snapshot: about 2.9k pages, past its small budget.
const BIG: u32 = 20_000;
const SRC: &str = "jetstream";
const MIB: u64 = 1 << 20;

fn snapshot() -> (tempfile::TempDir, PathBuf) {
    snapshot_of(N)
}

fn snapshot_of(n: u32) -> (tempfile::TempDir, PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("snap");
    marco_server::synth::write(&p, n, 8).unwrap();
    (d, p)
}

/// The engine with `memory` bytes and the minimum-budget check off.
fn engine(memory: u64) -> Storage {
    Storage { memory, max_frames: Some(u32::MAX) }
}

fn cfg(snap: &Path, storage: Storage, compact: bool) -> Config {
    Config {
        storage,
        segment_interval: Duration::from_secs(3600),
        checkpoint_ops: u64::MAX,
        checkpoint_age: Duration::from_secs(365 * 86_400),
        compaction: if compact {
            CompactPolicy { ratio: 1.0, min_garbage: 0 }
        } else {
            CompactPolicy { ratio: 0.0, min_garbage: u64::MAX }
        },
        // compactions read past the cache a list at a time: unpaced, to keep the tests short
        background: marco_core::store::io::Rate::UNLIMITED,
        ..Config::new(snap.to_path_buf())
    }
}

/// Round `r`'s writes over `n` actors: follows added across every shard,
/// and some of the round before's deleted.
fn round(r: u32, n: u32) -> Vec<Op> {
    let t0 = marco_server::app::now_ms() * 1000 - 3_600_000_000;
    let mut ops = Vec::new();
    let mut x = 0x2545_f491_4f6c_dd1du64 ^ r as u64;
    let mut next = |m: u32| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        (x % m as u64) as u32
    };
    for i in 0..400u32 {
        let a = did(next(n));
        let t = t0 + (r * 1000 + i) as i64;
        ops.push(Op::Add {
            uri: uri(&a, FOLLOW, &tid(70_000_000 + (r * 1000 + i) as u64, 1)),
            subject: did(next(n)),
            list: None,
            time_us: t,
        });
        if r > 0 && i % 4 == 0 {
            let b = did(next(n));
            ops.push(Op::Delete {
                uri: uri(&b, FOLLOW, &tid(70_000_000 + ((r - 1) * 1000 + next(400)) as u64, 1)),
                time_us: t,
            });
        }
    }
    ops
}

async fn feed(app: &App, r: u32) {
    feed_of(app, r, N).await
}

async fn feed_of(app: &App, r: u32, n: u32) {
    for (i, chunk) in round(r, n).chunks(50).enumerate() {
        app.apply_from(SRC, &format!("{r:04}{i:04}"), chunk).await.unwrap();
    }
}

/// Every list of every actor, read whole.
fn dump(g: &Graph) -> Vec<String> {
    let mut out = Vec::new();
    for i in 0..N {
        let d = did(i);
        let (o, _) = g.read(|cx| g.out_list(cx, 0, &d, 1_000_000, None)).unwrap();
        let (n, _) = g.read(|cx| g.in_list(cx, 0, &d, 1_000_000, None)).unwrap();
        out.push(format!("{d} {o:?} {n:?} {:?}", g.read(|cx| g.counts(cx, 0, &d)).unwrap()));
    }
    out
}

/// Waits for the compactions a checkpoint starts: every shard back to a base.
async fn compacted(app: &App) {
    let until = Instant::now() + Duration::from_secs(60);
    while app.graph().snapshot().unwrap().shards.iter().flatten().any(|s| s.parts.len() > 1) {
        assert!(Instant::now() < until, "the compactions didn't finish");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// What the served snapshot's files hold against the budget, by their own
/// count: each file's sums and slots, and the pinned sections of the
/// directory and of each shard's newest file.
fn files_borrowed(app: &App) -> u64 {
    let g = app.graph();
    let s = g.snapshot().unwrap();
    s.files().map(|(_, f)| f.file.borrowed() + f.pinned_bytes()).sum()
}

#[test]
fn a_fits_all_server_stays_at_zero_misses_across_ten_checkpoints() {
    let _one = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let rt = marco_server::runtime(Some(2)).unwrap();
    rt.block_on(async {
        let (_d, snap) = snapshot();
        let app = App::open(cfg(&snap, engine(256 * MIB), true)).unwrap();
        let pages = app.graph().snapshot().unwrap().paged_pages();
        let b = app.engine.bytes();
        assert_eq!(b.main_frames, pages, "open loaded every page");
        let base = app.engine.stats();
        // the same writes on a server that neither checkpoints nor compacts
        let (ref_d, ref_snap) = snapshot();
        let reference = App::open(cfg(&ref_snap, engine(256 * MIB), false)).unwrap();
        for r in 0..10 {
            feed(&app, r).await;
            feed(&reference, r).await;
            app.checkpoint().await.unwrap();
            compacted(&app).await;
            assert_eq!(dump(&app.graph()), dump(&reference.graph()), "round {r}");
            let s = app.engine.stats();
            assert_eq!(
                (s.misses, s.fetched, s.past_pages, s.write_through_skipped),
                (base.misses, base.fetched, base.past_pages, base.write_through_skipped),
                "round {r}: nothing read from disk after open: {s:?}"
            );
        }
        let s = app.engine.stats();
        let d = app.durability.lock().unwrap().clone();
        assert!(d.checkpoint == 10 && d.compactions >= 10, "{d:?}");
        assert!(s.written_through > 0);
        println!("10 checkpoints, {} compactions, {} pages written through", d.compactions, s.written_through);
        app.shutdown().await.unwrap();
        reference.shutdown().await.unwrap();
        drop(ref_d);
    });
}

#[test]
fn older_patches_give_their_index_and_heads_back() {
    let _one = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let rt = marco_server::runtime(Some(2)).unwrap();
    rt.block_on(async {
        let (_d, snap) = snapshot();
        let app = App::open(cfg(&snap, engine(256 * MIB), false)).unwrap();
        let at_open = app.engine.bytes().borrowed;
        for r in 0..6 {
            feed(&app, r).await;
            app.checkpoint().await.unwrap();
            // the copies the switch replaced are freed on a thread of their own
            let until = Instant::now() + Duration::from_secs(10);
            while app.engine.bytes().borrowed != files_borrowed(&app) {
                assert!(
                    Instant::now() < until,
                    "round {r}: {} borrowed, the files hold {}",
                    app.engine.bytes().borrowed,
                    files_borrowed(&app)
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        let g = app.graph();
        let s = g.snapshot().unwrap();
        let patches = s.shards.iter().flatten().map(|f| f.parts.len() - 1).sum::<usize>();
        assert!(patches >= 6 * 6, "most shards have six patches: {patches}");
        for f in s.shards.iter().flatten() {
            for (i, p) in f.parts.iter().enumerate() {
                assert_eq!(
                    p.file.file.has_pinned(),
                    i + 1 == f.parts.len(),
                    "{}: only the newest keeps its pinned sections",
                    p.name
                );
            }
        }
        let now = app.engine.bytes().borrowed;
        println!("borrowed at open {} KiB, after six checkpoints {} KiB", at_open >> 10, now >> 10);
        assert!(now < at_open + at_open / 2, "older patches keep only their sums: {at_open} -> {now}");
        drop(g);
        app.shutdown().await.unwrap();
    });
}

fn rss_anon() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    let l = s.lines().find(|l| l.starts_with("RssAnon:"))?;
    Some(l.split_whitespace().nth(1)?.parse::<u64>().ok()? * 1024)
}

/// Heap outside the budget: the graph's copies and their deltas, the
/// runtime, the test's own dumps.
const OUTSIDE: u64 = 96 * MIB;

/// A checkpoint's and its compactions' reads count under source="job", so
/// the hit ratio of requests and writes (MarcoStoreHitRatioLow's) leaves out
/// a compaction missing on pages nobody serves.
#[test]
fn checkpoints_and_compactions_count_their_lookups_as_jobs() {
    let _one = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let rt = marco_server::runtime(Some(2)).unwrap();
    rt.block_on(async {
        let (_d, snap) = snapshot_of(BIG);
        let app = App::open(cfg(&snap, engine(8 * MIB), true)).unwrap();
        let sum = |s: Source| app.engine.stats().lookups[s as usize].iter().flatten().sum::<u64>();
        let misses =
            |s: Source| app.engine.stats().lookups[s as usize].iter().map(|f| f[Found::Miss as usize]).sum::<u64>();
        let (writes, jobs, job_misses) = (sum(Source::Write), sum(Source::Job), misses(Source::Job));
        feed_of(&app, 0, BIG).await;
        assert!(sum(Source::Write) > writes, "a write's reads count as write");
        app.checkpoint().await.unwrap();
        compacted(&app).await;
        assert!(app.durability.lock().unwrap().compactions > 0);
        assert!(sum(Source::Job) > jobs && misses(Source::Job) > job_misses, "{:?}", app.engine.stats());
        assert_eq!(sum(Source::Request), 0, "no request ran");
        app.shutdown().await.unwrap();
    });
}

#[test]
fn store_memory_and_rss_stay_under_the_budget_through_checkpoints() {
    let _one = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let rt = marco_server::runtime(Some(2)).unwrap();
    for (budget, fits) in [(8 * MIB, false), (64 * MIB, true)] {
        rt.block_on(async {
            let (_d, snap) = snapshot_of(BIG);
            let base = rss_anon();
            let app = App::open(cfg(&snap, engine(budget), true)).unwrap();
            let pages = app.graph().snapshot().unwrap().paged_pages();
            assert_eq!(app.engine.bytes().main_frames == pages, fits, "the budget holds every page: {fits}");
            let stop = Arc::new(AtomicBool::new(false));
            let monitor = {
                let (e, stop) = (app.engine.clone(), stop.clone());
                std::thread::spawn(move || {
                    let (mut used, mut rss) = (0, 0);
                    while !stop.load(Relaxed) {
                        let b = e.bytes();
                        assert!(b.used() <= budget, "store memory past the budget: {b:?}");
                        used = used.max(b.used());
                        if let (Some(b), Some(r)) = (base, rss_anon()) {
                            rss = rss.max(r.saturating_sub(b));
                        }
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    (used, rss)
                })
            };
            for r in 0..5 {
                feed_of(&app, r, BIG).await;
                app.checkpoint().await.unwrap();
                compacted(&app).await;
            }
            stop.store(true, Relaxed);
            let (used, rss) = monitor.join().unwrap();
            let d = app.durability.lock().unwrap().clone();
            println!(
                "budget {} MiB: store at most {} KiB, RSS up {} MiB, {} compactions, stats {:?}",
                budget >> 20,
                used >> 10,
                rss >> 20,
                d.compactions,
                app.engine.stats()
            );
            assert!(d.checkpoint == 5 && d.compactions >= 5 && d.checkpoint_error.is_empty(), "{d:?}");
            if base.is_some() {
                assert!(rss <= budget + OUTSIDE, "RSS grew {} MiB over a {} MiB budget", rss >> 20, budget >> 20);
            }
            app.shutdown().await.unwrap();
        });
    }
}

#[test]
fn a_bad_page_under_a_checkpoint_fails_it_cleanly() {
    let _one = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let rt = marco_server::runtime(Some(2)).unwrap();
    rt.block_on(async {
        let (_d, snap) = snapshot();
        let fault = Fault::new(default_device());
        // a cache too small to keep what the writes read, so the checkpoint reads again
        let mut ec = EngineConfig::new(64 * MIB).device(fault.clone());
        ec.max_frames = Some(64);
        let e = Arc::new(Engine::with_config(ec).unwrap());
        let mut c = cfg(&snap, Storage { memory: 64 * MIB, max_frames: Some(64) }, false);
        c.load = false;
        let app = App::open_on(c, e).unwrap();
        feed(&app, 0).await;
        // every shard's base fails its reads: the patch's merge needs old versions
        for k in 0..8 {
            fault.eio(&shard_file(k, 0), 0..1_000_000, Times::Always);
        }
        let err = app.checkpoint().await.unwrap_err().to_string();
        assert!(err.contains("shard-") && err.contains("page") && err.contains("eio"), "{err}");
        let d = app.durability.lock().unwrap().clone();
        assert_eq!((d.checkpoint, d.checkpoint_failures), (0, 1), "{d:?}");
        assert!(d.checkpoint_error.contains("eio"), "{d:?}");
        let left: Vec<String> = std::fs::read_dir(&snap)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("shard-") && !n.ends_with("-00000000.marco"))
            .collect();
        assert!(left.is_empty(), "a failed checkpoint leaves no files: {left:?}");

        // writes still land, in segments
        feed(&app, 1).await;
        app.flush().await.unwrap();
        assert!(app.durability.lock().unwrap().segments >= 1);

        fault.clear();
        app.start_verify().unwrap();
        while app.job().running {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(app.engine.stats().bad_pages, 0, "a verify that reads them clean clears the marks");
        app.checkpoint().await.unwrap();
        // the failed one's id isn't used again
        assert_eq!(app.durability.lock().unwrap().checkpoint, 2);
        app.shutdown().await.unwrap();
        let reopened = App::open(cfg(&snap, engine(256 * MIB), false)).unwrap();
        assert_eq!(reopened.graph().snapshot().unwrap().manifest.checkpoint, 2);
        reopened.shutdown().await.unwrap();
    });
}
