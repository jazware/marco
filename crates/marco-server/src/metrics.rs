//! The server's own metrics, beside marco-core's op histograms: wire time
//! and errors per RPC, build info, the store's budget and reads.

use std::sync::LazyLock;
use std::time::Instant;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use prometheus::{
    exponential_buckets, register_histogram, register_histogram_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge, register_int_gauge_vec, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec,
};

pub static RPC_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "marco_rpc_seconds",
        "RPC time in the server, request decoded to response encoded, by method and path: fast (served on its \
         worker, nothing missing) or pool (moved to the miss pool)",
        &["rpc", "path"],
        exponential_buckets(1e-6, 2.0, 24).unwrap()
    )
    .unwrap()
});

pub static REQUEST_PAGES: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "marco_store_request_pages",
        "Pages a request read from disk (or waited on another's read of), by method",
        &["rpc"],
        vec![0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 4096.0, 16384.0]
    )
    .unwrap()
});

pub static REQUEST_ROUNDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "marco_store_request_rounds",
        "Fetch rounds a request waited on disk for, by method",
        &["rpc"],
        vec![0.0, 1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 16.0, 32.0]
    )
    .unwrap()
});

pub static POOL_BUSY: LazyLock<IntGauge> =
    LazyLock::new(|| register_int_gauge!("marco_store_miss_pool_busy", "Requests on the miss pool's threads").unwrap());

pub static POOL_WAIT: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "marco_store_miss_pool_wait_seconds",
        "How long a request that missed waited for a miss pool thread",
        exponential_buckets(1e-6, 2.0, 22).unwrap()
    )
    .unwrap()
});

pub static BUSY: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "marco_store_busy_total",
        "Requests refused because no miss pool thread was free within a second (or their deadline)"
    )
    .unwrap()
});

tokio::task_local! {
    /// The path label of the RPC this task is serving: a handler that moves
    /// to the miss pool sets it.
    pub static PATH: std::cell::Cell<&'static str>;
}

/// Labels the RPC this task serves as having gone through the miss pool.
pub fn set_path(path: &'static str) {
    let _ = PATH.try_with(|p| p.set(path));
}

pub static RPC_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_rpc_errors_total",
        "RPCs that failed, by method and HTTP status",
        &["rpc", "status"]
    )
    .unwrap()
});

pub static VIA_TRUNCATED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_via_truncated_total",
        "Via reads of a subscriber's lists that stopped short, by method and reason: cap, the subscriber has more \
         records than --via-max-records; deletes, the read passed as many records deleted since the last checkpoint \
         as it may",
        &["rpc", "reason"]
    )
    .unwrap()
});

/// Counts a request's via reads its caps cut.
pub fn via_truncated(rpc: &str, c: marco_core::ViaCuts) {
    if c.cap > 0 {
        VIA_TRUNCATED.with_label_values(&[rpc, "cap"]).inc_by(c.cap);
    }
    if c.deletes > 0 {
        VIA_TRUNCATED.with_label_values(&[rpc, "deletes"]).inc_by(c.deletes);
    }
}

pub static BUILD: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!("marco_build_info", "1, labelled with the version and commit", &["version", "commit"])
        .unwrap()
});

pub static READY: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("marco_ready", "1 once every expected source has caught up since start").unwrap()
});

pub static READS_BEFORE_READY: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("marco_reads_before_ready_total", "Graph reads that came before marco was ready").unwrap()
});

pub static SEGMENT_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "marco_segment_write_seconds",
        "Writing one change segment: encode, write, fsync, rename",
        exponential_buckets(1e-5, 2.0, 22).unwrap()
    )
    .unwrap()
});

pub static SEGMENT_BYTES: LazyLock<IntCounter> =
    LazyLock::new(|| register_int_counter!("marco_segment_bytes_total", "Change segment bytes written").unwrap());

pub static SEGMENT_ERRORS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("marco_segment_errors_total", "Change segment writes that failed (each is retried)").unwrap()
});

pub static CHECKPOINT_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "marco_checkpoint_seconds",
        "A checkpoint, cut to new copies serving",
        exponential_buckets(0.01, 2.0, 18).unwrap()
    )
    .unwrap()
});

pub static CHECKPOINT_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_checkpoint_errors_total",
        "Checkpoint failures, and failures after a commit (cleanup, reopen), by stage",
        &["stage"]
    )
    .unwrap()
});

pub static CHECKPOINT_BYTES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "marco_store_checkpoint_bytes",
        "The last checkpoint's bytes: patch (R, changed lists and blocks), pinned (P_new, the patches' index, heads and tables), base (new bases)",
        &["part"]
    )
    .unwrap()
});

pub static COMPACTION_SHARDS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("marco_store_compaction_shards_total", "Shards compacted into a new base").unwrap()
});

pub static COMPACTION_BYTES: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("marco_store_compaction_bytes_total", "Bytes of the bases compactions wrote").unwrap()
});

pub static COMPACTION_GARBAGE: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("marco_store_compaction_garbage_bytes_total", "Bytes of garbage compactions cleared").unwrap()
});

pub static COMPACTION_PENDING: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("marco_store_compaction_pending", "Shards the running compaction has left, 0 when none runs")
        .unwrap()
});

pub static BESIDE: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "marco_process_beside_bytes",
        "What the process holds beside the store: its RSS less the store's books"
    )
    .unwrap()
});

pub static RESERVE_SHORT: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "marco_store_reserve_short_bytes",
        "What the process holds beside the store past its memory limit's room that the store couldn't give back"
    )
    .unwrap()
});

pub static COMPACTION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "marco_store_compaction_seconds",
        "One shard's compaction, paced writes included",
        exponential_buckets(0.01, 2.0, 18).unwrap()
    )
    .unwrap()
});

pub fn init(version: &str, commit: &str) {
    BUILD.with_label_values(&[version, commit]).set(1);
    let _ = prometheus::register(Box::new(prometheus::process_collector::ProcessCollector::for_self()));
    LazyLock::force(&RPC_SECONDS);
    LazyLock::force(&REQUEST_PAGES);
    LazyLock::force(&REQUEST_ROUNDS);
    LazyLock::force(&POOL_BUSY);
    LazyLock::force(&POOL_WAIT);
    LazyLock::force(&BUSY);
    LazyLock::force(&RPC_ERRORS);
    // every series at 0 from the start, so a rate over it never misses its first cut
    for rpc in ["Graph/Relationships", "Graph/BlockPairs", "Graph/Filter"] {
        for reason in ["cap", "deletes"] {
            VIA_TRUNCATED.with_label_values(&[rpc, reason]);
        }
    }
    LazyLock::force(&READY);
    LazyLock::force(&crate::peer::REFUSED);
    LazyLock::force(&READS_BEFORE_READY);
    LazyLock::force(&SEGMENT_SECONDS);
    LazyLock::force(&SEGMENT_BYTES);
    LazyLock::force(&SEGMENT_ERRORS);
    LazyLock::force(&CHECKPOINT_SECONDS);
    LazyLock::force(&CHECKPOINT_ERRORS);
    LazyLock::force(&CHECKPOINT_BYTES);
    LazyLock::force(&COMPACTION_SHARDS);
    LazyLock::force(&COMPACTION_BYTES);
    LazyLock::force(&COMPACTION_GARBAGE);
    LazyLock::force(&COMPACTION_SECONDS);
    LazyLock::force(&COMPACTION_PENDING);
    LazyLock::force(&BESIDE);
    LazyLock::force(&RESERVE_SHORT);
}

/// `Graph/Has` from `/marco.v1.Graph/Has`.
pub fn rpc_name(path: &str) -> Option<&str> {
    path.strip_prefix("/marco.v1.")
}

pub async fn time_rpcs(req: Request, next: Next) -> Response {
    // a worker that hasn't parked yet isn't marked; one serving an RPC is
    marco_core::read::mark_async_thread();
    let Some(rpc) = rpc_name(req.uri().path()).map(str::to_string) else { return next.run(req).await };
    let t = Instant::now();
    let path = std::cell::Cell::new("fast");
    let (res, path) = PATH
        .scope(path, async move {
            let res = next.run(req).await;
            (res, PATH.with(|p| p.get()))
        })
        .await;
    RPC_SECONDS.with_label_values(&[&rpc, path]).observe(t.elapsed().as_secs_f64());
    let grpc_err = res.headers().get("grpc-status").is_some_and(|v| v != "0");
    if !res.status().is_success() || grpc_err {
        RPC_ERRORS.with_label_values(&[&rpc, res.status().as_str()]).inc();
    }
    res
}

/// Where the store's budget is, by part, in bytes: the numbers behind
/// `marco_store_bytes` and the console's Cache panel.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreBytes {
    pub budget: u64,
    pub pinned_dir: u64,
    pub pinned_index: u64,
    pub pinned_heads: u64,
    /// Page sums, slots, side tables, statuses and the engine's own tables.
    pub pinned_tables: u64,
    pub cache_main: u64,
    pub cache_small: u64,
    pub retired: u64,
    /// Cached pages held by a request (inside main and small).
    pub pinned_frames: u64,
    pub free: u64,
    /// Borrowed by anything but the served snapshot's files: a checkpoint's
    /// new files before their switch, a job's borrow.
    pub borrowed: u64,
    /// Paged bytes in the shards' files that no index names (cached at full
    /// residency, until compaction), as of the last checkpoint or compaction.
    pub garbage: u64,
    /// Borrowed for what the process holds beside the store past the room its
    /// memory limit leaves ([`crate::memory`]).
    pub reserved: u64,
}

impl StoreBytes {
    pub fn parts(&self) -> [(&'static str, u64); 13] {
        [
            ("budget", self.budget),
            ("pinned_dir", self.pinned_dir),
            ("pinned_index", self.pinned_index),
            ("pinned_heads", self.pinned_heads),
            ("pinned_tables", self.pinned_tables),
            ("cache_main", self.cache_main),
            ("cache_small", self.cache_small),
            ("retired", self.retired),
            ("pinned_frames", self.pinned_frames),
            ("free", self.free),
            ("borrowed", self.borrowed),
            ("garbage", self.garbage),
            ("reserved", self.reserved),
        ]
    }
}

/// The store's budget by part, from the engine's books and the served
/// snapshot's files.
pub fn store_bytes(app: &crate::app::App) -> StoreBytes {
    use marco_core::store::StoreFile;
    let b = app.engine.bytes();
    let page = marco_core::store::page::PAGE as u64;
    let mut s = StoreBytes {
        budget: b.budget,
        cache_main: b.main_frames * page,
        cache_small: b.small_frames * page,
        retired: b.retired_frames * page,
        pinned_frames: b.pinned_frames * page,
        garbage: app.garbage_bytes.load(std::sync::atomic::Ordering::Relaxed),
        ..Default::default()
    };
    let mut files = 0u64;
    if let Some(snap) = app.graph().snapshot() {
        for (i, (_, f)) in snap.files().enumerate() {
            let StoreFile { file, pinned } = f;
            files += file.borrowed();
            s.pinned_tables += file.borrowed();
            let Some(p) = pinned else { continue };
            files += p.borrowed();
            if i == 0 {
                s.pinned_dir += p.borrowed();
                continue;
            }
            let (mut idx, mut head) = (0, 0);
            for (name, len) in p.sections() {
                if name.ends_with(".idx") {
                    idx += len as u64;
                } else if name.ends_with(".head") {
                    head += len as u64;
                }
            }
            s.pinned_index += idx;
            s.pinned_heads += head;
            s.pinned_tables += p.borrowed().saturating_sub(idx + head);
        }
    }
    s.pinned_tables += b.tables;
    s.reserved = app.beside.reserved.load(std::sync::atomic::Ordering::Relaxed);
    s.borrowed = b.borrowed.saturating_sub(files + s.reserved);
    s.free =
        b.budget.saturating_sub(b.borrowed + b.tables + (b.main_frames + b.small_frames + b.retired_frames) * page);
    s
}

static WATCHED: std::sync::Mutex<std::sync::Weak<crate::app::App>> = std::sync::Mutex::new(std::sync::Weak::new());

/// Makes `app` the one whose engine the store metrics describe.
pub fn watch(app: &std::sync::Arc<crate::app::App>) {
    *WATCHED.lock().unwrap() = std::sync::Arc::downgrade(app);
    LazyLock::force(&STORE_METRICS);
}

static STORE_METRICS: LazyLock<()> = LazyLock::new(|| {
    let _ = prometheus::register(Box::new(StoreMetrics::new()));
});

/// The engine's metrics, read from it at each scrape (§13 of the storage
/// design): where the budget is, lookups by structure, the I/O lanes, read
/// errors, the policy's evictions.
struct StoreMetrics {
    descs: Vec<prometheus::core::Desc>,
}

const STORE_FAMILIES: [(&str, &str, &[&str]); 15] = [
    ("marco_store_bytes", "Where the store's budget is, by part (bytes)", &["part"]),
    (
        "marco_store_lookups_total",
        "Page lookups by structure, hit or miss, the queue a hit was in, and who read (request, write, job)",
        &["structure", "result", "queue", "source"],
    ),
    ("marco_store_io_seconds", "Device read latency per read call, by lane (request, background)", &["queue"]),
    ("marco_store_io_bytes_total", "Bytes read from the device, by lane", &["queue"]),
    ("marco_store_io_queue_depth", "Reads waiting for an I/O thread, by lane", &["queue"]),
    (
        "marco_store_read_errors_total",
        "Page reads that failed (checksum, eio, short) or took a retry to succeed (eio_retried)",
        &["kind"],
    ),
    ("marco_store_bad_pages", "Pages marked bad: their requests fail until a verify reads them clean", &[]),
    (
        "marco_store_failing_pages",
        "Distinct pages whose reads failed on the device in the last 60 s; 64 take /readyz down",
        &[],
    ),
    ("marco_store_evictions_total", "Frames the policy evicted, by queue", &["queue"]),
    ("marco_store_ghost_hits_total", "Misses the ghost remembered, sent straight to the main queue", &[]),
    (
        "marco_store_write_through_pages_total",
        "Pages checkpoints and compactions wrote into the cache as they wrote them, or let go",
        &["result"],
    ),
    ("marco_store_past_pages_total", "Pages read past the cache into a job's own memory (compaction, list stats)", &[]),
    (
        "marco_store_stalled_requests_total",
        "Requests whose missing pages stopped shrinking for 16 rounds: a bug signal",
        &[],
    ),
    (
        "marco_store_bytes_used_total",
        "Bytes of lists and blocks requests decoded, by structure, each block once per pass: bytes read over this is the read amplification",
        &["structure"],
    ),
    ("marco_store_io_queue_wait_seconds", "How long reads waited in the queue for an I/O thread, by lane", &["queue"]),
];

impl StoreMetrics {
    fn new() -> StoreMetrics {
        let descs = STORE_FAMILIES
            .iter()
            .map(|(n, h, l)| {
                prometheus::core::Desc::new(
                    n.to_string(),
                    h.to_string(),
                    l.iter().map(|s| s.to_string()).collect(),
                    Default::default(),
                )
                .unwrap()
            })
            .collect();
        StoreMetrics { descs }
    }
}

fn family(i: usize, t: prometheus::proto::MetricType) -> prometheus::proto::MetricFamily {
    let mut f = prometheus::proto::MetricFamily::default();
    f.set_name(STORE_FAMILIES[i].0.into());
    f.set_help(STORE_FAMILIES[i].1.into());
    f.set_field_type(t);
    f
}

fn metric(labels: &[(&str, &str)]) -> prometheus::proto::Metric {
    let mut m = prometheus::proto::Metric::default();
    m.set_label(
        labels
            .iter()
            .map(|(k, v)| {
                let mut l = prometheus::proto::LabelPair::default();
                l.set_name(k.to_string());
                l.set_value(v.to_string());
                l
            })
            .collect(),
    );
    m
}

fn gauge(f: &mut prometheus::proto::MetricFamily, labels: &[(&str, &str)], v: f64) {
    let mut m = metric(labels);
    let mut g = prometheus::proto::Gauge::default();
    g.set_value(v);
    m.set_gauge(g);
    f.mut_metric().push(m);
}

fn counter(f: &mut prometheus::proto::MetricFamily, labels: &[(&str, &str)], v: u64) {
    let mut m = metric(labels);
    let mut c = prometheus::proto::Counter::default();
    c.set_value(v as f64);
    m.set_counter(c);
    f.mut_metric().push(m);
}

/// One lane's [`marco_core::store::io::IoLatency`] as a histogram: buckets of 2^k µs.
fn io_hist(labels: &[(&str, &str)], l: &marco_core::store::io::IoLatency) -> prometheus::proto::Metric {
    let mut m = metric(labels);
    let mut hist = prometheus::proto::Histogram::default();
    hist.set_sample_count(l.count);
    hist.set_sample_sum(l.nanos as f64 / 1e9);
    let mut cum = 0;
    let mut bs = Vec::new();
    for (k, &n) in l.buckets.iter().take(marco_core::store::io::IO_BUCKETS).enumerate() {
        cum += n;
        let mut b = prometheus::proto::Bucket::default();
        b.set_cumulative_count(cum);
        b.set_upper_bound((1u64 << k) as f64 / 1e6);
        bs.push(b);
    }
    hist.set_bucket(bs);
    m.set_histogram(hist);
    m
}

impl prometheus::core::Collector for StoreMetrics {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        use marco_core::read::{Found, Source, Structure};
        use prometheus::proto::MetricType::{COUNTER, GAUGE, HISTOGRAM};
        let Some(app) = WATCHED.lock().unwrap().upgrade() else { return Vec::new() };
        let e = &app.engine;
        let st = e.stats();
        let mut out = Vec::with_capacity(STORE_FAMILIES.len());

        let mut f = family(0, GAUGE);
        for (part, v) in store_bytes(&app).parts() {
            gauge(&mut f, &[("part", part)], v as f64);
        }
        out.push(f);

        let mut f = family(1, COUNTER);
        for src in Source::ALL {
            for s in Structure::ALL {
                for h in Found::ALL {
                    let (result, queue) = match h {
                        Found::Small => ("hit", "small"),
                        Found::Main => ("hit", "main"),
                        Found::Other => ("hit", "none"),
                        Found::Miss => ("miss", "none"),
                    };
                    counter(
                        &mut f,
                        &[("structure", s.name()), ("result", result), ("queue", queue), ("source", src.name())],
                        st.lookups[src as usize][s as usize][h as usize],
                    );
                }
            }
        }
        out.push(f);

        let lat = e.io_latency();
        let (depth_r, depth_b) = e.io_depth();
        let lanes = ["request", "background"];
        let mut h = family(2, HISTOGRAM);
        let mut bytes = family(3, COUNTER);
        let mut depth = family(4, GAUGE);
        for (i, l) in lat.iter().enumerate() {
            h.mut_metric().push(io_hist(&[("queue", lanes[i])], l));
            counter(&mut bytes, &[("queue", lanes[i])], l.bytes);
            gauge(&mut depth, &[("queue", lanes[i])], [depth_r, depth_b][i] as f64);
        }
        out.extend([h, bytes, depth]);

        let mut f = family(5, COUNTER);
        for (kind, n) in
            [("checksum", st.checksum), ("eio", st.eio), ("short", st.short), ("eio_retried", st.eio_retried)]
        {
            counter(&mut f, &[("kind", kind)], n);
        }
        out.push(f);
        let mut f = family(6, GAUGE);
        gauge(&mut f, &[], st.bad_pages as f64);
        out.push(f);
        let mut f = family(7, GAUGE);
        gauge(&mut f, &[], e.failing_pages(marco_core::store::engine::STORM_WINDOW) as f64);
        out.push(f);
        let mut f = family(8, COUNTER);
        counter(&mut f, &[("queue", "small")], st.evict_small);
        counter(&mut f, &[("queue", "main")], st.evict_main);
        out.push(f);
        let mut f = family(9, COUNTER);
        counter(&mut f, &[], st.ghost_hits);
        out.push(f);
        let mut f = family(10, COUNTER);
        counter(&mut f, &[("result", "published")], st.written_through);
        counter(&mut f, &[("result", "skipped")], st.write_through_skipped);
        out.push(f);
        let mut f = family(11, COUNTER);
        counter(&mut f, &[], st.past_pages);
        out.push(f);
        let mut f = family(12, COUNTER);
        counter(&mut f, &[], st.stalled_requests);
        out.push(f);
        let mut f = family(13, COUNTER);
        for s in Structure::ALL {
            counter(&mut f, &[("structure", s.name())], st.bytes_used[s as usize]);
        }
        out.push(f);
        let mut f = family(14, HISTOGRAM);
        for (i, l) in e.io_queue_wait().iter().enumerate() {
            f.mut_metric().push(io_hist(&[("queue", lanes[i])], l));
        }
        out.push(f);
        out
    }
}
