//! Every metric seek exports, in the default registry. [`init`] touches each
//! label set, so every series exists at 0 from startup. Names and meanings
//! are in docs/operations-seek.md.

use std::sync::{Arc, LazyLock};

use prometheus::{
    register_gauge, register_int_counter, register_int_counter_vec, register_int_gauge, register_int_gauge_vec, Gauge,
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
};

use crate::hist::{self, Family, Hist};

macro_rules! counter_vec {
    ($n:expr, $h:expr, $l:expr) => {
        LazyLock::new(|| register_int_counter_vec!($n, $h, $l).unwrap())
    };
}
macro_rules! counter {
    ($n:expr, $h:expr) => {
        LazyLock::new(|| register_int_counter!($n, $h).unwrap())
    };
}
macro_rules! gauge {
    ($n:expr, $h:expr) => {
        LazyLock::new(|| register_int_gauge!($n, $h).unwrap())
    };
}
macro_rules! fgauge {
    ($n:expr, $h:expr) => {
        LazyLock::new(|| register_gauge!($n, $h).unwrap())
    };
}

pub const MODES: [&str; 2] = ["logged_out", "logged_in"];
pub const STAGES: [&str; 5] = ["viewer", "prefix", "personal", "global", "filter"];
pub const QUERY_ERRORS: [&str; 5] = ["bad_request", "not_ready", "marco", "no_marco", "internal"];
pub const MARCO_METHODS: [&str; 3] = ["filter", "list_out", "intersect"];
pub const MARCO_ERRORS: [&str; 3] = ["timeout", "unavailable", "other"];
pub const EVENT_KINDS: [&str; 5] = ["profile", "identity", "account", "follow", "skipped"];
pub const VERIFY_RESULTS: [&str; 4] = ["ok", "mismatch", "error", "rate_limited"];
pub const VERIFY_METHODS: [&str; 3] = ["dns", "well_known", "did_doc"];
pub const BUILDER_STEPS: [&str; 8] = ["ingest", "mirror", "pagerank", "ranks", "build", "verify", "upload", "state"];

// ---- queries
pub static QUERIES: LazyLock<IntCounterVec> = counter_vec!(
    "seek_queries_total",
    "Typeahead queries answered, by mode and result (ok, empty, error)",
    &["mode", "result"]
);
pub static QUERY_ERRORS_TOTAL: LazyLock<IntCounterVec> =
    counter_vec!("seek_query_errors_total", "Queries that failed, by kind", &["kind"]);
pub static RESULTS: LazyLock<IntCounterVec> =
    counter_vec!("seek_query_results_total", "Actors returned, by mode", &["mode"]);
pub static IN_FLIGHT: LazyLock<IntGauge> = gauge!("seek_queries_in_flight", "Queries being answered now");
pub static QUERY_SECONDS: LazyLock<Arc<Family>> =
    LazyLock::new(|| hist::seconds("seek_query_seconds", "Query latency inside seek, by mode", &["mode"]));
pub static STAGE_SECONDS: LazyLock<Arc<Family>> = LazyLock::new(|| {
    hist::seconds(
        "seek_query_stage_seconds",
        "Query latency by stage: viewer (the cache, marco on a miss), prefix (base range and exact handles), personal (the viewer's follows), global (the fill), filter (marco's block filter)",
        &["stage"],
    )
});
pub static FALLBACKS: LazyLock<IntCounter> = counter!(
    "seek_query_logged_out_fallbacks_total",
    "Logged-in queries answered logged out because the viewer's follows couldn't be read"
);

pub static BLOCK_DROPPED: LazyLock<IntCounter> =
    counter!("seek_query_blocked_total", "Candidates the block filter dropped (blocks either way, or hidden in marco)");
pub static EXACT_KEPT: LazyLock<IntCounter> = counter!(
    "seek_query_exact_kept_total",
    "Exact handle matches kept on the page though the block filter would have dropped them"
);

// ---- the viewer cache
pub static VIEWER_HITS: LazyLock<IntCounter> = counter!("seek_viewer_cache_hits_total", "Viewer cache hits");
pub static VIEWER_MISSES: LazyLock<IntCounter> =
    counter!("seek_viewer_cache_misses_total", "Viewer cache misses (a build)");
pub static VIEWER_EVICTIONS: LazyLock<IntCounterVec> = counter_vec!(
    "seek_viewer_cache_evictions_total",
    "Viewer cache entries removed, by cause (idle, size, invalidated)",
    &["cause"]
);
pub static VIEWER_ENTRIES: LazyLock<IntGauge> = gauge!("seek_viewer_cache_entries", "Viewers in the cache");
pub static VIEWER_BYTES: LazyLock<IntGauge> = gauge!("seek_viewer_cache_bytes", "The viewer cache's weighed size");
pub static VIEWER_BUILD: LazyLock<Arc<Family>> = LazyLock::new(|| {
    hist::seconds("seek_viewer_build_seconds", "Building a viewer's entry: marco's follows and mutuals", &[])
});
pub static VIEWER_KEYS: LazyLock<Arc<Family>> = LazyLock::new(|| {
    hist::seconds("seek_viewer_keys_seconds", "Folding a viewer's follows' keys against the current artifact", &[])
});
pub static VIEWER_FOLLOWS: LazyLock<Arc<Family>> =
    LazyLock::new(|| hist::counts("seek_viewer_follows", "Follows of each viewer built", &[]));

// ---- marco
pub static MARCO_SECONDS: LazyLock<Arc<Family>> =
    LazyLock::new(|| hist::seconds("seek_marco_seconds", "marco call latency, by method", &["method"]));
pub static MARCO_ERRS: LazyLock<IntCounterVec> = counter_vec!(
    "seek_marco_errors_total",
    "marco calls that failed, by method and kind (timeout, unavailable, other)",
    &["method", "kind"]
);

// ---- the artifact
pub static ARTIFACT_INFO: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!("seek_artifact_info", "1 for the artifact being served, by version", &["version"]).unwrap()
});
pub static ARTIFACT_BUILT: LazyLock<Gauge> =
    fgauge!("seek_artifact_built_timestamp_seconds", "When the served artifact was built");
pub static ARTIFACT_CURSOR: LazyLock<Gauge> =
    fgauge!("seek_artifact_cursor_timestamp_seconds", "The Jetstream time the served artifact is consistent with");
pub static ARTIFACT_AGE: LazyLock<Gauge> =
    fgauge!("seek_artifact_age_seconds", "Now less the served artifact's build time (set at scrape)");
pub static ARTIFACT_BUILD_SECS: LazyLock<Gauge> =
    fgauge!("seek_artifact_build_seconds", "How long the served artifact's index build took");
pub static ARTIFACT_ACTORS: LazyLock<IntGauge> = gauge!("seek_artifact_actors", "Actors in the served artifact");
pub static MAPPED: LazyLock<IntGauge> = gauge!("seek_artifact_mapped_bytes", "Bytes of artifacts mapped now");
pub static DOWNLOAD_BYTES: LazyLock<IntCounter> =
    counter!("seek_artifact_download_bytes_total", "Artifact bytes downloaded");
pub static DOWNLOAD_SECONDS: LazyLock<Arc<Family>> =
    LazyLock::new(|| hist::seconds("seek_artifact_download_seconds", "Downloading and verifying an artifact", &[]));
pub static VERIFY_FAILURES: LazyLock<IntCounter> =
    counter!("seek_artifact_verify_failures_total", "Artifacts refused: bad checksum, version or layout");
pub static SWAPS: LazyLock<IntCounter> = counter!("seek_artifact_swaps_total", "Artifacts swapped in");
pub static SWAP_SECONDS: LazyLock<Arc<Family>> = LazyLock::new(|| {
    hist::seconds("seek_artifact_swap_seconds", "Opening a new artifact to serving it: catch-up and the swap", &[])
});
pub static STALE: LazyLock<IntGauge> = gauge!(
    "seek_refusing_stale",
    "1 while seek refuses to serve: its artifact's cursor is older than Jetstream's replay window"
);
pub static REFUSED: LazyLock<IntCounterVec> = counter_vec!(
    "seek_artifact_refused_total",
    "Artifacts not swapped in, by reason (invalid, older, uncovered: the ring can't replay from its cursor, gap)",
    &["reason"]
);
pub static POLL_FAILURES: LazyLock<IntCounter> =
    counter!("seek_artifact_poll_failures_total", "Polls of the artifact source that failed");
pub static RING: LazyLock<IntGauge> =
    gauge!("seek_ring_entries", "Changes kept for replay into the next artifact's overlay");
pub static SWAP_REPLAYED: LazyLock<IntGauge> =
    gauge!("seek_artifact_swap_replayed", "Changes the last swap replayed into the new overlay");
pub static INGEST_PANICS: LazyLock<IntCounter> =
    counter!("seek_ingest_panics_total", "Changes that panicked while applied (skipped; a bug)");
pub static READY: LazyLock<IntGauge> = gauge!("seek_ready", "1 while seek serves queries (see /readyz for why not)");

// ---- the overlay and Jetstream
pub static OVERLAY_ACTORS: LazyLock<IntGauge> = gauge!("seek_overlay_actors", "Actors the live overlay speaks for");
pub static OVERLAY_KEYS: LazyLock<IntGauge> = gauge!("seek_overlay_keys", "Keys in the live overlay");
pub static OVERLAY_BYTES: LazyLock<IntGauge> =
    gauge!("seek_overlay_bytes", "The live overlay's keys' approximate size");
pub static EVENTS: LazyLock<IntCounterVec> =
    counter_vec!("seek_jetstream_events_total", "Jetstream events read, by kind", &["kind"]);
pub static JS_LAG: LazyLock<Gauge> = fgauge!("seek_jetstream_lag_seconds", "Now less the last Jetstream event's time");
pub static JS_CURSOR: LazyLock<Gauge> =
    fgauge!("seek_jetstream_cursor_timestamp_seconds", "The last Jetstream event's time");
pub static JS_RECONNECTS: LazyLock<IntCounter> = counter!("seek_jetstream_reconnects_total", "Jetstream redials");
pub static JS_DECODE: LazyLock<IntCounter> =
    counter!("seek_jetstream_decode_errors_total", "Jetstream messages that didn't decode");
pub static JS_GAPS: LazyLock<IntCounter> = counter!(
    "seek_jetstream_gaps_total",
    "Replays whose first event came after the cursor asked for: the cursor was past Jetstream's window"
);

// ---- handle verification
pub static VERIFY_QUEUE: LazyLock<IntGauge> = gauge!("seek_verify_queue", "Handles waiting to be verified");
pub static VERIFY_CHECKS: LazyLock<IntCounterVec> = counter_vec!(
    "seek_verify_checks_total",
    "Handle checks, by result (ok, mismatch, error, rate_limited) and method (dns, well_known, did_doc)",
    &["result", "method"]
);
pub static VERIFY_SECONDS: LazyLock<Arc<Family>> =
    LazyLock::new(|| hist::seconds("seek_verify_seconds", "Handle check latency, by method", &["method"]));

// ---- the builder
pub static STEP_SECONDS: LazyLock<Arc<Family>> =
    LazyLock::new(|| hist::seconds("seek_builder_step_seconds", "Each builder step, per cycle", &["step"]));
pub static STEP_LAST: LazyLock<prometheus::GaugeVec> = LazyLock::new(|| {
    prometheus::register_gauge_vec!("seek_builder_step_last_seconds", "The last cycle's time in each step", &["step"])
        .unwrap()
});
pub static CYCLES: LazyLock<IntCounterVec> =
    counter_vec!("seek_builder_cycles_total", "Builder cycles, by result (ok, error)", &["result"]);
pub static LAST_SUCCESS: LazyLock<Gauge> =
    fgauge!("seek_builder_last_success_timestamp_seconds", "When the builder last published an artifact");
pub static PR_PASSES: LazyLock<IntGauge> = gauge!("seek_pagerank_passes", "The last PageRank run's passes");
pub static PR_L1: LazyLock<Gauge> = fgauge!("seek_pagerank_l1", "The last PageRank run's final L1 change");
pub static PR_SECONDS: LazyLock<Gauge> = fgauge!("seek_pagerank_seconds", "The last PageRank run's wall time");
pub static PR_RUNS: LazyLock<IntCounterVec> =
    counter_vec!("seek_pagerank_runs_total", "PageRank runs the builder started, by result", &["result"]);
pub static ARTIFACT_BYTES: LazyLock<IntGauge> = gauge!("seek_builder_artifact_bytes", "The last artifact's size");
pub static UPLOAD_SECONDS: LazyLock<Gauge> = fgauge!("seek_builder_upload_seconds", "The last artifact's upload time");
pub static CHECKPOINT_AGE: LazyLock<Gauge> =
    fgauge!("seek_builder_ranks_age_seconds", "Now less the creation time of the ranks baked into the last artifact");
pub static BUILDER_ACTORS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "seek_builder_actors",
        "Actors the builder holds, by state (known, indexed, handle_verified, handle_unverified, hidden)",
        &["state"]
    )
    .unwrap()
});

/// Registers every metric and creates every label set at 0, plus the process collector.
pub fn init() {
    for m in MODES {
        for r in ["ok", "empty", "error"] {
            QUERIES.with_label_values(&[m, r]);
        }
        RESULTS.with_label_values(&[m]);
        QUERY_SECONDS.with(&[m]);
    }
    for s in STAGES {
        STAGE_SECONDS.with(&[s]);
    }
    for k in QUERY_ERRORS {
        QUERY_ERRORS_TOTAL.with_label_values(&[k]);
    }
    for r in ["invalid", "older", "uncovered", "gap"] {
        REFUSED.with_label_values(&[r]);
    }
    LazyLock::force(&BLOCK_DROPPED);
    LazyLock::force(&EXACT_KEPT);
    LazyLock::force(&POLL_FAILURES);
    LazyLock::force(&RING);
    LazyLock::force(&SWAP_REPLAYED);
    LazyLock::force(&INGEST_PANICS);
    for m in MARCO_METHODS {
        MARCO_SECONDS.with(&[m]);
        for k in MARCO_ERRORS {
            MARCO_ERRS.with_label_values(&[m, k]);
        }
    }
    for c in ["idle", "size", "invalidated"] {
        VIEWER_EVICTIONS.with_label_values(&[c]);
    }
    for k in EVENT_KINDS {
        EVENTS.with_label_values(&[k]);
    }
    for r in VERIFY_RESULTS {
        for m in VERIFY_METHODS {
            VERIFY_CHECKS.with_label_values(&[r, m]);
        }
    }
    for m in VERIFY_METHODS {
        VERIFY_SECONDS.with(&[m]);
    }
    for s in BUILDER_STEPS {
        STEP_SECONDS.with(&[s]);
        STEP_LAST.with_label_values(&[s]);
    }
    for r in ["ok", "error"] {
        CYCLES.with_label_values(&[r]);
        PR_RUNS.with_label_values(&[r]);
    }
    for s in ["known", "indexed", "handle_verified", "handle_unverified", "hidden"] {
        BUILDER_ACTORS.with_label_values(&[s]);
    }
    LazyLock::force(&IN_FLIGHT);
    LazyLock::force(&FALLBACKS);
    LazyLock::force(&VIEWER_HITS);
    LazyLock::force(&VIEWER_MISSES);
    LazyLock::force(&VIEWER_ENTRIES);
    LazyLock::force(&VIEWER_BYTES);
    LazyLock::force(&VIEWER_BUILD).with(&[]);
    LazyLock::force(&VIEWER_KEYS).with(&[]);
    LazyLock::force(&VIEWER_FOLLOWS).with(&[]);
    LazyLock::force(&ARTIFACT_INFO);
    LazyLock::force(&ARTIFACT_BUILT);
    LazyLock::force(&ARTIFACT_CURSOR);
    LazyLock::force(&ARTIFACT_AGE);
    LazyLock::force(&ARTIFACT_BUILD_SECS);
    LazyLock::force(&ARTIFACT_ACTORS);
    LazyLock::force(&MAPPED);
    LazyLock::force(&DOWNLOAD_BYTES);
    LazyLock::force(&DOWNLOAD_SECONDS).with(&[]);
    LazyLock::force(&VERIFY_FAILURES);
    LazyLock::force(&SWAPS);
    LazyLock::force(&SWAP_SECONDS).with(&[]);
    LazyLock::force(&STALE);
    LazyLock::force(&READY);
    LazyLock::force(&OVERLAY_ACTORS);
    LazyLock::force(&OVERLAY_KEYS);
    LazyLock::force(&OVERLAY_BYTES);
    LazyLock::force(&JS_LAG);
    LazyLock::force(&JS_CURSOR);
    LazyLock::force(&JS_RECONNECTS);
    LazyLock::force(&JS_DECODE);
    LazyLock::force(&JS_GAPS);
    LazyLock::force(&VERIFY_QUEUE);
    LazyLock::force(&LAST_SUCCESS);
    LazyLock::force(&PR_PASSES);
    LazyLock::force(&PR_L1);
    LazyLock::force(&PR_SECONDS);
    LazyLock::force(&ARTIFACT_BYTES);
    LazyLock::force(&UPLOAD_SECONDS);
    LazyLock::force(&CHECKPOINT_AGE);
    let pc = prometheus::process_collector::ProcessCollector::for_self();
    let _ = prometheus::register(Box::new(pc));
}

/// The query path's histograms, resolved once so a keystroke does no label lookup.
pub struct QueryHists {
    pub mode: [Arc<Hist>; 2],
    pub stage: [Arc<Hist>; 5],
    pub marco: [Arc<Hist>; 3],
    /// seek_queries_total by mode, then result (ok, empty, error).
    pub queries: [[IntCounter; 3]; 2],
    pub results: [IntCounter; 2],
    pub errors: [IntCounter; 5],
}

impl QueryHists {
    pub fn new() -> QueryHists {
        QueryHists {
            mode: MODES.map(|m| QUERY_SECONDS.with(&[m])),
            stage: STAGES.map(|s| STAGE_SECONDS.with(&[s])),
            marco: MARCO_METHODS.map(|m| MARCO_SECONDS.with(&[m])),
            queries: MODES.map(|m| ["ok", "empty", "error"].map(|r| QUERIES.with_label_values(&[m, r]))),
            results: MODES.map(|m| RESULTS.with_label_values(&[m])),
            errors: QUERY_ERRORS.map(|k| QUERY_ERRORS_TOTAL.with_label_values(&[k])),
        }
    }

    pub fn error(&self, kind: &str) {
        if let Some(i) = QUERY_ERRORS.iter().position(|k| *k == kind) {
            self.errors[i].inc();
        }
    }
}

impl Default for QueryHists {
    fn default() -> Self {
        QueryHists::new()
    }
}

pub fn now_secs() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0.0, |d| d.as_secs_f64())
}

pub fn now_us() -> u64 {
    (now_secs() * 1e6) as u64
}
