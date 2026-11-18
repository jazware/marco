//! The ops endpoint every marco process can serve, on its own thread and
//! runtime so it never competes with the engine's:
//!
//! ```text
//! GET  /metrics                      Prometheus text, the default registry (+ process_*)
//! GET  /debug/pprof/profile?seconds  CPU profile, pprof protobuf (feature cpu-profile)
//! GET  /debug/pprof/heap             jemalloc heap in use, pprof (feature jemalloc)
//! GET  /debug/tracing                the trace ratio; POST ?ratio=0.01 sets it
//! POST /v1/traces                    an OTLP sink that drops what it gets (serve only)
//! ```
//!
//! Paths match vlpds and vlRelay, so the same scrapers can pull from it.
//! [`router`] has no auth of its own: marco-server mounts it behind the
//! admin token. [`serve`], marco-load's endpoint, checks no one, so bind it
//! to loopback. Only it has the sink, which `marco-load bench --otlp`
//! exports its spans to.
//!
//! [`drain`] stops a process's listeners cleanly behind a load balancer.

pub mod drain;

use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::body::Bytes;
use axum::extract::Query;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use prometheus::{register_int_counter, Encoder, IntCounter, TextEncoder};
use serde::Deserialize;

pub use marco_core::obs::{set_trace_ratio, trace_ratio};
#[cfg(feature = "jemalloc")]
pub use tikv_jemallocator;

static SINK_BYTES: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("marco_ops_otlp_sink_bytes_total", "OTLP bytes the bench sink received and dropped").unwrap()
});

/// Declares jemalloc the global allocator with its heap sampler on (one
/// sample per 512 KiB allocated), which `/debug/pprof/heap` reads.
#[macro_export]
macro_rules! jemalloc {
    () => {
        #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
        #[global_allocator]
        static GLOBAL: $crate::tikv_jemallocator::Jemalloc = $crate::tikv_jemallocator::Jemalloc;

        #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
        #[allow(non_upper_case_globals)]
        #[export_name = "malloc_conf"]
        pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";
    };
}

pub type Refresh = Arc<dyn Fn() + Send + Sync>;

/// Serves the ops routes on `addr` from a thread of its own. `refresh` runs
/// before each scrape (graph gauges).
pub fn serve(addr: &str, refresh: Option<Refresh>) -> Result<SocketAddr> {
    let std_listener = std::net::TcpListener::bind(addr).with_context(|| format!("bind ops {addr}"))?;
    std_listener.set_nonblocking(true)?;
    let local = std_listener.local_addr()?;
    let pc = prometheus::process_collector::ProcessCollector::for_self();
    let _ = prometheus::register(Box::new(pc));
    std::thread::Builder::new().name("marco-ops".into()).spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("ops runtime");
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(std_listener).expect("ops listener");
            let app = router(refresh).route("/v1/traces", axum::routing::post(sink));
            axum::serve(listener, app).await.expect("ops server");
        });
    })?;
    Ok(local)
}

pub fn router(refresh: Option<Refresh>) -> Router {
    read_router(refresh).route("/debug/tracing", get(get_tracing).post(set_tracing))
}

/// /metrics and the pprof GETs: nothing in it changes the process.
pub fn read_router(refresh: Option<Refresh>) -> Router {
    Router::new()
        .route("/metrics", get(move || metrics(refresh.clone())))
        .route("/debug/pprof/profile", get(profile))
        .route("/debug/pprof/heap", get(heap))
}

/// The default registry as Prometheus text.
pub fn metrics_text() -> String {
    let mut buf = Vec::new();
    TextEncoder::new().encode(&prometheus::gather(), &mut buf).expect("text encoding");
    String::from_utf8(buf).unwrap_or_default()
}

async fn metrics(refresh: Option<Refresh>) -> Response {
    if let Some(f) = refresh {
        f();
    }
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], metrics_text()).into_response()
}

#[derive(Deserialize)]
struct Seconds {
    seconds: Option<u64>,
}

async fn profile(Query(q): Query<Seconds>) -> Response {
    let secs = q.seconds.unwrap_or(30).clamp(1, 300);
    match cpu_profile(Duration::from_secs(secs)).await {
        Ok(b) => ([(header::CONTENT_TYPE, "application/octet-stream")], b).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[cfg(feature = "cpu-profile")]
static CPU_BUSY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// A CPU profile over `d`, as pprof protobuf. One at a time: there's one
/// SIGPROF timer per process.
#[cfg(feature = "cpu-profile")]
pub async fn cpu_profile(d: Duration) -> Result<Vec<u8>> {
    use pprof::protos::Message;
    use std::sync::atomic::Ordering;
    if CPU_BUSY.swap(true, Ordering::AcqRel) {
        anyhow::bail!("a CPU profile is already running");
    }
    let r = async {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(99)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()?;
        tokio::time::sleep(d).await;
        let profile = guard.report().build()?.pprof()?;
        Ok(profile.encode_to_vec())
    }
    .await;
    CPU_BUSY.store(false, Ordering::Release);
    r
}

#[cfg(not(feature = "cpu-profile"))]
pub async fn cpu_profile(_d: Duration) -> Result<Vec<u8>> {
    anyhow::bail!("built without the cpu-profile feature")
}

async fn heap() -> Response {
    match heap_profile().await {
        Ok(b) => ([(header::CONTENT_TYPE, "application/octet-stream")], b).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[cfg(feature = "jemalloc")]
pub async fn heap_profile() -> Result<Vec<u8>> {
    let Some(ctl) = jemalloc_pprof::PROF_CTL.as_ref() else {
        anyhow::bail!("jemalloc profiling isn't on (the binary needs marco_ops::jemalloc!())")
    };
    let mut ctl = ctl.lock().await;
    if !ctl.activated() {
        anyhow::bail!("jemalloc profiling is off");
    }
    ctl.dump_pprof().map_err(|e| anyhow::anyhow!("{e}"))
}

#[cfg(not(feature = "jemalloc"))]
pub async fn heap_profile() -> Result<Vec<u8>> {
    anyhow::bail!("built without the jemalloc feature")
}

#[derive(Deserialize)]
struct Ratio {
    ratio: f64,
}

async fn get_tracing() -> String {
    format!("{}\n", trace_ratio())
}

async fn set_tracing(Query(q): Query<Ratio>) -> String {
    set_trace_ratio(q.ratio);
    format!("{}\n", trace_ratio())
}

async fn sink(body: Bytes) -> StatusCode {
    SINK_BYTES.inc_by(body.len() as u64);
    StatusCode::OK
}

/// Spans out as OTLP/HTTP protobuf to `endpoint` (a collector's base URL),
/// batched on the SDK's own thread. Sampling is marco's own
/// ([`set_trace_ratio`]), so the SDK keeps every span it's handed.
pub fn init_tracing(endpoint: &str, service: &str) -> Result<opentelemetry_sdk::trace::SdkTracerProvider> {
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_otlp::WithExportConfig;
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
        .with_endpoint(format!("{}/v1/traces", endpoint.trim_end_matches('/')))
        .with_timeout(Duration::from_secs(5))
        .build()?;
    let batch = opentelemetry_sdk::trace::BatchSpanProcessor::builder(exporter)
        .with_batch_config(opentelemetry_sdk::trace::BatchConfigBuilder::default().with_max_queue_size(65_536).build())
        .build();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_resource(opentelemetry_sdk::Resource::builder().with_service_name(service.to_string()).build())
        .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
        .with_span_processor(batch)
        .build();
    let tracer = provider.tracer("marco");
    let subscriber = tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::set_global_default(subscriber).context("a tracing subscriber is already set")?;
    Ok(provider)
}

#[cfg(all(test, feature = "cpu-profile"))]
mod tests {
    #[test]
    fn cpu_profile_survives_busy_threads() {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut x = 0u64;
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        x = std::hint::black_box(x.wrapping_mul(6364136223846793005).wrapping_add(1));
                    }
                })
            })
            .collect();
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let pb = rt.block_on(super::cpu_profile(std::time::Duration::from_secs(1))).unwrap();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for w in workers {
            w.join().unwrap();
        }
        assert!(pb.len() > 100, "{} bytes", pb.len());
    }
}
