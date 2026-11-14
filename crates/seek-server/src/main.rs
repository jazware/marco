//! seek: actor typeahead for atproto. `seek serve` is the pod; `seek builder`
//! keeps the names and publishes the artifact pods serve.
//!
//! ```text
//! seek serve   --source URL|DIR [--marco URL] [--jetstream URL]     the pod
//! seek builder --dir DIR [--bootstrap FILE --cursor-us T] [--ranks URL|DIR]   the builder
//! seek build   --state FILE --out DIR [--ranks FILE]                one artifact from a saved state
//! seek inspect --artifact FILE [--query Q]                          an artifact's meta, sizes and a query
//! ```
//!
//! Every flag has an env var (`SEEK_*`). docs/operations-seek.md has them all, the metrics and
//! the alerts.

use seek_server::{bench, builder, jetstream, marco, metrics, serve, store, verify};

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tokio::sync::mpsc;

marco_ops::jemalloc!();

#[derive(Parser)]
#[command(version, about = "seek: actor typeahead for atproto")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Args)]
struct Common {
    /// /metrics, /debug/pprof/{profile,heap} and /debug/tracing (the trace switch). Bind it privately.
    #[arg(long, env = "SEEK_OPS_LISTEN", default_value = "127.0.0.1:7311")]
    ops_listen: String,
    /// The share of queries (or builder steps) traced, 0 to 1; /debug/tracing changes it live.
    #[arg(long, env = "SEEK_TRACE_RATIO", default_value_t = 0.0)]
    trace_ratio: f64,
    /// An OTLP/HTTP collector's base URL for traces.
    #[arg(long, env = "SEEK_OTLP")]
    otlp: Option<String>,
    /// Log format: json or text.
    #[arg(long, env = "SEEK_LOG_FORMAT", default_value = "json")]
    log_format: String,
}

#[derive(clap::Args)]
struct Ingest {
    /// Jetstream's base URL.
    #[arg(long, env = "SEEK_JETSTREAM", default_value = "wss://jetstream2.us-east.bsky.network")]
    jetstream: String,
    /// PLC directory for handle verification.
    #[arg(long, env = "SEEK_PLC", default_value = "https://plc.directory")]
    plc: String,
    /// PLC directory reads a second.
    #[arg(long, env = "SEEK_PLC_RATE", default_value_t = 10.0)]
    plc_rate: f64,
    /// Handle lookups a second (DNS TXT and HTTPS well-known, to hosts the handles name).
    #[arg(long, env = "SEEK_VERIFY_RATE", default_value_t = 10.0)]
    verify_rate: f64,
    #[arg(long, env = "SEEK_VERIFY_WORKERS", default_value_t = 8)]
    verify_workers: usize,
    /// Allow http and private addresses in verification (tests only).
    #[arg(long, env = "SEEK_INSECURE", hide = true)]
    insecure: bool,
    /// Verify no handles: every handle change leaves the actor with no handle until the builder's
    /// next artifact (tests, and pods that leave verification to the builder).
    #[arg(long, env = "SEEK_NO_VERIFY")]
    no_verify: bool,
}

#[derive(clap::Args)]
struct S3 {
    #[arg(long, env = "SEEK_S3_ENDPOINT")]
    s3_endpoint: Option<String>,
    #[arg(long, env = "SEEK_S3_REGION", default_value = "auto")]
    s3_region: String,
    /// A JSON file: {"access_key_id": …, "secret_access_key": …}.
    #[arg(long, env = "SEEK_S3_CREDENTIALS")]
    s3_credentials: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Cmd {
    /// The pod: serve bsky.Service/SearchActorsTypeahead.
    Serve {
        /// The API listener: Connect, gRPC and gRPC-Web over HTTP/1.1 and h2c, /healthz, /readyz.
        #[arg(long, env = "SEEK_LISTEN", default_value = "0.0.0.0:7310")]
        listen: String,
        /// Where artifacts come from: the builder (http://…/artifact/latest), s3://bucket/prefix,
        /// or a local file or directory.
        #[arg(long, env = "SEEK_SOURCE")]
        source: String,
        /// Where downloaded artifacts live while mapped (two at a swap).
        #[arg(long, env = "SEEK_DATA", default_value = "/var/lib/seek")]
        data: PathBuf,
        #[arg(long, env = "SEEK_POLL_SECONDS", default_value_t = 30)]
        poll_seconds: u64,
        /// marco's API for logged-in queries; without it every query is logged out.
        #[arg(long, env = "SEEK_MARCO")]
        marco: Option<String>,
        #[arg(long, env = "SEEK_MARCO_TIMEOUT_MS", default_value_t = 500)]
        marco_timeout_ms: u64,
        /// The most follows a viewer's entry reads (newest first).
        #[arg(long, env = "SEEK_MAX_FOLLOWS", default_value_t = 50_000)]
        max_follows: usize,
        #[arg(long, env = "SEEK_VIEWER_CACHE_MB", default_value_t = 256)]
        viewer_cache_mb: u64,
        #[arg(long, env = "SEEK_VIEWER_IDLE_SECONDS", default_value_t = 600)]
        viewer_idle_seconds: u64,
        /// Follows first: `wall` (every follow ahead of every stranger) or a boost in nats.
        #[arg(long, env = "SEEK_FOLLOWS", default_value = "wall")]
        follows: String,
        /// Jetstream's replay window: an artifact whose cursor is older can't be caught up.
        #[arg(long, env = "SEEK_REPLAY_WINDOW_HOURS", default_value_t = 24.0)]
        replay_window_hours: f64,
        /// Read follow records too, so a viewer's follow or unfollow drops their cache entry.
        #[arg(long, env = "SEEK_FOLLOW_EVENTS", default_value_t = true, action = clap::ArgAction::Set)]
        follow_events: bool,
        #[command(flatten)]
        ingest: Ingest,
        #[command(flatten)]
        s3: S3,
        #[command(flatten)]
        common: Common,
    },
    /// The builder: names state, Jetstream, handle verification, an artifact every interval.
    Builder {
        /// The state, the artifacts and the ranks it pulled.
        #[arg(long, env = "SEEK_BUILDER_DIR")]
        dir: PathBuf,
        /// `/artifact/latest` and health.
        #[arg(long, env = "SEEK_LISTEN", default_value = "0.0.0.0:7320")]
        listen: String,
        /// The first state, when DIR has none: the archive export (JSON lines, zstd).
        #[arg(long, env = "SEEK_BOOTSTRAP")]
        bootstrap: Option<PathBuf>,
        /// The Jetstream time the bootstrap is consistent with.
        #[arg(long, env = "SEEK_BOOTSTRAP_CURSOR_US")]
        cursor_us: Option<u64>,
        /// Ranks: marco's /admin/ranks/latest, or a ranks file or directory.
        #[arg(long, env = "SEEK_RANKS")]
        ranks: Option<String>,
        /// marco's admin token, for --ranks over HTTP.
        #[arg(long, env = "SEEK_RANKS_TOKEN_FILE")]
        ranks_token_file: Option<PathBuf>,
        #[arg(long, env = "SEEK_INTERVAL_SECONDS", default_value_t = 900)]
        interval_seconds: u64,
        #[arg(long, env = "SEEK_KEEP", default_value_t = 4)]
        keep: usize,
        #[arg(long, env = "SEEK_INDEX", value_enum, default_value = "profiles")]
        index: builder::IndexSet,
        /// Save the state every this many cycles (and on the first).
        #[arg(long, env = "SEEK_STATE_EVERY", default_value_t = 4)]
        state_every: u32,
        #[arg(long, env = "SEEK_VERIFY_PER_CYCLE", default_value_t = 9000)]
        verify_per_cycle: usize,
        #[arg(long, env = "SEEK_RECHECK_DAYS", default_value_t = 30)]
        recheck_days: u64,
        /// Also upload each artifact here (s3://bucket/prefix).
        #[arg(long, env = "SEEK_BUCKET")]
        bucket: Option<String>,
        /// Build once and exit (benchmarks, tests).
        #[arg(long)]
        once: bool,
        #[command(flatten)]
        ingest: Ingest,
        #[command(flatten)]
        s3: S3,
        #[command(flatten)]
        common: Common,
    },
    /// Query latency through a running pod, logged out and in, at each trace ratio.
    Bench(bench::Args),
    /// An artifact's meta and section sizes, and optionally a query against it.
    Inspect {
        #[arg(long)]
        artifact: PathBuf,
        #[arg(long)]
        query: Vec<String>,
    },
}

fn init_logs(c: &Common, service: &str) -> Result<Option<opentelemetry_sdk::trace::SdkTracerProvider>> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Layer;
    marco_ops::set_trace_ratio(c.trace_ratio);
    let filter = tracing_subscriber::EnvFilter::try_from_env("SEEK_LOG").unwrap_or_else(|_| "info".into());
    let fmt = if c.log_format == "text" {
        tracing_subscriber::fmt::layer().with_target(false).boxed()
    } else {
        tracing_subscriber::fmt::layer()
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .boxed()
    };
    let (otel, provider) = match &c.otlp {
        Some(ep) => {
            let (l, p) = otel_layer(ep, service)?;
            (Some(l), Some(p))
        }
        None => (None, None),
    };
    let sub = tracing_subscriber::registry().with(otel).with(fmt.with_filter(filter));
    tracing::subscriber::set_global_default(sub).context("a tracing subscriber is already set")?;
    Ok(provider)
}

type OtelLayer =
    tracing_opentelemetry::OpenTelemetryLayer<tracing_subscriber::Registry, opentelemetry_sdk::trace::Tracer>;

/// Spans out as OTLP/HTTP protobuf, batched on the SDK's own thread with a bounded queue (a slow
/// collector drops spans, never holds a query), as marco exports them.
fn otel_layer(endpoint: &str, service: &str) -> Result<(OtelLayer, opentelemetry_sdk::trace::SdkTracerProvider)> {
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_otlp::WithExportConfig;
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
    let tracer = provider.tracer("seek");
    Ok((tracing_opentelemetry::layer().with_tracer(tracer), provider))
}

fn ops(addr: &str, refresh: marco_ops::Refresh) -> Result<SocketAddr> {
    let l = std::net::TcpListener::bind(addr).with_context(|| format!("bind --ops-listen {addr}"))?;
    l.set_nonblocking(true)?;
    let local = l.local_addr()?;
    let r = marco_ops::router(Some(refresh));
    tokio::spawn(async move {
        let l = tokio::net::TcpListener::from_std(l).expect("ops listener");
        let _ = axum::serve(l, r).await;
    });
    Ok(local)
}

fn verifier(i: &Ingest) -> (Option<verify::Queue>, mpsc::Receiver<(String, verify::Outcome)>) {
    let (tx, rx) = mpsc::channel(10_000);
    if i.no_verify {
        return (None, rx);
    }
    let v = Arc::new(verify::Verifier::new(&i.plc, i.plc_rate, i.verify_rate, i.insecure));
    (Some(verify::Queue::start(v, i.verify_workers, tx)), rx)
}

fn follows_policy(s: &str) -> Result<seek_core::query::FollowPolicy> {
    Ok(match s {
        "wall" => seek_core::query::FollowPolicy::Wall,
        n => seek_core::query::FollowPolicy::Boost(
            (n.parse::<f64>().with_context(|| format!("--follows {n:?}: wall or nats"))? * seek_core::index::NAT as f64)
                as i32,
        ),
    })
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    vlatproto::http::set_user_agent(concat!("seek/", env!("CARGO_PKG_VERSION")));
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    rt.block_on(run(cli))
}

async fn run(cli: Cli) -> Result<()> {
    match cli.cmd {
        Cmd::Serve {
            listen,
            source,
            data,
            poll_seconds,
            marco,
            marco_timeout_ms,
            max_follows,
            viewer_cache_mb,
            viewer_idle_seconds,
            follows,
            replay_window_hours,
            follow_events,
            ingest,
            s3,
            common,
        } => {
            let _p = init_logs(&common, "seek")?;
            metrics::init();
            std::fs::create_dir_all(&data).with_context(|| format!("create {}", data.display()))?;
            // a restart's leftovers: nothing maps them now
            for e in std::fs::read_dir(&data)?.flatten() {
                if e.file_name().to_string_lossy().ends_with(".seek")
                    || e.file_name().to_string_lossy().ends_with(".part")
                {
                    let _ = std::fs::remove_file(e.path());
                }
            }
            let marco = marco
                .map(|u| marco::Marco::new(&u, Duration::from_millis(marco_timeout_ms), max_follows))
                .transpose()?;
            let cfg = serve::Config {
                params: seek_core::query::Params { policy: follows_policy(&follows)?, ..Default::default() },
                replay_window: Duration::from_secs_f64(replay_window_hours * 3600.0),
                viewer_cache_bytes: viewer_cache_mb << 20,
                viewer_idle: Duration::from_secs(viewer_idle_seconds),
                replay_keep: Duration::from_secs(6 * 3600),
            };
            let pod = serve::Pod::new(cfg, marco);
            let p = pod.clone();
            let oa = ops(&common.ops_listen, Arc::new(move || p.refresh_gauges()))?;
            let l = tokio::net::TcpListener::bind(&listen).await.with_context(|| format!("bind --listen {listen}"))?;
            let la = l.local_addr()?;
            let r = serve::api_router(&pod);
            tokio::spawn(async move { axum::serve(l, r).await });
            tracing::info!(listen = %la, ops = %oa, "seek serving");

            let src =
                store::Source::parse(&source, s3.s3_endpoint.as_deref(), &s3.s3_region, s3.s3_credentials.as_deref())?;
            let (art_tx, art_rx) = mpsc::channel(1);
            let (js_tx, js_rx) = mpsc::channel(200_000);
            let (queue, vrx) = verifier(&ingest);
            let (start_tx, start_rx) = tokio::sync::oneshot::channel();
            let ingest_task = tokio::spawn(pod.clone().run_ingest(art_rx, js_rx, vrx, queue, start_tx));
            tokio::spawn(async move {
                let mut have: Option<String> = None;
                loop {
                    match src.poll(have.as_deref(), &data).await {
                        Ok(Some(f)) => {
                            tracing::info!(version = %f.version, bytes = f.bytes, "artifact downloaded");
                            have = Some(f.version.clone());
                            if art_tx.send((f.path, f.version)).await.is_err() {
                                return;
                            }
                        }
                        Ok(None) => {}
                        Err(e) => tracing::warn!(error = %format!("{e:#}"), "artifact poll failed"),
                    }
                    tokio::time::sleep(Duration::from_secs(poll_seconds)).await;
                }
            });
            let js = ingest.jetstream.clone();
            tokio::spawn(async move {
                if let Ok(cursor) = start_rx.await {
                    jetstream::run(js, cursor, follow_events, js_tx).await;
                }
            });
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = ingest_task => {}
            }
            Ok(())
        }
        Cmd::Builder {
            dir,
            listen,
            bootstrap,
            cursor_us,
            ranks,
            ranks_token_file,
            interval_seconds,
            keep,
            index,
            state_every,
            verify_per_cycle,
            recheck_days,
            bucket,
            once,
            ingest,
            s3,
            common,
        } => {
            let _p = init_logs(&common, "seek-builder")?;
            metrics::init();
            std::fs::create_dir_all(&dir)?;
            let sp = dir.join("state.seekstate");
            let t = std::time::Instant::now();
            let st = if sp.exists() {
                tokio::task::block_in_place(|| builder::State::load(&sp))?
            } else {
                let b = bootstrap.context("no state in --dir: --bootstrap FILE --cursor-us T makes the first")?;
                let c = cursor_us.context("--bootstrap needs --cursor-us")?;
                tokio::task::block_in_place(|| builder::State::bootstrap(&b, c))?
            };
            tracing::info!(
                actors = st.len(),
                cursor_us = st.cursor_us,
                secs = t.elapsed().as_secs_f64(),
                "state loaded"
            );
            let cfg = builder::Config {
                dir: dir.clone(),
                interval: Duration::from_secs(interval_seconds),
                keep,
                set: index,
                state_every,
                verify_per_cycle,
                recheck: Duration::from_secs(recheck_days * 86400),
                bucket: bucket
                    .map(|b| {
                        store::Bucket::open(&b, s3.s3_endpoint.as_deref(), &s3.s3_region, s3.s3_credentials.as_deref())
                    })
                    .transpose()?,
                ranks: ranks.map(|r| builder::RanksSource::parse(&r, ranks_token_file.as_deref())).transpose()?,
            };
            if once {
                return builder::build_once(cfg, st).await;
            }
            let published = Arc::new(Mutex::new(builder::Published::default()));
            let oa = ops(&common.ops_listen, Arc::new(|| {}))?;
            let l = tokio::net::TcpListener::bind(&listen).await.with_context(|| format!("bind --listen {listen}"))?;
            let r = builder::router(published.clone());
            tokio::spawn(async move { axum::serve(l, r).await });
            tracing::info!(listen = %listen, ops = %oa, "seek builder");
            let (js_tx, js_rx) = mpsc::channel(500_000);
            let js = ingest.jetstream.clone();
            let cursor = st.cursor_us;
            tokio::spawn(async move { jetstream::run(js, cursor, false, js_tx).await });
            let (tx, vrx) = mpsc::channel(10_000);
            let v = Arc::new(verify::Verifier::new(&ingest.plc, ingest.plc_rate, ingest.verify_rate, ingest.insecure));
            let queue = verify::Queue::start(v, ingest.verify_workers, tx);
            builder::run(cfg, st, js_rx, vrx, queue, published).await
        }
        Cmd::Bench(a) => {
            println!("{}", serde_json::to_string_pretty(&bench::run(a).await?)?);
            Ok(())
        }
        Cmd::Inspect { artifact, query } => {
            let t = std::time::Instant::now();
            let ix = seek_core::index::Index::open(&artifact, true)?;
            let open_s = t.elapsed().as_secs_f64();
            let sections: Vec<serde_json::Value> =
                ix.artifact().sections.iter().map(|s| serde_json::json!({"name": s.name, "bytes": s.len})).collect();
            let ov = seek_core::overlay::Overlay::new(&ix);
            let p = seek_core::query::Params::default();
            let mut qs = serde_json::Map::new();
            for q in &query {
                let hits = seek_core::query::search(&ix, &ov, None, q, 10, &p);
                qs.insert(q.clone(), hits.iter().map(|h| serde_json::json!([ix.did(h.id), h.score])).collect());
            }
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &serde_json::json!({"meta": ix.meta, "bytes": ix.artifact().len(), "open_verify_secs": open_s, "sections": sections, "queries": qs})
                )?
            );
            Ok(())
        }
    }
}
