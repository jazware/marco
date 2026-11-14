//! marco: the graph server.
//!
//! ```text
//! marco serve --snapshot DIR [--admin-token-file F]   serve a snapshot (and write to it)
//!     [--jetstream URL | --jetstream-v2 URL | --firehose URL]               and fill it from a stream
//! marco synth --out DIR [--actors N]                   write a synthetic snapshot (fake DIDs)
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::parser::ValueSource;
use clap::{ArgMatches, Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use marco_ingest::{Proto, Source};
use marco_server::app::{App, Config, FlagInfo};
use marco_server::auth::{AuthConfig, Cidr};

marco_ops::jemalloc!();

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Serve(Box<Serve>),
    /// Write a synthetic snapshot with fake DIDs (tests, demos).
    Synth {
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 2000)]
        actors: u32,
        #[arg(long, default_value_t = 8)]
        shards: usize,
    },
}

#[derive(Args)]
struct Serve {
    /// The snapshot directory (marco-load build's --out).
    #[arg(long, env = "MARCO_SNAPSHOT")]
    snapshot: PathBuf,
    /// marco.v1.Graph.
    #[arg(long, env = "MARCO_LISTEN", default_value = "0.0.0.0:7300")]
    listen: String,
    /// Graph, marco.v1.Admin, /metrics, /debug/* and the console at /admin. Keep it private.
    #[arg(long, env = "MARCO_ADMIN_LISTEN", default_value = "127.0.0.1:7301")]
    admin_listen: String,
    /// /metrics and /debug/pprof/{profile,heap} with no token, for a scraper. Keep it private; off by default.
    #[arg(long, env = "MARCO_OPS_LISTEN")]
    ops_listen: Option<String>,
    /// A file holding the admin token. Without it (or a proxy header) Admin and Graph writes are off.
    #[arg(long, env = "MARCO_ADMIN_TOKEN_FILE")]
    admin_token_file: Option<PathBuf>,
    /// A header a proxy in front of --admin-listen sets to the operator's login.
    #[arg(long, env = "MARCO_ADMIN_PROXY_HEADER")]
    admin_proxy_header: Option<String>,
    /// The proxy's own addresses (CIDRs); the header from anywhere else is ignored.
    #[arg(long, env = "MARCO_ADMIN_PROXY_FROM", value_delimiter = ',', requires = "admin_proxy_header")]
    admin_proxy_from: Vec<String>,
    /// The logins the proxy header may name.
    #[arg(long, env = "MARCO_ADMIN_OPERATORS", value_delimiter = ',', requires = "admin_proxy_header")]
    admin_operators: Vec<String>,
    /// Addresses (CIDRs) whose connections, on --listen and --admin-listen, start with a PROXY v2
    /// header naming the caller in TLV 0xE0. One without a good header is closed.
    #[arg(long, env = "MARCO_PROXY_PROTOCOL_FROM", value_delimiter = ',')]
    proxy_protocol_from: Vec<String>,
    /// Believe the proxy header only on a connection whose PROXY v2 header names one of these callers.
    #[arg(
        long,
        env = "MARCO_ADMIN_PROXY_PEERS",
        value_delimiter = ',',
        requires = "admin_proxy_header",
        requires = "proxy_protocol_from"
    )]
    admin_proxy_peers: Vec<String>,
    /// A file holding a token (16 characters at least) that opens the checkpoint export
    /// (/admin/checkpoint) and nothing else on the admin listener: for a mirror, such as seek's
    /// builder, that shouldn't hold the admin token.
    #[arg(long, env = "MARCO_EXPORT_TOKEN_FILE")]
    export_token_file: Option<PathBuf>,
    /// With --compact-min, a list's delta merges past this share of its base.
    #[arg(long, env = "MARCO_MERGE_RATIO", default_value_t = 0.125)]
    merge_ratio: f64,
    /// Merge a list's delta inside the write once it holds this many entries; 0 never (checkpoints fold deltas).
    #[arg(long, env = "MARCO_COMPACT_MIN", default_value_t = 0)]
    compact_min: usize,
    /// The share of ops that get a span, 0 to 1.
    #[arg(long, env = "MARCO_TRACE_RATIO", default_value_t = 0.0)]
    trace_ratio: f64,
    /// An OTLP/HTTP collector's base URL for spans.
    #[arg(long, env = "MARCO_OTLP")]
    otlp: Option<String>,
    /// Refuse every write: Graph writes, actor statuses and snapshot writes.
    #[arg(long, env = "MARCO_READ_ONLY")]
    read_only: bool,
    /// Don't route Graph writes on --listen at all (404, token or not). The admin
    /// listener and --jetstream/--jetstream-v2/--firehose still write.
    #[arg(long, env = "MARCO_API_READ_ONLY")]
    api_read_only: bool,
    /// A built console (ui/dist) to serve instead of the one built into the binary.
    #[arg(long, env = "MARCO_UI_DIR")]
    ui_dir: Option<PathBuf>,
    /// The API listener's public origin (https://marco.example.com): the docs' and landing page's
    /// card tags, and the docs' read examples. Unset, the tags are relative.
    #[arg(long, env = "MARCO_PUBLIC_URL")]
    public_url: Option<String>,
    /// An XRPC host for com.atproto.identity.resolveHandle (https://public.api.bsky.app), behind
    /// /resolve on the API listener for the explorer's handles. Off by default.
    #[arg(long, env = "MARCO_HANDLE_RESOLVER")]
    handle_resolver: Option<String>,
    /// Seconds between change segments: the writes a kill -9 can lose, for a source to replay.
    #[arg(long, env = "MARCO_SEGMENT_INTERVAL", default_value_t = 5.0)]
    segment_interval: f64,
    /// Checkpoint once the segments since the last hold this many ops (what a restart replays)…
    #[arg(long, env = "MARCO_CHECKPOINT_OPS", default_value_t = 3_000_000)]
    checkpoint_ops: u64,
    /// …or the last is this many seconds old.
    #[arg(long, env = "MARCO_CHECKPOINT_AGE", default_value_t = 86_400.0)]
    checkpoint_age: f64,
    /// Answer Graph reads before every expected source has caught up (they're counted either way).
    #[arg(long, env = "MARCO_SERVE_BEFORE_READY")]
    serve_before_ready: bool,
    /// Checkpoints drop tombstones older than this many days: past what any source still replays.
    #[arg(long, env = "MARCO_TOMBSTONE_DAYS", default_value_t = 7.0)]
    tombstone_days: f64,
    /// Compact a shard once its files are this many times what its index still names (0: only when a
    /// space nears its cap).
    #[arg(long, env = "MARCO_COMPACTION_RATIO", default_value_t = 1.43)]
    compaction_ratio: f64,
    /// …and its garbage is at least this many MiB.
    #[arg(long, env = "MARCO_COMPACTION_MIN_GARBAGE_MB", default_value_t = 64)]
    compaction_min_garbage_mb: u64,
    /// The pace of background I/O, MB a second and operations a second (0: unpaced): compactions,
    /// checkpoint writes, verify and list stats. Requests always go first.
    #[arg(long, env = "MARCO_BACKGROUND_IO", default_value = "200,5000", value_parser = parse_rate)]
    background_io: marco_core::store::io::Rate,
    /// The store's budget: its pinned sections, page cache and tables (44GiB, 13GiB, 6G). Pages are
    /// read with O_DIRECT into the cache, every one checked against its sum. It must hold the pinned
    /// tier plus 2 GiB; the graph's deltas and the process live outside it.
    #[arg(long, env = "MARCO_MEMORY")]
    memory: String,
    /// The process's memory limit (8GiB; "none" for none). The store gives back cache to keep the process
    /// under it: what the graph's deltas, a checkpoint and requests take beside --memory, past what the limit
    /// leaves less 512 MiB, comes out of the cache. Default: the cgroup's memory.max.
    #[arg(long, env = "MARCO_MEMORY_LIMIT")]
    memory_limit: Option<String>,
    /// Threads a request that misses the engine's cache moves to.
    #[arg(long, env = "MARCO_MISS_THREADS", default_value_t = 64)]
    miss_threads: usize,
    /// Caps the engine's frames below what --memory allows, skipping the minimum-budget check:
    /// for tests that want a starved cache.
    #[arg(long, env = "MARCO_MAX_FRAMES", hide = true)]
    max_frames: Option<u32>,
    /// Follow a Jetstream instance (wss://host or wss://host/subscribe) for the relations' collections.
    #[arg(long, env = "MARCO_JETSTREAM", conflicts_with_all = ["jetstream_v2", "firehose"])]
    jetstream: Option<String>,
    /// Follow a Jetstream v2 instance's network.bsky.jetstream.subscribeEvents (wss://host) by seq.
    #[arg(long, env = "MARCO_JETSTREAM_V2", conflicts_with = "firehose")]
    jetstream_v2: Option<String>,
    /// Follow a relay's com.atproto.sync.subscribeRepos (wss://host).
    #[arg(long, env = "MARCO_FIREHOSE")]
    firehose: Option<String>,
    /// How far back the Jetstream instance replays, hours (measure yours with
    /// marco-harness retention). A bulk-built snapshot older than this is
    /// refused: following it would miss what changed in between.
    #[arg(long, env = "MARCO_SOURCE_RETENTION_HOURS", default_value_t = 36.0)]
    source_retention_hours: f64,
    /// Follow a bulk-built snapshot past the retention, or from a firehose (which
    /// starts at the live tail).
    #[arg(long, env = "MARCO_ALLOW_GAP")]
    allow_gap: bool,
}

fn flags(m: &ArgMatches, s: &Serve) -> Vec<FlagInfo> {
    let opt = |v: &Option<PathBuf>| v.as_ref().map_or("—".to_string(), |p| p.display().to_string());
    let list = |v: &[String]| if v.is_empty() { "—".to_string() } else { v.join(",") };
    let rows: Vec<(&str, String)> = vec![
        ("listen", s.listen.clone()),
        ("admin_listen", s.admin_listen.clone()),
        ("ops_listen", s.ops_listen.clone().unwrap_or_else(|| "—".into())),
        ("snapshot", s.snapshot.display().to_string()),
        ("admin_token_file", opt(&s.admin_token_file)),
        ("admin_proxy_header", s.admin_proxy_header.clone().unwrap_or_else(|| "—".into())),
        ("admin_proxy_from", list(&s.admin_proxy_from)),
        ("admin_operators", list(&s.admin_operators)),
        ("proxy_protocol_from", list(&s.proxy_protocol_from)),
        ("admin_proxy_peers", list(&s.admin_proxy_peers)),
        ("export_token_file", s.export_token_file.as_ref().map_or("—".into(), |p| p.display().to_string())),
        ("merge_ratio", s.merge_ratio.to_string()),
        ("compact_min", s.compact_min.to_string()),
        ("trace_ratio", s.trace_ratio.to_string()),
        ("otlp", s.otlp.clone().unwrap_or_else(|| "—".into())),
        ("read_only", s.read_only.to_string()),
        ("api_read_only", s.api_read_only.to_string()),
        ("ui_dir", opt(&s.ui_dir)),
        ("public_url", s.public_url.clone().unwrap_or_else(|| "—".into())),
        ("handle_resolver", s.handle_resolver.clone().unwrap_or_else(|| "—".into())),
        ("segment_interval", s.segment_interval.to_string()),
        ("checkpoint_ops", s.checkpoint_ops.to_string()),
        ("checkpoint_age", s.checkpoint_age.to_string()),
        ("serve_before_ready", s.serve_before_ready.to_string()),
        ("tombstone_days", s.tombstone_days.to_string()),
        ("compaction_ratio", s.compaction_ratio.to_string()),
        ("compaction_min_garbage_mb", s.compaction_min_garbage_mb.to_string()),
        ("background_io", format!("{},{}", s.background_io.bytes_per_s / 1_000_000, s.background_io.ops_per_s)),
        ("memory", s.memory.clone()),
        ("memory_limit", s.memory_limit.clone().unwrap_or_else(|| "cgroup".into())),
        ("miss_threads", s.miss_threads.to_string()),
        ("jetstream", s.jetstream.clone().unwrap_or_else(|| "—".into())),
        ("jetstream_v2", s.jetstream_v2.clone().unwrap_or_else(|| "—".into())),
        ("firehose", s.firehose.clone().unwrap_or_else(|| "—".into())),
        ("source_retention_hours", s.source_retention_hours.to_string()),
        ("allow_gap", s.allow_gap.to_string()),
    ];
    rows.into_iter()
        .map(|(id, value)| {
            let source = match m.value_source(id) {
                Some(ValueSource::CommandLine) => "flag",
                Some(ValueSource::EnvVariable) => "env",
                Some(ValueSource::DefaultValue) => "default",
                _ => "unset",
            };
            FlagInfo { name: format!("--{}", id.replace('_', "-")), value, source: source.into() }
        })
        .collect()
}

/// `MB,IOPS` (`200,5000`), or `MB` alone with no cap on operations.
fn parse_rate(v: &str) -> Result<marco_core::store::io::Rate, String> {
    let (mb, ops) = v.split_once(',').unwrap_or((v, "0"));
    let n = |x: &str| {
        x.trim().parse::<u64>().map_err(|_| format!("{v:?}: MB a second, then operations a second (200,5000)"))
    };
    Ok(marco_core::store::io::Rate { bytes_per_s: n(mb)? * 1_000_000, ops_per_s: n(ops)? })
}

/// An http(s) origin with no path, trailing slash dropped.
fn origin(flag: &str, v: Option<&str>) -> Result<Option<String>> {
    let Some(v) = v.map(str::trim).filter(|v| !v.is_empty()) else { return Ok(None) };
    let v = v.trim_end_matches('/');
    let Some(host) = v.strip_prefix("https://").or_else(|| v.strip_prefix("http://")) else {
        bail!("{flag} {v}: needs http:// or https://");
    };
    if host.is_empty() || host.contains(['/', '?', '#', '"', '\'', '<', '>', ' ']) {
        bail!("{flag} {v}: needs an origin (scheme and host, no path)");
    }
    Ok(Some(v.to_string()))
}

fn auth(s: &Serve) -> Result<AuthConfig> {
    let token = match &s.admin_token_file {
        Some(p) => {
            let t = std::fs::read_to_string(p).with_context(|| format!("--admin-token-file {}", p.display()))?;
            let t = t.trim().to_string();
            if t.len() < 16 {
                bail!("--admin-token-file {}: the token needs at least 16 characters", p.display());
            }
            Some(t)
        }
        None => None,
    };
    let export_token = match &s.export_token_file {
        Some(p) => {
            let t = std::fs::read_to_string(p).with_context(|| format!("--export-token-file {}", p.display()))?;
            let t = t.trim().to_string();
            if t.len() < 16 {
                bail!("--export-token-file {}: the token needs at least 16 characters", p.display());
            }
            if token.as_deref() == Some(t.as_str()) {
                bail!("--export-token-file must differ from --admin-token-file");
            }
            Some(t)
        }
        None => None,
    };
    let proxy_header = match &s.admin_proxy_header {
        Some(h) => {
            if s.admin_proxy_from.is_empty() || s.admin_operators.is_empty() {
                bail!("--admin-proxy-header needs --admin-proxy-from and --admin-operators");
            }
            Some(http::HeaderName::from_bytes(h.as_bytes()).with_context(|| format!("--admin-proxy-header {h:?}"))?)
        }
        None => None,
    };
    let cidrs = |v: &[String]| v.iter().map(|c| c.parse::<Cidr>().map_err(anyhow::Error::msg)).collect::<Result<_>>();
    Ok(AuthConfig {
        token,
        proxy_header,
        proxy_from: cidrs(&s.admin_proxy_from)?,
        operators: s.admin_operators.clone(),
        proxy_protocol_from: cidrs(&s.proxy_protocol_from)?,
        proxy_peers: s.admin_proxy_peers.clone(),
        export_token,
    })
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

fn main() -> Result<()> {
    let matches = Cli::command().get_matches();
    let cli = Cli::from_arg_matches(&matches)?;
    match cli.cmd {
        Cmd::Synth { out, actors, shards } => {
            marco_server::synth::write(&out, actors, shards)?;
            println!("{}", out.display());
            Ok(())
        }
        Cmd::Serve(s) => {
            let m = matches.subcommand_matches("serve").expect("serve matches");
            if !(0.0..=1.0).contains(&s.merge_ratio) || s.merge_ratio == 0.0 {
                bail!("--merge-ratio must be above 0 and at most 1");
            }
            let _otel = match &s.otlp {
                Some(url) => Some(marco_ops::init_tracing(url, "marco")?),
                None => None,
            };
            marco_ops::set_trace_ratio(s.trace_ratio);
            if s.segment_interval <= 0.0 || s.checkpoint_age <= 0.0 {
                bail!("--segment-interval and --checkpoint-age must be above 0");
            }
            let source = match (&s.jetstream, &s.jetstream_v2, &s.firehose) {
                (Some(u), _, _) => Some(Source::new(Proto::Jetstream, u)?),
                (_, Some(u), _) => Some(Source::new(Proto::JetstreamV2, u)?),
                (_, _, Some(u)) => Some(Source::new(Proto::Firehose, u)?),
                _ => None,
            };
            if source.is_some() && s.read_only {
                bail!("--read-only can't follow a source");
            }
            let storage = marco_core::store::Storage {
                memory: marco_core::store::parse_bytes(&s.memory).context("--memory")?,
                max_frames: s.max_frames,
            };
            let memory_limit = match s.memory_limit.as_deref() {
                Some("none") => None,
                Some(l) => Some(marco_core::store::parse_bytes(l).context("--memory-limit")?),
                None => marco_server::memory::cgroup_limit(),
            };
            if s.miss_threads == 0 {
                bail!("--miss-threads must be at least 1");
            }
            let cfg = Config {
                read_only: s.read_only,
                api_read_only: s.api_read_only,
                merge_ratio: s.merge_ratio,
                compact_min: s.compact_min,
                api_listen: s.listen.clone(),
                admin_listen: s.admin_listen.clone(),
                ops_listen: s.ops_listen.clone(),
                auth: auth(&s)?,
                flags: flags(m, &s),
                segment_interval: std::time::Duration::from_secs_f64(s.segment_interval),
                checkpoint_ops: s.checkpoint_ops,
                checkpoint_age: std::time::Duration::from_secs_f64(s.checkpoint_age),
                serve_before_ready: s.serve_before_ready,
                compaction: marco_core::CompactPolicy {
                    ratio: s.compaction_ratio.max(0.0),
                    min_garbage: s.compaction_min_garbage_mb << 20,
                },
                background: s.background_io,
                tombstone_ttl: std::time::Duration::from_secs_f64(s.tombstone_days.max(0.0) * 86_400.0),
                expected_sources: source.iter().map(|s| s.name.clone()).collect(),
                storage,
                miss_threads: s.miss_threads,
                memory_limit,
                ..Config::new(s.snapshot.clone())
            };
            let site = marco_server::ui::Site {
                public_url: origin("--public-url", s.public_url.as_deref())?,
                handle_resolver: origin("--handle-resolver", s.handle_resolver.as_deref())?,
            };
            let ui = Arc::new(marco_server::ui::Ui::load_with(s.ui_dir.as_deref(), site)?);
            let rt = marco_server::runtime(None)?;
            rt.block_on(async move {
                let app = App::open(cfg)?;
                let (api, admin) = marco_server::serve(app.clone(), ui).await?;
                if let Some(ops) = marco_server::serve_ops(&app).await? {
                    eprintln!("metrics on http://{ops}/metrics");
                }
                eprintln!(
                    "marco {} on {}: {} DIDs, graph on http://{api}, admin on http://{admin}/admin/, docs on http://{api}/docs (opened in {:.1} ms{})",
                    marco_server::VERSION,
                    s.snapshot.display(),
                    app.graph().n_ids(),
                    app.open_seconds * 1e3,
                    if s.read_only { ", read-only" } else { "" },
                );
                let (stop, stopped) = tokio::sync::watch::channel(false);
                let ingest = match source {
                    Some(src) => {
                        let retention = std::time::Duration::from_secs_f64(s.source_retention_hours.max(0.0) * 3600.0);
                        let start = marco_server::ingest::start_point(&app, &src, retention, s.allow_gap).await?;
                        eprintln!("following {} ({})", src.name, src.url);
                        Some(marco_server::ingest::start(&app, src, start, stopped))
                    }
                    None => None,
                };
                shutdown_signal().await;
                let _ = stop.send(true);
                if let Some(t) = ingest {
                    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), t).await;
                }
                if !s.read_only {
                    // the writes since the last segment, so a clean stop loses none
                    app.shutdown().await?;
                }
                Ok(())
            })
        }
    }
}
