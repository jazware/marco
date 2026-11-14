//! marco's server. The API listener serves marco.v1.Graph and the docs at
//! /docs. The admin listener (loopback by default) serves Graph too,
//! marco.v1.Admin, the ops routes (/metrics, /debug/pprof/*, /debug/tracing),
//! the console at /admin and the docs.

pub mod admin_svc;
pub mod app;
pub mod auth;
pub mod export;
pub mod graph_svc;
pub mod ingest;
pub mod liststats;
pub mod memory;
pub mod metrics;
pub mod peer;
pub mod pool;
pub mod series;
pub mod synth;
pub mod sys;
pub mod ui;
pub mod writer;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use marco_proto::connect::marco::v1::{AdminExt, GraphExt};

use crate::app::App;
use crate::auth::Listener;
use crate::peer::{Peer, PeerListener};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const COMMIT: &str = match option_env!("MARCO_COMMIT") {
    Some(c) => c,
    None => "dev",
};

fn connect(app: &Arc<App>, admin: bool) -> axum::Router {
    let mut r = Arc::new(graph_svc::GraphApi(app.clone(), !admin)).register(connectrpc::Router::new());
    if admin {
        r = Arc::new(admin_svc::AdminApi(app.clone())).register(r);
    }
    let svc = r.into_axum_service();
    let mut router = axum::Router::new().route_service("/marco.v1.Graph/{*method}", svc.clone());
    if !admin && app.cfg.api_read_only {
        // connectrpc looks a procedure up by its exact path, so these two
        // shadow the only paths that reach a write.
        for path in auth::GRAPH_WRITES {
            router = router.route(path, axum::routing::any(|| async { http::StatusCode::NOT_FOUND }));
        }
    }
    if admin {
        router = router.route_service("/marco.v1.Admin/{*method}", svc);
    }
    router
}

/// 200 once every expected source has caught up since start and reads
/// aren't failing across the disk, else 503 and why.
async fn readyz(axum::extract::State(app): axum::extract::State<Arc<App>>) -> (http::StatusCode, String) {
    match app.ready_status() {
        (true, _) => (http::StatusCode::OK, "ready\n".into()),
        (false, why) => (http::StatusCode::SERVICE_UNAVAILABLE, format!("{why}\n")),
    }
}

pub fn api_router(app: &Arc<App>) -> axum::Router {
    connect(app, false)
        .route("/healthz", axum::routing::get(|| async { "ok\n" }))
        .route("/readyz", axum::routing::get(readyz).with_state(app.clone()))
        .layer(axum::middleware::from_fn_with_state((app.clone(), Listener::Api), auth::gate))
        .layer(axum::middleware::from_fn(metrics::time_rpcs))
}

pub fn admin_router(app: &Arc<App>, ui: Arc<ui::Ui>) -> axum::Router {
    let refresh = {
        let app = app.clone();
        Arc::new(move || app.graph().refresh_gauges()) as marco_ops::Refresh
    };
    connect(app, true)
        .merge(marco_ops::router(Some(refresh)))
        .merge(export::routes(app, export::Export::new(&app.cfg.snapshot)))
        .merge(ui::routes(ui))
        .route("/healthz", axum::routing::get(|| async { "ok\n" }))
        .route("/readyz", axum::routing::get(readyz).with_state(app.clone()))
        .layer(axum::middleware::from_fn_with_state((app.clone(), Listener::Admin), auth::gate))
        .layer(axum::middleware::from_fn(metrics::time_rpcs))
}

/// The ops listener (`--ops-listen`), if set: /metrics and the pprof GETs,
/// open, for a scraper that can't send the admin token. Bind it privately.
pub async fn serve_ops(app: &Arc<App>) -> Result<Option<SocketAddr>> {
    let Some(addr) = &app.cfg.ops_listen else { return Ok(None) };
    let l = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("bind --ops-listen {addr}"))?;
    let local = l.local_addr()?;
    let refresh = {
        let app = app.clone();
        Arc::new(move || app.graph().refresh_gauges()) as marco_ops::Refresh
    };
    let r = marco_ops::read_router(Some(refresh)).route("/healthz", axum::routing::get(|| async { "ok\n" }));
    tokio::spawn(async move { axum::serve(l, r).await });
    Ok(Some(local))
}

/// The server's runtime: `threads` workers (one per core by default),
/// each marked so a blocking read on one panics in debug builds
/// ([`marco_core::read::mark_async_thread`]). Only workers park, so the
/// park hooks mark them and leave the blocking pool's threads alone; the
/// RPC middleware marks a worker that hasn't parked yet.
pub fn runtime(threads: Option<usize>) -> std::io::Result<tokio::runtime::Runtime> {
    let mut b = tokio::runtime::Builder::new_multi_thread();
    if let Some(n) = threads {
        b.worker_threads(n);
    }
    b.enable_all()
        .on_thread_park(marco_core::read::mark_async_thread)
        .on_thread_unpark(marco_core::read::mark_async_thread)
        .build()
}

/// Binds both listeners and serves them on the current runtime.
pub async fn serve(app: Arc<App>, ui: Arc<ui::Ui>) -> Result<(SocketAddr, SocketAddr)> {
    metrics::init(VERSION, COMMIT);
    metrics::watch(&app);
    let api = tokio::net::TcpListener::bind(&app.cfg.api_listen)
        .await
        .with_context(|| format!("bind --listen {}", app.cfg.api_listen))?;
    let admin = tokio::net::TcpListener::bind(&app.cfg.admin_listen)
        .await
        .with_context(|| format!("bind --admin-listen {}", app.cfg.admin_listen))?;
    let (a1, a2) = (api.local_addr()?, admin.local_addr()?);
    let r1 = api_router(&app).merge(ui::docs_routes(ui.clone(), false)).into_make_service_with_connect_info::<Peer>();
    let r2 = admin_router(&app, ui).into_make_service_with_connect_info::<Peer>();
    let from = &app.cfg.auth.proxy_protocol_from;
    let (api, admin) = (PeerListener::new(api, from.clone())?, PeerListener::new(admin, from.clone())?);
    tokio::spawn(async move { axum::serve(api, r1).await });
    tokio::spawn(async move { axum::serve(admin, r2).await });
    series::spawn(app);
    Ok((a1, a2))
}
