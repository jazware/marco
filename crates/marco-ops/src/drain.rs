//! A clean stop for a process's HTTP listeners behind a load balancer. On
//! SIGTERM or Ctrl-C, [`Drain::run`]:
//!
//! 1. turns the process lame duck: `/readyz` answers 503 (the caller's handler
//!    asks [`Drain::draining`]) while every listener keeps serving, and an
//!    HTTP/1 response says `Connection: close` ([`close_http1`]), so a
//!    load balancer takes the process out and keep-alive clients dial again
//!    before it goes;
//! 2. after the drain period (or a second signal), stops every listener
//!    registered with [`Drain::spawn`]: no new connections, a GOAWAY on each
//!    HTTP/2 connection, an HTTP/1 connection closed once its response is out;
//! 3. waits up to the grace for the requests in flight.
//!
//! The listeners are axum servers with `with_graceful_shutdown(drain.stopped())`.

use std::future::{Future, IntoFuture};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Version};
use axum::middleware::Next;
use axum::response::Response;
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct Drain(Arc<Inner>);

struct Inner {
    draining: AtomicBool,
    stop: watch::Sender<bool>,
    servers: Mutex<Vec<JoinHandle<()>>>,
}

impl Default for Drain {
    fn default() -> Drain {
        Drain::new()
    }
}

impl Drain {
    pub fn new() -> Drain {
        Drain(Arc::new(Inner {
            draining: AtomicBool::new(false),
            stop: watch::channel(false).0,
            servers: Mutex::new(Vec::new()),
        }))
    }

    /// Whether the drain has begun: `/readyz` should answer 503.
    pub fn draining(&self) -> bool {
        self.0.draining.load(Ordering::Relaxed)
    }

    /// Resolves when the listeners stop: an axum server's `with_graceful_shutdown`.
    pub fn stopped(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut rx = self.0.stop.subscribe();
        async move {
            // the sender lives as long as any Drain: only [`Drain::stop`] ends a server
            if rx.wait_for(|s| *s).await.is_err() {
                std::future::pending::<()>().await
            }
        }
    }

    /// Runs `server` (`axum::serve(..).with_graceful_shutdown(drain.stopped())`)
    /// as one [`Drain::stop`] waits for.
    pub fn spawn<F>(&self, server: F)
    where
        F: IntoFuture<Output = std::io::Result<()>>,
        F::IntoFuture: Send + 'static,
    {
        let f = server.into_future();
        let h = tokio::spawn(async move {
            let _ = f.await;
        });
        self.0.servers.lock().unwrap().push(h);
    }

    /// Step 1: `/readyz` answers 503 from now on; the listeners keep serving.
    pub fn lame_duck(&self) {
        self.0.draining.store(true, Ordering::Relaxed);
    }

    /// Steps 2 and 3: the listeners stop accepting and close their connections
    /// gracefully, and this waits up to `grace` for the requests in flight.
    /// False when some were still running at the grace.
    pub async fn stop(&self, grace: Duration) -> bool {
        self.lame_duck();
        self.0.stop.send_replace(true);
        let servers = std::mem::take(&mut *self.0.servers.lock().unwrap());
        tokio::time::timeout(grace, async {
            for s in servers {
                let _ = s.await;
            }
        })
        .await
        .is_ok()
    }

    /// The whole sequence, once a [`signal`] has come: lame duck for `drain`
    /// (a second signal cuts it short), then [`Drain::stop`].
    pub async fn run(&self, drain: Duration, grace: Duration) -> bool {
        self.lame_duck();
        if !drain.is_zero() {
            tokio::select! {
                _ = tokio::time::sleep(drain) => {}
                _ = signal() => {}
            }
        }
        self.stop(grace).await
    }
}

/// SIGTERM or Ctrl-C.
pub async fn signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

/// Middleware (`axum::middleware::from_fn_with_state(drain, close_http1)`):
/// while draining, an HTTP/1 response carries `Connection: close`, so a
/// keep-alive client dials again, through its load balancer, instead of
/// reusing a connection the stop would close under it. HTTP/2 clients get
/// their GOAWAY at the stop.
pub async fn close_http1(State(d): State<Drain>, req: Request, next: Next) -> Response {
    let h1 = req.version() < Version::HTTP_2;
    let mut res = next.run(req).await;
    if h1 && d.draining() {
        res.headers_mut().insert(header::CONNECTION, HeaderValue::from_static("close"));
    }
    res
}

/// Seconds from a `--shutdown-*-secs` flag.
pub fn secs(flag: &str, v: f64) -> anyhow::Result<Duration> {
    Duration::try_from_secs_f64(v).map_err(|_| anyhow::anyhow!("{flag} {v}: needs 0 or more seconds"))
}
