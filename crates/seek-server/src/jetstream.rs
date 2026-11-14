//! Jetstream for seek: profile records, `#identity` and `#account` events
//! (and, for a pod's viewer cache, follow records), read from a cursor with
//! redials. A cursor older than the instance's replay window gets the oldest
//! events it has, silently; [`Gap`] is how a reader finds out.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::metrics::*;

/// Jetstream's `time_us` may repeat across events, so a redial starts this far back.
pub const OVERLAP_US: u64 = 1_000_000;
/// A first event this far past the cursor asked for means the cursor was out of the window: the
/// collections seek reads see several events a second network-wide.
pub const GAP_US: u64 = 120_000_000;
/// No event for this long and the socket is redialled.
pub const STALL: Duration = Duration::from_secs(60);

pub const PROFILE: &str = "app.bsky.actor.profile";
pub const FOLLOW: &str = "app.bsky.graph.follow";

#[derive(Clone, Debug, PartialEq)]
pub enum Ev {
    /// A profile record written (name: its display name) or deleted.
    Profile {
        did: String,
        time_us: u64,
        name: Option<String>,
        deleted: bool,
    },
    /// The DID's identity changed; `handle` is the claim Jetstream passed on, unverified.
    Identity {
        did: String,
        time_us: u64,
        handle: Option<String>,
    },
    Account {
        did: String,
        time_us: u64,
        active: bool,
    },
    /// The DID followed or unfollowed someone.
    Follow {
        did: String,
        time_us: u64,
    },
    /// The replay started past the cursor asked for: events between were lost.
    Gap {
        asked_us: u64,
        first_us: u64,
    },
}

impl Ev {
    pub fn time_us(&self) -> u64 {
        match self {
            Ev::Profile { time_us, .. }
            | Ev::Identity { time_us, .. }
            | Ev::Account { time_us, .. }
            | Ev::Follow { time_us, .. } => *time_us,
            Ev::Gap { first_us, .. } => *first_us,
        }
    }
}

#[derive(Deserialize)]
struct Raw {
    did: String,
    time_us: u64,
    kind: String,
    commit: Option<RawCommit>,
    identity: Option<RawIdentity>,
    account: Option<RawAccount>,
}

#[derive(Deserialize)]
struct RawCommit {
    operation: String,
    collection: String,
    rkey: String,
    record: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct RawIdentity {
    handle: Option<String>,
}

#[derive(Deserialize)]
struct RawAccount {
    active: bool,
}

/// One Jetstream message, or None for what seek doesn't use.
pub fn parse(text: &str) -> Option<Ev> {
    let r: Raw = match serde_json::from_str(text) {
        Ok(r) => r,
        Err(_) => {
            JS_DECODE.inc();
            return None;
        }
    };
    let (did, time_us) = (r.did, r.time_us);
    let ev = match r.kind.as_str() {
        "commit" => {
            let c = r.commit?;
            match c.collection.as_str() {
                PROFILE if c.rkey == "self" => {
                    let deleted = c.operation == "delete";
                    let name = c
                        .record
                        .as_ref()
                        .and_then(|v| v.get("displayName"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.chars().take(256).collect());
                    Ev::Profile { did, time_us, name: if deleted { None } else { name }, deleted }
                }
                FOLLOW => Ev::Follow { did, time_us },
                _ => return None,
            }
        }
        "identity" => Ev::Identity { did, time_us, handle: r.identity.and_then(|i| i.handle) },
        "account" => Ev::Account { did, time_us, active: r.account?.active },
        _ => return None,
    };
    Some(ev)
}

/// The subscribe URL for `base` (`wss://host` or `wss://host/subscribe`).
pub fn subscribe_url(base: &str, follows: bool, cursor_us: Option<u64>) -> String {
    let mut u = base.trim_end_matches('/').to_string();
    if !u.ends_with("/subscribe") {
        u.push_str("/subscribe");
    }
    u.push_str("?wantedCollections=");
    u.push_str(PROFILE);
    if follows {
        u.push_str("&wantedCollections=");
        u.push_str(FOLLOW);
    }
    if let Some(c) = cursor_us {
        u.push_str(&format!("&cursor={c}"));
    }
    u
}

fn tls() -> Arc<rustls::ClientConfig> {
    static C: std::sync::LazyLock<Arc<rustls::ClientConfig>> = std::sync::LazyLock::new(|| {
        let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
        let c = rustls::ClientConfig::builder_with_provider(vlatproto::http::tls_provider())
            .with_safe_default_protocol_versions()
            .expect("rustls protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        Arc::new(c)
    });
    C.clone()
}

/// Reads `base` from `cursor_us` into `tx` until `tx` closes, redialling with backoff from the
/// last event read.
pub async fn run(base: String, cursor_us: u64, follows: bool, tx: mpsc::Sender<Ev>) {
    let mut cursor = cursor_us;
    let mut backoff = Duration::from_millis(500);
    let mut first = true;
    loop {
        let url = subscribe_url(&base, follows, Some(cursor.saturating_sub(if first { 0 } else { OVERLAP_US })));
        let connector = if url.starts_with("wss://") {
            Some(tokio_tungstenite::Connector::Rustls(tls()))
        } else {
            Some(tokio_tungstenite::Connector::Plain)
        };
        let dial = tokio::time::timeout(
            Duration::from_secs(15),
            tokio_tungstenite::connect_async_tls_with_config(url.as_str(), None, true, connector),
        )
        .await;
        match dial {
            Ok(Ok((mut ws, _))) => {
                let asked = cursor;
                let mut seen_first = false;
                let start = std::time::Instant::now();
                // pings keep a socket open that carries no events: only data counts as alive
                let mut last_data = std::time::Instant::now();
                loop {
                    let left = STALL.saturating_sub(last_data.elapsed());
                    let msg = match tokio::time::timeout(left, ws.next()).await {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => {
                            tracing::warn!(error = %e, "jetstream read failed");
                            break;
                        }
                        Ok(None) => break,
                        Err(_) => {
                            tracing::warn!(cursor_us = cursor, "no Jetstream events for 60 s: redialling");
                            break;
                        }
                    };
                    let text = match msg {
                        Message::Text(t) => {
                            last_data = std::time::Instant::now();
                            t
                        }
                        Message::Close(_) => break,
                        _ => continue,
                    };
                    let Some(ev) = parse(&text) else {
                        EVENTS.with_label_values(&["skipped"]).inc();
                        continue;
                    };
                    let t = ev.time_us();
                    if !seen_first {
                        seen_first = true;
                        if t > asked + GAP_US && now_us() > asked + GAP_US {
                            JS_GAPS.inc();
                            if tx.send(Ev::Gap { asked_us: asked, first_us: t }).await.is_err() {
                                return;
                            }
                        }
                    }
                    cursor = cursor.max(t);
                    let kind = match &ev {
                        Ev::Profile { .. } => "profile",
                        Ev::Identity { .. } => "identity",
                        Ev::Account { .. } => "account",
                        Ev::Follow { .. } => "follow",
                        Ev::Gap { .. } => "skipped",
                    };
                    EVENTS.with_label_values(&[kind]).inc();
                    JS_CURSOR.set(t as f64 / 1e6);
                    JS_LAG.set((now_us().saturating_sub(t)) as f64 / 1e6);
                    if tx.send(ev).await.is_err() {
                        return;
                    }
                }
                if start.elapsed() > Duration::from_secs(60) {
                    backoff = Duration::from_millis(500);
                }
            }
            Ok(Err(e)) => tracing::warn!(error = %e, "jetstream dial failed"),
            Err(_) => tracing::warn!("jetstream dial timed out"),
        }
        if tx.is_closed() {
            return;
        }
        first = false;
        JS_RECONNECTS.inc();
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_what_seek_reads() {
        let p = parse(
            r#"{"did":"did:plc:a","time_us":5,"kind":"commit","commit":{"rev":"x","operation":"update","collection":"app.bsky.actor.profile","rkey":"self","record":{"displayName":"Alice"}}}"#,
        );
        assert_eq!(
            p,
            Some(Ev::Profile { did: "did:plc:a".into(), time_us: 5, name: Some("Alice".into()), deleted: false })
        );
        let d = parse(
            r#"{"did":"did:plc:a","time_us":6,"kind":"commit","commit":{"operation":"delete","collection":"app.bsky.actor.profile","rkey":"self"}}"#,
        );
        assert!(matches!(d, Some(Ev::Profile { deleted: true, name: None, .. })));
        let i = parse(
            r#"{"did":"did:plc:a","time_us":7,"kind":"identity","identity":{"did":"did:plc:a","handle":"a.example.com","seq":1,"time":"t"}}"#,
        );
        assert_eq!(i, Some(Ev::Identity { did: "did:plc:a".into(), time_us: 7, handle: Some("a.example.com".into()) }));
        let a = parse(
            r#"{"did":"did:plc:a","time_us":8,"kind":"account","account":{"active":false,"status":"takendown","did":"did:plc:a","seq":2,"time":"t"}}"#,
        );
        assert_eq!(a, Some(Ev::Account { did: "did:plc:a".into(), time_us: 8, active: false }));
        let f = parse(
            r#"{"did":"did:plc:a","time_us":9,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.graph.follow","rkey":"3k","record":{"subject":"did:plc:b"}}}"#,
        );
        assert_eq!(f, Some(Ev::Follow { did: "did:plc:a".into(), time_us: 9 }));
        assert!(parse(r#"{"did":"did:plc:a","time_us":9,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"3k"}}"#).is_none());
        assert!(parse("not json").is_none());
    }

    #[test]
    fn urls() {
        assert_eq!(
            subscribe_url("wss://js.example", true, Some(7)),
            "wss://js.example/subscribe?wantedCollections=app.bsky.actor.profile&wantedCollections=app.bsky.graph.follow&cursor=7"
        );
        assert_eq!(
            subscribe_url("ws://h/subscribe", false, None),
            "ws://h/subscribe?wantedCollections=app.bsky.actor.profile"
        );
    }
}
