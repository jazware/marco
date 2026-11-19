//! Jetstream for seek: profile records, `#identity` and `#account` events
//! (and, for a pod's viewer cache, follow records), read from a cursor with
//! redials. A cursor older than the instance's replay window gets the oldest
//! events it has, silently; [`Gap`] is how a reader finds out.
//!
//! An instance whose store has a hole replays up to the hole and then sends
//! nothing, to every redial at that cursor (jetstream us-east, 2026-11-19). So a
//! socket that goes quiet while seek is behind and read nothing past its cursor
//! counts as stuck: the next dial goes to the next instance listed, and once
//! every instance has been stuck there it asks for a cursor past the hole,
//! doubling the step each time (the replay's [`Gap`] reports what was lost).

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
/// The first step past a hole that every instance is stuck at.
pub const SKIP_US: u64 = 1_000_000;

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
    /// The DID followed or unfollowed someone: `subject` on a follow (an unfollow's delete doesn't
    /// say whom).
    Follow {
        did: String,
        time_us: u64,
        subject: Option<String>,
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
                FOLLOW => {
                    let subject = c
                        .record
                        .as_ref()
                        .and_then(|v| v.get("subject"))
                        .and_then(|v| v.as_str())
                        .filter(|s| s.starts_with("did:") && s.len() <= 2048)
                        .map(String::from);
                    Ev::Follow { did, time_us, subject }
                }
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

/// Reads `bases` (one instance or several, comma-separated) from `cursor_us` into `tx` until `tx`
/// closes, redialling with backoff from the last event read.
pub async fn run(bases: String, cursor_us: u64, follows: bool, tx: mpsc::Sender<Ev>) {
    follow(&bases, cursor_us, follows, tx, STALL).await
}

/// Where the next dial goes after a socket ends: `strikes` sockets in a row were stuck at the
/// same cursor. Each strike moves to the next instance; past one strike per instance (with one
/// instance, a plain redial first) the cursor asked for skips ahead of the last event read.
fn next_dial(n: usize, at: usize, strikes: u32) -> (usize, u64) {
    let at = if strikes > 0 { (at + 1) % n } else { at };
    let skip = match strikes.checked_sub(n as u32 + 1) {
        Some(k) => SKIP_US << k.min(16),
        None => 0,
    };
    (at, skip)
}

async fn follow(bases: &str, cursor_us: u64, follows: bool, tx: mpsc::Sender<Ev>, stall: Duration) {
    let bases: Vec<&str> = bases.split(',').map(str::trim).filter(|b| !b.is_empty()).collect();
    assert!(!bases.is_empty(), "no Jetstream instance given");
    let mut at = 0;
    let mut cursor = cursor_us;
    let mut backoff = Duration::from_millis(500);
    let mut first = true;
    let mut strikes = 0u32;
    let mut skip = 0u64;
    loop {
        let base = bases[at];
        let ask = if skip > 0 {
            (cursor + skip).min(now_us().max(cursor))
        } else {
            cursor.saturating_sub(if first { 0 } else { OVERLAP_US })
        };
        let url = subscribe_url(base, follows, Some(ask));
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
        let mut stuck = false;
        match dial {
            Ok(Ok((mut ws, _))) => {
                let asked = cursor;
                let mut seen_first = false;
                let start = std::time::Instant::now();
                // pings keep a socket open that carries no events: only data counts as alive
                let mut last_data = std::time::Instant::now();
                loop {
                    let left = stall.saturating_sub(last_data.elapsed());
                    let msg = match tokio::time::timeout(left, ws.next()).await {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => {
                            tracing::warn!(error = %e, "jetstream read failed");
                            break;
                        }
                        Ok(None) => break,
                        Err(_) => {
                            let behind = now_us().saturating_sub(cursor) > stall.as_micros() as u64;
                            stuck = behind && cursor <= asked;
                            tracing::warn!(
                                cursor_us = cursor,
                                base,
                                stuck,
                                "no Jetstream events for {} s: redialling",
                                stall.as_secs()
                            );
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
                if start.elapsed() > stall {
                    backoff = Duration::from_millis(500);
                }
            }
            Ok(Err(e)) => tracing::warn!(error = %e, base, "jetstream dial failed"),
            Err(_) => tracing::warn!(base, "jetstream dial timed out"),
        }
        if tx.is_closed() {
            return;
        }
        strikes = if stuck { strikes + 1 } else { 0 };
        (at, skip) = next_dial(bases.len(), at, strikes);
        if stuck {
            JS_STUCK.inc();
            tracing::error!(
                cursor_us = cursor,
                strikes,
                next = bases[at],
                skip_us = skip,
                "Jetstream stuck at the cursor"
            );
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
        assert_eq!(f, Some(Ev::Follow { did: "did:plc:a".into(), time_us: 9, subject: Some("did:plc:b".into()) }));
        let u = parse(
            r#"{"did":"did:plc:a","time_us":10,"kind":"commit","commit":{"operation":"delete","collection":"app.bsky.graph.follow","rkey":"3k"}}"#,
        );
        assert_eq!(u, Some(Ev::Follow { did: "did:plc:a".into(), time_us: 10, subject: None }));
        assert!(parse(r#"{"did":"did:plc:a","time_us":9,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"3k"}}"#).is_none());
        assert!(parse("not json").is_none());
    }

    #[test]
    fn stuck_cursors_move_on() {
        // one instance: a plain redial, then steps past the hole that double
        assert_eq!(next_dial(1, 0, 0), (0, 0));
        assert_eq!(next_dial(1, 0, 1), (0, 0));
        assert_eq!(next_dial(1, 0, 2), (0, SKIP_US));
        assert_eq!(next_dial(1, 0, 3), (0, 2 * SKIP_US));
        assert_eq!(next_dial(1, 0, 99), (0, SKIP_US << 16));
        // two: the other instance first, and a skip only once both were stuck there
        assert_eq!(next_dial(2, 0, 0), (0, 0));
        assert_eq!(next_dial(2, 0, 1), (1, 0));
        assert_eq!(next_dial(2, 1, 2), (0, 0));
        assert_eq!(next_dial(2, 0, 3), (1, SKIP_US));
    }

    /// A Jetstream instance whose store lacks `hole`: a replay from before it stops at its start
    /// and sends nothing more, as jetstream us-east did; a cursor past its start replays from its
    /// end. Each dial's cursor goes to `dials`.
    async fn holed_jetstream(hole: (u64, u64), events: Vec<u64>, dials: mpsc::UnboundedSender<u64>) -> String {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (sock, _) = l.accept().await.unwrap();
                let (events, dials) = (events.clone(), dials.clone());
                tokio::spawn(async move {
                    let mut asked = 0u64;
                    #[allow(clippy::result_large_err)]
                    let cb = |req: &tokio_tungstenite::tungstenite::handshake::server::Request, resp| {
                        let q = req.uri().query().unwrap_or("");
                        asked =
                            q.split('&').find_map(|kv| kv.strip_prefix("cursor=")).map_or(0, |c| c.parse().unwrap());
                        Ok(resp)
                    };
                    let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(sock, cb).await else { return };
                    let _ = dials.send(asked);
                    for t in events.iter().copied().filter(|&t| t >= asked) {
                        if asked <= hole.0 && t > hole.0 {
                            break;
                        }
                        if t > hole.0 && t < hole.1 {
                            continue;
                        }
                        let m = format!(
                            r#"{{"did":"did:plc:a","time_us":{t},"kind":"commit","commit":{{"operation":"update","collection":"app.bsky.actor.profile","rkey":"self","record":{{"displayName":"A"}}}}}}"#
                        );
                        if futures::SinkExt::send(&mut ws, Message::text(m)).await.is_err() {
                            return;
                        }
                    }
                    // the socket stays open and quiet
                    while ws.next().await.is_some() {}
                });
            }
        });
        format!("ws://{addr}")
    }

    async fn read_until(rx: &mut mpsc::Receiver<Ev>, t: u64) -> Vec<Ev> {
        let mut got = Vec::new();
        while let Some(ev) = tokio::time::timeout(Duration::from_secs(20), rx.recv()).await.expect("events stopped") {
            let done = ev.time_us() >= t && !matches!(ev, Ev::Gap { .. });
            got.push(ev);
            if done {
                return got;
            }
        }
        panic!("Jetstream reader ended");
    }

    #[tokio::test]
    async fn a_hole_in_one_instance_goes_to_the_next() {
        let now = now_us();
        let start = now - 600_000_000;
        // an event every 10 s up to now; us-east's store lacks 131 s from the 3rd
        let events: Vec<u64> = (0..60).map(|i| start + i * 10_000_000).collect();
        let hole = (events[2], events[2] + 131_000_000);
        let (dtx, mut drx) = mpsc::unbounded_channel();
        let east = holed_jetstream(hole, events.clone(), dtx.clone()).await;
        let west = holed_jetstream((0, 0), events.clone(), dtx).await;

        let (tx, mut rx) = mpsc::channel(1000);
        let bases = format!("{east},{west}");
        let r = tokio::spawn(async move { follow(&bases, start, false, tx, Duration::from_millis(300)).await });
        let got = read_until(&mut rx, *events.last().unwrap()).await;
        r.abort();
        // nothing lost and no gap: the other instance had the hole's events
        assert!(!got.iter().any(|e| matches!(e, Ev::Gap { .. })));
        let mut ts: Vec<u64> = got.iter().map(Ev::time_us).collect();
        ts.dedup();
        assert_eq!(ts, events);
        assert_eq!(drx.recv().await, Some(start));
        assert_eq!(drx.recv().await, Some(hole.0 - OVERLAP_US), "the stuck cursor, at the other instance");
    }

    #[tokio::test]
    async fn a_hole_in_the_only_instance_is_skipped_with_a_gap() {
        let now = now_us();
        let start = now - 600_000_000;
        let events: Vec<u64> = (0..60).map(|i| start + i * 10_000_000).collect();
        let hole = (events[2], events[2] + 131_000_000);
        let (dtx, mut drx) = mpsc::unbounded_channel();
        let east = holed_jetstream(hole, events.clone(), dtx).await;

        let (tx, mut rx) = mpsc::channel(1000);
        let r = tokio::spawn(async move { follow(&east, start, false, tx, Duration::from_millis(300)).await });
        let got = read_until(&mut rx, *events.last().unwrap()).await;
        r.abort();
        let gap = got.iter().find_map(|e| match e {
            Ev::Gap { asked_us, first_us } => Some((*asked_us, *first_us)),
            _ => None,
        });
        let after = events.iter().copied().find(|&t| t >= hole.1).unwrap();
        assert_eq!(gap, Some((hole.0, after)), "the skip is reported as a gap from the stuck cursor");
        assert_eq!(drx.recv().await, Some(start));
        assert_eq!(drx.recv().await, Some(hole.0 - OVERLAP_US), "a plain redial first");
        assert_eq!(drx.recv().await, Some(hole.0 - OVERLAP_US));
        assert_eq!(drx.recv().await, Some(hole.0 + SKIP_US), "then a step past the hole");
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
