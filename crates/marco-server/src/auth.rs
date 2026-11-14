//! Who may call what. On the API listener, Graph reads are open and its
//! writes need the admin token. On the admin listener everything but the
//! console's files and GetSession needs the token, or a login from the
//! trusted proxy header (off unless configured, and believed only from the
//! proxy's own addresses, as vlRelay's console gate does, and with
//! `--admin-proxy-peers` only on a connection whose PROXY v2 header names
//! one of them).

use std::net::IpAddr;
use std::sync::Arc;

use axum::extract::{ConnectInfo, Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use connectrpc::ConnectError;
use http::{header, HeaderMap, StatusCode};

use crate::app::App;
use crate::peer::Peer;

#[derive(Clone, Default)]
pub struct AuthConfig {
    pub token: Option<String>,
    pub proxy_header: Option<http::HeaderName>,
    pub proxy_from: Vec<Cidr>,
    pub operators: Vec<String>,
    /// Connections from here start with a PROXY v2 header (crate::peer).
    pub proxy_protocol_from: Vec<Cidr>,
    /// When set, the header login is believed only from these callers.
    pub proxy_peers: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
pub struct Cidr {
    addr: IpAddr,
    bits: u8,
}

impl std::str::FromStr for Cidr {
    type Err = String;
    fn from_str(s: &str) -> Result<Cidr, String> {
        let (a, b) = s.split_once('/').unwrap_or((s, ""));
        let addr: IpAddr = a.parse().map_err(|_| format!("{s:?} isn't an address or CIDR"))?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        let bits = if b.is_empty() { max } else { b.parse().map_err(|_| format!("{s:?}: bad prefix length"))? };
        if bits > max {
            return Err(format!("{s:?}: prefix longer than {max}"));
        }
        Ok(Cidr { addr, bits })
    }
}

impl Cidr {
    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = match ip {
            IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
            v4 => v4,
        };
        match (self.addr, ip) {
            (IpAddr::V4(a), IpAddr::V4(b)) => {
                let m = if self.bits == 0 { 0 } else { u32::MAX << (32 - self.bits) };
                u32::from(a) & m == u32::from(b) & m
            }
            (IpAddr::V6(a), IpAddr::V6(b)) => {
                let m = if self.bits == 0 { 0 } else { u128::MAX << (128 - self.bits) };
                u128::from(a) & m == u128::from(b) & m
            }
            _ => false,
        }
    }
}

/// How a caller got in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Caller {
    Token,
    Proxy(String),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Listener {
    Api,
    Admin,
}

fn eq_ct(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The caller, none, or the refusal to send.
pub fn identify(
    cfg: &AuthConfig,
    kind: Listener,
    headers: &HeaderMap,
    peer: IpAddr,
    caller: Option<&str>,
) -> Result<Option<Caller>, Refusal> {
    if let Some(v) = headers.get(header::AUTHORIZATION) {
        let Some(want) = &cfg.token else {
            return Err(Refusal::unauthenticated("this marco has no --admin-token-file"));
        };
        let got = v.to_str().ok().and_then(|s| s.strip_prefix("Bearer ")).unwrap_or("");
        return if eq_ct(got.trim().as_bytes(), want.as_bytes()) {
            Ok(Some(Caller::Token))
        } else {
            Err(Refusal::unauthenticated("wrong token"))
        };
    }
    let Some(h) = &cfg.proxy_header else { return Ok(None) };
    if kind != Listener::Admin || !cfg.proxy_from.iter().any(|c| c.contains(peer)) {
        return Ok(None);
    }
    let vals: Vec<_> = headers.get_all(h).iter().collect();
    if vals.is_empty() {
        return Ok(None);
    }
    // The proxy may deliver other callers' streams from the same address:
    // only the ones named here may sign an operator in.
    if !cfg.proxy_peers.is_empty() && !caller.is_some_and(|c| cfg.proxy_peers.iter().any(|p| p == c)) {
        let who = caller.unwrap_or("a connection with no PROXY v2 header");
        return Err(Refusal::denied(&format!("{who} may not sign operators in")));
    }
    let login = match vals.as_slice() {
        [v] => v.to_str().unwrap_or("").trim(),
        _ => return Err(Refusal::denied("the proxy sent more than one login")),
    };
    if login.is_empty() || !cfg.operators.iter().any(|o| o == login) {
        return Err(Refusal::denied(&format!("{login:?} isn't an operator here")));
    }
    Ok(Some(Caller::Proxy(login.to_string())))
}

pub struct Refusal {
    code: StatusCode,
    err: ConnectError,
}

impl Refusal {
    fn unauthenticated(m: &str) -> Refusal {
        Refusal { code: StatusCode::UNAUTHORIZED, err: ConnectError::unauthenticated(m) }
    }
    fn denied(m: &str) -> Refusal {
        Refusal { code: StatusCode::FORBIDDEN, err: ConnectError::permission_denied(m) }
    }

    fn into_response(self, connect: bool) -> Response {
        if connect {
            (self.code, [(header::CONTENT_TYPE, "application/json")], self.err.to_json()).into_response()
        } else {
            (self.code, format!("{}\n", self.err.message.as_deref().unwrap_or(""))).into_response()
        }
    }
}

pub const GRAPH_WRITES: [&str; 2] = ["/marco.v1.Graph/Write", "/marco.v1.Graph/WriteBatch"];

/// Admin RPCs that change nothing, which a proxy login may call without the
/// browser's same-origin marks (curl through the proxy).
fn is_read(path: &str) -> bool {
    match path.strip_prefix("/marco.v1.Admin/") {
        Some(m) => ["Get", "List", "Resolve"].iter().any(|p| m.starts_with(p)),
        None => path.starts_with("/marco.v1.Graph/") && !GRAPH_WRITES.contains(&path),
    }
}

/// A login is ambient, like a cookie: a call made with one has to come from
/// the console's own page, never from another site or a link.
fn same_origin(headers: &HeaderMap, write: bool) -> bool {
    let site = headers.get("sec-fetch-site").and_then(|v| v.to_str().ok());
    match site {
        Some("same-origin") => true,
        // a page the operator opened by typing it or from a bookmark
        Some("none") => !write,
        Some(_) => false,
        None if !write => true,
        None => {
            let host = headers.get(header::HOST).and_then(|v| v.to_str().ok()).unwrap_or("");
            let origin = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()).unwrap_or("");
            !host.is_empty() && (origin == format!("http://{host}") || origin == format!("https://{host}"))
        }
    }
}

pub async fn gate(
    State((app, kind)): State<(Arc<App>, Listener)>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    mut req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path().to_string();
    let connect = path.starts_with("/marco.v1.");
    let needs = match kind {
        Listener::Api => !app.cfg.api_read_only && GRAPH_WRITES.contains(&path.as_str()),
        Listener::Admin => {
            (connect
                && path != "/marco.v1.Admin/GetSession"
                && !(path.starts_with("/marco.v1.Graph/") && is_read(&path)))
                || path.starts_with("/metrics")
                || path.starts_with("/debug/")
                || path.starts_with("/admin/checkpoint")
        }
    };
    let caller = match identify(&app.cfg.auth, kind, req.headers(), peer.addr.ip(), peer.caller.as_deref()) {
        Ok(c) => c,
        Err(r) if needs || path == "/marco.v1.Admin/GetSession" => return r.into_response(connect),
        Err(_) => None,
    };
    // The console's page and assets carry no data, so a link to them is fine;
    // the RPCs and ops routes it then calls are what a login must guard.
    let guarded =
        connect || path.starts_with("/metrics") || path.starts_with("/debug/") || path.starts_with("/admin/checkpoint");
    if let (Some(Caller::Proxy(_)), true) = (&caller, guarded) {
        let write = !is_read(&path) && !(path.starts_with("/metrics") || req.method() == http::Method::GET);
        if !same_origin(req.headers(), write) {
            return Refusal::denied("a proxy login works only from the console's own page").into_response(connect);
        }
    }
    if needs && caller.is_none() {
        let why = if app.cfg.auth.token.is_none() && app.cfg.auth.proxy_header.is_none() {
            "this marco has no --admin-token-file"
        } else {
            "needs Authorization: Bearer <admin token>"
        };
        return Refusal::unauthenticated(why).into_response(connect);
    }
    if let Some(c) = caller {
        req.extensions_mut().insert(c);
    }
    next.run(req).await
}
