//! Who connected. A connection from `--proxy-protocol-from` starts with a
//! PROXY protocol v2 header its proxy wrote, naming the caller it verified
//! in TLV 0xE0. When that proxy delivers every caller's streams from its own
//! address, the address alone can't say which caller sent a login, and the
//! header can: `--admin-proxy-peers` believes a login only from the callers
//! it lists.

use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use axum::extract::connect_info::Connected;
use axum::serve::IncomingStream;
use prometheus::{register_int_counter, IntCounter};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::auth::Cidr;

/// The v2 signature every header starts with.
const SIGNATURE: [u8; 12] = *b"\r\n\r\n\0\r\nQUIT\n";
/// The caller's identity (a SPIFFE ID), UTF-8.
pub const TLV_CALLER: u8 = 0xE0;
const MAX_BODY: usize = 4096;
/// A proxy writes its header before any byte of the stream: one that takes
/// longer isn't a proxy.
const HEADER_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) static REFUSED: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "marco_proxy_header_refused_total",
        "Connections from --proxy-protocol-from closed for a missing or bad PROXY v2 header"
    )
    .unwrap()
});

/// The socket's address and, from `--proxy-protocol-from`, the caller its
/// header named.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
    pub addr: SocketAddr,
    pub caller: Option<String>,
}

impl From<SocketAddr> for Peer {
    fn from(addr: SocketAddr) -> Peer {
        Peer { addr, caller: None }
    }
}

/// The caller a PROXY v2 header names, from its 16 fixed bytes and the rest.
pub fn parse(fixed: &[u8; 16], body: &[u8]) -> Result<String, String> {
    let n = body_len(fixed)?;
    if n != body.len() {
        return Err("PROXY v2 body length doesn't match".into());
    }
    let addrs = match fixed[13] {
        0x11 => 12,
        0x21 => 36,
        f => return Err(format!("PROXY v2 family {f:#04x}, want TCP over IPv4 or IPv6")),
    };
    let mut rest = body.get(addrs..).ok_or("PROXY v2 header too short for its addresses")?;
    let mut caller = None;
    while !rest.is_empty() {
        if rest.len() < 3 {
            return Err("PROXY v2 TLV cut short".into());
        }
        let (t, n) = (rest[0], u16::from_be_bytes([rest[1], rest[2]]) as usize);
        let v = rest.get(3..3 + n).ok_or("PROXY v2 TLV runs past the header")?;
        if t == TLV_CALLER {
            if caller.is_some() {
                return Err("PROXY v2 header names two callers".into());
            }
            caller = Some(String::from_utf8(v.to_vec()).map_err(|_| "PROXY v2 caller isn't UTF-8")?);
        }
        rest = &rest[3 + n..];
    }
    caller.filter(|c| !c.is_empty()).ok_or_else(|| "PROXY v2 header names no caller".into())
}

fn body_len(fixed: &[u8; 16]) -> Result<usize, String> {
    if fixed[..12] != SIGNATURE {
        return Err("no PROXY v2 header".into());
    }
    if fixed[12] != 0x21 {
        return Err(format!("PROXY v2 command {:#04x}, want PROXY", fixed[12]));
    }
    let n = u16::from_be_bytes([fixed[14], fixed[15]]) as usize;
    if n > MAX_BODY {
        return Err(format!("PROXY v2 header of {n} bytes, past {MAX_BODY}"));
    }
    Ok(n)
}

/// Reads exactly the header, so the stream goes on at the request's first byte.
async fn read_header(s: &mut TcpStream) -> Result<String, String> {
    let mut fixed = [0u8; 16];
    s.read_exact(&mut fixed).await.map_err(|e| e.to_string())?;
    let mut body = vec![0u8; body_len(&fixed)?];
    s.read_exact(&mut body).await.map_err(|e| e.to_string())?;
    parse(&fixed, &body)
}

/// A TCP listener that reads the header off each connection from `from`
/// (closing one without a good header) and passes every other connection
/// through as it is. Headers are read off the accept loop, so a slow one
/// holds up nobody else. Dropping it closes the socket: a connect after
/// that is refused.
pub struct PeerListener {
    rx: mpsc::Receiver<(TcpStream, Peer)>,
    local: SocketAddr,
}

impl PeerListener {
    pub fn new(mut l: TcpListener, from: Vec<Cidr>) -> std::io::Result<PeerListener> {
        let local = l.local_addr()?;
        let from = Arc::new(from);
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let (mut s, addr) = tokio::select! {
                    c = axum::serve::Listener::accept(&mut l) => c,
                    _ = tx.closed() => return,
                };
                // Without TCP_NODELAY, an HTTP/2 response written as two
                // frames waits on the client's delayed ACK: 40 ms for a page.
                let _ = s.set_nodelay(true);
                if !from.iter().any(|c| c.contains(addr.ip())) {
                    if tx.send((s, Peer::from(addr))).await.is_err() {
                        return;
                    }
                    continue;
                }
                let tx = tx.clone();
                tokio::spawn(async move {
                    let r = tokio::time::timeout(HEADER_TIMEOUT, read_header(&mut s))
                        .await
                        .unwrap_or_else(|_| Err("no PROXY v2 header in time".into()));
                    match r {
                        Ok(caller) => {
                            let _ = tx.send((s, Peer { addr, caller: Some(caller) })).await;
                        }
                        Err(e) => {
                            REFUSED.inc();
                            tracing::warn!(%addr, "closed a connection from --proxy-protocol-from: {e}");
                        }
                    }
                });
            }
        });
        Ok(PeerListener { rx, local })
    }
}

impl axum::serve::Listener for PeerListener {
    type Io = TcpStream;
    type Addr = Peer;

    async fn accept(&mut self) -> (TcpStream, Peer) {
        match self.rx.recv().await {
            Some(c) => c,
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<Peer> {
        Ok(Peer::from(self.local))
    }
}

impl Connected<IncomingStream<'_, PeerListener>> for Peer {
    fn connect_info(s: IncomingStream<'_, PeerListener>) -> Peer {
        s.remote_addr().clone()
    }
}

/// A header naming `caller`, as a proxy writes it (tests, the harness).
pub fn encode(src: SocketAddr, dst: SocketAddr, caller: &str) -> Vec<u8> {
    let v6 = |a: SocketAddr| match a {
        SocketAddr::V4(v) => v.ip().to_ipv6_mapped(),
        SocketAddr::V6(v) => *v.ip(),
    };
    let mut body = Vec::with_capacity(36 + 3 + caller.len());
    body.extend_from_slice(&v6(src).octets());
    body.extend_from_slice(&v6(dst).octets());
    body.extend_from_slice(&src.port().to_be_bytes());
    body.extend_from_slice(&dst.port().to_be_bytes());
    body.push(TLV_CALLER);
    body.extend_from_slice(&(caller.len() as u16).to_be_bytes());
    body.extend_from_slice(caller.as_bytes());
    let mut out = SIGNATURE.to_vec();
    out.extend_from_slice(&[0x21, 0x21]);
    out.extend_from_slice(&(body.len() as u16).to_be_bytes());
    out.extend_from_slice(&body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(b: &[u8]) -> ([u8; 16], &[u8]) {
        (b[..16].try_into().unwrap(), &b[16..])
    }

    #[test]
    fn reads_the_caller() {
        let h = encode("[fd00::1]:4000".parse().unwrap(), "[fd00::2]:7301".parse().unwrap(), "spiffe://x/caddy/caddy");
        let (f, b) = split(&h);
        assert_eq!(parse(&f, b).unwrap(), "spiffe://x/caddy/caddy");

        // an IPv4 header with another TLV ahead of the caller
        let mut body = vec![10, 0, 0, 1, 10, 0, 0, 2, 0, 1, 0, 2, 0xE1, 0, 2, 9, 9, TLV_CALLER, 0, 3];
        body.extend_from_slice(b"abc");
        let mut h = SIGNATURE.to_vec();
        h.extend_from_slice(&[0x21, 0x11, 0, body.len() as u8]);
        h.extend_from_slice(&body);
        let (f, b) = split(&h);
        assert_eq!(parse(&f, b).unwrap(), "abc");
    }

    #[test]
    fn refuses_what_it_should() {
        let good = encode("127.0.0.1:1".parse().unwrap(), "127.0.0.1:2".parse().unwrap(), "who");
        let (f, b) = split(&good);
        // HTTP where a header should be
        let mut http = [0u8; 16];
        http.copy_from_slice(b"GET / HTTP/1.1\r\n");
        assert!(parse(&http, b).is_err());
        // LOCAL, not PROXY
        let mut local = f;
        local[12] = 0x20;
        assert!(parse(&local, b).is_err());
        // UDP
        let mut udp = f;
        udp[13] = 0x22;
        assert!(parse(&udp, b).is_err());
        // cut short
        assert!(parse(&f, &b[..b.len() - 1]).is_err());
        // no caller, or two
        let none = &b[..36];
        let mut f2 = f;
        f2[14..16].copy_from_slice(&(none.len() as u16).to_be_bytes());
        assert!(parse(&f2, none).is_err());
        let mut two = b.to_vec();
        two.extend_from_slice(&b[36..]);
        let mut f3 = f;
        f3[14..16].copy_from_slice(&(two.len() as u16).to_be_bytes());
        assert!(parse(&f3, &two).is_err());
        // past the cap
        let mut big = f;
        big[14..16].copy_from_slice(&5000u16.to_be_bytes());
        assert!(body_len(&big).is_err());
    }
}
