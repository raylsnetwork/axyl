//! Embedded dev dashboard: a self-contained block-explorer / chain-status UI.
//!
//! A tiny static HTTP server (std-only, no axum/tower, no async runtime) serves a
//! single embedded HTML page. Chain data is fetched client-side from the node's
//! JSON-RPC (dev mode sets a permissive CORS policy). The one server-side helper is
//! `/api/sign`, which signs a transfer from a well-known dev account so the page's
//! "send tx" buttons work without bundling a JS crypto library — the browser then
//! submits the signed tx via `eth_sendRawTransaction`.
//!
//! Dev-only: it serves an unauthenticated page bound to localhost, signs with
//! public dev keys, and is started solely by the `dev` subcommand.

use rayls_infrastructure_types::RaylsNetwork;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
};
use tracing::{info, warn};

/// The dashboard page. `__RPC_URL__` is replaced with the node's RPC endpoint at
/// serve time (the page also lets the user edit it).
const DASHBOARD_HTML: &str = include_str!("dashboard.html");

/// Spawn the dev dashboard on `127.0.0.1:port`, serving a page wired to `rpc_url`.
///
/// Non-fatal: a bind failure (e.g. port in use) only logs a warning so it can
/// never take down the node. Runs on detached threads for the process lifetime.
pub(super) fn spawn_dashboard(port: u16, rpc_url: String) {
    let addr = format!("127.0.0.1:{port}");
    let listener = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            warn!(target: "rl::dev", %addr, %e, "could not start dev dashboard (continuing without it)");
            return;
        }
    };

    let page = Arc::new(DASHBOARD_HTML.replace("__RPC_URL__", &rpc_url));
    info!(target: "rl::dev", "dev dashboard:  http://{addr}  (chain explorer + status)");

    let spawned = std::thread::Builder::new().name("dev-dashboard".into()).spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let page = Arc::clone(&page);
                    // One short-lived thread per connection; localhost dev traffic only.
                    let _ = std::thread::Builder::new().name("dev-dashboard-conn".into()).spawn(
                        move || {
                            if let Err(e) = handle_conn(stream, &page) {
                                warn!(target: "rl::dev", %e, "dev dashboard connection error");
                            }
                        },
                    );
                }
                Err(e) => warn!(target: "rl::dev", %e, "dev dashboard accept error"),
            }
        }
    });
    if let Err(e) = spawned {
        warn!(target: "rl::dev", %e, "could not spawn dev dashboard thread");
    }
}

/// The dev chain-id (matches `GenesisArgs::dev`). Used to EIP-155-sign dev txs.
const DEV_CHAIN_ID: u64 = RaylsNetwork::Local.chain_id();

/// Serve a response. Reads the request line (enough to route a GET) and always
/// closes the connection. Routes:
/// - `/` (or `/?...`): the dashboard HTML.
/// - `/api/sign?to=..&value=..&nonce=..`: sign a transfer from dev account #0 and return the raw tx
///   hex (the browser submits it via `eth_sendRawTransaction`).
/// - `/healthz`: liveness.
fn handle_conn(mut stream: TcpStream, page: &str) -> std::io::Result<()> {
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).ok();
    let mut buf = [0u8; 2048];
    let n = stream.read(&mut buf)?;
    let req = String::from_utf8_lossy(&buf[..n]);
    let path = req.split_whitespace().nth(1).unwrap_or("/");

    let (status, content_type, body): (&str, &str, String) =
        if path == "/" || path.is_empty() || path.starts_with("/?") {
            ("200 OK", "text/html; charset=utf-8", page.to_string())
        } else if path.starts_with("/api/sign") {
            match sign_from_query(path) {
                Ok(raw) => ("200 OK", "text/plain; charset=utf-8", raw),
                Err(e) => ("400 Bad Request", "text/plain; charset=utf-8", e),
            }
        } else if path == "/healthz" {
            ("200 OK", "text/plain; charset=utf-8", "ok".to_string())
        } else {
            ("404 Not Found", "text/plain; charset=utf-8", "not found".to_string())
        };

    let response = format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {len}\r\n\
         Cache-Control: no-store, must-revalidate\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        len = body.len(),
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()
}

/// Parse `to`/`value`/`nonce` from the `/api/sign` query and return a signed raw
/// transaction hex string, or a short error message.
fn sign_from_query(path: &str) -> Result<String, String> {
    let query = path.split_once('?').map(|(_, q)| q).unwrap_or("");
    let (mut to, mut value, mut nonce, mut data): (Option<String>, u128, u128, Vec<u8>) =
        (None, 0, 0, Vec::new());
    for pair in query.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        match k {
            // An empty `to` means contract creation.
            "to" => {
                if !v.is_empty() {
                    to = Some(v.to_string());
                }
            }
            "value" => value = v.parse().map_err(|_| "bad value".to_string())?,
            "nonce" => nonce = v.parse().map_err(|_| "bad nonce".to_string())?,
            "data" => data = hex_vec(v).ok_or_else(|| "bad data hex".to_string())?,
            _ => {}
        }
    }
    let to = match to {
        Some(s) => Some(parse_addr20(&s).ok_or_else(|| "bad `to` address".to_string())?),
        None => None, // contract creation
    };
    dev_sign_legacy_tx(to, value, nonce, data).ok_or_else(|| "sign failed".to_string())
}

/// Sign a legacy tx from dev account #0 (its key is public — dev only).
///
/// `to == None` is a contract creation; `data` is the calldata (or init code).
fn dev_sign_legacy_tx(
    to: Option<[u8; 20]>,
    value: u128,
    nonce: u128,
    data: Vec<u8>,
) -> Option<String> {
    use ethereum_tx_sign::{LegacyTransaction, Transaction};
    let key = hex_bytes::<32>(super::DEV_ACCOUNTS[0].1)?;
    // A plain native-token transfer is exactly 21k gas; anything with calldata or a
    // contract creation needs headroom. The dev chain has a 500M block gas limit and
    // a ~0 base fee, so over-providing gas is free.
    let gas = if to.is_some() && data.is_empty() { 21_000 } else { 5_000_000 };
    let tx = LegacyTransaction {
        chain: DEV_CHAIN_ID,
        nonce,
        to,
        value,
        gas_price: 1_000_000_000, // 1 gwei; dev account #0 is well funded
        gas,
        data,
    };
    let ecdsa = tx.ecdsa(&key).ok()?;
    let raw = tx.sign(&ecdsa);
    let mut out = String::with_capacity(2 + raw.len() * 2);
    out.push_str("0x");
    for b in raw {
        out.push_str(&format!("{b:02x}"));
    }
    Some(out)
}

/// Decode a variable-length hex string (optionally `0x`-prefixed) into bytes.
fn hex_vec(s: &str) -> Option<Vec<u8>> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let mut i = 0;
    while i < s.len() {
        out.push(u8::from_str_radix(s.get(i..i + 2)?, 16).ok()?);
        i += 2;
    }
    Some(out)
}

/// Decode a 20-byte address (optionally `0x`-prefixed) into bytes.
fn parse_addr20(s: &str) -> Option<[u8; 20]> {
    hex_bytes::<20>(s)
}

/// Decode an `N`-byte hex string (optionally `0x`-prefixed) into a fixed array.
fn hex_bytes<const N: usize>(s: &str) -> Option<[u8; N]> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}
