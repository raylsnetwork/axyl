//! TCP health + readiness endpoint for monitoring service availability.
//!
//! A minimal HTTP/1.1 server exposing two probes:
//! - **liveness** (any path, e.g. `GET /`): always `200 OK` while the process is accepting
//!   connections — the pre-existing behaviour.
//! - **readiness** (`GET /readyz`, alias `GET /ready`): `200 OK` only when the node is actually
//!   participating — voting (`CvvActive`) or an operational observer — and `503 Service
//!   Unavailable` while a validator is still catching up (`CvvInactive`). This lets a load balancer
//!   / on-call distinguish "process up" from "actually voting in the current epoch".
//!
//! Designed for integration with GCP load balancers and similar health monitoring systems.
use std::{io::ErrorKind, net::SocketAddr, time::Duration};

use rayls_consensus_primary::NodeMode;
use rayls_infrastructure_types::TaskSpawner;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::watch,
    time::sleep,
};
use tracing::info;

/// Liveness / readiness HTTP responder for service monitoring.
///
/// Binds a TCP port and answers:
/// - liveness (any path): `200 OK` — the process is up.
/// - readiness (`/readyz`): `200` when voting/observing, `503` while catching up.
///
/// # Security Considerations
///
/// This endpoint accepts connections from any source. Node operators must protect it with a
/// firewall. It is off by default and enabled via the CLI node command. Each connection is
/// handled in the accept loop and closed immediately after the response.
///
/// To enable on node startup, use `rayls-network node --healthcheck <PORT>`.
/// See `rayls-network-cli::node` for more info.
#[derive(Debug)]
pub(crate) struct HealthcheckServer;

const LIVE_200: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";
const READY_200: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nREADY";
const NOT_READY_503: &[u8] =
    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 9\r\n\r\nNOT_READY";

/// True if the HTTP request line targets the readiness path (`/readyz` or `/ready`).
fn request_targets_readiness(req: &[u8]) -> bool {
    // Request line looks like `GET /readyz HTTP/1.1`; match the path token, ignoring the
    // method and version.
    let line = req.split(|&b| b == b'\r' || b == b'\n').next().unwrap_or(req);
    let mut tokens = line.split(|&b| b == b' ').filter(|t| !t.is_empty());
    let _method = tokens.next();
    matches!(tokens.next(), Some(path) if path == b"/readyz" || path == b"/ready")
}

/// Readiness response for a node mode: ready (`200`) when voting (`CvvActive`) or an operational
/// observer; not ready (`503`) while a validator is still catching up (`CvvInactive`).
fn readiness_response(mode: NodeMode) -> &'static [u8] {
    if mode.is_active_cvv() || mode.is_observer() {
        READY_200
    } else {
        NOT_READY_503
    }
}

impl HealthcheckServer {
    /// Spawn the health/readiness server, returning the bound address.
    ///
    /// Binds `0.0.0.0:port` (all interfaces, for external health checkers). `node_mode` is a
    /// watch receiver of the node's consensus mode (from the consensus bus); the readiness probe
    /// reads its current value per request.
    pub(crate) async fn spawn(
        task_spawner: TaskSpawner,
        port: u16,
        node_mode: watch::Receiver<NodeMode>,
    ) -> eyre::Result<SocketAddr> {
        // IMPORTANT: use firewall to protect this endpoint
        let addr: SocketAddr = ([0, 0, 0, 0], port).into();
        let listener = TcpListener::bind(addr).await?;
        let listen_on = listener.local_addr()?;
        info!(target: "epoch-manager", ?listen_on, "healthcheck listening");

        task_spawner.spawn_task("healthcheck", async move {
            let initial_backoff = Duration::from_millis(100);
            let mut backoff = initial_backoff;
            let max_backoff = Duration::from_secs(5);

            loop {
                match listener.accept().await {
                    Ok((mut socket, _)) => {
                        // Reset backoff after a healthy accept so a past transient error doesn't
                        // keep the delay elevated.
                        backoff = initial_backoff;

                        // Handle each connection in its own task: the request read is bounded by a
                        // short timeout, but a client that connects and sends nothing would still
                        // hold the accept loop for the full timeout, delaying every other probe.
                        // A per-connection task keeps the accept loop free. `node_mode` is a cheap
                        // watch receiver, cloned per connection.
                        let node_mode = node_mode.clone();
                        tokio::spawn(async move {
                            let mut buf = [0u8; 256];
                            let wants_readiness = matches!(
                                tokio::time::timeout(
                                    Duration::from_millis(200),
                                    socket.read(&mut buf),
                                )
                                .await,
                                Ok(Ok(n)) if n > 0 && request_targets_readiness(&buf[..n])
                            );

                            let response = if wants_readiness {
                                readiness_response(*node_mode.borrow())
                            } else {
                                // Any other path (incl. `/`) is a liveness probe: process is up.
                                LIVE_200
                            };

                            // write response, ignore errors (client disconnect, etc.), then drop.
                            if let Err(e) = socket.write_all(response).await {
                                tracing::error!(target: "healthcheck", ?e, "error writing healthcheck response");
                            }
                        });
                    }
                    Err(ref e) if matches!(
                        e.kind(),
                        ErrorKind::WouldBlock
                        | ErrorKind::Interrupted
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::ConnectionReset
                        | ErrorKind::Other
                    ) => {
                        // transient errors that can be ignored
                        tracing::warn!(target: "healthcheck", ?e, "transient error accepting healthcheck connection");
                        sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                    }
                    Err(e) => {
                        // unexpected errors: log and break to avoid spinning on fatal errors
                        tracing::error!(target: "healthcheck", ?e, "error accepting healthcheck connection");
                        break;
                    }
                }
            }
        });

        Ok(listen_on)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_types::{get_available_tcp_port, TaskManager};
    use tokio::net::TcpStream;

    async fn request(addr: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(format!("GET {path} HTTP/1.1\r\n\r\n").as_bytes()).await.unwrap();
        let mut response = vec![0u8; 1024];
        let n = stream.read(&mut response).await.unwrap();
        String::from_utf8_lossy(&response[..n]).into_owned()
    }

    #[test]
    fn readiness_path_is_matched() {
        assert!(request_targets_readiness(b"GET /readyz HTTP/1.1\r\n\r\n"));
        assert!(request_targets_readiness(b"GET /ready HTTP/1.1\r\n\r\n"));
        assert!(!request_targets_readiness(b"GET / HTTP/1.1\r\n\r\n"));
        assert!(!request_targets_readiness(b"GET /healthz HTTP/1.1\r\n\r\n"));
    }

    #[test]
    fn readiness_response_tracks_mode() {
        assert_eq!(readiness_response(NodeMode::CvvActive), READY_200);
        assert_eq!(readiness_response(NodeMode::Observer), READY_200);
        assert_eq!(readiness_response(NodeMode::CvvInactive), NOT_READY_503);
    }

    #[tokio::test]
    async fn liveness_always_ok_and_readiness_follows_mode() -> eyre::Result<()> {
        let task_manager = TaskManager::default();
        let task_spawner = task_manager.get_spawner();
        let (tx, rx) = watch::channel(NodeMode::CvvInactive);

        let port = get_available_tcp_port("127.0.0.1").expect("tcp port assigned by host");
        let addr = HealthcheckServer::spawn(task_spawner, port, rx).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Liveness: always 200, even while catching up.
        assert!(request(addr, "/").await.starts_with("HTTP/1.1 200 OK"));

        // Readiness: 503 while CvvInactive (catching up)...
        assert!(request(addr, "/readyz").await.starts_with("HTTP/1.1 503"));

        // ...and 200 once the node is voting.
        tx.send(NodeMode::CvvActive).unwrap();
        assert!(request(addr, "/readyz").await.starts_with("HTTP/1.1 200 OK"));

        Ok(())
    }
}
