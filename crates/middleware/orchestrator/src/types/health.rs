//! HTTP health + readiness endpoint for monitoring service availability.
//!
//! Built on `axum` (already a workspace dependency, same as `consensus-metrics`' Prometheus
//! server) exposing two probes:
//! - **liveness** (any path, e.g. `GET /`): always `200 OK` while the process is accepting
//!   connections — the pre-existing behaviour.
//! - **readiness** (`GET /readyz`, alias `GET /ready`): `200 OK` only when the node is actually
//!   participating — voting (`CvvActive`) or an operational observer — and `503 Service
//!   Unavailable` while a validator is still catching up (`CvvInactive`).
//!
//! **Why 503 specifically, and why only for `CvvInactive`:** liveness and readiness answer
//! different questions on purpose (the standard Kubernetes/GCP-load-balancer distinction this
//! endpoint is built for). Liveness means "is the process alive — should the orchestrator avoid
//! restarting it?"; a `CvvInactive` validator is alive and mid-recovery, so restarting it would
//! only set it back further, and liveness correctly stays `200` throughout. Readiness means "is
//! it currently able to do its job — should traffic/expectations be routed to it right now?" A
//! `CvvInactive` validator is a committee member that is temporarily **not voting or proposing**
//! (still syncing to the current epoch/round) — exactly the condition a readiness probe exists to
//! surface. `503 Service Unavailable` is the standard HTTP status for "temporarily can't serve,
//! don't route here, but don't tear anything down either" — it's what causes a load balancer to
//! pull the instance out of rotation without restarting it, and what causes it to be added back
//! automatically once the probe flips to `200` (the node has caught up and started voting again).
//! Any other status would either be misleading (`200`, hiding real non-participation) or wrong in
//! kind (a `4xx`/`5xx` implying a request/server error rather than a temporary state).
//!
//! Designed for integration with GCP load balancers and similar health monitoring systems.
use std::net::SocketAddr;

use axum::{extract::State, http::StatusCode, routing::get, Router};
use rayls_consensus_primary::NodeMode;
use rayls_infrastructure_types::TaskSpawner;
use tokio::{net::TcpListener, sync::watch};
use tracing::info;

/// Liveness / readiness HTTP responder for service monitoring.
///
/// Binds a TCP port and answers:
/// - liveness (any path): `200 OK` — the process is up.
/// - readiness (`/readyz`, `/ready`): `200` when voting/observing, `503` while catching up.
///
/// # Security Considerations
///
/// This endpoint accepts connections from any source. Node operators must protect it with a
/// firewall. It is off by default and enabled via the CLI node command.
///
/// To enable on node startup, use `rayls-network node --healthcheck <PORT>`.
/// See `rayls-network-cli::node` for more info.
#[derive(Debug)]
pub(crate) struct HealthcheckServer;

async fn liveness() -> &'static str {
    "OK"
}

/// Readiness handler: ready (`200`) when voting (`CvvActive`) or an operational observer; not
/// ready (`503`) while a validator is still catching up (`CvvInactive`). See the module doc for
/// why 503 is the right status for exactly this state.
async fn readiness(
    State(node_mode): State<watch::Receiver<NodeMode>>,
) -> (StatusCode, &'static str) {
    let mode = *node_mode.borrow();
    if mode.is_active_cvv() || mode.is_observer() {
        (StatusCode::OK, "READY")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "NOT_READY")
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

        let app = Router::new()
            .route("/readyz", get(readiness))
            .route("/ready", get(readiness))
            .fallback(get(liveness))
            .with_state(node_mode);

        task_spawner.spawn_task("healthcheck", async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!(target: "healthcheck", ?e, "healthcheck server error");
            }
        });

        Ok(listen_on)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_types::{get_available_tcp_port, TaskManager};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    async fn request(addr: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8_lossy(&response).into_owned()
    }

    #[tokio::test]
    async fn liveness_always_ok_and_readiness_follows_mode() -> eyre::Result<()> {
        let task_manager = TaskManager::default();
        let task_spawner = task_manager.get_spawner();
        let (tx, rx) = watch::channel(NodeMode::CvvInactive);

        let port = get_available_tcp_port("127.0.0.1").expect("tcp port assigned by host");
        let addr = HealthcheckServer::spawn(task_spawner, port, rx).await?;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Liveness: always 200, even while catching up, on any non-readiness path.
        assert!(request(addr, "/").await.starts_with("HTTP/1.1 200 OK"));
        assert!(request(addr, "/healthz").await.starts_with("HTTP/1.1 200 OK"));

        // Readiness: 503 while CvvInactive (catching up)...
        assert!(request(addr, "/readyz").await.starts_with("HTTP/1.1 503"));
        assert!(request(addr, "/ready").await.starts_with("HTTP/1.1 503"));

        // ...and 200 once the node is voting.
        tx.send(NodeMode::CvvActive).unwrap();
        assert!(request(addr, "/readyz").await.starts_with("HTTP/1.1 200 OK"));

        // Observers are always ready (never CvvActive/CvvInactive).
        tx.send(NodeMode::Observer).unwrap();
        assert!(request(addr, "/readyz").await.starts_with("HTTP/1.1 200 OK"));

        Ok(())
    }
}
