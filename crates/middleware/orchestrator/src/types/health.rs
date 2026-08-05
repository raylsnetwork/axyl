//! Simple TCP healthcheck endpoint for monitoring service availability.
//!
//! Implements a minimal HTTP/1.1 server that responds with status 200 to all requests.
//! This is designed for integration with GCP load balancers and similar health monitoring systems.
use std::{io::ErrorKind, net::SocketAddr, time::Duration};

use rayls_infrastructure_types::TaskSpawner;
use tokio::{io::AsyncWriteExt, net::TcpListener, time::sleep};
use tracing::info;

/// Minimal HTTP health check responder for service monitoring.
///
/// Binds to a TCP port and responds with HTTP 200 to any connection.
/// Uses raw TCP sockets for minimal overhead and dependencies.
///
/// # Security Considerations
///
/// This endpoint accepts connections from any source and responds unconditionally.
///
/// Node operators must ensure the endpoint is protected by a firewall.
/// This service is off by default, but can be enabled through the CLI node command.
/// Each connection is handled synchronously in the main accept loop.
/// No connection limits or rate limiting are implemented.
/// Connections are immediately closed after sending response.
///
/// To enable on node startup, use `rayls-network node --healthcheck <PORT>`.
/// See `rayls-network-cli::node` for more info.
#[derive(Debug)]
pub(crate) struct HealthcheckServer;

impl HealthcheckServer {
    /// Spawns the health check server task and returns the bound address.
    ///
    /// Binds to port specified by `HEALTHCHECK_PORT` environment variable,
    /// or lets the OS assign a port if unset or set to 0.
    ///
    /// # Network Binding
    ///
    /// Binds to 0.0.0.0 (all interfaces) to allow external health checkers.
    /// This makes the service accessible on all network interfaces including public IPs.
    ///
    /// # Protocol
    ///
    /// Implements minimal HTTP/1.1 with a fixed response:
    /// - Status: 200 OK
    /// - Body: "OK" (2 bytes)
    /// - No request parsing or validation
    /// - No custom headers to avoid information disclosure
    pub(crate) async fn spawn(task_spawner: TaskSpawner, port: u16) -> eyre::Result<SocketAddr> {
        // IMPORTANT: use firewall to protect this endpoint
        let addr: SocketAddr = ([0, 0, 0, 0], port).into();
        let listener = TcpListener::bind(addr).await?;
        let listen_on = listener.local_addr()?;
        info!(target: "epoch-manager", ?listen_on, "healthcheck listening");

        task_spawner.spawn_task("healthcheck", async move {
            // minimal valid HTTP
            let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";

            let mut backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(5);

            loop {
                match listener.accept().await {
                    Ok((mut socket, _)) => {
                        // write response, ignore errors (client disconnect, etc.)
                        // then drop connection
                        if let Err(e) = socket.write_all(response).await {
                            tracing::error!(target: "healthcheck", ?e, "error writing healthcheck response");
                        }
                    }
                    Err(ref e) if matches!(
                        e.kind(),
                        ErrorKind::WouldBlock
                        | ErrorKind::Interrupted
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::ConnectionReset
                        | ErrorKind::Other
                    )=> {
                        // transient errors that can be ignored
                        tracing::warn!(target: "healthcheck", ?e, "transient error accepting healthcheck connection");
                        sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);

                    }
                    Err(e) => {
                        // unexpected errors should be logged and break the loop to avoid spinning on fatal errors
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
    use rayls_infrastructure_types::{get_available_tcp_port, TaskManager};
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    use crate::types::HealthcheckServer;

    #[tokio::test]
    async fn test_tcp_healthcheck() -> eyre::Result<()> {
        let task_manager = TaskManager::default();
        let task_spawner = task_manager.get_spawner();

        let port = get_available_tcp_port("127.0.0.1").expect("tcp port assigned by host");
        // spawn server and get the bound address
        let addr = HealthcheckServer::spawn(task_spawner.clone(), port).await?;

        // give server time to start listening
        tokio::time::sleep(Duration::from_millis(10)).await;

        tokio::time::timeout(Duration::from_millis(500), async move {
            // request healthcheck
            let mut stream = TcpStream::connect(addr).await?;

            // send minimal HTTP request
            stream.write_all(b"GET / HTTP/1.1\r\n\r\n").await?;

            // read response
            let mut response = vec![0u8; 1024];
            let n = stream.read(&mut response).await?;
            response.truncate(n);
            let response_str = String::from_utf8_lossy(&response);

            // verify http status line
            assert!(
                response_str.starts_with("HTTP/1.1 200 OK"),
                "Expected 200 OK, got: {}",
                response_str
            );

            // verify body
            assert!(response_str.ends_with("OK"), "Expected body 'OK', got: {}", response_str);

            // verify content-length header
            assert!(
                response_str.contains("Content-Length: 2"),
                "Missing or incorrect Content-Length header"
            );

            Ok::<(), eyre::Error>(())
        })
        .await
        .expect("response received")?;

        Ok(())
    }
}
