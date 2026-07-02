//! Minimal circuit-relay-v2 server for the Rayls local test network.
//!
//! Adapted from the upstream `rust-libp2p` `relay-server` example (pinned to the same libp2p
//! version this workspace uses). It exists so the local testnet can route validator p2p through a
//! relay: each validator advertises a `<relay>/p2p-circuit/p2p/<node-key>` address (see
//! `keytool generate --relay` and `etc/test-network/local-testnet.sh --relay`) and reserves a slot
//! on the relay this binary runs.
//!
//! Everything is configured via environment variables so the test script can spawn one relay per
//! validator without any per-index logic baked in here:
//!
//! - `RELAY_SEED_HEX` (required): 64 hex chars (32 bytes) used as the ed25519 secret seed. This
//!   fixes the relay's peer id; it must match the peer id baked into the validators' node-info
//!   (see `etc/test-network/RELAY_KEYS.md`).
//! - `RELAY_PORT` (required): UDP (QUIC) and TCP listen port, bound on `0.0.0.0`.
//! - `RELAY_MAX_CIRCUIT_DURATION_SECS` (optional): override circuit lifetime. libp2p default is
//!   120s, which force-closes long-lived consensus links; raise it for stable runs.
//! - `RELAY_MAX_CIRCUIT_BYTES` (optional): override per-circuit byte cap. libp2p default is
//!   131072 (128 KiB); raise it for stable runs.
//! - `RELAY_MAX_RESERVATIONS` / `RELAY_MAX_CIRCUITS` (optional): override global caps.

use futures::StreamExt as _;
use libp2p::{
    core::multiaddr::{Multiaddr, Protocol},
    identify, identity, noise, ping, relay,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, PeerId,
};
use std::{env, net::Ipv4Addr, time::Duration};
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Relay server behaviour: the relay hop protocol plus ping/identify (identify lets clients learn
/// their observed address; both are harmless if a client does not speak them).
#[derive(NetworkBehaviour)]
struct Behaviour {
    relay: relay::Behaviour,
    ping: ping::Behaviour,
    identify: identify::Behaviour,
}

fn env_required(key: &str) -> eyre::Result<String> {
    env::var(key).map_err(|_| eyre::eyre!("missing required env var {key}"))
}

/// Parse an optional env var into `T`, returning `None` when unset.
fn env_parse<T: std::str::FromStr>(key: &str) -> eyre::Result<Option<T>>
where
    T::Err: std::fmt::Display,
{
    match env::var(key) {
        Ok(v) => v
            .parse::<T>()
            .map(Some)
            .map_err(|e| eyre::eyre!("invalid {key}: {e}")),
        Err(_) => Ok(None),
    }
}

/// Build the relay config from libp2p defaults, applying any env overrides.
fn relay_config() -> eyre::Result<relay::Config> {
    let mut cfg = relay::Config::default();
    if let Some(secs) = env_parse::<u64>("RELAY_MAX_CIRCUIT_DURATION_SECS")? {
        cfg.max_circuit_duration = Duration::from_secs(secs);
    }
    if let Some(bytes) = env_parse::<u64>("RELAY_MAX_CIRCUIT_BYTES")? {
        cfg.max_circuit_bytes = bytes;
    }
    if let Some(n) = env_parse::<usize>("RELAY_MAX_RESERVATIONS")? {
        cfg.max_reservations = n;
    }
    if let Some(n) = env_parse::<usize>("RELAY_MAX_CIRCUITS")? {
        cfg.max_circuits = n;
    }
    Ok(cfg)
}

/// Derive the ed25519 keypair from a 32-byte hex seed in `RELAY_SEED_HEX`.
fn keypair_from_seed_env() -> eyre::Result<identity::Keypair> {
    let seed_hex = env_required("RELAY_SEED_HEX")?;
    let bytes = hex::decode(seed_hex.trim())
        .map_err(|e| eyre::eyre!("RELAY_SEED_HEX is not valid hex: {e}"))?;
    let seed: [u8; 32] = bytes
        .try_into()
        .map_err(|_| eyre::eyre!("RELAY_SEED_HEX must decode to exactly 32 bytes"))?;
    // `ed25519_from_bytes` treats the 32 bytes as the secret seed (matches RELAY_KEYS.md).
    Ok(identity::Keypair::ed25519_from_bytes(seed)?)
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .try_init();

    let key = keypair_from_seed_env()?;
    let local_peer_id = PeerId::from(key.public());
    let port: u16 = env_required("RELAY_PORT")?
        .parse()
        .map_err(|e| eyre::eyre!("invalid RELAY_PORT: {e}"))?;

    info!(%local_peer_id, port, "starting rayls relay");

    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(key)
        .with_tokio()
        .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
        .with_quic()
        .with_behaviour(|key| Behaviour {
            relay: relay::Behaviour::new(key.public().to_peer_id(), relay_config().expect("relay config")),
            ping: ping::Behaviour::new(ping::Config::new()),
            identify: identify::Behaviour::new(identify::Config::new(
                "/rayls-relay/0.0.1".to_string(),
                key.public(),
            )),
        })?
        .build();

    // Listen on QUIC (what the axyl client dials) and TCP, both on 0.0.0.0:<port>.
    let quic_addr = Multiaddr::empty()
        .with(Protocol::Ip4(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Udp(port))
        .with(Protocol::QuicV1);
    let tcp_addr = Multiaddr::empty()
        .with(Protocol::Ip4(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Tcp(port));
    swarm.listen_on(quic_addr)?;
    swarm.listen_on(tcp_addr)?;

    loop {
        tokio::select! {
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    info!(%address, %local_peer_id, "relay listening (dial as <addr>/p2p/{local_peer_id})");
                }
                SwarmEvent::Behaviour(BehaviourEvent::Relay(e)) => {
                    info!(?e, "relay event");
                }
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    info!(%peer_id, "connection established");
                }
                _ => {}
            },
            _ = tokio::signal::ctrl_c() => {
                info!("shutting down relay");
                break;
            }
        }
    }

    Ok(())
}
