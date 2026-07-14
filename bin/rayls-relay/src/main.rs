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
//!   fixes the relay's peer id; it must match the peer id baked into the validators' node-info (see
//!   `etc/test-network/RELAY_KEYS.md`).
//! - `RELAY_PORT` (required): UDP (QUIC) and TCP listen port, bound on `0.0.0.0`.
//! - `RELAY_MAX_CIRCUIT_DURATION_SECS` (optional): tighten circuit lifetime. Defaults to
//!   effectively unlimited (libp2p's own default of 120s would force-close consensus links).
//! - `RELAY_MAX_CIRCUIT_BYTES` (optional): tighten per-circuit byte cap. Defaults to unlimited
//!   (libp2p's own default is 128 KiB).
//! - `RELAY_MAX_RESERVATIONS` / `RELAY_MAX_CIRCUITS` (optional): tighten global caps (defaults
//!   raised to 1024).

use futures::StreamExt as _;
use libp2p::{
    core::multiaddr::{Multiaddr, Protocol},
    identify, identity, noise, ping, relay,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, PeerId,
};
use rayls_infrastructure_config::QuicConfig;
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
        Ok(v) => v.parse::<T>().map(Some).map_err(|e| eyre::eyre!("invalid {key}: {e}")),
        Err(_) => Ok(None),
    }
}

/// Build the relay config, applying any env overrides.
///
/// The libp2p defaults describe a *limited* relay (2 min / 128 KiB per circuit) which force-closes
/// the long-lived, high-volume links a consensus network needs. Since this is a dedicated test
/// relay that all traffic hairpins through, we start from effectively-unlimited caps and let env
/// vars tighten them if desired.
fn relay_config() -> eyre::Result<relay::Config> {
    let mut cfg = relay::Config::default();
    // Effectively unlimited: max_circuit_duration may not exceed u32::MAX seconds (~136 years).
    cfg.max_circuit_duration = Duration::from_secs(u32::MAX as u64);
    cfg.max_circuit_bytes = u64::MAX;
    cfg.max_reservations = 1024;
    cfg.max_reservations_per_peer = 64;
    cfg.max_circuits = 1024;
    cfg.max_circuits_per_peer = 64;
    // Drop the default per-peer/per-IP rate limiters. They cap circuits/reservations to
    // 30 per 2 min per source peer and 60 per hour per source IP -- fine for a public relay, but
    // this is a dedicated test relay that all validators hairpin through, often from the SAME IP
    // (127.0.0.1 locally). Reconnect churn (killing/reviving relays, re-reservation retries) then
    // trips the per-IP limiter and the relay denies circuits with ResourceLimitExceeded. Empty
    // vecs = no rate limiting.
    cfg.reservation_rate_limiters = Vec::new();
    cfg.circuit_src_rate_limiters = Vec::new();

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
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();

    let key = keypair_from_seed_env()?;
    let local_peer_id = PeerId::from(key.public());
    let port: u16 =
        env_required("RELAY_PORT")?.parse().map_err(|e| eyre::eyre!("invalid RELAY_PORT: {e}"))?;

    info!(%local_peer_id, port, "starting rayls relay");

    let relay_cfg = relay_config()?;
    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(key)
        .with_tokio()
        .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
        // Apply the axyl node's QuicConfig instead of libp2p defaults. Every circuit to a
        // validator multiplexes as a stream over that validator's single reservation connection,
        // so the relay's per-connection flow-control window is the aggregate in-flight cap for
        // ALL of the validator's consensus traffic; the libp2p defaults (15 MB connection / 10 MB
        // stream) are ~7x smaller than what the nodes on either side of the circuit are
        // provisioned for and would throttle the relay leg first.
        .with_quic_config(|config| QuicConfig::default().apply(config))
        .with_behaviour(|key| Behaviour {
            relay: relay::Behaviour::new(key.public().to_peer_id(), relay_cfg),
            ping: ping::Behaviour::new(ping::Config::new()),
            identify: identify::Behaviour::new(identify::Config::new(
                "/rayls-relay/0.0.1".to_string(),
                key.public(),
            )),
        })?
        // CRITICAL: libp2p 0.56 defaults idle_connection_timeout to Duration::ZERO, which makes
        // the relay drop any connection the instant it has no keep-alive-forcing substream. That
        // closes peer connections during the brief circuit-setup window (outbound dials then time
        // out) and tears down active circuits at any idle moment (the destination sees the circuit
        // stream reset with Stopped(0) ~1ms after it establishes). A relay must never idle-close
        // its reserving/relaying peers, so disable the idle timeout entirely -- exactly what the
        // libp2p relay-server example does. (Connections carrying a live reservation/circuit are
        // kept alive by the relay behaviour anyway; this also protects genuinely-idle ones from
        // being reaped mid-handshake.)
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(u64::MAX)))
        .build();

    // Listen on QUIC (what the axyl client dials) and TCP, both on 0.0.0.0:<port>.
    let quic_addr = Multiaddr::empty()
        .with(Protocol::Ip4(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Udp(port))
        .with(Protocol::QuicV1);
    let tcp_addr =
        Multiaddr::empty().with(Protocol::Ip4(Ipv4Addr::UNSPECIFIED)).with(Protocol::Tcp(port));
    swarm.listen_on(quic_addr)?;
    swarm.listen_on(tcp_addr)?;

    loop {
        tokio::select! {
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    // Confirm each listen address as external so the relay includes it in the
                    // reservations it grants. Without this the reservation carries no addresses and
                    // clients fail with `NoAddressesInReservation` (the relay behaviour sources
                    // reservation addrs from confirmed external addresses, not listen addresses,
                    // and our clients don't run identify to teach the relay an observed address).
                    swarm.add_external_address(address.clone());
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
