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
//! - `RELAY_PORT` (required): UDP port for the QUIC listener, bound on `0.0.0.0`.
//! - `RELAY_MAX_CIRCUIT_DURATION_SECS` (optional): tighten circuit lifetime. Defaults to
//!   effectively unlimited (libp2p's own default of 120s would force-close consensus links).
//! - `RELAY_MAX_CIRCUIT_BYTES` (optional): tighten per-circuit byte cap. Defaults to unlimited
//!   (libp2p's own default is 128 KiB).
//! - `RELAY_MAX_RESERVATIONS` / `RELAY_MAX_CIRCUITS` (optional): tighten global caps (defaults
//!   raised to 1024).
//! - `RELAY_ALLOWED_RESERVERS` (optional): `;`-separated list of peer ids allowed to reserve on
//!   this relay (the validator(s) it fronts). When set, only those peers may hold a reservation;
//!   every other peer's reservation is denied so the relay can't be squatted as free inbound
//!   infrastructure. Circuit *sources* stay unrestricted -- other validators must still dial
//!   through to reach a fronted one. Unset = any peer may reserve (the default the local testnet
//!   relies on).

use futures::StreamExt as _;
use libp2p::{
    core::multiaddr::{Multiaddr, Protocol},
    identify, identity, ping, relay,
    swarm::{NetworkBehaviour, SwarmEvent},
    PeerId,
};
use rayls_infrastructure_config::QuicConfig;
use std::{
    collections::{HashMap, HashSet},
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};
use tracing::info;

/// Render a peer's direct-connection endpoint (`ip:port/proto`, e.g. `127.0.0.1:54321/quic-v1`) for
/// log annotation. The relay's events are peer-id-only and the swarm exposes no per-peer address
/// lookup, so this is extracted from `ConnectionEstablished` and cached. A relayed peer's *own*
/// address is never visible here -- only the endpoint of whoever holds the direct leg to the relay.
/// Reads the multiaddr's typed protocols (not string parsing); `None` if it names no ip+port
/// transport.
fn endpoint_str(addr: &Multiaddr) -> Option<String> {
    let mut ip = None;
    let mut port = None;
    let mut proto = "?";
    for p in addr.iter() {
        match p {
            Protocol::Ip4(a) => ip = Some(IpAddr::V4(a)),
            Protocol::Ip6(a) => ip = Some(IpAddr::V6(a)),
            Protocol::Udp(x) => {
                port = Some(x);
                proto = "udp";
            }
            Protocol::Tcp(x) => {
                port = Some(x);
                proto = "tcp";
            }
            Protocol::QuicV1 | Protocol::Quic => proto = "quic-v1",
            Protocol::Ws(_) | Protocol::Wss(_) => proto = "ws",
            Protocol::P2p(_) | Protocol::P2pCircuit => break,
            _ => {}
        }
    }
    Some(format!("{}/{}", SocketAddr::new(ip?, port?), proto))
}
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

/// Parse a `;`-separated peer id list into an allow-list set.
///
/// Errors if any entry is not a valid peer id, or if the list yields no ids at all (that would deny
/// every reservation and make the relay useless -- almost certainly a misconfiguration, so fail
/// loudly rather than silently lock everyone out).
fn parse_allowed_reservers(raw: &str) -> eyre::Result<HashSet<PeerId>> {
    let set = raw
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<PeerId>()
                .map_err(|e| eyre::eyre!("invalid peer id {s:?} in RELAY_ALLOWED_RESERVERS: {e}"))
        })
        .collect::<eyre::Result<HashSet<PeerId>>>()?;
    if set.is_empty() {
        eyre::bail!("RELAY_ALLOWED_RESERVERS is set but lists no peer ids");
    }
    Ok(set)
}

/// Read `RELAY_ALLOWED_RESERVERS` into an allow-list set; `None` when the var is unset (allow-list
/// disabled -- any peer may reserve).
fn env_allowed_reservers() -> eyre::Result<Option<HashSet<PeerId>>> {
    match env::var("RELAY_ALLOWED_RESERVERS") {
        Ok(raw) => parse_allowed_reservers(&raw).map(Some),
        Err(_) => Ok(None),
    }
}

/// Build the reservation gate: a [`relay::RateLimiter`] that grants a reservation only to peers in
/// `allowed`. `reservation_rate_limiters` is libp2p's only per-peer hook on the RESERVE path, so we
/// use it as a plain membership check with no rate/time component (`now` is ignored).
fn reservation_allow_list(allowed: HashSet<PeerId>) -> Box<dyn relay::RateLimiter> {
    Box::new(move |peer: PeerId, _addr: &Multiaddr, _now| allowed.contains(&peer))
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

    // Reservation allow-list. Each relay fronts a known, fixed set of validators, so only those
    // peers should ever hold a reservation on it -- anyone else reserving is squatting a slot /
    // using us as free inbound relay infrastructure. `reservation_rate_limiters` is libp2p's only
    // per-peer hook on the RESERVE path, so we use it as a plain boolean gate (no rate component):
    // a reservation is granted iff every limiter returns true, so one membership check denies all
    // peers outside the set. `circuit_src_rate_limiters` is intentionally left open -- other
    // validators must be able to open circuits *toward* a fronted one, and with no reservation of
    // their own they can't use us for anything else. Unset = any peer may reserve (what the local
    // testnet relies on).
    if let Some(allowed) = env_allowed_reservers()? {
        info!(count = allowed.len(), "reservation allow-list active: only these peers may reserve");
        cfg.reservation_rate_limiters = vec![reservation_allow_list(allowed)];
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
        // No ANSI colors: the relay's output is redirected to a log file, where escape sequences
        // would show up as `^[[2m…` noise. Plain text; colorize downstream if desired.
        .with_ansi(false)
        .try_init();

    let key = keypair_from_seed_env()?;
    let local_peer_id = PeerId::from(key.public());
    let port: u16 =
        env_required("RELAY_PORT")?.parse().map_err(|e| eyre::eyre!("invalid RELAY_PORT: {e}"))?;

    info!(%local_peer_id, port, "starting rayls relay");

    let relay_cfg = relay_config()?;
    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(key)
        .with_tokio()
        // QUIC only: validators dial the relay over /udp/<port>/quic-v1, so there is no TCP
        // transport (and thus no TCP listener) -- the relay neither offers nor accepts TCP.
        //
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

    // Listen on QUIC (what the axyl client dials) on 0.0.0.0:<port>.
    let quic_addr = Multiaddr::empty()
        .with(Protocol::Ip4(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Udp(port))
        .with(Protocol::QuicV1);
    swarm.listen_on(quic_addr)?;

    // peer id -> the IP of that peer's direct connection to this relay, so the peer-id-only relay
    // events can be logged with an address for easy correlation. The events themselves carry no
    // address (and the swarm exposes no per-peer address lookup), so we cache it from
    // `ConnectionEstablished` and evict when the peer's last connection closes.
    let mut peer_eps: HashMap<PeerId, String> = HashMap::new();
    let ep_of = |peer: &PeerId, m: &HashMap<PeerId, String>| {
        m.get(peer).cloned().unwrap_or_else(|| "?".to_string())
    };

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
                    // Annotate the high-traffic circuit/reservation events with the src/dst
                    // endpoints the relay sees (each peer's direct leg to this relay, ip:port/proto);
                    // fall back to the raw event.
                    match &e {
                        relay::Event::ReservationReqAccepted { src_peer_id, renewed } => {
                            info!(src = %src_peer_id, src_addr = %ep_of(src_peer_id, &peer_eps), renewed = *renewed, "reservation accepted");
                        }
                        relay::Event::CircuitReqAccepted { src_peer_id, dst_peer_id } => {
                            info!(src = %src_peer_id, src_addr = %ep_of(src_peer_id, &peer_eps), dst = %dst_peer_id, dst_addr = %ep_of(dst_peer_id, &peer_eps), "circuit accepted");
                        }
                        relay::Event::CircuitReqDenied { src_peer_id, dst_peer_id, status } => {
                            info!(src = %src_peer_id, src_addr = %ep_of(src_peer_id, &peer_eps), dst = %dst_peer_id, dst_addr = %ep_of(dst_peer_id, &peer_eps), ?status, "circuit denied");
                        }
                        relay::Event::CircuitClosed { src_peer_id, dst_peer_id, error } => {
                            info!(src = %src_peer_id, src_addr = %ep_of(src_peer_id, &peer_eps), dst = %dst_peer_id, dst_addr = %ep_of(dst_peer_id, &peer_eps), ?error, "circuit closed");
                        }
                        _ => info!(?e, "relay event"),
                    }
                }
                SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                    let addr = endpoint.get_remote_address();
                    if let Some(ep) = endpoint_str(addr) {
                        peer_eps.insert(peer_id, ep);
                    }
                    info!(%peer_id, %addr, "connection established");
                }
                SwarmEvent::ConnectionClosed { peer_id, num_established, .. } => {
                    // Evict only when the peer has no remaining connections to this relay.
                    if num_established == 0 {
                        peer_eps.remove(&peer_id);
                    }
                    info!(%peer_id, "connection closed");
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

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::{noise, yamux, Swarm, SwarmBuilder};
    use web_time::Instant;

    #[test]
    fn parses_semicolon_list_trimming_whitespace_and_blanks() {
        let a = PeerId::random();
        let b = PeerId::random();
        // leading/trailing spaces and a trailing empty segment must all be tolerated
        let raw = format!("  {a} ; {b} ;  ");
        assert_eq!(parse_allowed_reservers(&raw).unwrap(), HashSet::from([a, b]));
    }

    #[test]
    fn rejects_malformed_peer_id() {
        assert!(parse_allowed_reservers("not-a-peer-id").is_err());
        let good = PeerId::random();
        // one bad entry poisons the whole list rather than being silently dropped
        assert!(parse_allowed_reservers(&format!("{good};nope")).is_err());
    }

    #[test]
    fn rejects_list_with_no_ids() {
        // set-but-empty would deny every reservation, so it must fail loudly, not parse to {}
        assert!(parse_allowed_reservers("").is_err());
        assert!(parse_allowed_reservers("  ;  ; ").is_err());
    }

    /// The built gate must grant reservations to listed peers and deny everyone else -- this
    /// exercises the actual `RateLimiter` we install, proving enforcement, not just parsing.
    #[test]
    fn gate_allows_only_listed_peers() {
        let allowed = PeerId::random();
        let stranger = PeerId::random();
        let mut gate = reservation_allow_list(HashSet::from([allowed]));
        let addr: Multiaddr = "/ip4/127.0.0.1/udp/4001/quic-v1".parse().unwrap();

        assert!(gate.try_next(allowed, &addr, Instant::now()), "listed peer may reserve");
        assert!(!gate.try_next(stranger, &addr, Instant::now()), "unlisted peer is denied");
    }

    // ---- end-to-end: prove libp2p's relay behaviour actually enforces the allow-list ----
    //
    // The tests above prove our gate returns the right boolean; these prove libp2p consults it on
    // the RESERVE path and denies. Two in-process swarms talk over QUIC (the production transport):
    // a relay running our real `Behaviour`, and a relay *client* that requests a reservation.

    /// A minimal relay-client node: enough to request a reservation over a circuit listen address.
    #[derive(NetworkBehaviour)]
    struct RelayClient {
        relay: relay::client::Behaviour,
    }

    #[derive(Debug, PartialEq)]
    enum Outcome {
        Accepted,
        Denied,
        Timeout,
    }

    /// The relay under test. `allowed` mirrors the binary's `RELAY_ALLOWED_RESERVERS` branch
    /// exactly: `Some(set)` installs the allow-list gate; `None` leaves the limiters empty (var
    /// unset = open, any peer may reserve). QUIC + 30s idle timeout mirror the binary.
    fn build_relay_swarm(allowed: Option<HashSet<PeerId>>) -> Swarm<Behaviour> {
        let mut cfg = relay::Config::default();
        cfg.reservation_rate_limiters = match allowed {
            Some(set) => vec![reservation_allow_list(set)],
            None => Vec::new(),
        };
        cfg.circuit_src_rate_limiters = Vec::new();
        SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_quic_config(|c| QuicConfig::default().apply(c))
            .with_behaviour(|key| Behaviour {
                relay: relay::Behaviour::new(key.public().to_peer_id(), cfg),
                ping: ping::Behaviour::new(ping::Config::new()),
                identify: identify::Behaviour::new(identify::Config::new(
                    "/rayls-relay-test/1".to_string(),
                    key.public(),
                )),
            })
            .expect("relay behaviour")
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))
            .build()
    }

    /// A relay client over QUIC. `with_relay_client` upgrades the relayed leg with noise+yamux --
    /// the reason those are test-only deps (the relay server forwards bytes and needs neither).
    fn build_client_swarm(key: identity::Keypair) -> Swarm<RelayClient> {
        SwarmBuilder::with_existing_identity(key)
            .with_tokio()
            .with_quic_config(|c| QuicConfig::default().apply(c))
            .with_relay_client(noise::Config::new, yamux::Config::default)
            .expect("relay client transport")
            .with_behaviour(|_key, relay| RelayClient { relay })
            .expect("relay client behaviour")
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))
            .build()
    }

    /// Bring up the relay, then have `client_key`'s node reserve through it; report what the relay
    /// decided for that peer (or `Timeout` if neither event arrived).
    async fn run_reservation(
        allowed: Option<HashSet<PeerId>>,
        client_key: identity::Keypair,
    ) -> Outcome {
        let client_peer = client_key.public().to_peer_id();

        let mut relay = build_relay_swarm(allowed);
        relay.listen_on("/ip4/127.0.0.1/udp/0/quic-v1".parse().unwrap()).expect("relay listen");

        // learn the relay's ephemeral listen address, then confirm it external so a granted
        // reservation carries an address (mirrors the binary's NewListenAddr handling).
        let relay_addr = loop {
            if let SwarmEvent::NewListenAddr { address, .. } = relay.select_next_some().await {
                break address;
            }
        };
        let relay_peer = *relay.local_peer_id();
        relay.add_external_address(relay_addr.clone());

        let circuit = relay_addr.with(Protocol::P2p(relay_peer)).with(Protocol::P2pCircuit);

        let mut client = build_client_swarm(client_key);
        // listening on a /p2p-circuit address is what triggers the reservation request.
        client.listen_on(circuit).expect("client circuit listen");

        let drive = async {
            loop {
                tokio::select! {
                    ev = relay.select_next_some() => {
                        if let SwarmEvent::Behaviour(BehaviourEvent::Relay(e)) = ev {
                            match e {
                                relay::Event::ReservationReqAccepted { src_peer_id, .. }
                                    if src_peer_id == client_peer => return Outcome::Accepted,
                                relay::Event::ReservationReqDenied { src_peer_id, .. }
                                    if src_peer_id == client_peer => return Outcome::Denied,
                                _ => {}
                            }
                        }
                    }
                    // drive the client so its dial + RESERVE actually make progress.
                    _ = client.select_next_some() => {}
                }
            }
        };

        tokio::time::timeout(Duration::from_secs(15), drive).await.unwrap_or(Outcome::Timeout)
    }

    /// A peer absent from the allow-list must be refused a reservation by the relay behaviour.
    #[tokio::test]
    async fn non_listed_peer_reservation_is_denied() {
        // relay fronts some other validator; the connecting client is not on the list.
        let fronted = PeerId::random();
        let stranger = identity::Keypair::generate_ed25519();
        assert_eq!(
            run_reservation(Some(HashSet::from([fronted])), stranger).await,
            Outcome::Denied,
            "a peer outside RELAY_ALLOWED_RESERVERS must not obtain a reservation",
        );
    }

    /// The fronted validator (on the allow-list) must still get its reservation.
    #[tokio::test]
    async fn listed_peer_reservation_is_accepted() {
        let key = identity::Keypair::generate_ed25519();
        let peer = key.public().to_peer_id();
        assert_eq!(
            run_reservation(Some(HashSet::from([peer])), key).await,
            Outcome::Accepted,
            "the fronted validator must still obtain its reservation",
        );
    }

    /// With no allow-list configured (`RELAY_ALLOWED_RESERVERS` unset), the relay must stay open --
    /// any peer may reserve. Pins the default-open behaviour so it can't silently flip to closed.
    #[tokio::test]
    async fn open_relay_accepts_any_peer() {
        let key = identity::Keypair::generate_ed25519();
        assert_eq!(
            run_reservation(None, key).await,
            Outcome::Accepted,
            "with no allow-list, any peer must be able to reserve",
        );
    }
}
