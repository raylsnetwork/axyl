//! Unit tests for network types.rs

use super::{ConnectionPath, Endpoint, NodeRecord, Transport};
use crate::common::create_multiaddr;
use libp2p::{core::ConnectedPoint, Multiaddr, PeerId};
use rayls_infrastructure_config::KeyConfig;
use rayls_infrastructure_types::{BlsKeypair, BlsSigner};

#[test]
fn test_node_record() {
    let multiaddr = create_multiaddr(None);
    let bls_keypair = BlsKeypair::generate(&mut rand::rng());
    let pubkey = *bls_keypair.public();
    let key_config = KeyConfig::new_with_testing_key(bls_keypair);

    // build a valid node record
    let node_record =
        NodeRecord::build(key_config.primary_network_public_key(), multiaddr, |data| {
            key_config.request_signature_direct(data)
        });
    assert!(node_record.clone().verify(&pubkey).is_ok());

    // assert returned values match
    assert!(node_record.verify(&pubkey).is_ok());

    // assert incorrect pubkey fails
    let bad_keypair = BlsKeypair::generate(&mut rand::rng());
    assert!(node_record.verify(bad_keypair.public()).is_err());
}

/// Classification covers all four endpoint shapes: an outbound circuit and an inbound circuit are
/// `Circuit` (the inbound marker sits on the local address, its send-back address is a bare
/// `/p2p`), a direct leg to a known relay is `RelayDirect`, and a direct connection to anything
/// else is `DirectNonRelay`.
#[test]
fn test_connection_path_classification() {
    let relay = PeerId::random();
    let dst = PeerId::random();
    let src = PeerId::random();
    let relay_leg: Multiaddr =
        format!("/ip4/127.0.0.1/udp/4001/quic-v1/p2p/{relay}").parse().unwrap();
    let outbound_circuit: Multiaddr =
        format!("/ip4/127.0.0.1/udp/4001/quic-v1/p2p/{relay}/p2p-circuit/p2p/{dst}")
            .parse()
            .unwrap();

    // outbound circuit: the dial address carries the marker and names the relay
    let dialed = ConnectedPoint::Dialer {
        address: outbound_circuit,
        role_override: libp2p::core::Endpoint::Dialer,
        port_use: libp2p::core::transport::PortUse::Reuse,
    };
    assert_eq!(
        ConnectionPath::classify(&dialed, false),
        ConnectionPath::Circuit {
            relay: Some(relay),
            relay_endpoint: Some(Endpoint {
                addr: "127.0.0.1:4001".parse().unwrap(),
                transport: Transport::Quic,
            }),
        }
    );

    // inbound circuit: the marker is on the local (listen) address; send-back is a bare /p2p
    let inbound = ConnectedPoint::Listener {
        local_addr: format!("/ip4/127.0.0.1/udp/4001/quic-v1/p2p/{relay}/p2p-circuit")
            .parse()
            .unwrap(),
        send_back_addr: format!("/p2p/{src}").parse().unwrap(),
    };
    assert_eq!(
        ConnectionPath::classify(&inbound, false),
        ConnectionPath::Circuit {
            relay: Some(relay),
            relay_endpoint: Some(Endpoint {
                addr: "127.0.0.1:4001".parse().unwrap(),
                transport: Transport::Quic,
            }),
        }
    );

    // direct leg to a registered relay vs a direct connection to a non-relay peer
    let direct = ConnectedPoint::Dialer {
        address: relay_leg,
        role_override: libp2p::core::Endpoint::Dialer,
        port_use: libp2p::core::transport::PortUse::Reuse,
    };
    assert_eq!(
        ConnectionPath::classify(&direct, true),
        ConnectionPath::RelayDirect {
            endpoint: Some(Endpoint {
                addr: "127.0.0.1:4001".parse().unwrap(),
                transport: Transport::Quic
            }),
        }
    );
    assert_eq!(
        ConnectionPath::classify(&direct, false),
        ConnectionPath::DirectNonRelay {
            endpoint: Some(Endpoint {
                addr: "127.0.0.1:4001".parse().unwrap(),
                transport: Transport::Quic
            }),
        }
    );
}
