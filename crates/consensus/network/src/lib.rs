// SPDX-License-Identifier: BUSL-1.1
//! Peer-to-peer network interface for Rayls Network built using libp2p.

#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

mod codec;
mod consensus;
pub mod error;
pub mod kad;
mod metrics;
mod peers;
pub mod types;

// export types
pub use codec::{RLCodec, RLMessage};
pub use consensus::ConsensusNetwork;
pub use metrics::*;
pub use peers::{PeerExchangeMap, Penalty};

// re-export specific libp2p types
pub use libp2p::{
    gossipsub::{Message as GossipMessage, TopicHash},
    identity::PeerId,
    request_response::ResponseChannel,
    Multiaddr,
};
#[cfg(test)]
#[path = "./tests/common.rs"]
pub(crate) mod common;
