//! Peer management for byzantine peers.
//!
//! Peers that score poorly are eventually banned.
use super::peer::Peer;
use libp2p::PeerId;
use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
};

#[cfg(test)]
#[path = "../tests/banned_peers.rs"]
mod banned_peers;

/// The threshold of banned peers before an IP address is blocked.
/// Currently set to 1, so ips are banned if more than one peer is banned.
const BANNED_PEERS_PER_IP_THRESHOLD: usize = 1;

/// The ip addresses charged for each banned peer, and the resulting per-ip counts.
#[derive(Debug, Default)]
pub(super) struct BannedPeers {
    /// The ip addresses charged when each peer was banned.
    ///
    /// A ban must be released against exactly the addresses it charged. A peer's address set
    /// changes while it is banned, through record updates and address pruning, so re-reading it at
    /// release time credits addresses the ban never debited and leaves others charged forever.
    /// Keying on the peer also makes the total a derived value rather than a counter that can
    /// drift from the map it is supposed to describe.
    charged_ips: HashMap<PeerId, Vec<IpAddr>>,
    /// The number of banned peers by IP address.
    banned_peers_by_ip: HashMap<IpAddr, usize>,
}

impl BannedPeers {
    /// Release the ban charge for a peer, returning the ip addresses that are no longer banned.
    ///
    /// Releasing a peer that holds no charge is a no-op, so a caller that unbans indiscriminately
    /// cannot drive the total below the number of banned peers.
    pub(super) fn remove_banned_peer(&mut self, peer_id: &PeerId) -> Vec<IpAddr> {
        let Some(ip_addresses) = self.charged_ips.remove(peer_id) else {
            return Vec::new();
        };

        ip_addresses
            .into_iter()
            .filter(|ip| {
                match self.banned_peers_by_ip.get_mut(ip) {
                    Some(count) => {
                        // reduce count
                        *count = count.saturating_sub(1);
                        let new_count = *count;

                        // Check if IP is no longer banned after decrement
                        let no_longer_banned = new_count <= BANNED_PEERS_PER_IP_THRESHOLD;

                        // Clean up entry if count reaches zero to prevent memory leak
                        if new_count == 0 {
                            self.banned_peers_by_ip.remove(ip);
                        }

                        // return ip if no longer associated with a banned peer
                        no_longer_banned
                    }
                    None => false,
                }
            })
            .collect()
    }

    /// Charge a banned peer's ip addresses.
    ///
    /// Charging a peer that is already charged is a no-op, so a peer cannot hold two charges for
    /// one ban.
    pub(super) fn add_banned_peer(&mut self, peer_id: &PeerId, peer: &Peer) {
        if self.charged_ips.contains_key(peer_id) {
            return;
        }

        let ip_addresses: Vec<_> = peer.known_ip_addresses().collect();
        for address in &ip_addresses {
            tracing::debug!(target: "peer-manager", ?address, "known ip address for banned peer");
            *self.banned_peers_by_ip.entry(*address).or_insert(0) += 1;
        }
        self.charged_ips.insert(*peer_id, ip_addresses);
    }

    /// Return the number of banned peers.
    pub(super) fn total(&self) -> usize {
        self.charged_ips.len()
    }

    /// Return a [HashSet] of banned IP addresses.
    pub(super) fn banned_ips(&self) -> HashSet<IpAddr> {
        self.banned_peers_by_ip
            .iter()
            .filter_map(
                |(ip, count)| {
                    if *count > BANNED_PEERS_PER_IP_THRESHOLD {
                        Some(*ip)
                    } else {
                        None
                    }
                },
            )
            .collect()
    }

    /// Bool indicating an IP address is currently banned.
    ///
    /// IP addresses are banned if the number of banned peers exceeds the
    /// [BANNED_PEERS_PER_IP_THRESHOLD].
    pub(super) fn ip_banned(&self, ip: &IpAddr) -> bool {
        self.banned_peers_by_ip.get(ip).is_some_and(|count| *count > BANNED_PEERS_PER_IP_THRESHOLD)
    }
}
