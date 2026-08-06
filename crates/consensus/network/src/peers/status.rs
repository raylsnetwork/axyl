//! Status of the peer.
//!
//! The connection status and sync status for the peer.

use super::types::ConnectionDirection;
use libp2p::Multiaddr;
use std::time::Instant;

/// Why this node is disconnecting from a peer.
///
/// The reason decides two independent things: whether this node shares its peer table on the way
/// out, and whether the completed disconnect becomes a ban. A single boolean cannot express the
/// middle case, where a peer has earned a disconnect but not a ban.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum DisconnectReason {
    /// Connection limits were reached and this peer was chosen to make room.
    ///
    /// The peer is healthy, so peer-exchange data is shared to help it find other peers.
    ExcessPeers,
    /// The peer's score crossed the disconnect threshold.
    ///
    /// No peer exchange, and no ban: the score is expected to decay back into range.
    Penalized,
    /// The peer's score crossed the ban threshold.
    ///
    /// No peer exchange, and the disconnect completes into a ban.
    Banned,
}

impl DisconnectReason {
    /// Whether the completed disconnect bans the peer.
    pub(super) fn bans_peer(&self) -> bool {
        matches!(self, Self::Banned)
    }

    /// Whether to share peer-exchange data with the peer on the way out.
    pub(super) fn shares_peers(&self) -> bool {
        matches!(self, Self::ExcessPeers)
    }
}

/// Connection status of the peer.
#[derive(Debug, Copy, Clone, Default)]
pub(super) enum ConnectionStatus {
    /// The peer is connected.
    Connected {
        /// The number of ingoing connections.
        num_in: u8,
        /// The number of outgoing connections.
        num_out: u8,
    },
    /// The peer is in the process of disconnecing.
    Disconnecting {
        /// Why the peer is being disconnected.
        reason: DisconnectReason,
    },
    /// The peer has disconnected.
    Disconnected {
        /// The last known connected instant.
        instant: Instant,
    },
    /// The peer is banned and disconnected.
    Banned {
        /// The moment when the peer was banned.
        instant: Instant,
    },
    /// The peer is being dialed.
    Dialing {
        /// The last known peer connection.
        instant: Instant,
    },
    /// The connection status has not been specified.
    #[default]
    Unknown,
}

impl ConnectionStatus {
    /// Matches the connection status.
    pub(super) fn is_banned(&self) -> bool {
        matches!(self, Self::Banned { .. })
    }

    /// Matches the connection status.
    pub(super) fn is_connected(&self) -> bool {
        matches!(self, Self::Connected { .. })
    }

    /// Matches the connection status.
    pub(super) fn is_dialing(&self) -> bool {
        matches!(self, Self::Dialing { .. })
    }

    /// Matches the connection status if the peer is already connected or dialing.
    pub(super) fn is_connected_or_dialing(&self) -> bool {
        self.is_connected() || self.is_dialing()
    }
}

/// Enum for managing a peer's transition to new connection status.
#[derive(Debug)]
pub(super) enum NewConnectionStatus {
    /// A peer has established a connection.
    Connected {
        /// The multiaddress associated with the connection.
        multiaddr: Multiaddr,
        /// The direction, incoming/outgoing.
        direction: ConnectionDirection,
    },
    /// The peer is being disconnected.
    Disconnecting {
        /// Why the peer is being disconnected, which decides peer exchange and whether the
        /// completed disconnect bans the peer.
        reason: DisconnectReason,
    },
    /// A peer is being dialed.
    Dialing,
    /// The peer was fully disconnected from this node.
    Disconnected,
    /// The peer was banned.
    Banned,
    /// The peer was unbanned.
    Unbanned,
}

impl NewConnectionStatus {
    /// Returns true if this status is valid as an initial state for a previously unknown peer.
    ///
    /// Some connection states are only valid for previously tracked peers (like being banned
    /// or having an unknown status). Other states  occur during peer discovery, such as
    /// establishing a new connection or initiating a dial.
    ///
    /// Edge cases like disconnecting or instant disconnection also occur during the
    /// discovery process, such as when a peer responds with a different ID than expected.
    pub(super) fn valid_initial_state(&self) -> bool {
        matches!(
            self,
            // valid initial states for unknown peers
            NewConnectionStatus::Connected { .. }          // new connection established
                | NewConnectionStatus::Disconnecting { .. }// edge case: disconnecting during discovery
                | NewConnectionStatus::Dialing
                | NewConnectionStatus::Disconnected // edge case: instant disconnect
        )
    }
}
