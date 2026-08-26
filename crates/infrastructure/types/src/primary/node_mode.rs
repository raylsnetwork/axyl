//! The node's consensus participation mode.

/// Node mode, seeded at boot from `Config::observer` and updated by `identify_node_mode`.
///
/// No `Default`: an unseeded `CvvActive` snapshot would leak voting behavior to
/// any task that subscribes before `identify_node_mode` runs.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NodeMode {
    /// Full CVV actively voting in the current committee.
    CvvActive,
    /// Staked CVV catching up, allowed to sync past the GC window and rejoin.
    CvvInactive,
    /// Follower not in the committee (staked or unstaked).
    Observer,
}

impl NodeMode {
    /// True if this node is an active CVV.
    pub fn is_active_cvv(&self) -> bool {
        matches!(self, NodeMode::CvvActive)
    }

    /// True if this node is a CVV (i.e. staked and able to participate in a committee).
    pub fn is_cvv(&self) -> bool {
        matches!(self, NodeMode::CvvActive | NodeMode::CvvInactive)
    }

    /// True if this node is only an observer and will never participate in a committee.
    pub fn is_observer(&self) -> bool {
        matches!(self, NodeMode::Observer)
    }

    /// True if this node should run a batch builder (active CVVs sequence into consensus).
    ///
    /// An `Observer` is not batch-producing: it cannot seal, so it forwards its pending
    /// transactions to the committee instead (see the transaction forwarder). A catching-up
    /// `CvvInactive` node must not either: with no proposer draining `our_digests`, a sealed batch
    /// wedges the worker batch-builder on `report_own_batch`, and that Drainable task never
    /// observes shutdown, stalling the epoch-transition drain.
    pub fn is_batch_producing(&self) -> bool {
        matches!(self, NodeMode::CvvActive)
    }
}
