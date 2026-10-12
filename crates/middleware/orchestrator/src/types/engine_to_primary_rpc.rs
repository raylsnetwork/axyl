use rayls_consensus_primary::{ConsensusBus, NodeMode};
use rayls_execution_rpc::{EngineToPrimary, NodeRole, NodeStatus};
use rayls_infrastructure_storage::{ConsensusStore, EpochStore};
use rayls_infrastructure_types::{
    BlockHash, ConsensusHeader, Database, Epoch, EpochCertificate, EpochRecord,
};

#[derive(Debug)]
pub struct EngineToPrimaryRpc<DB> {
    /// Container for consensus channels.
    consensus_bus: ConsensusBus,
    /// Consensus DB
    db: DB,
}

impl<DB: Database> EngineToPrimaryRpc<DB> {
    pub fn new(consensus_bus: ConsensusBus, db: DB) -> Self {
        Self { consensus_bus, db }
    }

    /// Retrieve the consensus header by number.
    fn get_epoch_by_number(&self, epoch: Epoch) -> Option<(EpochRecord, EpochCertificate)> {
        if let Some((r, Some(c))) = self.db.get_epoch_by_number(epoch) {
            Some((r, c))
        } else {
            None
        }
    }

    /// Retrieve the consensus header by hash
    fn get_epoch_by_hash(&self, hash: BlockHash) -> Option<(EpochRecord, EpochCertificate)> {
        if let Some((r, Some(c))) = self.db.get_epoch_by_hash(hash) {
            Some((r, c))
        } else {
            None
        }
    }
}

impl<DB: Database> EngineToPrimary for EngineToPrimaryRpc<DB> {
    fn get_latest_consensus_block(&self) -> ConsensusHeader {
        // the node's own durable tip, kept by the subscriber on every role; `last_consensus_header`
        // is a peer-derived signal that a validator never advances and every epoch resets
        (**self.consensus_bus.local_consensus_tip().borrow()).clone()
    }

    fn consensus_block_by_number(&self, number: u64) -> Option<ConsensusHeader> {
        self.db.get_consensus_by_number(number)
    }

    fn consensus_block_by_hash(&self, hash: BlockHash) -> Option<ConsensusHeader> {
        self.db.get_consensus_by_hash(hash)
    }

    fn epoch(
        &self,
        epoch: Option<Epoch>,
        hash: Option<BlockHash>,
    ) -> Option<(EpochRecord, EpochCertificate)> {
        match (epoch, hash) {
            (_, Some(hash)) => self.get_epoch_by_hash(hash),
            (Some(epoch), _) => self.get_epoch_by_number(epoch),
            (None, None) => None,
        }
    }

    fn node_status(&self) -> NodeStatus {
        let role = match *self.consensus_bus.node_mode().borrow() {
            NodeMode::CvvActive => NodeRole::ActiveCvv,
            NodeMode::CvvInactive => NodeRole::InactiveCvv,
            NodeMode::Observer => NodeRole::Observer,
        };
        // one Arc clone; every field below reads the same snapshot
        let tip = self.consensus_bus.local_consensus_tip().borrow().clone();
        // CvvActive: caught up by construction (promotion gate).
        // CvvInactive: still catching up by definition; promotion to CvvActive is the readiness
        // signal. Observer: never participates in consensus, so compare the saved tip vs the
        // gossipped network tip. `network == 0` guard avoids a false "caught up" before any peer
        // gossip has arrived.
        let is_caught_up = match role {
            NodeRole::ActiveCvv => true,
            NodeRole::InactiveCvv => false,
            NodeRole::Observer => {
                let (network, _) = *self.consensus_bus.last_published_consensus_num_hash().borrow();
                network > 0 && tip.number >= network
            }
        };
        NodeStatus {
            role,
            is_caught_up,
            epoch: tip.sub_dag.leader_epoch(),
            committed_round: *self.consensus_bus.committed_round_updates().borrow(),
            primary_round: *self.consensus_bus.primary_round_updates().borrow(),
            gc_round: *self.consensus_bus.gc_round_updates().borrow(),
            last_canonical_block: self
                .consensus_bus
                .recently_executed_blocks()
                .borrow()
                .latest_block_num_hash()
                .number,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_storage::mem_db::MemDatabase;
    use rayls_infrastructure_types::{Certificate, CommittedSubDag, Header, ReputationScores};
    use std::sync::Arc;

    fn header(number: u64, epoch: Epoch) -> ConsensusHeader {
        let mut leader = Certificate::default();
        leader.header = Header { epoch, ..Default::default() };
        let sub_dag = CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None);
        ConsensusHeader { number, sub_dag, ..Default::default() }
    }

    /// Tip and epoch come from the node's own tip watch, not from the peer-derived header watch
    /// (which a validator never advances) and not from the DB.
    // the bus registers metrics histograms that need a runtime handle
    #[tokio::test]
    async fn latest_header_and_epoch_come_from_the_local_tip_watch() {
        let db = MemDatabase::default();
        let rpc = EngineToPrimaryRpc::new(ConsensusBus::new(), db.clone());
        assert_eq!(rpc.get_latest_consensus_block().number, 0);
        assert_eq!(rpc.node_status().epoch, 0);

        // rows in the DB alone change nothing: the RPC never reads them
        db.write_subdag_for_test(3, header(3, 2).sub_dag);
        assert_eq!(rpc.get_latest_consensus_block().number, 0);

        rpc.consensus_bus.local_consensus_tip().send_replace(Arc::new(header(3, 2)));
        // peer-derived watch still at the default header
        assert_eq!(rpc.consensus_bus.last_consensus_header().borrow().number, 0);
        assert_eq!(rpc.get_latest_consensus_block().number, 3);
        assert_eq!(rpc.node_status().epoch, 2);

        // the transition resets the peer-derived watch, not this one
        rpc.consensus_bus.last_consensus_header().send_replace(ConsensusHeader::default());
        assert_eq!(rpc.get_latest_consensus_block().number, 3);
        assert_eq!(rpc.node_status().epoch, 2);

        // first commit of the next epoch moves both
        rpc.consensus_bus.local_consensus_tip().send_replace(Arc::new(header(4, 3)));
        assert_eq!(rpc.get_latest_consensus_block().number, 4);
        assert_eq!(rpc.node_status().epoch, 3);
    }

    /// An observer is caught up once its saved tip reaches the gossiped network tip.
    #[tokio::test]
    async fn observer_is_caught_up_compares_the_local_tip() {
        let rpc = EngineToPrimaryRpc::new(ConsensusBus::new(), MemDatabase::default());
        rpc.consensus_bus.node_mode().send_replace(NodeMode::Observer);

        // no gossip yet
        rpc.consensus_bus.local_consensus_tip().send_replace(Arc::new(header(7, 0)));
        assert!(!rpc.node_status().is_caught_up, "network tip 0 means no peer gossip yet");

        // gossip ahead of the tip
        rpc.consensus_bus.last_published_consensus_num_hash().send_replace((9, BlockHash::ZERO));
        assert!(!rpc.node_status().is_caught_up);

        // tip reaches it
        rpc.consensus_bus.local_consensus_tip().send_replace(Arc::new(header(9, 0)));
        assert!(rpc.node_status().is_caught_up);
    }
}
