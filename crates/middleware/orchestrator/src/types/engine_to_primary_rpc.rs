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
        self.consensus_bus.last_consensus_header().borrow().clone()
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
        // CvvActive: caught up by construction (promotion gate).
        // CvvInactive: still catching up by definition; promotion to CvvActive is the readiness
        // signal. Observer: never participates in consensus, so compare DB tip vs gossipped
        // network tip. `network == 0` guard avoids a false "caught up" before any peer
        // gossip has arrived.
        let is_caught_up = match role {
            NodeRole::ActiveCvv => true,
            NodeRole::InactiveCvv => false,
            NodeRole::Observer => {
                let (network, _) = *self.consensus_bus.last_published_consensus_num_hash().borrow();
                let local = self.consensus_bus.last_consensus_header().borrow().number;
                network > 0 && local >= network
            }
        };
        NodeStatus {
            role,
            is_caught_up,
            epoch: self.consensus_bus.last_consensus_header().borrow().sub_dag.leader_epoch(),
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
