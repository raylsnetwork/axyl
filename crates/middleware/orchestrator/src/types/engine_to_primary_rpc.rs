use rayls_consensus_primary::{ConsensusBus, NodeMode};
use rayls_execution_rpc::{EngineToPrimary, NodeRole, NodeStatus};
use rayls_infrastructure_storage::{tables::EpochRecords, ConsensusStore, EpochStore};
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
        // from the DB; the watch resets every epoch and a validator never advances it
        self.db.get_latest_consensus_header().unwrap_or_default()
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
        // read the tip once for both fields
        let tip = self.db.get_latest_consensus_header();
        let is_caught_up = match role {
            NodeRole::ActiveCvv => true,
            NodeRole::InactiveCvv => false,
            NodeRole::Observer => {
                let (network, _) = *self.consensus_bus.last_published_consensus_num_hash().borrow();
                // stored tip, not the last header seen
                let local = tip.as_ref().map(|h| h.number).unwrap_or(0);
                network > 0 && local >= network
            }
        };
        NodeStatus {
            role,
            is_caught_up,
            epoch: current_epoch(tip.as_ref(), last_closed_epoch(&self.db)),
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

/// Highest epoch with a certified record. The epoch-0 placeholder is unsigned and does not count.
fn last_closed_epoch<DB: Database>(db: &DB) -> Option<Epoch> {
    let (epoch, _) = db.last_record::<EpochRecords>()?;
    matches!(db.get_epoch_by_number(epoch), Some((_, Some(_)))).then_some(epoch)
}

/// One after the last closed epoch, or the tip's epoch if higher.
fn current_epoch(tip: Option<&ConsensusHeader>, last_closed: Option<Epoch>) -> Epoch {
    let from_tip = tip.map(|h| h.sub_dag.leader_epoch()).unwrap_or(0);
    let from_records = last_closed.map(|epoch| epoch + 1).unwrap_or(0);
    from_tip.max(from_records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_storage::mem_db::MemDatabase;
    use rayls_infrastructure_types::{Certificate, CommittedSubDag, Header, ReputationScores};

    fn sub_dag(epoch: Epoch) -> CommittedSubDag {
        let mut leader = Certificate::default();
        leader.header = Header { epoch, ..Default::default() };
        CommittedSubDag::new(vec![], leader, 0, ReputationScores::default(), None)
    }

    /// Tip and epoch come from the database, not the watch (which a validator never advances).
    // the bus registers metrics histograms that need a runtime handle
    #[tokio::test]
    async fn latest_header_and_epoch_come_from_the_database() {
        let db = MemDatabase::default();
        let rpc = EngineToPrimaryRpc::new(ConsensusBus::new(), db.clone());
        assert_eq!(rpc.get_latest_consensus_block().number, 0);
        assert_eq!(rpc.node_status().epoch, 0);

        for number in 1..=3 {
            db.write_subdag_for_test(number, sub_dag(0));
        }
        // watch still at the default header
        assert_eq!(rpc.consensus_bus.last_consensus_header().borrow().number, 0);
        assert_eq!(rpc.get_latest_consensus_block().number, 3);
        assert_eq!(rpc.node_status().epoch, 0);

        // unsigned epoch-0 placeholder does not close it
        let record = EpochRecord { epoch: 0, ..Default::default() };
        db.save_epoch_record(&record).unwrap();
        assert_eq!(rpc.node_status().epoch, 0);

        // epoch 0 closes with its certificate
        let cert = EpochCertificate {
            epoch_hash: record.digest(),
            signature: Default::default(),
            signed_authorities: Default::default(),
        };
        db.save_epoch_record_with_cert(&record, &cert).unwrap();
        assert_eq!(rpc.node_status().epoch, 1);
        db.write_subdag_for_test(4, sub_dag(1));
        assert_eq!(rpc.get_latest_consensus_block().number, 4);
        assert_eq!(rpc.node_status().epoch, 1);

        // tip wins when ahead of the records
        db.write_subdag_for_test(5, sub_dag(3));
        assert_eq!(rpc.node_status().epoch, 3);

        // highest row by number, not by write order or string sort
        db.write_subdag_for_test(256, sub_dag(3));
        db.write_subdag_for_test(255, sub_dag(3));
        assert_eq!(rpc.get_latest_consensus_block().number, 256);
    }

    /// An observer is caught up once its tip reaches the gossiped network tip.
    #[tokio::test]
    async fn observer_is_caught_up_compares_the_canonical_tip() {
        let db = MemDatabase::default();
        let rpc = EngineToPrimaryRpc::new(ConsensusBus::new(), db.clone());
        rpc.consensus_bus.node_mode().send_replace(NodeMode::Observer);

        // no gossip yet
        db.write_subdag_for_test(7, sub_dag(0));
        assert!(!rpc.node_status().is_caught_up, "network tip 0 means no peer gossip yet");

        // gossip ahead of the tip
        rpc.consensus_bus.last_published_consensus_num_hash().send_replace((9, BlockHash::ZERO));
        assert!(!rpc.node_status().is_caught_up);

        // tip reaches it
        db.write_subdag_for_test(8, sub_dag(0));
        db.write_subdag_for_test(9, sub_dag(0));
        assert!(rpc.node_status().is_caught_up);
    }
}
