use super::{
    error::{WorkerNetworkError, WorkerNetworkResult},
    message::WorkerGossip,
    WorkerNetworkHandle,
};
use crate::WorkerResponse;
use rayls_consensus_network::GossipMessage;
use rayls_infrastructure_config::{ConsensusConfig, LibP2pConfig};
use rayls_infrastructure_network_types::{WorkerOthersBatchMessage, WorkerToPrimaryClient};
use rayls_infrastructure_storage::tables::Batches;
use rayls_infrastructure_types::{
    encode, ensure, now, try_decode, Batch, BatchValidation, BlockHash, BlsPublicKey, Database,
    DbTx, SealedBatch, WorkerId,
};
use std::sync::{Arc, LazyLock};
use tracing::{debug, error};

/// The minimal length of a single, encoded, default [Batch] used to set a local min for
/// message validation.
static LOCAL_MIN_REQUEST_SIZE: LazyLock<usize> = LazyLock::new(|| encode(&Batch::default()).len());
/// The minimal response wrapper using a default, empty message.
static MESSAGE_OVERHEAD: LazyLock<usize> =
    LazyLock::new(|| encode(&WorkerResponse::RequestBatches(vec![])).len());

/// The type that handles requests from peers.
#[derive(Clone, Debug)]
pub struct RequestHandler<DB> {
    /// This worker's id.
    id: WorkerId,
    /// The type that validates batches received from peers.
    validator: Arc<dyn BatchValidation>,
    /// Consensus config with access to database.
    consensus_config: ConsensusConfig<DB>,
    /// Network handle- so we can respond to gossip.
    network_handle: WorkerNetworkHandle,
}

impl<DB> RequestHandler<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        id: WorkerId,
        validator: Arc<dyn BatchValidation>,
        consensus_config: ConsensusConfig<DB>,
        network_handle: WorkerNetworkHandle,
    ) -> Self {
        Self { id, validator, consensus_config, network_handle }
    }

    /// Process gossip from the committee.
    ///
    /// Workers gossip the Batch Digests once accepted so that non-committee peers can request the
    /// Batch.
    pub(super) async fn process_gossip(&self, msg: &GossipMessage) -> WorkerNetworkResult<()> {
        // deconstruct message
        let GossipMessage { data, source: _, sequence_number: _, topic } = msg;

        // gossip is uncompressed
        let gossip = try_decode(data)?;

        match gossip {
            WorkerGossip::Batch(batch_hash) => {
                ensure!(
                    topic.to_string().eq(&LibP2pConfig::worker_batch_topic()),
                    WorkerNetworkError::InvalidTopic
                );
                // Retrieve the block...
                let store = self.consensus_config.node_storage();
                if !matches!(store.get::<Batches>(&batch_hash), Ok(Some(_))) {
                    // If we don't have this batch already then try to get it.
                    // If we are a CVV then we should already have it.
                    // This allows non-CVVs to pre fetch batches they will soon need.
                    match self.network_handle.request_batches(vec![batch_hash]).await {
                        Ok(batches) => {
                            if let Some(batch) = batches.first() {
                                store.insert::<Batches>(&batch.digest(), batch).map_err(|e| {
                                    WorkerNetworkError::Internal(format!(
                                        "failed to write to batch store: {e}"
                                    ))
                                })?;
                            }
                        }
                        Err(e) => {
                            tracing::error!(target: "worker:network", "failed to get gossipped batch {batch_hash}: {e}");
                        }
                    }
                }
            }
            WorkerGossip::Txn(tx_bytes) => {
                ensure!(
                    topic.to_string().eq(&LibP2pConfig::worker_txn_topic()),
                    WorkerNetworkError::InvalidTopic
                );
                if let Some(authority) = self.consensus_config.authority() {
                    let committee = self.consensus_config.committee();
                    let authorities = committee.authorities();
                    let size = authorities.len();
                    for (slot, auth) in authorities.into_iter().enumerate() {
                        if &auth == authority {
                            if let Err(e) = self.validator.submit_batch_if_mine(
                                &tx_bytes,
                                size as u64,
                                slot as u64,
                            ) {
                                error!(target: "worker:network", "failed to submit batch: {e}");
                            }
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Process a new reported batch.
    pub(super) async fn process_report_batch(
        &self,
        peer: &BlsPublicKey,
        sealed_batch: SealedBatch,
    ) -> WorkerNetworkResult<()> {
        // return error if reporter isn't in current committee
        if !self.consensus_config.committee_pub_keys().contains(peer) {
            return Err(WorkerNetworkError::NonCommitteeBatch);
        }

        let client = self.consensus_config.local_network().clone();
        let store = self.consensus_config.node_storage().clone();
        // validate batch - log error if invalid
        self.validator.validate_batch(sealed_batch.clone()).await?;

        let (mut batch, digest) = sealed_batch.split();

        // Set received_at timestamp for remote batch.
        batch.set_received_at(now());
        store.insert::<Batches>(&digest, &batch).map_err(|e| {
            WorkerNetworkError::Internal(format!("failed to write to batch store: {e}"))
        })?;

        // notify primary for payload store
        client
            .report_others_batch(WorkerOthersBatchMessage { digest, worker_id: self.id })
            .await
            .map_err(|e| WorkerNetworkError::Internal(e.to_string()))?;

        Ok(())
    }

    /// Attempt to return requested batches.
    ///
    /// MDBX reads are offloaded to `spawn_blocking` so assembling a large response does not stall
    /// the async runtime under load.
    pub(super) async fn process_request_batches(
        &self,
        batch_digests: Vec<BlockHash>,
        max_response_size: usize,
    ) -> WorkerNetworkResult<Vec<Batch>> {
        let consensus_config = self.consensus_config.clone();
        tokio::task::spawn_blocking(move || {
            collect_requested_batches_blocking(batch_digests, max_response_size, consensus_config)
        })
        .await
        .map_err(|e| WorkerNetworkError::Internal(format!("batch-collector join error: {e}")))?
    }
}

/// Reads the requested batches from storage, truncated to the response size cap.
fn collect_requested_batches_blocking<DB: Database>(
    batch_digests: Vec<BlockHash>,
    max_response_size: usize,
    consensus_config: ConsensusConfig<DB>,
) -> WorkerNetworkResult<Vec<Batch>> {
    // assume reasonable min is 1 encoded batch (no transactions)
    // NOTE: caller needs to account for batches + msg overhead, and batches must have
    // transactions
    if max_response_size < *LOCAL_MIN_REQUEST_SIZE {
        debug!(target: "cert-collector", "batch request max size too small: {}", max_response_size);
        return Err(WorkerNetworkError::InvalidRequest("Request size too small".into()));
    }

    // return error for empty batches
    if batch_digests.is_empty() {
        debug!(target: "cert-collector", "batch request empty");
        return Err(WorkerNetworkError::InvalidRequest("Empty batch digests".into()));
    }

    // use the min value between this node's max rpc message size and the requestor's reported
    // max message size
    //
    // NOTE: assume safe overhead is accounted for because the codec will also compress messages
    let local_max =
        consensus_config.network_config().libp2p_config().max_rpc_message_size - *MESSAGE_OVERHEAD;
    let max_message_size = max_response_size.min(local_max);

    let store = consensus_config.node_storage();

    // Serve from one cold-aware read txn: `tx.get` resolves each digest hot-first and falls through
    // to cold within the same txn (see `ColdTx`), so a lagging peer's archived batches are served
    // without opening a fresh read txn per digest.
    //
    // Stop at the response budget: requesters rely on responders truncating below the requested
    // set, so reading further would only decode (often large) batches to discard them.
    store
        .with_read_txn(|tx| {
            let mut batches = Vec::new();
            let mut total_size = 0usize;
            for digest in &batch_digests {
                let Some(batch) = tx.get::<Batches>(digest)? else { continue };
                let batch_size = batch.size();
                if total_size + batch_size > max_message_size {
                    break;
                }
                total_size += batch_size;
                batches.push(batch);
            }
            Ok(batches)
        })
        .map_err(|e| {
            WorkerNetworkError::Internal(format!("failed to read from batch store: {e:?}"))
        })
}

// support IT tests
#[cfg(any(test, feature = "test-utils"))]
impl<DB> RequestHandler<DB>
where
    DB: Database,
{
    // /// Publicly available for tests.
    // /// See [Self::process_gossip].
    // pub async fn pub_process_gossip(&self, msg: &GossipMessage) -> WorkerNetworkResult<()> {
    //     self.process_gossip(msg).await
    // }

    // /// Publicly available for tests.
    // /// See [Self::process_report_batch].
    // pub async fn pub_process_report_batch(
    //     &self,
    //     peer: &BlsPublicKey,
    //     sealed_batch: SealedBatch,
    // ) -> WorkerNetworkResult<()> {
    //     self.process_report_batch(peer, sealed_batch).await
    // }

    // /// Publicly available for tests.
    // /// See [Self::process_request_batches].
    // pub async fn pub_process_request_batches(
    //     &self,
    //     batch_digests: Vec<BlockHash>,
    //     max_response_size: usize,
    // ) -> WorkerNetworkResult<Vec<Batch>> {
    //     self.process_request_batches(batch_digests, max_response_size).await
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_storage::mem_db::MemDatabase;
    use rayls_infrastructure_types::B256;
    use rayls_testing_test_utils::CommitteeFixture;

    /// The responder returns the prefix of requested batches that fits the size budget, in order,
    /// and skips digests with no stored batch.
    #[test]
    fn responder_truncates_to_size_budget() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let config = fixture.first_authority().consensus_config();
        let store = config.node_storage().clone();

        // Equal-sized batches so the budget maps cleanly to a batch count.
        let sample = Batch { transactions: vec![vec![1u8; 256]], ..Default::default() };
        let batch_size = sample.size();

        const TOTAL: usize = 20;
        const FIT: usize = 5;
        let mut digests = Vec::with_capacity(TOTAL + 1);
        for _ in 0..TOTAL {
            let digest = B256::random();
            store.insert::<Batches>(&digest, &sample).expect("write batch to db");
            digests.push(digest);
        }
        // A digest with no stored batch must be skipped, not returned.
        digests.push(B256::random());

        // Budget sits between FIT and FIT+1 batches, so exactly FIT come back.
        let budget = batch_size * FIT + batch_size / 2;
        let batches =
            collect_requested_batches_blocking(digests, budget, config).expect("collect batches");

        assert_eq!(batches.len(), FIT, "must return exactly the batches that fit the budget");
        assert!(
            batches.iter().map(Batch::size).sum::<usize>() <= budget,
            "returned batches must fit within the budget",
        );
    }

    /// A batch present only in the cold tier (archived and pruned from hot) is still served: the
    /// responder's read txn falls through to cold, so a lagging peer can fetch history past the
    /// hot window.
    #[test]
    fn responder_serves_cold_archived_batches() {
        use rayls_infrastructure_storage::{
            cold::{ColdConfig, ColdDatabase, ColdLocation},
            layered_db::LayeredDatabase,
            mdbx::MdbxDatabase,
            tables::ColdBatchLocations,
        };
        use rayls_infrastructure_types::encode;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let next = AtomicUsize::new(0);
        // The node's cold-tiered mdbx stack, one subdir per authority. Built explicitly (not via
        // `open_db`) so an `--all-features` build, where the redb backend takes over the
        // `DatabaseType` alias, still tests the cold composition.
        let fixture = CommitteeFixture::builder(|| {
            let dir = tmp.path().join(format!("authority-{}", next.fetch_add(1, Ordering::SeqCst)));
            std::fs::create_dir_all(dir.join("hot")).expect("create db dir");
            let layered =
                LayeredDatabase::open(MdbxDatabase::open(dir.join("hot")).expect("open mdbx"));
            let db = ColdDatabase::open(layered, &ColdConfig { dir: dir.join("cold") })
                .expect("open cold store");
            db.open_table::<Batches>().expect("open Batches");
            db.open_table::<ColdBatchLocations>().expect("open ColdBatchLocations");
            db
        })
        .build();
        let config = fixture.first_authority().consensus_config();
        let store = config.node_storage().clone();

        let hot_digest = B256::repeat_byte(0x11);
        let hot_batch = Batch { transactions: vec![vec![1u8; 64]], ..Default::default() };
        store.insert::<Batches>(&hot_digest, &hot_batch).expect("insert hot batch");

        // The cold batch exists only as a jar row plus its auxiliary-index entry, never hot.
        let cold_digest = B256::repeat_byte(0x22);
        let cold_batch = Batch { transactions: vec![vec![2u8; 64]], ..Default::default() };
        let cold = store.cold();
        cold.batches().begin_epoch(1, 0).expect("begin epoch");
        cold.batches()
            .append_row(&[cold_digest.as_slice(), &encode(&cold_batch)])
            .expect("append row");
        cold.batches().commit().expect("commit");
        store
            .insert::<ColdBatchLocations>(&cold_digest, &ColdLocation { epoch: 1, row: 0 })
            .expect("insert cold location");

        // Request hot + cold + absent: both stored batches serve, the absent digest is skipped.
        let digests = vec![hot_digest, cold_digest, B256::repeat_byte(0x33)];
        let batches =
            collect_requested_batches_blocking(digests, usize::MAX, config).expect("collect");
        assert_eq!(batches.len(), 2, "hot and cold batches must both serve");
        assert!(
            batches.iter().any(|b| encode(b) == encode(&cold_batch)),
            "the cold-archived batch must serve through the fall-through"
        );
    }
}
