use crate::{engine::ExecutionNode, epoch_manager::types::EpochManager, primary::PrimaryNode};
use eyre::eyre;
use rayls_consensus_primary::{NodeMode, RecentlyExecutedBlocks};
use rayls_execution_evm::system_calls::ConsensusRegistry;
use rayls_infrastructure_config::{Config, ConfigFmt, ConfigTrait as _, RaylsDirs};
use rayls_infrastructure_storage::{
    tables::{
        BatchSeqCounter, CertificateDigestByOrigin, CertificateDigestByRound, Certificates,
        ConsensusBlocks, EpochTransitionCheckpoints, KadProviderRecords, KadRecords,
        KadWorkerProviderRecords, KadWorkerRecords, LastProposed, LastProposedByAuthority,
        NodeBatchesCache, NodeIdentity, Payload, Votes,
    },
    CertificateStore as _, EpochStore as _, ProposerStore as _, LAST_PROPOSAL_KEY,
};
use rayls_infrastructure_types::{
    AuthorityIdentifier, BlsPublicKey, Committee, CommitteeBuilder, ConsensusHeader,
    Database as ReDatabase, DbTxMut, Epoch, EpochRecord, B256,
};
use std::collections::HashMap;
use tracing::{debug, error, info, trace, warn};

impl<P, DB> EpochManager<P, DB>
where
    P: RaylsDirs + Clone + 'static,
    DB: ReDatabase,
{
    /// Force `CvvInactive` unless booted as Observer.
    fn force_cvv_inactive(&self, reason: &'static str) {
        // try_restore_state runs before identify_node_mode, so the config
        // flag is the authoritative identity source here
        if self.builder.rayls_infrastructure_config.observer {
            warn!(target: "epoch-manager", reason, "refusing to demote Observer to CvvInactive");
            return;
        }
        self.consensus_bus.node_mode().send_replace(NodeMode::CvvInactive);
    }

    /// Record the epoch record for the just-completed epoch.
    ///
    /// `epoch` is the closing epoch and must come from the boundary output or the
    /// recovery checkpoint, never from the current committee or on-chain registry:
    /// during recovery both already report closing+1 (the primary is primed for the
    /// new epoch and the boundary block advanced the registry), and building the
    /// record for closing+1 demands the closing epoch's record as parent - a record
    /// that may not be certified anywhere yet, turning a locally recoverable state
    /// into a fatal peer-fetch failure.
    pub(super) async fn write_epoch_record(
        &mut self,
        primary: &PrimaryNode<DB>,
        engine: &ExecutionNode,
        epoch: Epoch,
        boundary_consensus_hash: B256,
    ) -> eyre::Result<()> {
        if epoch == 0 {
            if let Some((epoch_rec, Some(_))) = self.consensus_db.get_epoch_by_number(epoch) {
                self.epoch_record = Some(epoch_rec);
                return Ok(());
            }
        } else if let Some((epoch_rec, _)) = self.consensus_db.get_epoch_by_number(epoch) {
            self.epoch_record = Some(epoch_rec);
            return Ok(());
        }
        let committee_keys = engine.validators_for_epoch(epoch).await?;
        let next_committee_keys = engine.validators_for_epoch(epoch + 1).await?;
        let parent_hash = if epoch == 0 {
            B256::default()
        } else {
            let mut prev = resolve_local_prev_epoch_record(
                &self.consensus_db,
                self.prev_epoch_record.as_ref(),
                epoch,
            );
            // Neither memory nor disk has it — e.g. a restart before vote quorum
            // persisted this node's copy. Peers closed the previous epoch and hold
            // its certified record, so fetch it directly instead of failing and
            // waiting for the async collector to backfill (which may not win the
            // race against this boundary). Trusted because it carries a valid cert
            // and its next_committee matches the committee we derived for this epoch
            // from chain state.
            if prev.is_none() {
                let network = primary.network_handle().await;
                match network.request_epoch_cert(Some(epoch - 1), None).await {
                    Ok((peer_rec, cert)) => {
                        // Validate before trusting; log the specific reason on
                        // rejection so a live incident can tell bad peer data apart
                        // from no data.
                        if peer_rec.epoch != epoch - 1 {
                            warn!(
                                target: "epoch-manager",
                                want = epoch - 1, got = peer_rec.epoch,
                                "peer returned wrong epoch for previous epoch record; ignoring",
                            );
                        } else if !peer_rec.verify_with_cert(&cert) {
                            warn!(
                                target: "epoch-manager",
                                epoch = epoch - 1,
                                "peer-provided previous epoch record failed cert verification; ignoring",
                            );
                        } else if committee_keys != peer_rec.next_committee {
                            warn!(
                                target: "epoch-manager",
                                epoch = epoch - 1,
                                "peer-provided previous epoch record next_committee does not match this epoch's committee; ignoring",
                            );
                        } else {
                            info!(
                                target: "epoch-manager",
                                epoch = epoch - 1,
                                "fetched previous epoch record from a peer for parent_hash",
                            );
                            if let Err(e) =
                                self.consensus_db.save_epoch_record_with_cert(&peer_rec, &cert)
                            {
                                error!(target: "epoch-manager", "failed to persist peer-fetched previous epoch record: {e}");
                            }
                            prev = Some(peer_rec);
                        }
                    }
                    Err(e) => {
                        warn!(
                            target: "epoch-manager",
                            epoch = epoch - 1, ?e,
                            "could not fetch previous epoch record from any peer",
                        );
                    }
                }
            }
            let Some(prev) = prev else {
                error!(
                    target: "epoch-manager",
                    "failed to find previous epoch record when starting epoch",
                );
                return Err(eyre!("failed to find previous epoch record when starting epoch"));
            };
            if committee_keys != prev.next_committee {
                error!(
                    target: "epoch-manager",
                    "Last epochs next committee not equal to this epochs committee! previous {:?}, current {:?}",
                    prev.next_committee,
                    committee_keys
                );
                return Err(eyre!(
                    "Last epochs next committee not equal to this epochs committee!"
                ));
            }
            prev.digest()
        };
        // Use deterministic sources: boundary_consensus_hash from the boundary output (immutable)
        // and the durable canonical tip. Read the reth canonical head directly rather than the
        // in-memory recently_executed_blocks (which is fed asynchronously by the engine-update
        // task): the tip is the epoch-closing block the engine finalized before this runs,
        // so parent_state is deterministic and race-free — and identical to the value the
        // pre-anchor code committed.
        let parent_state = engine.get_reth_env().await.canonical_tip().num_hash();

        let epoch_rec = EpochRecord {
            epoch,
            committee: committee_keys,
            next_committee: next_committee_keys,
            parent_hash,
            parent_state,
            parent_consensus: boundary_consensus_hash,
        };

        // Intentionally not persisted here: EpochRecord and EpochCertificate must be
        // written in a single txn (save_epoch_record_with_cert). Writing the record
        // alone would leave an unrecoverable half-state if the process dies before
        // the cert write that happens after vote quorum.
        self.epoch_record = Some(epoch_rec);
        Ok(())
    }

    /// Create the [Committee] for the current epoch.
    ///
    /// This is the first step for configuring consensus.
    pub(super) async fn create_committee_from_state(
        &self,
        epoch: Epoch,
        validators: HashMap<BlsPublicKey, &ConsensusRegistry::ValidatorInfo>,
    ) -> eyre::Result<Committee> {
        info!(target: "epoch-manager", "creating committee from state");

        let mut committee_builder = CommitteeBuilder::new(epoch);
        for (bls, validator) in &validators {
            committee_builder.add_authority(*bls, 1, validator.validatorAddress);
        }

        let genesis_committee: Committee = Config::load_from_path_or_default(
            self.rayls_datadir.committee_path(),
            ConfigFmt::YAML,
        )?;
        for (bls, bootstrap) in genesis_committee.bootstrap_servers() {
            if validators.contains_key(&bls) {
                committee_builder.add_bootstrap_server(
                    bls,
                    bootstrap.primary.clone(),
                    bootstrap.worker.clone(),
                );
            }
        }
        let committee = committee_builder.build();
        info!(target: "epoch-manager", ?committee, "Created committee from state");
        // load committee
        committee.load();

        Ok(committee)
    }

    /// Clear identity-bound tables if the consensus DB belongs to a different validator.
    ///
    /// Detection uses `NodeIdentity` (robust) with `LastProposed` as fallback.
    fn sanitize_foreign_consensus_db(&self) -> eyre::Result<bool> {
        let our_authority_id: AuthorityIdentifier = self.key_config.primary_public_key().into();

        let is_foreign = match self.consensus_db.get::<NodeIdentity>(&0) {
            Ok(Some(stored_id)) => stored_id != our_authority_id,
            _ => {
                // NodeIdentity not stamped yet; fall back to LastProposed
                match self.consensus_db.get::<LastProposed>(&LAST_PROPOSAL_KEY) {
                    Ok(Some(header)) => header.author != our_authority_id,
                    _ => false,
                }
            }
        };

        // stamp our identity regardless of whether sanitization was needed
        if let Err(e) = self.consensus_db.insert::<NodeIdentity>(&0, &our_authority_id) {
            warn!(target: "epoch-manager", ?e, "failed to stamp node identity");
        }

        if !is_foreign {
            return Ok(false);
        }

        warn!(
            target: "epoch-manager",
            %our_authority_id,
            "foreign consensus DB detected, clearing identity-bound tables"
        );

        let mut last_proposed_header = None;
        if let Ok(Some(header)) = self.consensus_db.get_last_proposed_by_authority(our_authority_id)
        {
            last_proposed_header = Some(header);
        }

        self.consensus_db.with_write_txn(|txn| {
            txn.clear_table::<LastProposed>()?;
            txn.clear_table::<Votes>()?;
            txn.clear_table::<Payload>()?;
            // node-specific long-lived tables
            txn.clear_table::<NodeBatchesCache>()?;
            txn.clear_table::<EpochTransitionCheckpoints>()?;
            txn.clear_table::<BatchSeqCounter>()?;
            // KAD record tables: cleared on snapshot recovery so find_authorities
            // re-queries fresh records, avoiding stale addresses from the snapshot epoch.
            txn.clear_table::<KadRecords>()?;
            txn.clear_table::<KadProviderRecords>()?;
            txn.clear_table::<KadWorkerRecords>()?;
            txn.clear_table::<KadWorkerProviderRecords>()?;

            if let Some(h) = last_proposed_header {
                txn.insert::<LastProposed>(&LAST_PROPOSAL_KEY, &h)?;
            }

            Ok(())
        })?;

        if !self.builder.rayls_infrastructure_config.observer {
            self.consensus_bus.node_mode().send_modify(|v| *v = NodeMode::CvvInactive);
            info!(target: "epoch-manager",
                "validator starting as CvvInactive after foreign DB sanitization");
        }

        Ok(true)
    }

    /// Restore execution state for the consensus components.
    pub(super) async fn try_restore_state(&self, engine: &ExecutionNode) -> eyre::Result<()> {
        // Only restore recently_executed_blocks from the chain DB on initial startup.
        // On later epoch transitions, recently_executed_blocks are already up to date
        // from the node-scoped engine update task and close_epoch(). Clearing
        // and restoring here would race with the reth DB flush and could
        // revert recently_executed_blocks to a stale state, causing get_missing_consensus()
        // to replay already-executed outputs.
        if self.initial_epoch {
            if self.sanitize_foreign_consensus_db()? {
                info!(target: "epoch-manager", "foreign consensus DB sanitized");
            }

            let block_capacity =
                self.consensus_bus.recently_executed_blocks().borrow().block_capacity();

            // clear recently_executed_blocks before restoring to avoid ordering issues
            self.consensus_bus
                .recently_executed_blocks()
                .send_replace(RecentlyExecutedBlocks::new(block_capacity as usize));

            // restore blocks from execution layer in ascending order
            let restored_blocks = engine.last_executed_output_blocks(block_capacity).await?;
            let restored_count = restored_blocks.len();

            debug!(target: "epoch-manager", restored_blocks=?restored_blocks, "Restoring recently-executed blocks from execution layer");
            for executed_block in restored_blocks {
                self.consensus_bus
                    .recently_executed_blocks()
                    .send_modify(|blocks| blocks.push_latest(executed_block));
            }

            let recent = self.consensus_bus.recently_executed_blocks().borrow();
            let latest = recent.latest_block();
            let (epoch, round) = (latest.subdag_leader_epoch(), latest.subdag_leader_round());

            if recent.is_empty() && latest.number() == 0 {
                trace!(
                    target: "epoch-manager",
                    "recently_executed_blocks empty - this is expected for fresh/genesis start"
                );
            } else if recent.is_empty() {
                error!(
                    target: "epoch-manager",
                    restored_count,
                    latest_block_number = latest.number(),
                    "CRITICAL: recently_executed_blocks is empty despite having execution history! \
                     State sync fork detection may not work correctly."
                );
            }

            trace!(
                target: "epoch-manager",
                recently_executed_blocks_len = recent.len(),
                restored_count,
                latest_block_number = latest.number(),
                latest_block_subdag_leader_epoch = %epoch,
                latest_block_subdag_leader_round = %round,
                latest_block_subdag_consensus_digest = ?latest.subdag_consensus_digest(),
                oldest_block_number = recent.oldest_block_number(),
                "restored recently_executed_blocks from execution layer"
            );
        }

        // prime the last consensus header from the DB
        let (_, last_db_block) = self
            .consensus_db
            .last_record::<ConsensusBlocks>()
            .unwrap_or_else(|| (0, ConsensusHeader::default()));

        // log consensus DB state and check for potential desync
        {
            let recent = self.consensus_bus.recently_executed_blocks().borrow();
            let exec_latest = recent.latest_block_num_hash();

            // check if consensus DB's referenced execution block is within recently_executed_blocks
            // window
            let consensus_exec_ref = last_db_block.sub_dag.leader.header().latest_execution_block;
            let in_window = consensus_exec_ref.number >= recent.oldest_block_number()
                && consensus_exec_ref.number <= exec_latest.number;

            trace!(
                target: "epoch-manager",
                last_db_block_number = last_db_block.number,
                last_db_block_leader_round = last_db_block.sub_dag.leader_round(),
                last_db_block_leader_epoch = last_db_block.sub_dag.leader_epoch(),
                last_db_block_digest = ?last_db_block.digest(),
                consensus_exec_ref_number = consensus_exec_ref.number,
                consensus_exec_ref_hash = ?consensus_exec_ref.hash,
                exec_ref_in_recent_window = in_window,
                "loaded last consensus block from DB"
            );

            // verify hash matches for early fork detection
            if in_window
                && consensus_exec_ref.number > 0
                && !recent.contains_hash(consensus_exec_ref.hash)
            {
                error!(
                    target: "epoch-manager",
                    consensus_exec_ref_number = consensus_exec_ref.number,
                    consensus_exec_ref_hash = ?consensus_exec_ref.hash,
                    "Consensus DB references execution block hash not in recently_executed_blocks - \
                        MDBX has divergent blocks from a previous run. \
                        Forcing CvvInactive to resync from peers."
                );
                self.force_cvv_inactive("divergent-blocks-recent-window");
            }
        }

        // NOTE: last_consensus_header must stay peer-derived only; local
        // writes cause premature rejoin in try_rejoin_consensus.
        self.check_execution_consensus_sync(engine, &last_db_block).await?;

        Ok(())
    }

    /// Check if execution layer is ahead of consensus layer and handle appropriately.
    async fn check_execution_consensus_sync(
        &self,
        engine: &ExecutionNode,
        last_consensus_block: &ConsensusHeader,
    ) -> eyre::Result<()> {
        // get execution layer tip
        let reth_env = engine.get_reth_env().await;
        let (exec_epoch, exec_round, exec_block_num) = match reth_env.execution_tip_epoch_round() {
            Ok(tip) => tip,
            Err(e) => {
                debug!(target: "epoch-manager", ?e, "Could not get execution tip - likely fresh start");
                return Ok(());
            }
        };

        // get consensus layer tip info
        let consensus_number = last_consensus_block.number;
        let consensus_round = last_consensus_block.sub_dag.leader_round();
        let consensus_epoch = last_consensus_block.sub_dag.leader_epoch();

        // skip check only if BOTH layers are at genesis (true fresh start)
        if exec_block_num == 0 && consensus_number == 0 {
            debug!(target: "epoch-manager", "Fresh start detected - skipping execution-consensus sync check");
            return Ok(());
        }

        // detect desync: MDBX has blocks from a previous run but consensus DB is empty.
        // this happens after total network collapse when the consensus DB was cleared
        // but MDBX retained blocks. Each validator may have flushed to a different block,
        // so participating in consensus would produce divergent execution state (fork).
        if exec_block_num > 0 && consensus_number == 0 {
            warn!(
                target: "epoch-manager",
                exec_epoch,
                exec_round,
                exec_block_num,
                "Execution-consensus desync detected: MDBX has blocks but consensus DB is empty. \
                 This indicates a restart after network collapse. \
                 Forcing CvvInactive mode to sync from peers before participating."
            );
            self.force_cvv_inactive("exec-ahead-of-empty-consensus-db");
            return Ok(());
        }

        // check for desync: execution ahead of consensus
        // uses Rust's lexicographic tuple comparison for clarity
        let execution_ahead = (exec_epoch, exec_round) > (consensus_epoch, consensus_round);

        if execution_ahead {
            // execution ahead of consensus - force inactive mode
            error!(
                target: "epoch-manager",
                exec_epoch,
                exec_round,
                exec_block_num,
                consensus_epoch,
                consensus_round,
                consensus_number,
                "CRITICAL: Execution layer is ahead of consensus layer! \
                 This indicates the node crashed between consensus persist and execution, \
                 or database inconsistency. The node will enter sync mode to recover. \
                 If this error persists, manual intervention may be required: \
                 1. Stop the node \
                 2. Backup the data directory \
                 3. Remove the consensus DB (not reth DB) \
                 4. Restart - the node will resync from peers"
            );

            // force inactive mode
            self.force_cvv_inactive("exec-ahead-of-consensus");

            warn!(
                target: "epoch-manager",
                "Node forced to CvvInactive mode due to execution-consensus desync"
            );
        } else {
            // epoch/round positions match; now verify block hashes agree
            let consensus_exec_ref =
                last_consensus_block.sub_dag.leader.header().latest_execution_block;
            if consensus_exec_ref.number > 0 {
                match reth_env.sealed_header_by_number(consensus_exec_ref.number) {
                    Ok(Some(mdbx_header)) if mdbx_header.hash() != consensus_exec_ref.hash => {
                        error!(
                            target: "epoch-manager",
                            block_number = consensus_exec_ref.number,
                            consensus_hash = ?consensus_exec_ref.hash,
                            mdbx_hash = ?mdbx_header.hash(),
                            "Execution-consensus hash mismatch: MDBX block hash differs from \
                             consensus DB reference. This indicates divergent blocks from a \
                             previous run. Forcing CvvInactive to resync from peers."
                        );
                        self.force_cvv_inactive("exec-consensus-hash-mismatch");
                        return Ok(());
                    }
                    Ok(None) => {
                        // deferred persistence gap -- subscriber replay handles this
                        warn!(
                            target: "epoch-manager",
                            block_number = consensus_exec_ref.number,
                            consensus_hash = ?consensus_exec_ref.hash,
                            mdbx_tip = exec_block_num,
                            "Consensus DB references block not yet in MDBX (deferred persistence gap). \
                             Subscriber will replay missing consensus outputs from local storage."
                        );
                    }
                    Err(e) => {
                        warn!(
                            target: "epoch-manager",
                            block_number = consensus_exec_ref.number,
                            ?e,
                            "Failed to look up MDBX block for hash comparison; skipping check"
                        );
                    }
                    _ => {
                        // hashes match
                    }
                }
            }

            info!(
                target: "epoch-manager",
                exec_epoch,
                exec_round,
                exec_block_num,
                consensus_epoch,
                consensus_round,
                consensus_number,
                "Execution-consensus sync check passed"
            );
        }

        Ok(())
    }

    /// Clear the epoch-related tables for consensus.
    ///
    /// These tables are epoch-specific. Complete historic data is stored
    /// in the `ConsensusBlocks` table. Idempotent: clearing already-empty
    /// tables is a no-op.
    pub(super) fn clear_consensus_db_for_next_epoch(&self) -> eyre::Result<()> {
        // log pre-clear cert-store high-water-mark so mid-epoch growth is visible.
        // per-write GC was removed; epoch boundary is the sole bound on cert-store size
        let highest_round = self.consensus_db.highest_round_number();
        info!(target: "epoch-manager::cert-store", highest_round, "clearing consensus tables at epoch boundary");

        self.consensus_db.with_write_txn(|txn| {
            txn.clear_table::<LastProposed>()?;
            txn.clear_table::<LastProposedByAuthority>()?;
            txn.clear_table::<Votes>()?;
            txn.clear_table::<Certificates>()?;
            txn.clear_table::<CertificateDigestByRound>()?;
            txn.clear_table::<CertificateDigestByOrigin>()?;
            txn.clear_table::<Payload>()?;

            Ok(())
        })?;

        Ok(())
    }
}

/// Resolve the previous epoch's record (`epoch - 1`) from local state when
/// building the record for the closing `epoch`.
///
/// The record is not eagerly persisted; it lands on disk atomically with its
/// cert once vote quorum is reached. Prefer a certified on-disk record (it is
/// what the committee agreed on, which may differ from the one built locally).
/// Otherwise reuse the in-memory record from the previous transition, falling
/// back to an uncertified/absent disk record only as a last resort (the epoch-0
/// dummy, or a restart before the peer-fetch backfill has restored it).
/// Returns `None` when neither source has it - the caller must fetch from a peer.
pub(crate) fn resolve_local_prev_epoch_record<DB: ReDatabase>(
    consensus_db: &DB,
    prev_in_mem: Option<&EpochRecord>,
    epoch: Epoch,
) -> Option<EpochRecord> {
    let prev_epoch = epoch.checked_sub(1)?;
    match consensus_db.get_epoch_by_number(prev_epoch) {
        Some((disk_rec, Some(_cert))) => {
            if let Some(mem) = prev_in_mem.filter(|r| r.epoch == prev_epoch) {
                let (local, certified) = (mem.digest(), disk_rec.digest());
                if local != certified {
                    warn!(
                        target: "epoch-manager",
                        ?local,
                        ?certified,
                        "local epoch record diverges from certified record; using certified",
                    );
                }
            }
            Some(disk_rec)
        }
        uncertified => prev_in_mem
            .filter(|r| r.epoch == prev_epoch)
            .cloned()
            .or_else(|| uncertified.map(|(rec, _)| rec)),
    }
}
