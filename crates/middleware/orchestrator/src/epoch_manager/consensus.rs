use crate::{
    engine::ExecutionNode, epoch_manager::types::EpochManager, primary::PrimaryNode,
    types::EngineToPrimaryRpc, worker::WorkerNode,
};
use eyre::eyre;
use rayls_execution_evm::{
    chainspec::{RaylsHardFork, RaylsHardforks as _},
    system_calls::EpochState,
};
use rayls_infrastructure_config::{ConsensusConfig, NetworkConfig, RaylsDirs};
use rayls_infrastructure_types::{
    gas_accumulator::GasAccumulator, BlsPublicKey, CommitteeLookahead, Database as ReDatabase,
    TaskManager,
};
use std::collections::{HashMap, HashSet};
use tracing::{debug, warn};

impl<P, DB> EpochManager<P, DB>
where
    P: RaylsDirs + Clone + 'static,
    DB: ReDatabase,
{
    /// Helper method to create all consensus-related components for this epoch.
    ///
    /// Consensus components are short-lived and only relevant for the current epoch.
    pub(super) async fn create_consensus(
        &mut self,
        engine: &ExecutionNode,
        epoch_task_manager: &TaskManager,
        network_config: &NetworkConfig,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<(PrimaryNode<DB>, WorkerNode<DB>, ConsensusConfig<DB>)> {
        // create config for consensus
        let (consensus_config, preload_keys) =
            self.configure_consensus(engine, network_config).await?;

        // Restore execution state and initialize consensus round bounds BEFORE network starts.
        // This ensures committed_round is properly set from DB before accepting peer messages.
        self.try_restore_state(engine).await?;
        let _mode = self.identify_node_mode(&consensus_config).await?;

        let primary = self
            .create_primary_node_components(&consensus_config, epoch_task_manager.get_spawner())
            .await?;

        let engine_to_primary =
            EngineToPrimaryRpc::new(primary.consensus_bus().await, self.consensus_db.clone());
        // only spawns one worker for now
        let worker = self
            .spawn_worker_node_components(
                &consensus_config,
                engine,
                epoch_task_manager.get_spawner(),
                engine_to_primary,
                gas_accumulator,
            )
            .await?;

        // NOTE: try_restore_state and identify_node_mode moved above - before network starts
        // NOTE: spawn_engine_update_task moved to run() as a node-scoped task
        let primary_handle = primary.network_handle().await;
        let prefetches = preload_keys.clone();
        // Attempt to pre-load the next couple of committee's network info.
        let _ = primary_handle.inner_handle().find_authorities(prefetches).await;
        let worker_handle = worker.network_handle().await;
        let prefetches = preload_keys.clone();
        // Attempt to pre-load the next couple of committee's network info.
        let _ = worker_handle.inner_handle().find_authorities(prefetches).await;
        Ok((primary, worker, consensus_config))
    }

    /// Configure consensus for the current epoch.
    ///
    /// This method reads the canonical tip to read the epoch information needed
    /// to create the current committee and the consensus config.
    async fn configure_consensus(
        &mut self,
        engine: &ExecutionNode,
        network_config: &NetworkConfig,
    ) -> eyre::Result<(ConsensusConfig<DB>, Vec<BlsPublicKey>)> {
        // retrieve epoch information from canonical tip
        let EpochState { epoch, epoch_info, validators, epoch_start } =
            engine.epoch_state_from_canonical_tip().await?;
        debug!(target: "epoch-manager", ?epoch_info, "epoch state from canonical tip for epoch {}", epoch);
        let validators = validators
            .iter()
            .map(|v| {
                let decoded_bls = BlsPublicKey::from_literal_bytes(v.blsPubkey.as_ref());
                decoded_bls.map(|decoded| (decoded, v))
            })
            .collect::<Result<HashMap<_, _>, _>>()
            .map_err(|err| eyre!("failed to create bls key from on-chain bytes: {err:?}"))?;

        let epoch_boundary = epoch_start + epoch_info.epochDuration as u64;
        debug!(target: "epoch-manager", new_epoch_boundary=epoch_boundary, "epoch boundary for this epoch");

        debug!(target: "epoch-manager", ?validators, "creating committee for validators");

        let mut next_vals: HashSet<BlsPublicKey> = HashSet::new();
        next_vals.extend(validators.keys().copied());
        let committee = self.create_committee_from_state(epoch, validators).await?;

        let mut lookahead_entries = Vec::new();
        for offset in 1..=2 {
            let keys = engine.validators_for_epoch(epoch + offset).await?;
            next_vals.extend(keys.iter().copied());
            lookahead_entries.push((epoch + offset, keys));
        }
        let lookahead = CommitteeLookahead::from_entries(lookahead_entries);

        // Keys whose node records the network may keep in its kad store: every registered
        // validator, unioned with the committee and lookahead so the set is never narrower than
        // the keys consensus must be able to find. If the registry read fails, keep last epoch's
        // set rather than shrinking to committee and lookahead: a transient failure must not drop
        // the staked candidates for a whole epoch.
        match engine.registered_validators().await {
            Ok(keys) => self.registered_validators = keys.into_iter().collect(),
            Err(err) => warn!(
                target: "epoch-manager",
                ?err,
                "registry read failed; keeping last epoch's registered validators plus committee and lookahead",
            ),
        }
        self.registered_validators.extend(next_vals.iter().copied());

        // the certificate fetcher gates the request variant it sends on this fork, against the
        // executed tip it already tracks
        let certificate_fetch_epoch_v2_block = engine
            .get_reth_env()
            .await
            .rayls_chain_spec()
            .rayls_fork_activation_block(RaylsHardFork::CertificateFetchEpochV2);

        // create config for consensus
        let consensus_config = ConsensusConfig::new_for_epoch(
            self.builder.rayls_infrastructure_config.clone(),
            self.consensus_db.clone(),
            self.key_config.clone(),
            committee,
            lookahead,
            network_config.clone(),
            epoch_boundary,
            self.initial_epoch,
        )?
        .with_certificate_fetch_epoch_v2_block(certificate_fetch_epoch_v2_block);

        Ok((consensus_config, next_vals.into_iter().collect()))
    }
}
