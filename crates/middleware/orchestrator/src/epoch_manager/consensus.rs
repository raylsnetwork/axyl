use super::{
    dkg::{self, OwnShare},
    dkg_hooks::{self, CrashPoint},
};
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
    dkg::DkgOutcome, gas_accumulator::GasAccumulator, BlsPublicKey, CommitteeLookahead,
    Database as ReDatabase, TaskManager,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tracing::{debug, error, info, warn};

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
        let (consensus_config, preload_keys, share_failed) =
            self.configure_consensus(engine, network_config).await?;
        if consensus_config.scheme_pending() || share_failed {
            self.spawn_pending_scheme_retry(engine, epoch_task_manager, &consensus_config);
        }

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

        self.spawn_dkg_dealer(engine, epoch_task_manager, &consensus_config).await;

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
    /// Also returns whether this node should hold a share of the epoch key but could not derive
    /// it, so the caller retries.
    async fn configure_consensus(
        &mut self,
        engine: &ExecutionNode,
        network_config: &NetworkConfig,
    ) -> eyre::Result<(ConsensusConfig<DB>, Vec<BlsPublicKey>, bool)> {
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

        // the certificate fetcher gates the request variant it sends on this fork, against the
        // executed tip it already tracks
        let certificate_fetch_epoch_v2_block = engine
            .get_reth_env()
            .await
            .rayls_chain_spec()
            .rayls_fork_activation_block(RaylsHardFork::CertificateFetchEpochV2);

        // Threshold certificates: this epoch's key is the outcome of the previous epoch's DKG,
        // read from the chain when not stored yet (a restart), and this node's share of it is
        // decrypted from the accepted dealings.
        let outcome = dkg::ensure_dkg_outcome_with_chain(engine, &self.consensus_db, epoch)
            .await
            .unwrap_or_else(|e| {
                error!(target: "epoch-manager", epoch, ?e, "cannot ensure the dkg outcome");
                None
            });
        let share = match &outcome {
            Some(DkgOutcome::Key(key)) => {
                dkg::own_share(engine, &self.consensus_db, key, &self.key_config).await
            }
            _ => OwnShare::NotRecipient,
        };
        let share_failed = matches!(share, OwnShare::Failed);
        let scheme = dkg::epoch_scheme(&committee, outcome, share.share());
        if scheme.threshold().is_some_and(|threshold| threshold.share.is_some()) {
            dkg_hooks::crash_point(CrashPoint::ShareDerived, epoch);
        }
        // whether this epoch deals decides where it closes: a node that cannot tell must not
        // guess, so wait until the registry or a record answers
        let mut dealt = dkg::dealt_epochs(engine, &self.consensus_db, epoch).await;
        while dealt.dealt(epoch).is_none() {
            warn!(target: "epoch-manager", epoch, "cannot tell yet whether this epoch deals the next key: waiting");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            dealt = dkg::dealt_epochs(engine, &self.consensus_db, epoch).await;
        }
        // an epoch that deals the next key stays open until that key exists
        let deals_next = dealt.dealt(epoch) == Some(true);

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
            scheme,
            dealt,
        )?
        .with_certificate_fetch_epoch_v2_block(certificate_fetch_epoch_v2_block);
        if deals_next {
            consensus_config.set_close_gate(Arc::new(dkg::KeyReadyGate::new(
                engine.clone(),
                self.consensus_bus.executed_anchor().subscribe(),
                epoch + 1,
                epoch_boundary + dkg::key_wait(epoch_info.epochDuration as u64),
            )));
        }

        Ok((consensus_config, next_vals.into_iter().collect(), share_failed))
    }

    /// Run this validator's DKG duties for the epoch (see [`dkg::spawn_dealer`]) once the
    /// `ThresholdCertificates` fork is active. Observers and nodes without an execution address
    /// have none.
    async fn spawn_dkg_dealer(
        &self,
        engine: &ExecutionNode,
        epoch_task_manager: &TaskManager,
        consensus_config: &ConsensusConfig<DB>,
    ) {
        let config = &self.builder.rayls_infrastructure_config;
        let own_address = config.node_info.execution_address;
        if config.observer || own_address.is_zero() || !dkg::fork_active_now(engine).await {
            return;
        }
        let in_committee =
            consensus_config.authority_id().is_some_and(|id| consensus_config.in_committee(&id));
        dkg::spawn_dealer(
            epoch_task_manager.get_spawner(),
            engine.clone(),
            self.consensus_db.clone(),
            self.key_config.clone(),
            own_address,
            consensus_config.epoch(),
            in_committee,
        );
    }

    /// Retry a pending DKG outcome, or a share that could not be derived, in the background (10 s
    /// doubling to 10 min) for the life of the epoch. Once both land, the committee gets the epoch
    /// key and the config the scheme, so the node votes and verifies again without a restart.
    fn spawn_pending_scheme_retry(
        &self,
        engine: &ExecutionNode,
        epoch_task_manager: &TaskManager,
        consensus_config: &ConsensusConfig<DB>,
    ) {
        let (engine, db) = (engine.clone(), self.consensus_db.clone());
        let (config, key_config) = (consensus_config.clone(), self.key_config.clone());
        let epoch = config.epoch();
        epoch_task_manager.get_spawner().spawn_task("dkg outcome retry", async move {
            for attempt in 1u32.. {
                tokio::time::sleep(dkg::pending_outcome_backoff(attempt)).await;
                match dkg::ensure_dkg_outcome_with_chain(&engine, &db, epoch).await {
                    Ok(Some(outcome)) => {
                        let share = match &outcome {
                            DkgOutcome::Key(key) => {
                                dkg::own_share(&engine, &db, key, &key_config).await
                            }
                            DkgOutcome::Fallback => OwnShare::NotRecipient,
                        };
                        if matches!(share, OwnShare::Failed) {
                            debug!(target: "epoch-manager", epoch, attempt, "threshold share still not derivable");
                            continue;
                        }
                        let scheme =
                            dkg::epoch_scheme(config.committee(), Some(outcome), share.share());
                        config.set_scheme(scheme);
                        info!(target: "epoch-manager", epoch, attempt, "dkg outcome landed: epoch scheme resolved");
                        return;
                    }
                    Ok(None) => debug!(target: "epoch-manager", epoch, attempt, "dkg outcome still pending"),
                    Err(e) => error!(target: "epoch-manager", epoch, attempt, ?e, "dkg outcome retry failed"),
                }
            }
        });
    }
}
