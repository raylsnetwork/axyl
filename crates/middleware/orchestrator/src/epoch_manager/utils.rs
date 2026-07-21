use crate::epoch_manager::types::EpochManager;
use rayls_consensus_state_sync::{highest_executed_anchor, last_executed_consensus_from_anchor};
use rayls_execution_evm::reth_env::RethEnv;
use rayls_infrastructure_config::RaylsDirs;
use rayls_infrastructure_storage::{
    mdbx::MdbxConfig, open_db_with_consensus_config, tables::ConsensusBlocks, DatabaseType,
};
use rayls_infrastructure_types::{
    gas_accumulator::GasAccumulator, AuthorityIdentifier, BlsPublicKey, ConsensusHeader,
    ConsensusHeaderMeta, Database, DbTx, EpochVote, B256, WALK_PROGRESS_LOG_EVERY,
};
// production-only: the hardcoded protocol base-fee floor used to seed the accumulator
#[cfg(not(feature = "dev-single-node-setup"))]
use rayls_infrastructure_types::MIN_RAYLS_PROTOCOL_BASE_FEE;
use std::collections::BTreeMap;
use tracing::info;

/// Number of recent EVM blocks scanned to recover the restart execution anchor.
///
/// A drained parked (out-of-order seq) batch's block carries its ORIGIN output's lower nonce and
/// that output's digest as `parent_beacon_block_root`, yet lands after the in-order filler and
/// becomes the canonical tip, so the tip can anchor to a PREVIOUS output. Scanning this window and
/// taking the max-nonce block recovers the true highest executed output's consensus-header digest.
/// Sized well above any plausible reorder/park run.
const LAST_EXECUTED_SCAN_DEPTH: u64 = 200;

/// Recovers the EL execution anchor at boot: the consensus header committed by the highest-nonce
/// EVM block in the recent window, or `None` when no executed block anchors to a known header.
///
/// The literal canonical tip is not a reliable anchor (see [`LAST_EXECUTED_SCAN_DEPTH`]); the
/// max-nonce block in the window identifies the true highest executed output. Callers must run
/// this before the engine starts, while the tip is frozen.
pub(crate) fn recover_executed_anchor<DB: Database>(
    reth_env: &RethEnv,
    db: &DB,
) -> eyre::Result<Option<ConsensusHeader>> {
    let tip = reth_env.canonical_tip();
    let start = tip.number.saturating_sub(LAST_EXECUTED_SCAN_DEPTH);
    let window = reth_env.blocks_for_range(start..=tip.number)?;
    let anchor = highest_executed_anchor(&window).or(tip.header().parent_beacon_block_root);
    Ok(last_executed_consensus_from_anchor(anchor, db))
}

/// Seed per-worker gas/block totals and the in-memory leader-count mirror from
/// executed blocks in the current epoch.
///
/// The authoritative close-epoch tally is recomputed from the consensus DB
/// on demand via `RewardsBackend::tally`; the mirror seeded here exists only
/// as a divergence check.
pub fn catchup_accumulator<DB: Database>(
    db: &DB,
    reth_env: RethEnv,
    gas_accumulator: &GasAccumulator,
) -> eyre::Result<()> {
    let block = reth_env.canonical_tip();
    let epoch_state = reth_env.epoch_state_from_canonical_tip()?;
    info!(target: "epoch-manager", "catchup_accumulator: tip + epoch state read");

    // Production: seed with the hardcoded protocol floor (unchanged from before dev mode).
    #[cfg(not(feature = "dev-single-node-setup"))]
    gas_accumulator.base_fee(0).set_base_fee(MIN_RAYLS_PROTOCOL_BASE_FEE);
    // Dev (single-node): seed from the chain spec's configured min base fee instead. On a
    // non-1559 gasless dev chain (`min_base_fee == 0`) nothing ever corrects the seed via
    // `update_base_fee_after_block`, so the hardcoded 48 gwei floor would make every batch
    // block demand 48 gwei and silently drop gasless dev txs (`GasPriceLessThanBasefee`).
    #[cfg(feature = "dev-single-node-setup")]
    gas_accumulator.base_fee(0).set_base_fee(reth_env.rayls_chain_spec().min_base_fee());

    let blocks = reth_env.blocks_for_range(epoch_state.epoch_info.blockHeight..=block.number)?;
    info!(target: "epoch-manager", n = blocks.len(), "catchup_accumulator: blocks_for_range done");

    for current in blocks {
        let lower64 = current.difficulty.into_limbs()[0];
        let worker_id = (lower64 & 0xffff) as u16;
        gas_accumulator.inc_block(worker_id, current.gas_used, current.gas_limit);
    }
    info!(target: "epoch-manager", "catchup_accumulator: EL gas seed done");

    let finalized_nonce: u64 = block.nonce.into();
    let (current_epoch, last_executed_round) = RethEnv::deconstruct_nonce(finalized_nonce);

    // reverse-walk into a local map, then install atomically via
    // set_leader_counts so a re-run replaces rather than double-counts.
    //
    // Wrapped in with_read_txn so we can opt this txn out of mdbx's 30s
    // long-read safety net: bootstrap is cold-cache by definition, the scan
    // covers an entire epoch's worth of ConsensusBlocks, and the walk reads
    // only immutable historical headers.
    let leader_counts: BTreeMap<AuthorityIdentifier, u32> = db.with_read_txn(|txn| {
        txn.disable_long_read_safety();
        info!(target: "epoch-manager", "catchup_accumulator: read txn opened");

        let mut leader_counts: BTreeMap<AuthorityIdentifier, u32> = BTreeMap::new();
        let mut walked: u64 = 0;
        for (_key_bytes, value_bytes) in txn.reverse_raw_iter::<ConsensusBlocks>() {
            walked += 1;
            let meta = ConsensusHeaderMeta::from_bytes(&value_bytes)?;
            let epoch = meta.leader_epoch;
            let round = meta.leader_round;

            // walked=1 isolates the cold-cache cost of positioning the
            // cursor at the rightmost leaf; subsequent ticks at every
            // WALK_PROGRESS_LOG_EVERY reads show per-chunk wall-clock via
            // log timestamp deltas.
            if walked == 1 || walked % WALK_PROGRESS_LOG_EVERY == 0 {
                info!(
                    target: "epoch-manager",
                    walked,
                    cur_epoch = epoch,
                    cur_round = round,
                    "catchup_accumulator: walk progress",
                );
            }

            if epoch != current_epoch {
                break;
            }
            if round == 0 || round > last_executed_round {
                continue;
            }
            *leader_counts.entry(meta.leader_author).or_insert(0) += 1;
        }
        info!(target: "epoch-manager", walked, "catchup_accumulator: walk done");
        Ok(leader_counts)
    })?;

    let total_leaders: u32 = leader_counts.values().sum();
    let distinct_leaders = leader_counts.len();
    gas_accumulator.rewards_counter().set_leader_counts(leader_counts);

    info!(
        target: "epoch-manager",
        current_epoch,
        last_executed_round,
        total_leaders,
        distinct_leaders,
        "catchup_accumulator complete",
    );

    Ok(())
}

/// Create a consensus DB that lives for program lifetime.
pub(crate) fn open_consensus_db<P: RaylsDirs + 'static>(
    rayls_datadir: &P,
    consensus_db_config: &MdbxConfig,
) -> eyre::Result<DatabaseType> {
    let consensus_db_path = rayls_datadir.consensus_db_path();

    let _ = std::fs::create_dir_all(&consensus_db_path);
    let db = open_db_with_consensus_config(&consensus_db_path, consensus_db_config);

    info!(target: "epoch-manager", ?consensus_db_path, "opened consensus storage");

    Ok(db)
}

impl<P, DB> EpochManager<P, DB>
where
    P: RaylsDirs + Clone + 'static,
    DB: Database,
{
    /// Used by `committee_epoch_certs`: returns the BLS pubkey of a committee
    /// member that signed `vote` matching `hash`, if any.
    pub(super) fn signed_by_committee(
        committee_keys: &[BlsPublicKey],
        vote: &EpochVote,
        hash: B256,
    ) -> Option<BlsPublicKey> {
        if vote.epoch_hash == hash
            && committee_keys.contains(&vote.public_key)
            && vote.check_signature()
        {
            return Some(vote.public_key);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_storage::{mdbx::MdbxDatabase, open_db_with_consensus_config};
    use rayls_infrastructure_types::ConsensusHeader;
    use std::time::Duration;
    use tempfile::tempdir;

    /// Proves the contract `catchup_accumulator` relies on, against the
    /// production DB stack: `LayeredDatabase<MdbxDatabase>` (= `DatabaseType`),
    /// not bare `MdbxDatabase`. The walk must survive past
    /// `max_read_transaction_duration` when `disable_long_read_safety()` is
    /// called; without it, mdbx's safety net aborts the persistent-layer
    /// cursor and the walk returns nothing for rows that live only on disk.
    ///
    /// Seeding goes through a raw `MdbxDatabase` (then dropped) before the
    /// `LayeredDatabase` is opened, so the rows live only in the persistent
    /// layer — otherwise `LayeredDatabase::insert` also populates `mem_db`,
    /// which would shadow the cursor failure and make both walks return all
    /// rows (see [`LayeredDatabase::insert`] in the storage crate).
    ///
    /// libmdbx's check thread polls every 5s, so the smallest wait that
    /// reliably exceeds "1s timeout + check interval" is ~7s. The two walks
    /// run on parallel threads so both stalls overlap under one wall clock.
    #[test]
    fn catchup_walk_survives_mdbx_long_read_safety_when_exempted() {
        const MAX_READ_TXN_DURATION: Duration = Duration::from_secs(1);
        // 1s timeout + 5s mdbx check interval + 1s jitter buffer.
        const WAIT_PAST_TIMEOUT: Duration = Duration::from_secs(7);
        const ROWS: u64 = 8;

        let temp_dir = tempdir().expect("failed to create temp dir");
        let cfg =
            MdbxConfig::default().with_max_read_transaction_duration(Some(MAX_READ_TXN_DURATION));

        // Phase 1: seed persistent layer only.
        {
            let raw_mdbx = MdbxDatabase::open_with_config(temp_dir.path(), cfg.clone())
                .expect("open raw mdbx for seeding");
            raw_mdbx.open_table::<ConsensusBlocks>().expect("open ConsensusBlocks");
            for n in 0..ROWS {
                let header = ConsensusHeader { number: n, ..ConsensusHeader::default() };
                raw_mdbx.insert::<ConsensusBlocks>(&n, &header).expect("seed ConsensusBlocks");
            }
        }

        // Phase 2: reopen as the production stack. mem_db starts empty, so
        // the merge-join walk must hit the persistent layer to see the rows.
        let db = open_db_with_consensus_config(temp_dir.path(), &cfg);

        // Fix under test: same call shape as `catchup_accumulator`. The
        // iterator is constructed BEFORE the sleep so the underlying mdbx
        // read txn is in the active list while the safety-net thread polls;
        // this mirrors a cold-cache walk that takes ~30s to drain.
        let exempted_db = db.clone();
        let exempted = std::thread::spawn(move || -> eyre::Result<usize> {
            exempted_db.with_read_txn(|txn| {
                txn.disable_long_read_safety();
                let iter = txn.reverse_iter::<ConsensusBlocks>();
                std::thread::sleep(WAIT_PAST_TIMEOUT);
                Ok(iter.count())
            })
        });

        // Baseline: same walk, without the opt-out.
        let bounded_db = db.clone();
        let bounded = std::thread::spawn(move || -> eyre::Result<usize> {
            bounded_db.with_read_txn(|txn| {
                let iter = txn.reverse_iter::<ConsensusBlocks>();
                std::thread::sleep(WAIT_PAST_TIMEOUT);
                Ok(iter.count())
            })
        });

        let exempted_count = exempted
            .join()
            .expect("exempted thread panicked")
            .expect("with_read_txn should return Ok");
        let bounded_count = bounded
            .join()
            .expect("bounded thread panicked")
            .expect("with_read_txn should return Ok");

        assert_eq!(
            exempted_count, ROWS as usize,
            "exempted walk must see every ConsensusBlocks row after the wait",
        );
        // mdbx tears down the timed-out txn's cursor, so reverse_iter on the
        // bounded walk falls back to an empty iterator.
        assert_eq!(
            bounded_count, 0,
            "bounded walk must NOT see rows once the safety net has fired",
        );
    }
}
