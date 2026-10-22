//! Threshold-certificate key generation at the epoch level.
//!
//! The key for epoch `E` is dealt during `E-1` through the `DkgManager` system contract: each
//! validator of `E-1`'s committee submits one publicly verifiable dealing ([`spawn_dealer`]),
//! which the chain accepts only after verifying it in full. An epoch deals the next epoch's key
//! when the dealing gate ([`deals`]) says so: the `ThresholdCertificates` fork is active at its
//! first block. The recipients are the members of `E`'s committee with an encryption key before
//! `E-1` began, fixed by the contract; when too few have one, `E` gets no key and uses aggregate
//! certificates. Once `E-1` has closed on-chain the accepted dealings are final, every node reads
//! the same ones ([`ensure_dkg_outcome_with_chain`]) and saves the same [`DkgOutcome`], and each
//! recipient decrypts its own share from them ([`own_share`]). The contract keeps only a hash of
//! each dealing, and the bytes are in block receipts that a pruned node drops after about 10,000
//! blocks, well within a day-long epoch, so every node keeps the dealings it executes
//! ([`spawn_dealing_archive`]).

use super::dkg_hooks::{self, CrashPoint};
use crate::engine::ExecutionNode;
use rayls_execution_evm::{
    reth_env::{
        dkg_manager::{submitter_key_message, DkgRecipients, DkgSubmitter},
        genesis::genesis_dealings_commitment,
        RethEnv,
    },
    txn_pool::recover_pooled_transaction,
    ForkCondition, RaylsHardFork, RaylsHardforks as _, WorkerTxPool,
};
use rayls_infrastructure_config::{
    Config, ConfigFmt, ConfigTrait as _, DealtEpochs, GenesisDealing,
};
use rayls_infrastructure_storage::EpochStore;
use rayls_infrastructure_types::{
    dkg::{DkgOutcome, EpochKey, EpochScheme, EpochThreshold},
    encode, keccak256,
    nidkg::{
        dealing_context, dealing_message, decryption_key_message, encryption_key_context,
        key_registration_message, BabySteps, Dealing, DecryptionKey,
    },
    quorum_threshold,
    threshold::{SecretPolynomial, SecretShare},
    try_decode, Address, BlsPublicKey, BlsSigner, Committee, ConsensusHeader, ConsensusOutput,
    Database as ReDatabase, Encodable2718 as _, Epoch, EpochCloseGate, EpochRecord, RecordBinding,
    TaskSpawner, TxHash,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

/// The dealing gate's answer when it needs no block: epoch 0, whose first block is genesis, deals
/// when the fork is active there, a fork that never activates never, and one active from genesis
/// always. `None`: the epoch's first block decides.
pub(super) fn deals_without_reads(fork: &ForkCondition, epoch: Epoch) -> Option<bool> {
    if epoch == 0 {
        return Some(fork.active_at_block(0));
    }
    match fork {
        ForkCondition::Never => Some(false),
        ForkCondition::Block(0) => Some(true),
        _ => None,
    }
}

/// The record of `epoch` this node holds: certified, else pending. The epoch-0 placeholder (no
/// cert, never pending) states no boundary and is never returned.
pub(super) fn local_record<DB: EpochStore>(db: &DB, epoch: Epoch) -> Option<EpochRecord> {
    match db.get_epoch_by_number(epoch) {
        Some((record, Some(_))) => Some(record),
        _ => db.pending_epoch_records().into_iter().find(|record| record.epoch == epoch),
    }
}

/// First block of `epoch` from local records: one past the block that closed `epoch - 1`.
pub(super) fn first_block_from_records<DB: EpochStore>(db: &DB, epoch: Epoch) -> Option<u64> {
    local_record(db, epoch.checked_sub(1)?).map(|record| record.parent_state.number + 1)
}

/// The `ThresholdCertificates` fork condition of the running chain.
async fn threshold_fork(engine: &ExecutionNode) -> ForkCondition {
    engine
        .get_reth_env()
        .await
        .rayls_chain_spec()
        .rayls_fork_activation(RaylsHardFork::ThresholdCertificates)
}

/// Whether the `ThresholdCertificates` fork is active at the canonical tip.
pub(super) async fn fork_active_now(engine: &ExecutionNode) -> bool {
    let fork = threshold_fork(engine).await;
    let tip = engine.get_reth_env().await.canonical_tip().number;
    fork.active_at_block(tip)
}

/// The dealing gate: whether `epoch` deals the next epoch's key, `None` when this node cannot
/// tell yet. Deals when `epoch > 0` and the fork is active at the epoch's first block, read from
/// the registry (`getEpochInfo(epoch).blockHeight`, kept for `current - 3 ..= current`) or else
/// from the previous epoch's record. Never from the canonical tip's epoch state, which at a
/// boundary already describes the next epoch.
pub(super) async fn deals<DB: EpochStore>(
    engine: &ExecutionNode,
    db: &DB,
    epoch: Epoch,
) -> Option<bool> {
    let fork = threshold_fork(engine).await;
    if let Some(answer) = deals_without_reads(&fork, epoch) {
        return Some(answer);
    }
    let registry = engine.epoch_info(epoch).await.map(|info| info.map(|info| info.blockHeight));
    deals_from(&fork, epoch, registry, db)
}

/// The dealing gate's decision from `epoch`'s first block: the registry's answer, else one past
/// the block that closed the previous epoch, from its record. `None` when neither states it.
pub(super) fn deals_from<DB: EpochStore>(
    fork: &ForkCondition,
    epoch: Epoch,
    registry: eyre::Result<Option<u64>>,
    db: &DB,
) -> Option<bool> {
    let first_block = match registry {
        Ok(Some(first_block)) => Some(first_block),
        Ok(None) => first_block_from_records(db, epoch),
        Err(e) => {
            warn!(target: "epoch-manager", epoch, ?e, "registry cannot answer the dealing gate");
            first_block_from_records(db, epoch)
        }
    }?;
    Some(fork.active_at_block(first_block))
}

/// The dealing gate's answers for `current - 3 ..= current`, the epochs the registry describes,
/// for verifying other epochs' certificates.
pub(super) async fn dealt_epochs<DB: EpochStore>(
    engine: &ExecutionNode,
    db: &DB,
    current: Epoch,
) -> DealtEpochs {
    let fork = threshold_fork(engine).await;
    if fork == ForkCondition::Never {
        return DealtEpochs::never();
    }
    // epoch 0 needs no read: genesis is its first block
    let mut known = BTreeMap::from([(0, fork.active_at_block(0))]);
    for epoch in current.saturating_sub(3)..=current {
        if let Some(dealt) = deals(engine, db, epoch).await {
            known.insert(epoch, dealt);
        }
    }
    DealtEpochs::from_known(known).with_genesis_key(genesis_has_key(engine).await)
}

/// Whether epoch 0 has a threshold key: genesis holds dealings for it and the fork is active
/// from genesis. Dealings in a genesis whose fork starts later are ignored, as the epoch's votes
/// and records follow the fork.
async fn genesis_has_key(engine: &ExecutionNode) -> bool {
    if !threshold_fork(engine).await.active_at_block(0) {
        return false;
    }
    let env = engine.get_reth_env().await;
    tokio::task::spawn_blocking(move || env.dkg_genesis_dealings())
        .await
        .ok()
        .and_then(Result::ok)
        .flatten()
        .is_some()
}

/// Where this node's genesis keeps the dealings of epoch 0's key, set once at startup.
static GENESIS_DEALINGS_FILE: OnceLock<std::path::PathBuf> = OnceLock::new();

/// Record where the genesis dealings file is (`RaylsDirs::genesis_dkg_dealings_path`). The first
/// call wins: a process runs one chain.
pub(super) fn set_genesis_dealings_file(path: std::path::PathBuf) {
    let _ = GENESIS_DEALINGS_FILE.set(path);
}

/// The dealings of epoch 0's key from the genesis file, checked against the commitment genesis
/// state holds. `None` when genesis gave epoch 0 no key. A missing or different file is an error:
/// this node's genesis is not the chain's.
fn genesis_dealings(env: &RethEnv) -> eyre::Result<Option<Vec<(Address, Dealing)>>> {
    let Some(commitment) = env.dkg_genesis_dealings()? else { return Ok(None) };
    let path = GENESIS_DEALINGS_FILE
        .get()
        .ok_or_else(|| eyre::eyre!("genesis dealings file location not set"))?;
    let entries: Vec<GenesisDealing> =
        Config::load_from_path(path, ConfigFmt::YAML).map_err(|e| {
            eyre::eyre!("genesis gives epoch 0 a key but {} is unreadable: {e}", path.display())
        })?;
    eyre::ensure!(
        genesis_dealings_commitment(&entries) == commitment,
        "{} is not the genesis's dealings of epoch 0's key",
        path.display()
    );
    entries
        .into_iter()
        .map(|entry| {
            let dealing = try_decode::<Dealing>(&entry.dealing).map_err(|e| {
                eyre::eyre!("genesis dealing of {} does not decode: {e}", entry.dealer)
            })?;
            Ok((entry.dealer, dealing))
        })
        .collect::<eyre::Result<Vec<_>>>()
        .map(Some)
}

/// Epoch 0's outcome: its key from the genesis dealings, else `Fallback`.
fn genesis_outcome(env: &RethEnv) -> eyre::Result<DkgOutcome> {
    let Some(dealings) = genesis_dealings(env)? else { return Ok(DkgOutcome::Fallback) };
    let members = env.dkg_committee_keys(0)?;
    let key = EpochKey::from_dealings(0, members, &dealings)?;
    info!(target: "epoch-manager", dealers = dealings.len(), "epoch 0 key read from the genesis dealings");
    Ok(DkgOutcome::Key(key))
}

/// The decryption table, built once per process (about 100 ms, a few megabytes).
fn baby_steps() -> &'static BabySteps {
    static TABLE: OnceLock<BabySteps> = OnceLock::new();
    TABLE.get_or_init(BabySteps::new)
}

/// This validator's decryption key, from its BLS signature over a message in the
/// never-published `DkgShare` scope.
fn decryption_key<S: BlsSigner>(signer: &S) -> eyre::Result<DecryptionKey> {
    Ok(DecryptionKey::from_signature(&signer.request_signature_direct(&decryption_key_message()))?)
}

/// The accepted dealings for the key of `epoch`, in acceptance order: the ones this node kept
/// ([`spawn_dealing_archive`]), else read back from the blocks that accepted them. `None` when
/// this node has neither for one of them.
fn accepted_dealings<DB: EpochStore>(
    env: &RethEnv,
    db: &DB,
    epoch: Epoch,
    dealers: &[Address],
) -> eyre::Result<Option<Vec<(Address, Dealing)>>> {
    let mut dealings = Vec::with_capacity(dealers.len());
    for dealer in dealers {
        let kept = env.dkg_dealing_hash(epoch, *dealer)?.and_then(|hash| {
            db.get_dkg_dealing(epoch, hash)
                .filter(|(kept_dealer, bytes)| kept_dealer == dealer && keccak256(bytes) == hash)
                .map(|(_, bytes)| bytes)
        });
        let bytes = match kept {
            Some(bytes) => bytes,
            None => match env.dkg_dealing(epoch, *dealer)? {
                Some(bytes) => bytes.to_vec(),
                None => return Ok(None),
            },
        };
        let dealing = try_decode::<Dealing>(&bytes)
            .map_err(|e| eyre::eyre!("accepted dealing of {dealer} does not decode: {e}"))?;
        dealings.push((*dealer, dealing));
    }
    Ok(Some(dealings))
}

/// The outcome for `epoch` from the chain, as `DkgManager.finalize` fixed it in the block that
/// closed `epoch - 1`: the sum of the dealings to the recipient set it recorded, or `Fallback`
/// when that set had no key (a member without an encryption key in time, or too few dealings,
/// which the close gate rules out unless the committee changed in the closing block). `None`
/// before `epoch - 1` closed, and when this node lacks a dealing's block.
fn outcome_from_chain<DB: EpochStore>(
    env: &RethEnv,
    db: &DB,
    epoch: Epoch,
) -> eyre::Result<Option<DkgOutcome>> {
    if !env.dkg_manager_deployed()? {
        warn!(target: "epoch-manager", epoch, "no DkgManager on this chain: the epoch uses aggregate certificates");
        return Ok(Some(DkgOutcome::Fallback));
    }
    let Some(fin) = env.dkg_final(epoch)? else {
        // the epoch before has not closed: dealing is still open
        return Ok(None);
    };
    if !fin.keyed {
        warn!(target: "epoch-manager", epoch, "no threshold key for the epoch: it uses aggregate certificates");
        return Ok(Some(DkgOutcome::Fallback));
    }
    let dealers = env.dkg_dealers(epoch)?;
    let Some(dealings) = accepted_dealings(env, db, epoch, &dealers)? else {
        warn!(target: "epoch-manager", epoch, "missing the blocks of some accepted dealings");
        return Ok(None);
    };
    let key = EpochKey::from_dealings(epoch, fin.members, &dealings)?;
    info!(target: "epoch-manager", epoch, dealers = dealers.len(), "dkg outcome read from the chain");
    Ok(Some(DkgOutcome::Key(key)))
}

/// The DKG outcome for `epoch`: the stored row, else computed and saved.
///
/// Epoch 0's key, if any, comes from the genesis dealings. For a later epoch, when the gate says
/// `epoch - 1` did not deal, `Fallback` is saved; when it did, the outcome is read from the
/// chain. `Ok(None)`, with nothing saved, when that cannot be done yet. Errors are save failures.
pub(super) async fn ensure_dkg_outcome_with_chain<DB: ReDatabase>(
    engine: &ExecutionNode,
    db: &DB,
    epoch: Epoch,
) -> eyre::Result<Option<DkgOutcome>> {
    if let Some(outcome) = db.get_dkg_outcome(epoch) {
        return Ok(Some(outcome));
    }
    let outcome = if epoch == 0 && !threshold_fork(engine).await.active_at_block(0) {
        // genesis dealings count only with the fork active from genesis
        DkgOutcome::Fallback
    } else if epoch == 0 {
        let env = engine.get_reth_env().await;
        match tokio::task::spawn_blocking(move || genesis_outcome(&env)).await? {
            Ok(outcome) => outcome,
            Err(e) => {
                error!(target: "epoch-manager", ?e, "cannot compute epoch 0's dkg outcome");
                return Ok(None);
            }
        }
    } else {
        match deals(engine, db, epoch - 1).await {
            None => return Ok(None),
            Some(false) => DkgOutcome::Fallback,
            Some(true) => {
                let (env, db) = (engine.get_reth_env().await, db.clone());
                let computed =
                    tokio::task::spawn_blocking(move || outcome_from_chain(&env, &db, epoch))
                        .await?;
                match computed {
                    Ok(Some(outcome)) => outcome,
                    Ok(None) => return Ok(None),
                    Err(e) => {
                        error!(target: "epoch-manager", epoch, ?e, "cannot compute dkg outcome");
                        return Ok(None);
                    }
                }
            }
        }
    };
    // what the certified record of the epoch before signs as this epoch's key must be what this
    // node computed from the chain; a disagreement makes both untrusted (`epoch_group_key`)
    let computed = match &outcome {
        DkgOutcome::Key(key) => Some(key.group_key()),
        DkgOutcome::Fallback => None,
    };
    if db.epoch_group_key(epoch).is_some_and(|signed| signed != computed) {
        error!(target: "epoch-manager", epoch, "the certified record of the previous epoch signs another key for this epoch than the chain gives");
    }
    db.save_dkg_outcome(epoch, &outcome)
        .map_err(|e| eyre::eyre!("failed to persist dkg outcome for epoch {epoch}: {e}"))?;
    dkg_hooks::crash_point(CrashPoint::OutcomeSaved, epoch);
    Ok(Some(outcome))
}

/// This node's share of an epoch key, or why it has none.
#[derive(Debug)]
pub(super) enum OwnShare {
    /// The node holds this share and votes with it.
    Share(SecretShare),
    /// The node gets no share this epoch: it is outside the committee, or the dealings skip it
    /// because its encryption key was not registered before the dealing epoch began.
    NotRecipient,
    /// The share should exist but could not be derived yet (logged); worth retrying.
    Failed,
}

impl OwnShare {
    pub(super) fn share(self) -> Option<SecretShare> {
        match self {
            Self::Share(share) => Some(share),
            Self::NotRecipient | Self::Failed => None,
        }
    }
}

/// This node's share of `key`: its share of every accepted dealing, decrypted and summed, at its
/// position in the committee the key was dealt to.
pub(super) async fn own_share<S: BlsSigner, DB: ReDatabase>(
    engine: &ExecutionNode,
    db: &DB,
    key: &EpochKey,
    signer: &S,
) -> OwnShare {
    let own_key = signer.public_key();
    let Some(position) = key.position_of(&own_key) else { return OwnShare::NotRecipient };
    let env = engine.get_reth_env().await;
    let decryption = match decryption_key(signer) {
        Ok(decryption) => decryption,
        Err(e) => {
            error!(target: "epoch-manager", epoch = key.epoch, ?e, "cannot derive the decryption key");
            return OwnShare::Failed;
        }
    };
    let (key, db) = (key.clone(), db.clone());
    let epoch = key.epoch;
    let derived = tokio::task::spawn_blocking(move || {
        let dealings = match key.epoch {
            0 => genesis_dealings(&env)?
                .ok_or_else(|| eyre::eyre!("genesis has no dealings for epoch 0's key"))?,
            _ => accepted_dealings(&env, &db, key.epoch, &key.dealers)?
                .ok_or_else(|| eyre::eyre!("missing the blocks of some accepted dealings"))?,
        };
        // every accepted dealing was verified against the same recipient set
        if dealings.first().is_some_and(|(_, dealing)| !dealing.positions().contains(&position)) {
            return Ok(None);
        }
        Ok::<_, eyre::Report>(Some(key.derive_share(
            position,
            &dealings,
            &decryption,
            baby_steps(),
        )?))
    })
    .await;
    match derived {
        Ok(Ok(Some(share))) => OwnShare::Share(share),
        Ok(Ok(None)) => {
            warn!(target: "epoch-manager", epoch, "no share of this epoch's key: the encryption key was not registered before the dealing epoch began");
            OwnShare::NotRecipient
        }
        Ok(Err(e)) => {
            error!(target: "epoch-manager", epoch, ?e, "cannot derive threshold share");
            OwnShare::Failed
        }
        Err(e) => {
            error!(target: "epoch-manager", epoch, ?e, "threshold share task failed");
            OwnShare::Failed
        }
    }
}

/// The current epoch's certificate scheme from its DKG outcome and this node's share of the key.
/// A key switches the committee to threshold certificates; a node without a share runs without
/// one and cannot vote this epoch. No outcome yet makes the scheme `Pending`: the node neither
/// votes nor verifies under the epoch's scheme. Neither is a key whose threshold is not the
/// committee's quorum, which no accepted dealing can produce.
pub(super) fn epoch_scheme(
    committee: &Committee,
    outcome: Option<DkgOutcome>,
    share: Option<SecretShare>,
) -> EpochScheme {
    let epoch = committee.epoch();
    let key = match outcome {
        Some(DkgOutcome::Key(key)) => key,
        Some(DkgOutcome::Fallback) => {
            committee.set_threshold_pending(false);
            debug!(target: "epoch-manager", epoch, "epoch uses aggregate certificates");
            return EpochScheme::Aggregate;
        }
        None => {
            committee.set_threshold_pending(true);
            error!(target: "epoch-manager", epoch, "dkg outcome not computable yet: not voting until it is");
            return EpochScheme::Pending;
        }
    };
    if !key_runs_with(&key, &committee.bls_keys()) {
        committee.set_threshold_pending(true);
        return EpochScheme::Pending;
    }
    committee.set_threshold_key(key.public.clone());
    info!(target: "epoch-manager", epoch, has_share = share.is_some(), "epoch uses threshold certificates");
    EpochScheme::Threshold(Arc::new(EpochThreshold { key, share }))
}

/// Whether a committee with keys `committee` runs with `key`: its threshold is the committee's
/// quorum and it was dealt to that committee (share indices are positions in it). No accepted
/// dealing makes a key that fails this; a node would neither vote nor verify with one.
pub(super) fn key_runs_with(key: &EpochKey, committee: &[BlsPublicKey]) -> bool {
    if key.public.threshold() != quorum_threshold(committee.len() as u64) as usize {
        error!(target: "epoch-manager", epoch = key.epoch, "dkg key threshold does not match the committee quorum");
        return false;
    }
    if !key.members.is_empty() && key.members != committee {
        error!(target: "epoch-manager", epoch = key.epoch, "dkg key was dealt to another committee than this node runs");
        return false;
    }
    true
}

/// How committee members sign the record of an epoch, and check votes for it.
#[derive(Debug)]
pub(super) struct RecordSigning {
    /// What the votes sign besides the record.
    pub(super) binding: RecordBinding,
    /// The epoch's key when its certificates are threshold ones: votes are then signed with
    /// shares of it.
    pub(super) key: Option<EpochKey>,
}

/// How `record` is signed: from the `ThresholdCertificates` fork on (the epoch dealt the next
/// key), with the next epoch's group key as that epoch runs with it (`None`: aggregate
/// certificates), checked against the record's next committee as `epoch_scheme` checks it; and
/// with shares of the epoch's own key when it has one. `None` when this node cannot tell yet (a
/// dealing gate or an outcome it cannot compute), or the next key is one no node runs with.
pub(super) async fn record_signing<DB: ReDatabase>(
    engine: &ExecutionNode,
    db: &DB,
    record: &EpochRecord,
) -> Option<RecordSigning> {
    let epoch = record.epoch;
    let binding = match deals(engine, db, epoch).await? {
        false => RecordBinding::Legacy,
        true => match ensure_dkg_outcome_with_chain(engine, db, epoch + 1).await.ok()?? {
            DkgOutcome::Fallback => RecordBinding::NextKey(None),
            DkgOutcome::Key(next) if key_runs_with(&next, &record.next_committee) => {
                RecordBinding::NextKey(Some(next.group_key()))
            }
            DkgOutcome::Key(_) => return None,
        },
    };
    let key = match ensure_dkg_outcome_with_chain(engine, db, epoch).await.ok()?? {
        DkgOutcome::Key(key) => Some(key),
        DkgOutcome::Fallback => None,
    };
    Some(RecordSigning { binding, key })
}

/// Backoff before retry `attempt` (1-based) of a pending DKG outcome: 10 s doubling to 10 min.
pub(super) fn pending_outcome_backoff(attempt: u32) -> Duration {
    const BASE: Duration = Duration::from_secs(10);
    const MAX: Duration = Duration::from_secs(600);
    BASE.checked_mul(1u32 << attempt.saturating_sub(1).min(31)).unwrap_or(MAX).min(MAX)
}

/// Least time an epoch waits past its boundary for the next key before closing without one.
const MIN_KEY_WAIT: u64 = 600;

/// How long past its boundary an epoch of `duration` seconds waits for the next key: one more
/// epoch, at least [`MIN_KEY_WAIT`]. Dealings land within seconds when the dealers can submit
/// them, so only a fault shared by most of the committee (unfunded submitters, a pool refusing
/// the transactions) reaches it; the next epoch then uses aggregate certificates instead of
/// the committee never changing again.
pub(super) fn key_wait(duration: u64) -> u64 {
    dkg_hooks::key_wait().unwrap_or(duration.max(MIN_KEY_WAIT))
}

/// Holds an epoch that deals the next epoch's key open until that key exists: the epoch may
/// close at an output only if `DkgManager.isReady(next_epoch)` holds once every earlier output has
/// executed (see [`EpochCloseGate`]), or once the output reaches `deadline`. The closing block's
/// `finalize` then records whether the dealings in by then make a key.
#[derive(Debug)]
pub(super) struct KeyReadyGate {
    engine: ExecutionNode,
    executed: watch::Receiver<ConsensusHeader>,
    next_epoch: Epoch,
    /// Commit timestamp from which the epoch closes with or without the key.
    deadline: u64,
    /// The answer given for each output, by number.
    answers: std::sync::Mutex<BTreeMap<u64, bool>>,
}

impl KeyReadyGate {
    pub(super) fn new(
        engine: ExecutionNode,
        executed: watch::Receiver<ConsensusHeader>,
        next_epoch: Epoch,
        deadline: u64,
    ) -> Self {
        Self { engine, executed, next_epoch, deadline, answers: Default::default() }
    }
}

/// Pause between attempts when the close gate cannot read an answer.
const GATE_RETRY: Duration = Duration::from_millis(200);

#[async_trait::async_trait]
impl EpochCloseGate for KeyReadyGate {
    async fn may_close(&self, output: &ConsensusOutput) -> bool {
        if let Some(answer) =
            self.answers.lock().unwrap_or_else(|e| e.into_inner()).get(&output.number)
        {
            return *answer;
        }
        let epoch = self.next_epoch;
        if output.reaches_epoch_boundary(self.deadline) {
            warn!(target: "epoch-manager", epoch, deadline = self.deadline, "the next key is still not ready: closing the epoch without it");
            self.answers.lock().unwrap_or_else(|e| e.into_inner()).insert(output.number, true);
            return true;
        }
        // the state after every output before this one: output `n - 1` executed, and output `n`
        // cannot have, since whoever asks has not forwarded it yet, so the canonical tip is
        // output `n - 1`'s last block (read with its unpersisted blocks)
        let before = output.number.saturating_sub(1);
        if self.executed.clone().wait_for(|anchor| anchor.number >= before).await.is_err() {
            // shutting down: keep the epoch open, the next start decides again
            return false;
        }
        // a node-local failure must not become a decision other nodes would not take: retry
        // until the chain answers
        let ready = loop {
            let env = self.engine.get_reth_env().await;
            match tokio::task::spawn_blocking(move || env.dkg_is_ready(epoch)).await {
                Ok(Ok(ready)) => break ready,
                Ok(Err(e)) => {
                    warn!(target: "epoch-manager", epoch, ?e, "cannot read whether the next key is ready: retrying");
                }
                Err(e) => {
                    warn!(target: "epoch-manager", epoch, ?e, "key readiness task failed: retrying");
                }
            }
            tokio::time::sleep(GATE_RETRY).await;
        };
        if !ready {
            dkg_hooks::crash_point(CrashPoint::EpochHeldOpen, epoch - 1);
        }
        // both cut points (the subscriber and the epoch boundary detection) ask about the same
        // output and must hear the same answer
        self.answers.lock().unwrap_or_else(|e| e.into_inner()).insert(output.number, ready);
        ready
    }
}

/// How far behind the canonical tip the dealing archive starts at node startup: the receipts a
/// pruned (`--full`) node keeps. A node that kept running scanned everything before that; a node
/// that was stopped did not move its tip, so what it executed last is within reach.
const ARCHIVE_RESCAN: u64 = 10_064;
/// Most blocks the archive scans per write.
const ARCHIVE_BATCH: u64 = 1_000;
/// Epochs of dealings the archive keeps behind the newest dealt key: enough for the current and
/// previous keys, and for certifying a record left pending by a stall.
const ARCHIVE_EPOCHS: Epoch = 8;

/// Keep every dealing the chain accepts, for the life of the node: scan each block this node
/// executes for `DealingAccepted` events while its receipts are fresh, and save the dealings in
/// the consensus DB. Epoch keys and shares are derived from them at the next boundary and after
/// every restart, long after a pruned node dropped those receipts.
pub(super) fn spawn_dealing_archive<DB: ReDatabase>(
    engine: ExecutionNode,
    db: DB,
    mut executed: watch::Receiver<ConsensusHeader>,
    spawner: TaskSpawner,
    shutdown: rayls_infrastructure_types::Noticer,
) {
    spawner.spawn_task("dkg dealing archive", async move {
        let fork = threshold_fork(&engine).await;
        let first = match fork {
            ForkCondition::Never => return,
            ForkCondition::Block(block) => block,
            _ => 0,
        };
        let env = engine.get_reth_env().await;
        let mut next = first.max(env.canonical_tip().number.saturating_sub(ARCHIVE_RESCAN));
        let mut newest_key: Epoch = 0;
        loop {
            let tip = env.canonical_tip().number;
            if tip >= next {
                let to = tip.min(next + ARCHIVE_BATCH - 1);
                let scan = env.clone();
                match tokio::task::spawn_blocking(move || scan.dkg_dealings_accepted_in(next, to))
                    .await
                {
                    Ok(Ok(found)) => {
                        let rows: Vec<_> = found
                            .into_iter()
                            .map(|d| (d.epoch, d.hash, d.dealer, d.dealing.to_vec()))
                            .collect();
                        if let Err(e) = db.save_dkg_dealings(&rows) {
                            error!(target: "epoch-manager", ?e, from = next, to, "cannot keep dkg dealings: retrying");
                            tokio::time::sleep(DEALER_POLL).await;
                            continue;
                        }
                        if !rows.is_empty() {
                            debug!(target: "epoch-manager", dealings = rows.len(), from = next, to, "kept dkg dealings");
                        }
                        if let Some(epoch) = rows.iter().map(|row| row.0).max() {
                            if epoch > newest_key {
                                newest_key = epoch;
                                // a record still pending needs its epoch's key to be signed
                                let pending =
                                    db.pending_epoch_records().first().map(|record| record.epoch);
                                let floor = newest_key
                                    .saturating_sub(ARCHIVE_EPOCHS)
                                    .min(pending.unwrap_or(Epoch::MAX));
                                if let Err(e) = db.prune_dkg_dealings(floor) {
                                    warn!(target: "epoch-manager", ?e, "cannot prune kept dkg dealings");
                                }
                            }
                        }
                        next = to + 1;
                        continue;
                    }
                    Ok(Err(e)) => {
                        warn!(target: "epoch-manager", ?e, from = next, to, "cannot scan blocks for dkg dealings: retrying");
                    }
                    Err(e) => {
                        warn!(target: "epoch-manager", ?e, "dkg dealing scan task failed: retrying");
                    }
                }
            }
            // wake at the next executed output, or every poll interval
            tokio::select! {
                _ = &shutdown => return,
                changed = executed.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                _ = tokio::time::sleep(DEALER_POLL) => {}
            }
        }
    });
}

/// How often the dealer task looks at the chain.
const DEALER_POLL: Duration = Duration::from_secs(5);
/// Polls between registration attempts while the key is still not registered.
const REGISTRATION_RETRY: u32 = 12;

/// This validator's part in the DKG for the life of epoch `epoch`: register its encryption key
/// once if genesis did not, and, when `epoch` deals and this node is in its committee, deal the
/// key of `epoch + 1` to the recipients the contract fixed. Transactions go to this node's own
/// worker pool, so its own batches carry them and no other validator can leave them out. The task
/// resubmits a transaction that leaves the pool without being executed, and ends when there is
/// nothing left to do or the epoch's tasks are dropped.
pub(super) fn spawn_dealer<S, DB>(
    spawner: TaskSpawner,
    engine: ExecutionNode,
    db: DB,
    signer: S,
    own_address: Address,
    epoch: Epoch,
    in_committee: bool,
) where
    S: BlsSigner,
    DB: ReDatabase,
{
    spawner.spawn_task("dkg dealer", async move {
        let mut dealer = Dealer::new(engine, signer, own_address, epoch).await;
        let deals_next = in_committee && deals(&dealer.engine, &db, epoch).await == Some(true);
        loop {
            match dealer.step(deals_next).await {
                Ok(true) => return,
                Ok(false) => {}
                Err(e) => warn!(target: "epoch-manager", epoch, ?e, "dkg dealer step failed"),
            }
            tokio::time::sleep(DEALER_POLL).await;
        }
    });
}

/// State of [`spawn_dealer`]'s task.
struct Dealer<S> {
    engine: ExecutionNode,
    signer: S,
    own_address: Address,
    epoch: Epoch,
    chain_id: u64,
    submitter: Option<DkgSubmitter>,
    registration: Option<TxHash>,
    polls_since_registration: u32,
    dealing: Option<TxHash>,
    warned_unfunded: bool,
}

impl<S: BlsSigner> Dealer<S> {
    async fn new(engine: ExecutionNode, signer: S, own_address: Address, epoch: Epoch) -> Self {
        let chain_id = engine.get_reth_env().await.dkg_chain_id();
        let submitter = DkgSubmitter::from_signature(
            &signer.request_signature_direct(&submitter_key_message()),
        )
        .inspect_err(|e| error!(target: "epoch-manager", ?e, "cannot derive the dkg submitter"))
        .ok();
        Self {
            engine,
            signer,
            own_address,
            epoch,
            chain_id,
            submitter,
            registration: None,
            polls_since_registration: 0,
            dealing: None,
            warned_unfunded: false,
        }
    }

    /// One look at the chain: `true` once nothing is left to do this epoch.
    async fn step(&mut self, deals_next: bool) -> eyre::Result<bool> {
        let Some(submitter) = self.submitter.clone() else { return Ok(true) };
        let env = self.engine.get_reth_env().await;
        if !env.dkg_manager_deployed()? {
            // a chain without DkgManager never deals
            return Ok(true);
        }
        let pool = self.engine.get_worker_transaction_pool(&0).await?;
        let pending = |hash: &Option<TxHash>| hash.is_some_and(|hash| pool.get(&hash).is_some());

        let own_key = self.signer.public_key();
        let (known, registered, dealt) = {
            let (env, own, epoch) = (env.clone(), self.own_address, self.epoch);
            tokio::task::spawn_blocking(move || {
                let known = env.dkg_is_known_validator(own)?;
                let registered = env.dkg_encryption_key(own)?.is_some();
                let dealt = env.dkg_dealers(epoch + 1)?.contains(&own);
                Ok::<_, eyre::Report>((known, registered, dealt))
            })
            .await??
        };

        if !known {
            // not a staked validator (yet): nothing to register or deal; look again later
            return Ok(false);
        }
        if !registered {
            // one attempt per REGISTRATION_RETRY polls: a registration that reverted would
            // otherwise be resent, and paid for, every poll
            self.polls_since_registration += 1;
            if !pending(&self.registration)
                && (self.registration.is_none()
                    || self.polls_since_registration >= REGISTRATION_RETRY)
            {
                self.polls_since_registration = 0;
                let decryption = decryption_key(&self.signer)?;
                let key = decryption.encryption_key();
                let proof =
                    decryption.prove(&own_key, &encryption_key_context(), &mut rand::rng())?;
                let signature = self
                    .signer
                    .request_signature_direct(&key_registration_message(self.chain_id, &key));
                let (nonce, fee) = self.nonce_and_fee(&env, &pool, &submitter)?;
                let tx = submitter.registration(
                    self.chain_id,
                    nonce,
                    fee,
                    self.own_address,
                    &key,
                    encode(&proof),
                    &signature,
                )?;
                self.registration = Some(self.submit(&pool, tx).await?);
                info!(target: "epoch-manager", epoch = self.epoch, submitter = %submitter.address(), "dkg encryption key registration submitted");
                dkg_hooks::crash_point(CrashPoint::RegistrationSubmitted, self.epoch);
            }
            // a key registered now is for later epochs; this epoch's recipients are fixed
        }
        if !deals_next {
            return Ok(registered);
        }
        // a dealing epoch's task runs for the whole epoch: an ejection changes the recipient set,
        // which voids accepted dealings and can give a keyless epoch a key, and both need a new
        // dealing
        if dealt {
            dkg_hooks::crash_point(CrashPoint::DealingAccepted, self.epoch);
            return Ok(false);
        }
        if pending(&self.dealing) {
            return Ok(false);
        }
        let next = self.epoch + 1;
        let (has_key, recipients) = {
            let env = env.clone();
            tokio::task::spawn_blocking(move || {
                Ok::<_, eyre::Report>((env.dkg_has_key(next)?, env.dkg_recipients(next)?))
            })
            .await??
        };
        if !has_key {
            debug!(target: "epoch-manager", epoch = next, "a committee member has no encryption key in time: no key to deal");
            return Ok(false);
        }
        if dkg_hooks::dealings_held() {
            debug!(target: "epoch-manager", epoch = next, "dealing held back by the test hook");
            return Ok(false);
        }
        let members = recipients.committee_size;
        let dealing = self.make_dealing(recipients).await?;
        let signature = self.signer.request_signature_direct(&dealing_message(
            self.chain_id,
            next,
            self.own_address,
            &dealing,
        ));
        dkg_hooks::crash_point(CrashPoint::DealingBuilt, self.epoch);
        let (nonce, fee) = self.nonce_and_fee(&env, &pool, &submitter)?;
        let tx = submitter.dealing(
            self.chain_id,
            nonce,
            fee,
            next,
            self.own_address,
            &signature,
            dealing,
            members,
        )?;
        self.dealing = Some(self.submit(&pool, tx).await?);
        info!(target: "epoch-manager", epoch = next, "dkg dealing submitted");
        dkg_hooks::crash_point(CrashPoint::DealingSubmitted, self.epoch);
        Ok(false)
    }

    /// A fresh dealing of the key of `epoch + 1` to `recipients`, encoded, at the threshold of
    /// the whole committee. Built on the blocking pool: it takes a few hundred milliseconds at
    /// full committee size.
    async fn make_dealing(&self, recipients: DkgRecipients) -> eyre::Result<Vec<u8>> {
        let context = dealing_context(self.chain_id, self.epoch + 1, self.own_address);
        tokio::task::spawn_blocking(move || {
            let threshold = quorum_threshold(recipients.committee_size as u64) as usize;
            let mut rng = rand::rng();
            let polynomial = SecretPolynomial::random(threshold, &mut rng)?;
            let dealing = Dealing::new(&context, &polynomial, &recipients.recipients, &mut rng)?;
            Ok::<_, eyre::Report>(encode(&dealing))
        })
        .await?
    }

    /// The next nonce (past any transaction of ours still pooled) and a fee cap of twice the
    /// canonical tip's base fee, with no priority fee: the dealer's own batches carry these
    /// transactions, so they need no tip to be included, and `DkgManager` refunds the gas at the
    /// base fee.
    fn nonce_and_fee(
        &mut self,
        env: &RethEnv,
        pool: &WorkerTxPool,
        submitter: &DkgSubmitter,
    ) -> eyre::Result<(u64, u128)> {
        let address = submitter.address();
        if env.latest()?.account_balance(&address)?.unwrap_or_default().is_zero()
            && !self.warned_unfunded
        {
            warn!(target: "epoch-manager", %address, "the dkg submitter account has no funds: send it enough for one transaction so this validator can register and deal (gas is refunded)");
            self.warned_unfunded = true;
        }
        let nonce =
            env.account_nonce(address)? + pool.get_transactions_by_sender(address).len() as u64;
        let base_fee = env.canonical_tip().base_fee_per_gas.unwrap_or(1);
        let fee = u128::from(base_fee).saturating_mul(2).max(1);
        Ok((nonce, fee))
    }

    async fn submit(
        &self,
        pool: &WorkerTxPool,
        tx: rayls_infrastructure_types::TransactionSigned,
    ) -> eyre::Result<TxHash> {
        let hash = *tx.tx_hash();
        let pooled = recover_pooled_transaction(&tx.encoded_2718())?;
        pool.add_transaction_local(pooled).await?;
        Ok(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_storage::mem_db::MemDatabase;
    use rayls_infrastructure_types::{
        BlockHash, BlockNumHash, BlsKeypair, BlsPublicKey, CommitteeBuilder,
    };

    fn record(
        epoch: Epoch,
        keys: &[BlsPublicKey],
        boundary: BlockHash,
        closing: u64,
    ) -> EpochRecord {
        EpochRecord {
            epoch,
            committee: keys.to_vec(),
            next_committee: keys.to_vec(),
            parent_hash: Default::default(),
            parent_state: BlockNumHash::new(closing, Default::default()),
            parent_consensus: boundary,
        }
    }

    #[test]
    fn the_gate_answers_from_the_fork_alone_when_it_can() {
        for epoch in [0, 1, 7] {
            assert_eq!(deals_without_reads(&ForkCondition::Never, epoch), Some(false));
            assert_eq!(deals_without_reads(&ForkCondition::Block(0), epoch), Some(true));
        }
        assert_eq!(deals_without_reads(&ForkCondition::Block(100), 0), Some(false));
        assert_eq!(deals_without_reads(&ForkCondition::Block(100), 3), None);
    }

    #[test]
    fn the_gate_reads_an_epochs_first_block_from_the_previous_record() {
        let db = MemDatabase::default();
        assert_eq!(first_block_from_records(&db, 5), None);
        // The epoch-0 placeholder (no cert, not pending) states no block.
        db.save_epoch_record(&EpochRecord { epoch: 0, ..Default::default() }).unwrap();
        assert_eq!(first_block_from_records(&db, 1), None);
        db.save_pending_epoch_record(&record(4, &[], BlockHash::default(), 999)).unwrap();
        assert_eq!(first_block_from_records(&db, 5), Some(1000));
        let fork = ForkCondition::Block(1000);
        assert!(fork.active_at_block(first_block_from_records(&db, 5).unwrap()));
        assert!(!ForkCondition::Block(1001).active_at_block(1000));
    }

    /// A committee of `keys` for `epoch`, one vote each.
    fn committee_over(epoch: Epoch, keys: &[BlsPublicKey]) -> Committee {
        let mut builder = CommitteeBuilder::new(epoch);
        for key in keys {
            builder.add_authority(*key, 1, Default::default());
        }
        let committee = builder.build();
        committee.load();
        committee
    }

    /// Around the epoch the fork activates in, the registry and the records give the gate the
    /// same answer. Only a fork block one past the block that closed epoch `X`, or one after
    /// that, tells an off-by-one in either source apart.
    #[test]
    fn the_gate_agrees_from_the_registry_and_from_records_around_activation() {
        const X: Epoch = 5;
        let closing: [(Epoch, u64); 3] = [(X - 1, 100), (X, 200), (X + 1, 300)];
        let db = MemDatabase::default();
        for (epoch, closed) in closing {
            db.save_pending_epoch_record(&record(epoch, &[], BlockHash::default(), closed))
                .unwrap();
        }
        let first_block = |epoch: Epoch| {
            closing.iter().find(|(closed, _)| *closed + 1 == epoch).map(|(_, block)| block + 1)
        };
        let no_records = MemDatabase::default();
        for (fork_block, expected) in [(201, [false, true, true]), (202, [false, false, true])] {
            let fork = ForkCondition::Block(fork_block);
            for (epoch, expected) in (X..=X + 2).zip(expected) {
                let registry = Ok(first_block(epoch));
                assert_eq!(
                    deals_from(&fork, epoch, registry, &no_records),
                    Some(expected),
                    "registry, epoch {epoch}, fork at {fork_block}"
                );
                assert_eq!(
                    deals_from(&fork, epoch, Ok(None), &db),
                    Some(expected),
                    "records, epoch {epoch}, fork at {fork_block}"
                );
                assert_eq!(
                    deals_from(&fork, epoch, Err(eyre::eyre!("registry down")), &db),
                    Some(expected),
                    "records after a registry error, epoch {epoch}, fork at {fork_block}"
                );
            }
        }
        assert_eq!(
            deals_from(&ForkCondition::Block(201), X + 3, Ok(None), &db),
            None,
            "no source states the first block"
        );
    }

    /// The scheme follows the outcome: none is pending, a fallback is aggregate, a key switches
    /// the committee to threshold certificates, and a key whose threshold is not the committee's
    /// quorum stays pending rather than falling back.
    #[test]
    fn epoch_scheme_follows_the_outcome() {
        let keys: Vec<BlsPublicKey> =
            (0..4).map(|_| *BlsKeypair::generate(&mut rand::rng()).public()).collect();
        let committee = committee_over(7, &keys);
        assert!(epoch_scheme(&committee, None, None).is_pending());
        assert!(committee.is_threshold_pending());
        let aggregate = epoch_scheme(&committee, Some(DkgOutcome::Fallback), None);
        assert!(matches!(aggregate, EpochScheme::Aggregate));
        assert!(!committee.is_threshold_pending() && committee.threshold_key().is_none());

        let polynomial = SecretPolynomial::random(3, &mut rand::rng()).unwrap();
        let key = EpochKey {
            epoch: 7,
            public: polynomial.commit().unwrap(),
            dealers: vec![],
            members: vec![],
        };
        let threshold = epoch_scheme(&committee, Some(DkgOutcome::Key(key.clone())), None);
        assert!(threshold.threshold().is_some_and(|t| t.share.is_none()));
        assert_eq!(committee.threshold_key().unwrap().group_key(), key.group_key());

        let wrong = SecretPolynomial::random(2, &mut rand::rng()).unwrap();
        let wrong = EpochKey {
            epoch: 7,
            public: wrong.commit().unwrap(),
            dealers: vec![],
            members: vec![],
        };
        assert!(epoch_scheme(&committee, Some(DkgOutcome::Key(wrong)), None).is_pending());
    }

    #[test]
    fn pending_outcome_backoff_doubles_from_ten_seconds_to_ten_minutes() {
        let secs: Vec<u64> =
            (1..=9).map(|attempt| pending_outcome_backoff(attempt).as_secs()).collect();
        assert_eq!(secs, vec![10, 20, 40, 80, 160, 320, 600, 600, 600]);
    }
}
