//! Threshold-certificate key generation at the epoch level.
//!
//! An epoch deals the next epoch's key when the dealing gate ([`deals`]) says so: the epoch is not
//! 0 and the `ThresholdCertificates` fork is active at its first block. Its committee puts deals
//! and complaints in headers, and every node replays the epoch's committed headers through
//! [`DealingReplay`] to get the same [`DkgOutcome`]. The outcome is persisted keyed by the epoch
//! the key is for; [`ensure_dkg_outcome`] computes it on demand from durable data only (the
//! registry, epoch records and committed headers), so a restart anywhere computes the same row.

use crate::engine::ExecutionNode;
use rayls_execution_evm::{ForkCondition, RaylsHardFork, RaylsHardforks as _};
use rayls_infrastructure_config::DealtEpochs;
use rayls_infrastructure_storage::{ConsensusStore, EpochStore};
use rayls_infrastructure_types::{
    dkg::{DealingParams, DealingReplay, DkgOutcome, EpochScheme, EpochThreshold},
    BlockHash, BlsPublicKey, BlsSignature, BlsSigner, Committee, ConsensusHeader,
    Database as ReDatabase, Epoch, EpochRecord, Signer,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tracing::{debug, error, info, warn};

/// Lets the DKG ask a [`BlsSigner`] for decryption keys without exposing the secret key.
pub(super) struct KeySigner<'a, S>(pub(super) &'a S);

impl<S: BlsSigner> Signer for KeySigner<'_, S> {
    fn sign(&self, msg: &[u8]) -> BlsSignature {
        self.0.request_signature_direct(msg)
    }
}

/// Dealing parameters for the key of `epoch + 1`, dealt to `next_keys` (bitmap order).
pub(super) fn dealing_params(epoch: Epoch, next_keys: Vec<BlsPublicKey>) -> DealingParams {
    DealingParams::new(epoch + 1, next_keys)
}

/// The dealing gate's answer when it needs no block: epoch 0 never deals, a fork that never
/// activates never, and one active from genesis always. `None`: the epoch's first block decides.
pub(super) fn deals_without_reads(fork: &ForkCondition, epoch: Epoch) -> Option<bool> {
    if epoch == 0 {
        return Some(false);
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
    if threshold_fork(engine).await == ForkCondition::Never {
        return DealtEpochs::never();
    }
    let mut known = BTreeMap::new();
    for epoch in current.saturating_sub(3)..=current {
        if let Some(dealt) = deals(engine, db, epoch).await {
            known.insert(epoch, dealt);
        }
    }
    DealtEpochs::from_known(known)
}

/// What one epoch's DKG is computed from: the dealing epoch's committees and its boundary.
#[derive(Clone, Debug)]
pub(super) struct DealingInputs {
    /// The epoch the key is for; the one before it deals.
    pub(super) epoch: Epoch,
    /// The dealing committee's keys in bitmap order.
    pub(super) dealers: Vec<BlsPublicKey>,
    /// The recipient committee's keys in bitmap order.
    pub(super) recipients: Vec<BlsPublicKey>,
    /// Hash of the dealing epoch's boundary, its last consensus header.
    pub(super) boundary: BlockHash,
}

impl DealingInputs {
    /// The inputs a dealing epoch's record states: its committee deals to the next committee up
    /// to its boundary.
    pub(super) fn from_record(record: &EpochRecord) -> Self {
        Self {
            epoch: record.epoch + 1,
            dealers: record.committee.clone(),
            recipients: record.next_committee.clone(),
            boundary: record.parent_consensus,
        }
    }
}

/// Number of the dealing epoch's first consensus header: one past the previous epoch's boundary
/// as that epoch's record states it, else found by walking back from `boundary` while the leader
/// epoch matches. `None` when a header the walk needs is missing.
fn first_dealing_header<DB: ConsensusStore + EpochStore>(
    db: &DB,
    dealing_epoch: Epoch,
    boundary: &ConsensusHeader,
) -> Option<u64> {
    let previous_boundary = dealing_epoch
        .checked_sub(1)
        .and_then(|previous| local_record(db, previous))
        .and_then(|record| db.get_canonical_consensus_by_hash(record.parent_consensus));
    if let Some(previous) = previous_boundary {
        if previous.sub_dag.leader_epoch() < dealing_epoch && previous.number < boundary.number {
            return Some(previous.number + 1);
        }
    }
    let mut first = boundary.number;
    while first > 1 {
        let header = db.get_consensus_by_number(first - 1)?;
        if header.sub_dag.leader_epoch() != dealing_epoch {
            break;
        }
        first -= 1;
    }
    Some(first)
}

/// Compute the outcome for `inputs.epoch` from the dealing epoch's committed headers in the local
/// DB, walking up from its first header to the boundary. `Ok(None)` when the boundary or a header
/// in between is missing; an error when the headers contradict the inputs.
///
/// Deterministic: the same committed headers give the same outcome on every node and at every
/// restart. Cost: every header of the dealing epoch is read and fully decoded, deals included.
pub(super) fn compute_dkg_outcome<DB: ConsensusStore + EpochStore>(
    db: &DB,
    inputs: &DealingInputs,
) -> eyre::Result<Option<DkgOutcome>> {
    let dealing_epoch =
        inputs.epoch.checked_sub(1).ok_or_else(|| eyre::eyre!("epoch 0 has no dealing epoch"))?;
    let Some(boundary) = db.get_canonical_consensus_by_hash(inputs.boundary) else {
        return Ok(None);
    };
    if boundary.sub_dag.leader_epoch() != dealing_epoch {
        return Err(eyre::eyre!(
            "boundary {} leads in epoch {}, not the dealing epoch {dealing_epoch}",
            inputs.boundary,
            boundary.sub_dag.leader_epoch(),
        ));
    }
    let Some(first) = first_dealing_header(db, dealing_epoch, &boundary) else {
        return Ok(None);
    };
    let started = std::time::Instant::now();
    let mut replay = DealingReplay::new(inputs.epoch, &inputs.dealers, inputs.recipients.clone());
    for number in first..boundary.number {
        let Some(header) = db.get_consensus_by_number(number) else {
            return Ok(None);
        };
        if header.sub_dag.leader_epoch() != dealing_epoch {
            return Err(eyre::eyre!(
                "consensus header {number} leads in epoch {}, inside dealing epoch {dealing_epoch}",
                header.sub_dag.leader_epoch(),
            ));
        }
        replay.feed(&header);
    }
    replay.feed(&boundary);
    let outcome = replay.outcome(boundary.sub_dag.leader_round());
    info!(
        target: "epoch-manager",
        epoch = inputs.epoch,
        headers = replay.headers,
        deals = replay.deals,
        complaints = replay.complaints,
        threshold = matches!(outcome, DkgOutcome::Key(_)),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "dkg outcome computed from committed headers",
    );
    Ok(Some(outcome))
}

/// The DKG outcome for `epoch`: the stored row, else computed and saved.
///
/// Epochs 0 and 1 are legacy. When `dealt` says `epoch - 1` did not deal, `Fallback` is saved
/// without reading headers; when it did, the outcome is computed from `inputs`, else from the
/// dealing epoch's record. `Ok(None)`, with nothing saved, when that cannot be done yet: the gate
/// cannot tell, or the inputs or headers are missing. Errors are save failures.
pub(super) fn ensure_dkg_outcome<DB: ConsensusStore + EpochStore>(
    db: &DB,
    epoch: Epoch,
    dealt: Option<bool>,
    inputs: Option<DealingInputs>,
) -> eyre::Result<Option<DkgOutcome>> {
    if let Some(outcome) = db.get_dkg_outcome(epoch) {
        return Ok(Some(outcome));
    }
    if epoch <= 1 {
        return Ok(Some(DkgOutcome::Fallback));
    }
    let outcome = match dealt {
        None => return Ok(None),
        Some(false) => DkgOutcome::Fallback,
        Some(true) => {
            let inputs = inputs
                .or_else(|| local_record(db, epoch - 1).map(|r| DealingInputs::from_record(&r)));
            let Some(inputs) = inputs else { return Ok(None) };
            match compute_dkg_outcome(db, &inputs) {
                Ok(Some(outcome)) => outcome,
                Ok(None) => return Ok(None),
                Err(e) => {
                    error!(target: "epoch-manager", epoch, ?e, "cannot compute dkg outcome");
                    return Ok(None);
                }
            }
        }
    };
    db.save_dkg_outcome(epoch, &outcome)
        .map_err(|e| eyre::eyre!("failed to persist dkg outcome for epoch {epoch}: {e}"))?;
    Ok(Some(outcome))
}

/// [`ensure_dkg_outcome`] with the gate answered from the chain and the computation on the
/// blocking pool. `boundary` is the dealing epoch's boundary when the caller holds it (an epoch
/// transition or a recovery checkpoint), so the outcome does not wait for the record: the
/// committees then come from the registry, which still describes both epochs.
pub(super) async fn ensure_dkg_outcome_with_chain<DB: ReDatabase>(
    engine: &ExecutionNode,
    db: &DB,
    epoch: Epoch,
    boundary: Option<BlockHash>,
) -> eyre::Result<Option<DkgOutcome>> {
    if let Some(outcome) = db.get_dkg_outcome(epoch) {
        return Ok(Some(outcome));
    }
    if epoch <= 1 {
        return Ok(Some(DkgOutcome::Fallback));
    }
    let dealt = deals(engine, db, epoch - 1).await;
    let mut inputs = None;
    if let (Some(true), Some(boundary)) = (dealt, boundary) {
        match (
            engine.validators_for_epoch(epoch - 1).await,
            engine.validators_for_epoch(epoch).await,
        ) {
            (Ok(dealers), Ok(recipients)) => {
                inputs = Some(DealingInputs { epoch, dealers, recipients, boundary });
            }
            (Err(e), _) | (_, Err(e)) => {
                warn!(target: "epoch-manager", epoch, ?e, "no dkg committees from the registry");
            }
        }
    }
    let db = db.clone();
    tokio::task::spawn_blocking(move || ensure_dkg_outcome(&db, epoch, dealt, inputs)).await?
}

/// The current epoch's certificate scheme from its DKG outcome. A key switches the committee to
/// threshold certificates and derives this node's share; a node whose share cannot be derived (a
/// qualified dealer sent it a bad one) runs without one and cannot vote this epoch. No outcome
/// yet makes the scheme `Pending`: the node neither votes nor verifies under the epoch's scheme.
pub(super) fn epoch_scheme<S: BlsSigner>(
    committee: &Committee,
    outcome: Option<DkgOutcome>,
    own_key: &BlsPublicKey,
    signer: &S,
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
    if key.public.threshold() != committee.quorum_threshold() as usize {
        committee.set_threshold_pending(false);
        error!(target: "epoch-manager", epoch, "dkg key threshold does not match the committee quorum");
        return EpochScheme::Aggregate;
    }
    committee.set_threshold_key(key.public.clone());
    let share = committee.bls_position(own_key).and_then(|position| {
        key.derive_share(position, &KeySigner(signer))
            .inspect_err(|e| {
                error!(target: "epoch-manager", epoch, ?e, "cannot derive threshold share");
            })
            .ok()
    });
    info!(target: "epoch-manager", epoch, has_share = share.is_some(), "epoch uses threshold certificates");
    EpochScheme::Threshold(Arc::new(EpochThreshold { key, share }))
}

/// Backoff before retry `attempt` (1-based) of a pending DKG outcome: 10 s doubling to 10 min.
pub(super) fn pending_outcome_backoff(attempt: u32) -> Duration {
    const BASE: Duration = Duration::from_secs(10);
    const MAX: Duration = Duration::from_secs(600);
    BASE.checked_mul(1u32 << attempt.saturating_sub(1).min(31)).unwrap_or(MAX).min(MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_storage::mem_db::MemDatabase;
    use rayls_infrastructure_types::{
        dkg::{dealers_needed, Deal, DkgPayload, DkgState, EpochKey},
        threshold::SecretPolynomial,
        AuthorityIdentifier, BlockNumHash, BlsKeypair, Certificate, CommittedSubDag,
        CommitteeBuilder, Header, Round,
    };
    use std::collections::HashMap;

    /// Minimal [`BlsSigner`] over a test key pair.
    #[derive(Clone)]
    struct TestSigner(Arc<BlsKeypair>);

    impl BlsSigner for TestSigner {
        fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature {
            self.0.sign(msg)
        }

        fn public_key(&self) -> BlsPublicKey {
            *self.0.public()
        }
    }

    /// The pre-gate computation, kept to prove the new walk gives the same outcome: walk back from
    /// the boundary while the leader epoch matches, then feed [`DkgState`] oldest first.
    fn reference_outcome<DB: ConsensusStore>(
        db: &DB,
        params: &DealingParams,
        dealer_keys: &[BlsPublicKey],
        boundary_hash: BlockHash,
    ) -> DkgOutcome {
        let dealing_epoch = params.epoch - 1;
        let boundary = db.get_canonical_consensus_by_hash(boundary_hash).unwrap();
        let final_round = boundary.sub_dag.leader_round();
        let mut headers = vec![boundary];
        loop {
            let number = headers.last().unwrap().number;
            let Some(prev) = number.checked_sub(1).and_then(|n| db.get_consensus_by_number(n))
            else {
                break;
            };
            if prev.sub_dag.leader_epoch() != dealing_epoch {
                break;
            }
            headers.push(prev);
        }
        headers.reverse();
        let positions: HashMap<AuthorityIdentifier, u32> = dealer_keys
            .iter()
            .enumerate()
            .map(|(position, key)| (AuthorityIdentifier::from(*key), position as u32))
            .collect();
        let mut state =
            DkgState::new(params.config(dealer_keys.len(), dealers_needed(dealer_keys.len())));
        for header in &headers {
            let round = header.sub_dag.leader_round();
            for cert in &header.sub_dag.certificates {
                let payload = cert.header().dkg();
                if payload.is_empty() {
                    continue;
                }
                let Some(&dealer) = positions.get(cert.header().author()) else { continue };
                if let Some(deal) = &payload.deal {
                    let _ = state.on_deal(dealer, round, deal.clone());
                }
                for complaint in &payload.complaints {
                    let _ = state.on_complaint(round, complaint);
                }
            }
        }
        match state.outcome(final_round) {
            Some(key) => DkgOutcome::Key(key),
            None => DkgOutcome::Fallback,
        }
    }

    /// Four validators dealing epoch `EPOCH`'s key during `EPOCH - 1`, whose consensus headers
    /// are numbers `FIRST..=BOUNDARY`; the headers around them lead in the epochs next to it.
    struct Seeded {
        db: MemDatabase,
        keypairs: Vec<Arc<BlsKeypair>>,
        keys: Vec<BlsPublicKey>,
        boundary: ConsensusHeader,
    }

    const EPOCH: Epoch = 3;
    const FIRST: u64 = 11;
    const BOUNDARY: u64 = 40;

    fn header_with(
        committee: &Committee,
        number: u64,
        epoch: Epoch,
        round: Round,
        certs: Vec<(BlsPublicKey, DkgPayload)>,
    ) -> ConsensusHeader {
        let certificate = |author: BlsPublicKey, dkg: DkgPayload| {
            let header = Header {
                author: AuthorityIdentifier::from(author),
                round,
                epoch,
                created_at: number,
                ..Default::default()
            }
            .with_dkg(dkg);
            Certificate::new_unsigned_for_test(committee, header, vec![]).unwrap()
        };
        let leader_key = certs.first().map(|(key, _)| *key).unwrap_or(committee.bls_keys()[0]);
        let leader = certificate(leader_key, DkgPayload::default());
        let certificates = certs.into_iter().map(|(key, dkg)| certificate(key, dkg)).collect();
        let sub_dag = CommittedSubDag::new(certificates, leader, number, Default::default(), None);
        ConsensusHeader { number, sub_dag, ..Default::default() }
    }

    /// Seeds the dealing epoch: dealers 0 and 1 deal at round 24 and dealer 3 at round 76 (too
    /// late to qualify before the boundary's round 80), dealer 0 deals again at round 28 (only the
    /// first counts), and the epochs before and after hold deals that must not count.
    fn seeded() -> Seeded {
        let mut rng = rand::rng();
        let keypairs: Vec<BlsKeypair> = (0..4).map(|_| BlsKeypair::generate(&mut rng)).collect();
        let mut builder = CommitteeBuilder::new(EPOCH - 1);
        for keypair in &keypairs {
            builder.add_authority(*keypair.public(), 1, Default::default());
        }
        let committee = builder.build();
        committee.load();
        let keys = committee.bls_keys();
        let params = DealingParams::new(EPOCH, keys.clone());
        let deal = |dealer: u32, epoch: Epoch| DkgPayload {
            deal: Some(
                Deal::new(epoch, dealer, params.threshold, &keys, &mut rand::rng()).unwrap(),
            ),
            complaints: vec![],
        };
        let db = MemDatabase::default();
        let mut boundary = None;
        for number in 1..=BOUNDARY + 3 {
            let epoch = match number {
                n if n < FIRST => EPOCH - 2,
                n if n <= BOUNDARY => EPOCH - 1,
                _ => EPOCH,
            };
            let round = number as Round * 2;
            let certs = match number {
                n if n == FIRST - 1 => vec![(keys[2], deal(2, EPOCH))],
                n if n == FIRST + 1 => vec![(keys[0], deal(0, EPOCH)), (keys[1], deal(1, EPOCH))],
                n if n == FIRST + 3 => vec![(keys[0], deal(0, EPOCH))],
                n if n == BOUNDARY - 2 => vec![(keys[3], deal(3, EPOCH))],
                n if n == BOUNDARY + 1 => vec![(keys[2], deal(2, EPOCH))],
                _ => vec![],
            };
            let header = header_with(&committee, number, epoch, round, certs);
            db.write_subdag_for_test(number, header.sub_dag.clone());
            if number == BOUNDARY {
                boundary = db.get_consensus_by_number(number);
            }
        }
        let keypairs = keypairs.into_iter().map(Arc::new).collect();
        Seeded { db, keypairs, keys, boundary: boundary.unwrap() }
    }

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
            assert_eq!(deals_without_reads(&ForkCondition::Block(0), epoch), Some(epoch > 0));
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

    #[test]
    fn ensure_uses_a_stored_row_and_treats_epochs_0_and_1_as_legacy() {
        let db = MemDatabase::default();
        for epoch in [0, 1] {
            assert_eq!(
                ensure_dkg_outcome(&db, epoch, None, None).unwrap(),
                Some(DkgOutcome::Fallback)
            );
            assert_eq!(db.get_dkg_outcome(epoch), None, "nothing saved for a legacy epoch");
        }
        db.save_dkg_outcome(5, &DkgOutcome::Fallback).unwrap();
        // The stored row wins over a gate that would compute.
        assert_eq!(
            ensure_dkg_outcome(&db, 5, Some(true), None).unwrap(),
            Some(DkgOutcome::Fallback)
        );
    }

    #[test]
    fn ensure_saves_fallback_for_a_non_dealing_epoch_and_waits_when_it_cannot_tell() {
        let db = MemDatabase::default();
        // No record and no headers: nothing is read for a non-dealing epoch.
        assert_eq!(
            ensure_dkg_outcome(&db, 4, Some(false), None).unwrap(),
            Some(DkgOutcome::Fallback)
        );
        assert_eq!(db.get_dkg_outcome(4), Some(DkgOutcome::Fallback));
        assert_eq!(ensure_dkg_outcome(&db, 6, None, None).unwrap(), None);
        // Dealing, but no record of the dealing epoch.
        assert_eq!(ensure_dkg_outcome(&db, 6, Some(true), None).unwrap(), None);
        assert_eq!(db.get_dkg_outcome(6), None);
    }

    #[test]
    fn ensure_computes_the_same_outcome_as_the_pre_gate_walk() {
        let s = seeded();
        let boundary = s.boundary.digest();
        let params = DealingParams::new(EPOCH, s.keys.clone());
        let expected = reference_outcome(&s.db, &params, &s.keys, boundary);
        let DkgOutcome::Key(key) = &expected else { panic!("two dealers qualify") };
        assert_eq!(key.deals.iter().map(|(dealer, _)| *dealer).collect::<Vec<_>>(), vec![0, 1]);

        // From the dealing epoch's pending record, first header found by walking back.
        s.db.save_pending_epoch_record(&record(EPOCH - 1, &s.keys, boundary, 500)).unwrap();
        let outcome = ensure_dkg_outcome(&s.db, EPOCH, Some(true), None).unwrap();
        assert_eq!(outcome.as_ref(), Some(&expected));
        assert_eq!(s.db.get_dkg_outcome(EPOCH), Some(expected.clone()));

        // From explicit inputs, first header from the previous epoch's record.
        let db = seeded_like(&s);
        let previous_boundary = db.get_consensus_by_number(FIRST - 1).unwrap().digest();
        db.save_pending_epoch_record(&record(EPOCH - 2, &s.keys, previous_boundary, 400)).unwrap();
        let inputs = DealingInputs {
            epoch: EPOCH,
            dealers: s.keys.clone(),
            recipients: s.keys.clone(),
            boundary,
        };
        assert_eq!(compute_dkg_outcome(&db, &inputs).unwrap(), Some(expected));
    }

    /// A copy of the seeded headers in a fresh DB.
    fn seeded_like(s: &Seeded) -> MemDatabase {
        let db = MemDatabase::default();
        for number in 1..=BOUNDARY + 3 {
            db.write_subdag_for_test(number, s.db.get_consensus_by_number(number).unwrap().sub_dag);
        }
        db
    }

    #[test]
    fn ensure_saves_nothing_when_a_header_is_missing() {
        let s = seeded();
        let db = MemDatabase::default();
        for number in (1..=BOUNDARY).filter(|n| *n != FIRST + 5) {
            db.write_subdag_for_test(number, s.db.get_consensus_by_number(number).unwrap().sub_dag);
        }
        let previous_boundary = db.get_consensus_by_number(FIRST - 1).unwrap().digest();
        db.save_pending_epoch_record(&record(EPOCH - 2, &s.keys, previous_boundary, 400)).unwrap();
        db.save_pending_epoch_record(&record(EPOCH - 1, &s.keys, s.boundary.digest(), 500))
            .unwrap();
        assert_eq!(ensure_dkg_outcome(&db, EPOCH, Some(true), None).unwrap(), None);
        assert_eq!(db.get_dkg_outcome(EPOCH), None);
        // An unknown boundary is missing data too.
        let inputs = DealingInputs {
            epoch: EPOCH,
            dealers: s.keys.clone(),
            recipients: s.keys.clone(),
            boundary: BlockHash::repeat_byte(7),
        };
        assert_eq!(compute_dkg_outcome(&db, &inputs).unwrap(), None);
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

    /// A key switches the committee to threshold certificates and gives a recipient its share. A
    /// node outside the committee verifies without a share, and a key whose threshold is not the
    /// committee's quorum is refused.
    #[test]
    fn epoch_scheme_derives_the_share_from_a_key() {
        let s = seeded();
        s.db.save_pending_epoch_record(&record(EPOCH - 1, &s.keys, s.boundary.digest(), 500))
            .unwrap();
        let Some(DkgOutcome::Key(key)) =
            ensure_dkg_outcome(&s.db, EPOCH, Some(true), None).unwrap()
        else {
            panic!("two dealers qualify");
        };

        let committee = committee_over(EPOCH, &s.keys);
        let signer = TestSigner(s.keypairs[0].clone());
        let own_key = signer.public_key();
        assert!(epoch_scheme(&committee, None, &own_key, &signer).is_pending());
        assert!(committee.is_threshold_pending());

        let outcome = Some(DkgOutcome::Key(key.clone()));
        let EpochScheme::Threshold(threshold) =
            epoch_scheme(&committee, outcome.clone(), &own_key, &signer)
        else {
            panic!("a key gives threshold certificates");
        };
        let position = committee.bls_position(&own_key).unwrap();
        assert_eq!(
            threshold.share.as_ref().map(|share| share.public_key()),
            Some(key.share_key(position)),
            "the recipient derives its share"
        );
        assert_eq!(committee.threshold_key().as_deref(), Some(&key.public));
        assert!(!committee.is_threshold_pending());

        let outsider = TestSigner(Arc::new(BlsKeypair::generate(&mut rand::rng())));
        let EpochScheme::Threshold(threshold) = epoch_scheme(
            &committee_over(EPOCH, &s.keys),
            outcome,
            &outsider.public_key(),
            &outsider,
        ) else {
            panic!("a key gives threshold certificates");
        };
        assert!(threshold.share.is_none(), "a node outside the committee holds no share");

        let committee = committee_over(EPOCH, &s.keys);
        committee.set_threshold_pending(true);
        let quorum = committee.quorum_threshold() as usize;
        let public =
            SecretPolynomial::random(quorum + 1, &mut rand::rng()).unwrap().commit().unwrap();
        let wrong = EpochKey { public, ..key };
        assert!(matches!(
            epoch_scheme(&committee, Some(DkgOutcome::Key(wrong)), &own_key, &signer),
            EpochScheme::Aggregate
        ));
        assert!(!committee.is_threshold_pending() && committee.threshold_key().is_none());
    }

    #[test]
    fn epoch_scheme_follows_the_outcome() {
        let mut builder = CommitteeBuilder::new(EPOCH);
        for _ in 0..4 {
            builder.add_authority(
                *BlsKeypair::generate(&mut rand::rng()).public(),
                1,
                Default::default(),
            );
        }
        let committee = builder.build();
        committee.load();
        let signer = TestSigner(Arc::new(BlsKeypair::generate(&mut rand::rng())));
        let pending = epoch_scheme(&committee, None, &signer.public_key(), &signer);
        assert!(pending.is_pending() && committee.is_threshold_pending());
        let aggregate =
            epoch_scheme(&committee, Some(DkgOutcome::Fallback), &signer.public_key(), &signer);
        assert!(matches!(aggregate, EpochScheme::Aggregate));
        assert!(!committee.is_threshold_pending() && committee.threshold_key().is_none());
    }

    #[test]
    fn pending_outcome_backoff_doubles_from_ten_seconds_to_ten_minutes() {
        let secs: Vec<u64> =
            (1..=9).map(|attempt| pending_outcome_backoff(attempt).as_secs()).collect();
        assert_eq!(secs, vec![10, 20, 40, 80, 160, 320, 600, 600, 600]);
    }
}
