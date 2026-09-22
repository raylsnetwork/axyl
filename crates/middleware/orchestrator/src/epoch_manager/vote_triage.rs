//! Triage of gossiped epoch votes.
//!
//! Committee members gossip an [`EpochVote`](rayls_infrastructure_types::EpochVote) at every
//! epoch close and every node queues them on `ConsensusBus::new_epoch_votes`. The queue is only
//! read while a "Collect Epoch Signatures" task runs, so a node that did not collect for a close
//! (it fetched the record directly on the catch-up fast path, or it was not an active validator)
//! keeps that close's votes. The next collection then reads them first and, before this triage,
//! fed them to the "alternate record" aggregator, which aborted the collection with
//! `Reached quorum on epoch record X instead of Y`.
//!
//! These helpers decide what a vote for a record digest other than the one being collected
//! means. They are pure so the decisions can be tested without a database or a network.

use rayls_infrastructure_types::{
    error::HeaderError, BlsPublicKey, Database, Epoch, EpochVote, RaylsReceiver, B256,
};
use std::collections::VecDeque;
use tokio::sync::oneshot;

/// What a vote for a digest other than the record being collected means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForeignVote {
    /// The digest belongs to a record of another epoch: stale gossip from an earlier close.
    /// It says nothing about the epoch being collected, so it is acked and ignored.
    Stale {
        /// Epoch of the record the vote is for.
        epoch: Epoch,
    },
    /// The digest belongs to *this* epoch: a genuine competing record, which may reach an
    /// alternate quorum and abort the collection.
    Competing,
    /// The digest is unknown locally; the caller has to resolve it (ask a peer) to decide.
    Unknown,
}

/// Classify a foreign digest from what the local database knows about it.
///
/// `local` is `(epoch of the record with that digest, certificate present)` as returned by
/// `EpochStore::get_epoch_by_hash`, or `None` when the digest is unknown here.
pub(crate) fn classify_foreign_digest(
    local: Option<(Epoch, bool)>,
    current_epoch: Epoch,
) -> ForeignVote {
    match local {
        // A record of another epoch: stale, whether or not we also hold its certificate.
        Some((epoch, _)) if epoch != current_epoch => ForeignVote::Stale { epoch },
        // Same epoch, different digest: a real fork candidate.
        Some(_) => ForeignVote::Competing,
        None => ForeignVote::Unknown,
    }
}

/// Classify a record that was fetched from a peer by its digest.
pub(crate) fn classify_fetched_record(record_epoch: Epoch, current_epoch: Epoch) -> ForeignVote {
    if record_epoch == current_epoch {
        ForeignVote::Competing
    } else {
        ForeignVote::Stale { epoch: record_epoch }
    }
}

/// True when a queued vote can be dropped (acked with `Ok`) without losing anything: the record
/// it votes for is already certified locally, so the vote can neither complete a quorum we still
/// need nor prove a competing record.
pub(crate) fn vote_is_settled(local: Option<(Epoch, bool)>) -> bool {
    matches!(local, Some((_, true)))
}

/// Whether a record fetched by digest is a backfill candidate: it belongs to an epoch older than
/// the one being collected and we do not already hold it certified.
///
/// The record itself is validated by `rayls_consensus_state_sync::epoch_record_valid`, the same
/// check the epoch record collector applies, so this only decides *whether* to consider it.
pub(crate) fn backfill_candidate(
    record_epoch: Epoch,
    current_epoch: Epoch,
    already_certified: bool,
) -> bool {
    record_epoch < current_epoch && !already_certified
}

/// A queued epoch vote with the channel the gossip handler waits on.
pub(crate) type QueuedVote = (EpochVote, oneshot::Sender<Result<(), HeaderError>>);

/// Take everything currently queued, ack and drop the settled votes, and return the rest in
/// arrival order.
///
/// Settled votes are gossip for a record this node already holds certified - left over from a
/// close it did not collect for - and are exactly what used to abort the next collection. They
/// are acked with `Ok(())` so the peers that sent them are not penalised.
pub(crate) fn split_settled_votes<R, DB>(rx: &mut R, db: &DB) -> (usize, VecDeque<QueuedVote>)
where
    R: RaylsReceiver<QueuedVote>,
    DB: Database,
{
    use rayls_infrastructure_storage::EpochStore as _;

    let mut kept = VecDeque::new();
    let mut settled = 0;
    while let Ok((vote, vote_tx)) = rx.try_recv() {
        if vote_is_settled(
            db.get_epoch_by_hash(vote.epoch_hash).map(|(rec, cert)| (rec.epoch, cert.is_some())),
        ) {
            let _ = vote_tx.send(Ok(()));
            settled += 1;
        } else {
            kept.push_back((vote, vote_tx));
        }
    }
    (settled, kept)
}

/// What the collection loop should do with a vote it just read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VoteAction {
    /// A committee signature for the record being collected: count it.
    Count,
    /// Stale gossip from an earlier close: ack it and keep collecting.
    IgnoreStale {
        /// Epoch of the record the vote is for.
        epoch: Epoch,
    },
    /// A different digest for the same epoch (or one that could not be placed): it may reach the
    /// alternate-record quorum.
    Alternate,
    /// The digest is unknown locally: resolve it over the network and decide again with the
    /// result.
    NeedsResolve,
    /// Not a committee member, or a bad signature: reject as before.
    Reject,
}

/// Decide what to do with one vote.
///
/// `local` is what `EpochStore::get_epoch_by_hash` knows about the vote's digest, and `resolved`
/// the outcome of a network resolution when one has already been made (or `None` if not).
pub(crate) fn triage_vote(
    vote: &EpochVote,
    epoch_hash: B256,
    current_epoch: Epoch,
    committee: &[BlsPublicKey],
    local: Option<(Epoch, bool)>,
    resolved: Option<ForeignVote>,
) -> VoteAction {
    if !committee.contains(&vote.public_key) || !vote.check_signature() {
        return VoteAction::Reject;
    }
    if vote.epoch_hash == epoch_hash {
        return VoteAction::Count;
    }
    let class = match classify_foreign_digest(local, current_epoch) {
        // Nothing local to go on: use the network answer if we have one.
        ForeignVote::Unknown => match resolved {
            Some(resolved) => resolved,
            None => return VoteAction::NeedsResolve,
        },
        known => known,
    };
    match class {
        ForeignVote::Stale { epoch } => VoteAction::IgnoreStale { epoch },
        // A competing record for this epoch, or a digest nobody could place: treat it as the
        // collection always did, so a genuine fork is still detected.
        ForeignVote::Competing | ForeignVote::Unknown => VoteAction::Alternate,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng as _};
    use rayls_consensus_primary::QueChannel;
    use rayls_infrastructure_storage::{mem_db::MemDatabase, EpochStore as _};
    use rayls_infrastructure_types::{
        BlsKeypair, BlsSignature, BlsSigner, EpochCertificate, EpochRecord, RaylsSender as _,
        Signer as _, VotesAggregator,
    };
    use std::sync::Arc;

    /// Minimal [`BlsSigner`] so tests can produce real, verifiable votes.
    #[derive(Clone)]
    struct TestSigner(Arc<BlsKeypair>);

    impl TestSigner {
        fn new(rng: &mut StdRng) -> Self {
            Self(Arc::new(BlsKeypair::generate(rng)))
        }
    }

    impl BlsSigner for TestSigner {
        fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature {
            self.0.sign(msg)
        }

        fn public_key(&self) -> BlsPublicKey {
            *self.0.public()
        }
    }

    fn record_for(epoch: Epoch, committee: &[BlsPublicKey], parent: B256) -> EpochRecord {
        EpochRecord {
            epoch,
            committee: committee.to_vec(),
            next_committee: committee.to_vec(),
            parent_hash: parent,
            ..Default::default()
        }
    }

    /// Store `record` with a certificate. Only the *presence* of the certificate matters here.
    fn store_certified(db: &MemDatabase, record: &EpochRecord, signer: &TestSigner) {
        let vote = record.sign_vote(signer);
        let cert = EpochCertificate {
            epoch_hash: record.digest(),
            signature: vote.signature,
            signed_authorities: roaring::RoaringBitmap::new(),
        };
        db.save_epoch_record_with_cert(record, &cert).unwrap();
    }

    // The abort this issue is about: a vote for a record of an earlier epoch must never be
    // treated as a competing record for the epoch being collected.
    #[test]
    fn older_epoch_digest_is_stale() {
        assert_eq!(classify_foreign_digest(Some((36, true)), 38), ForeignVote::Stale { epoch: 36 });
        // A record we hold without its certificate is still another epoch's record.
        assert_eq!(
            classify_foreign_digest(Some((36, false)), 38),
            ForeignVote::Stale { epoch: 36 }
        );
        // A later epoch (we fetched ahead) is equally not evidence about this one.
        assert_eq!(classify_foreign_digest(Some((40, true)), 38), ForeignVote::Stale { epoch: 40 });
    }

    // A different digest for the SAME epoch is the only case an alternate quorum may fire on.
    #[test]
    fn same_epoch_digest_is_competing() {
        assert_eq!(classify_foreign_digest(Some((38, true)), 38), ForeignVote::Competing);
        assert_eq!(classify_foreign_digest(Some((38, false)), 38), ForeignVote::Competing);
    }

    #[test]
    fn unknown_digest_needs_resolving() {
        assert_eq!(classify_foreign_digest(None, 38), ForeignVote::Unknown);
    }

    #[test]
    fn fetched_record_is_classified_by_its_epoch() {
        assert_eq!(classify_fetched_record(38, 38), ForeignVote::Competing);
        assert_eq!(classify_fetched_record(36, 38), ForeignVote::Stale { epoch: 36 });
        assert_eq!(classify_fetched_record(39, 38), ForeignVote::Stale { epoch: 39 });
    }

    // Only a certified record settles a vote: a record without its certificate may still need
    // the queued votes to build one.
    #[test]
    fn only_certified_records_settle_votes() {
        assert!(vote_is_settled(Some((36, true))));
        assert!(!vote_is_settled(Some((36, false))));
        assert!(!vote_is_settled(None));
    }

    #[test]
    fn backfill_candidates_are_older_uncertified_epochs() {
        assert!(backfill_candidate(36, 38, false));
        assert!(!backfill_candidate(36, 38, true), "already held certified");
        assert!(!backfill_candidate(38, 38, false), "the epoch being collected is not a backfill");
        assert!(!backfill_candidate(39, 38, false), "a later epoch is not a backfill");
    }

    #[test]
    fn triage_counts_committee_votes_for_the_record_being_collected() {
        let mut rng = StdRng::seed_from_u64(7);
        let signer = TestSigner::new(&mut rng);
        let committee = vec![signer.public_key()];
        let record = record_for(38, &committee, B256::ZERO);
        let vote = record.sign_vote(&signer);

        assert_eq!(
            triage_vote(&vote, record.digest(), 38, &committee, None, None),
            VoteAction::Count,
        );
    }

    #[test]
    fn triage_rejects_outsiders_and_bad_signatures() {
        let mut rng = StdRng::seed_from_u64(8);
        let signer = TestSigner::new(&mut rng);
        let outsider = TestSigner::new(&mut rng);
        let committee = vec![signer.public_key()];
        let record = record_for(38, &committee, B256::ZERO);

        let foreign_vote = record.sign_vote(&outsider);
        assert_eq!(
            triage_vote(&foreign_vote, record.digest(), 38, &committee, None, None),
            VoteAction::Reject,
        );

        let mut forged = record.sign_vote(&signer);
        forged.epoch_hash = B256::repeat_byte(4);
        assert_eq!(
            triage_vote(&forged, record.digest(), 38, &committee, None, None),
            VoteAction::Reject,
            "a signature that does not match the digest is rejected, not triaged",
        );
    }

    #[test]
    fn triage_ignores_stale_votes_and_resolves_unknown_digests() {
        let mut rng = StdRng::seed_from_u64(9);
        let signer = TestSigner::new(&mut rng);
        let committee = vec![signer.public_key()];
        let current = record_for(38, &committee, B256::ZERO);
        let older = record_for(36, &committee, B256::ZERO);
        let stale_vote = older.sign_vote(&signer);

        // Known locally as another epoch's record: stale.
        assert_eq!(
            triage_vote(&stale_vote, current.digest(), 38, &committee, Some((36, true)), None),
            VoteAction::IgnoreStale { epoch: 36 },
        );
        // Unknown locally: the loop has to ask a peer first.
        assert_eq!(
            triage_vote(&stale_vote, current.digest(), 38, &committee, None, None),
            VoteAction::NeedsResolve,
        );
        // Resolved as another epoch's record: stale after all.
        assert_eq!(
            triage_vote(
                &stale_vote,
                current.digest(),
                38,
                &committee,
                None,
                Some(ForeignVote::Stale { epoch: 36 }),
            ),
            VoteAction::IgnoreStale { epoch: 36 },
        );
        // Unresolvable: the old alternate-record path, so a real fork is still detected.
        assert_eq!(
            triage_vote(
                &stale_vote,
                current.digest(),
                38,
                &committee,
                None,
                Some(ForeignVote::Unknown)
            ),
            VoteAction::Alternate,
        );
        // A competing record for this very epoch always reaches the alternate path.
        let competing = record_for(38, &committee, B256::repeat_byte(1));
        let competing_vote = competing.sign_vote(&signer);
        assert_eq!(
            triage_vote(&competing_vote, current.digest(), 38, &committee, Some((38, false)), None),
            VoteAction::Alternate,
        );
    }

    /// The live failure, in order: three stale votes queued ahead of the current epoch's votes.
    /// Before the triage they reached the alternate-record quorum and aborted the collection;
    /// now they are ignored and the current votes still reach quorum.
    #[test]
    fn stale_votes_queued_first_do_not_stop_the_current_quorum() {
        let mut rng = StdRng::seed_from_u64(11);
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();
        let mut committee: Vec<BlsPublicKey> = signers.iter().map(|s| s.public_key()).collect();
        committee.sort_unstable();

        let older = record_for(36, &committee, B256::ZERO);
        let current = record_for(38, &committee, B256::repeat_byte(2));
        let epoch_hash = current.digest();
        let quorum = current.super_quorum();

        // Queue order: every stale vote first, then the votes for this close.
        let queued: Vec<EpochVote> = signers
            .iter()
            .take(3)
            .map(|s| older.sign_vote(s))
            .chain(signers.iter().map(|s| current.sign_vote(s)))
            .collect();

        let mut alt = VotesAggregator::<EpochVote>::new(quorum as u64);
        let mut counted = 0usize;
        let mut ignored = 0usize;
        for vote in &queued {
            // The stale record is held certified here, exactly as on a node that took the
            // fast path at the earlier close.
            let local = (vote.epoch_hash == older.digest()).then_some((36, true));
            match triage_vote(vote, epoch_hash, 38, &committee, local, None) {
                VoteAction::Count => counted += 1,
                VoteAction::IgnoreStale { epoch } => {
                    assert_eq!(epoch, 36);
                    ignored += 1;
                }
                VoteAction::Alternate => {
                    assert!(
                        !alt.append(*vote, 1).unwrap_or(false),
                        "stale votes must never reach an alternate quorum",
                    );
                }
                other => panic!("unexpected action {other:?}"),
            }
        }

        assert_eq!(ignored, 3, "all stale votes ignored");
        assert!(counted >= quorum, "the current epoch still reaches quorum: {counted} >= {quorum}");
    }

    #[tokio::test]
    async fn settled_votes_are_dropped_and_the_rest_kept_in_order() {
        let mut rng = StdRng::seed_from_u64(13);
        let signer = TestSigner::new(&mut rng);
        let committee = vec![signer.public_key()];
        let db = MemDatabase::default();

        let older = record_for(36, &committee, B256::ZERO);
        store_certified(&db, &older, &signer);
        let current = record_for(38, &committee, B256::repeat_byte(2));
        let other = record_for(38, &committee, B256::repeat_byte(3));

        let queue: QueChannel<QueuedVote> = QueChannel::new();
        let mut acks = Vec::new();
        for record in [&older, &current, &older, &other] {
            let (tx, rx) = oneshot::channel();
            queue.send((record.sign_vote(&signer), tx)).await.unwrap();
            acks.push(rx);
        }

        let mut receiver = queue.subscribe();
        let (settled, kept) = split_settled_votes(&mut receiver, &db);

        assert_eq!(settled, 2, "both votes for the record we already hold certified are dropped");
        let kept: Vec<B256> = kept.iter().map(|(vote, _)| vote.epoch_hash).collect();
        assert_eq!(
            kept,
            vec![current.digest(), other.digest()],
            "unsettled votes are kept, in arrival order",
        );
        // The dropped votes are acked, so the peers that gossiped them are not punished.
        assert!(matches!(acks.remove(0).await, Ok(Ok(()))));
        acks.remove(1); // the second `older` vote, same ack
    }

    #[tokio::test]
    async fn splitting_an_empty_queue_is_a_no_op() {
        let db = MemDatabase::default();
        let queue: QueChannel<QueuedVote> = QueChannel::new();
        let mut receiver = queue.subscribe();
        let (settled, kept) = split_settled_votes(&mut receiver, &db);
        assert_eq!(settled, 0);
        assert!(kept.is_empty());
    }
}
