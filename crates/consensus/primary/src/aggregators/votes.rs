//! Aggregate votes after proposing a header.

use rayls_consensus_primary_metrics::PrimaryMetrics;
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_types::{
    dkg::EpochScheme,
    ensure,
    error::{CertificateError, DagError, DagResult},
    threshold::ShareIndex,
    to_intent_message, AuthorityIdentifier, BlsSignature, Certificate, Committee, Database, Header,
    ProtocolSignature, SignatureVerificationState, Votable, Vote, VotesAggregator,
};
use std::sync::Arc;
use tracing::trace;

/// This node's vote for `header`. In a threshold epoch the vote is a partial signature by the
/// node's share, and `None` when the node has no share (a qualified dealer sent it a bad one) or
/// has not computed the epoch key yet: a vote under any other key would not verify, so the node
/// does not vote until it can.
pub(crate) fn sign_vote<DB: Database>(
    config: &ConsensusConfig<DB>,
    header: &Header,
    author: AuthorityIdentifier,
) -> Option<Vote> {
    match config.scheme() {
        EpochScheme::Threshold(threshold) => {
            threshold.share.as_ref().map(|share| Vote::new_with_signer(header, author, share))
        }
        EpochScheme::Aggregate => Some(Vote::new(header, author, config.key_config())),
        EpochScheme::Pending => None,
    }
}

/// Aggregates votes for a particular header to form a certificate.
pub(crate) struct HeaderVotesAggregator {
    inner: VotesAggregator<Vote>,
    /// (authority, signature) pairs for certificate construction.
    verified_votes: Vec<(AuthorityIdentifier, BlsSignature)>,
    metrics: Arc<PrimaryMetrics>,
}

impl HeaderVotesAggregator {
    pub(crate) fn new(metrics: Arc<PrimaryMetrics>, committee: &Committee) -> Self {
        metrics.votes_received_last_round.set(0);

        Self {
            inner: VotesAggregator::new(committee.quorum_threshold()),
            verified_votes: Vec::new(),
            metrics,
        }
    }

    /// Append the vote to the collection.
    pub(crate) fn append(
        &mut self,
        vote: Vote,
        committee: &Committee,
        header: &Header,
    ) -> DagResult<Option<Certificate>> {
        let author = vote.voter_id();

        // ensure digest matches the header
        ensure!(vote.header_digest == header.digest(), DagError::InvalidHeaderDigest);

        // ensure this came from a committee member and that the signature is valid
        let Some(auth) = committee.authority(&author) else {
            return Err(DagError::UnknownAuthority(author.to_string()));
        };
        // In a threshold epoch a vote is a partial signature by the voter's share. It verifies
        // against the share key the epoch's public polynomial gives the voter's position, the
        // index `Certificate::new_unverified` combines it under. Until the polynomial is known no
        // key applies: the long-term key would build an aggregate certificate peers reject.
        ensure!(!committee.is_threshold_pending(), CertificateError::SchemePending.into());
        let key = match committee.threshold_key() {
            Some(polynomial) => {
                let position = committee
                    .bls_position_by_id(&author)
                    .ok_or_else(|| DagError::UnknownAuthority(author.to_string()))?;
                polynomial.share_key(ShareIndex::from_position(position))
            }
            None => *auth.protocol_key(),
        };
        ensure!(
            vote.signature().verify_secure(&to_intent_message(vote.header_digest), &key),
            DagError::InvalidSignature
        );

        // dedup + weight accumulation via generic aggregator
        let weight = committee.voting_power_by_id(&author);
        let sig = *vote.signature();
        let quorum = self
            .inner
            .append(vote, weight)
            .map_err(|_| DagError::AuthorityReuse(author.to_string()))?;

        self.verified_votes.push((author, sig));

        // update metrics
        self.metrics.votes_received_last_round.set(self.verified_votes.len() as i64);

        // check if this vote reaches quorum
        if quorum {
            let mut cert = Certificate::new_unverified(
                committee,
                header.clone(),
                self.verified_votes.clone(),
            )?;
            trace!(target: "primary::votes_aggregator", ?cert, "certificate verified");
            cert.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
                cert.aggregated_signature().ok_or(DagError::InvalidSignature)?,
            ));

            return Ok(Some(cert));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_config::{DealtEpochs, EpochKeysStatus};
    use rayls_infrastructure_storage::{mem_db::MemDatabase, EpochStore as _};
    use rayls_infrastructure_types::{
        dkg::{DkgOutcome, EpochKey, EpochThreshold},
        threshold::SecretPolynomial,
        BlsPublicKey, CertificateScheme, CommitteeBuilder, CommitteeLookahead, Epoch, EpochKeys,
    };
    use rayls_testing_test_utils_committee::CommitteeFixture;
    use std::collections::BTreeMap;

    /// A config for epoch `EPOCH` over the fixture's authorities, as the first authority.
    fn config_for(
        fixture: &CommitteeFixture<MemDatabase>,
        scheme: EpochScheme,
        dealt: DealtEpochs,
        lookahead: CommitteeLookahead,
    ) -> ConsensusConfig<MemDatabase> {
        config_at(fixture, EPOCH, scheme, dealt, lookahead)
    }

    /// A config for `epoch` over the fixture's authorities, as the first authority.
    fn config_at(
        fixture: &CommitteeFixture<MemDatabase>,
        epoch: Epoch,
        scheme: EpochScheme,
        dealt: DealtEpochs,
        lookahead: CommitteeLookahead,
    ) -> ConsensusConfig<MemDatabase> {
        let base = fixture.authorities().next().unwrap().consensus_config();
        let mut builder = CommitteeBuilder::new(epoch);
        for authority in fixture.authorities() {
            builder.add_authority(authority.primary_public_key(), 1, Default::default());
        }
        let committee = builder.build();
        committee.load();
        ConsensusConfig::new_for_epoch(
            base.config().clone(),
            base.node_storage().clone(),
            base.key_config().clone(),
            committee,
            lookahead,
            base.network_config().clone(),
            u64::MAX,
            true,
            scheme,
            dealt,
        )
        .unwrap()
    }

    const EPOCH: u32 = 5;

    /// Threshold material for `config`'s epoch: a fresh polynomial and this node's share of it.
    fn threshold_for(config: &ConsensusConfig<MemDatabase>) -> (EpochThreshold, BlsPublicKey) {
        let committee = config.committee();
        let polynomial =
            SecretPolynomial::random(committee.quorum_threshold() as usize, &mut rand::rng())
                .unwrap();
        let position = committee.bls_position(&config.key_config().primary_public_key()).unwrap();
        let share = polynomial.share(ShareIndex::from_position(position)).unwrap();
        let key = EpochKey {
            epoch: EPOCH,
            public: polynomial.commit().unwrap(),
            dealers: vec![],
            members: vec![],
        };
        let share_key = key.share_key(position);
        (EpochThreshold { key, share: Some(share) }, share_key)
    }

    /// A four-member committee switched to threshold certificates, with every member's vote for
    /// one header signed by its share of a freshly dealt polynomial.
    struct ThresholdSetup {
        fixture: CommitteeFixture<MemDatabase>,
        committee: Committee,
        polynomial: SecretPolynomial,
        header: Header,
        votes: Vec<Vote>,
    }

    fn threshold_setup() -> ThresholdSetup {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let threshold = committee.quorum_threshold() as usize;
        let polynomial = SecretPolynomial::random(threshold, &mut rand::rng()).unwrap();
        committee.set_threshold_key(polynomial.commit().unwrap());
        let header = fixture.header_from_last_authority();
        let votes = fixture
            .authorities()
            .map(|authority| {
                let position = committee.bls_position(&authority.primary_public_key()).unwrap();
                let share = polynomial.share(ShareIndex::from_position(position)).unwrap();
                Vote::new_with_signer(&header, authority.id(), &share)
            })
            .collect();
        ThresholdSetup { fixture, committee, polynomial, header, votes }
    }

    /// Feed `votes` to a fresh aggregator and return the certificate the last one completes.
    fn certify(committee: &Committee, header: &Header, votes: &[Vote]) -> Certificate {
        let mut aggregator =
            HeaderVotesAggregator::new(Arc::new(PrimaryMetrics::default()), committee);
        let mut certificate = None;
        for vote in votes {
            assert!(certificate.is_none(), "certified before the last vote");
            certificate = aggregator.append(vote.clone(), committee, header).unwrap();
        }
        certificate.expect("a quorum of share votes certifies the header")
    }

    #[test]
    fn different_quorums_of_share_votes_certify_identical_threshold_certificates() {
        let s = threshold_setup();
        let threshold = s.committee.quorum_threshold() as usize;
        assert_eq!(s.votes.len(), 4);
        let first = certify(&s.committee, &s.header, &s.votes[..threshold]);
        let second = certify(&s.committee, &s.header, &s.votes[s.votes.len() - threshold..]);

        // No signer set, and the same signature bytes whichever quorum signed.
        assert!(first.signed_authorities().is_empty());
        assert!(second.signed_authorities().is_empty());
        let signature = first.aggregated_signature().unwrap();
        assert_eq!(signature.to_bytes(), second.aggregated_signature().unwrap().to_bytes());
        // the whole certificate is the same bytes, its creation time the header's
        assert_eq!(first.created_at(), &s.header.created_at);
        assert_eq!(
            rayls_infrastructure_types::encode(&first),
            rayls_infrastructure_types::encode(&second)
        );
        let mut stamped = first.clone().validate_received().unwrap();
        stamped.update_created_at_for_test(s.header.created_at + 1);
        assert!(matches!(
            stamped.validate_and_verify(&s.committee),
            Err(CertificateError::UnexpectedCreatedAt)
        ));

        let group_key = s.polynomial.commit().unwrap().group_key();
        for certificate in [first, second] {
            let verified = certificate
                .clone()
                .verify_cert_with(&CertificateScheme::Threshold(&group_key))
                .unwrap();
            assert!(verified.is_verified());
            // A receiver resets the state the aggregator set, then verifies by the committee.
            let received = certificate.clone().validate_received().unwrap();
            assert!(received.validate_and_verify(&s.committee).unwrap().is_verified());
            // The scheme is the verifier's choice: under the aggregate scheme the empty signer
            // set is no quorum.
            assert!(certificate.verify_cert(&s.committee.bls_keys()).is_err());
        }
    }

    #[test]
    fn a_node_votes_only_once_its_pending_epoch_key_lands() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let header = fixture.header_from_last_authority();
        let me = fixture.authorities().next().unwrap().id();
        let config =
            config_for(&fixture, EpochScheme::Pending, DealtEpochs::never(), Default::default());
        assert!(config.scheme_pending());
        assert!(
            sign_vote(&config, &header, me.clone()).is_none(),
            "no long-term vote while pending"
        );

        // While the committee's key is pending the aggregator refuses every vote, so no aggregate
        // certificate can form in a threshold epoch.
        let committee = config.committee();
        committee.set_threshold_pending(true);
        let long_term = fixture.first_authority().vote(&header);
        let mut aggregator =
            HeaderVotesAggregator::new(Arc::new(PrimaryMetrics::default()), committee);
        assert!(matches!(
            aggregator.append(long_term, committee, &header),
            Err(DagError::Certificate(CertificateError::SchemePending))
        ));

        // The key lands: the committee and the config switch in place and the node votes with its
        // share.
        let (threshold, share_key) = threshold_for(&config);
        committee.set_threshold_key(threshold.key.public.clone());
        config.set_scheme(EpochScheme::Threshold(Arc::new(threshold)));
        assert!(!config.scheme_pending() && !committee.is_threshold_pending());
        let vote = sign_vote(&config, &header, me).expect("a share vote");
        assert!(vote.signature().verify_secure(&to_intent_message(vote.header_digest), &share_key));
    }

    #[test]
    fn epoch_keys_status_follows_the_scheme_the_rows_and_the_gate() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let keys = fixture.committee().bls_keys();
        let lookahead = CommitteeLookahead::from_entries(
            [0, EPOCH - 1, EPOCH + 1].map(|epoch| (epoch, keys.clone())).to_vec(),
        );
        let dealt = DealtEpochs::from_known(BTreeMap::from([(EPOCH - 2, true), (EPOCH, true)]));
        let config = config_for(&fixture, EpochScheme::Pending, dealt, lookahead.clone());
        let mut keys = config.committee().bls_keys();
        keys.sort();
        let ready = |group_key| EpochKeysStatus::Ready(EpochKeys { keys: keys.clone(), group_key });

        // The current epoch's key is pending, and so are the next epoch (this one deals and its
        // boundary has not stored the row yet) and the previous one (dealt, no row stored).
        assert_eq!(config.epoch_keys_status(EPOCH), EpochKeysStatus::Pending);
        assert_eq!(config.epoch_keys_status(EPOCH + 1), EpochKeysStatus::Pending);
        assert_eq!(config.epoch_keys_status(EPOCH - 1), EpochKeysStatus::Pending);
        assert_eq!(config.epoch_keys(EPOCH + 1), None);
        // No committee keys at all, and epoch 0, which no epoch dealt to, is legacy whatever
        // the gate says.
        assert_eq!(config.epoch_keys_status(EPOCH + 4), EpochKeysStatus::NoCommittee);
        assert_eq!(config.epoch_keys_status(0), ready(None));

        // The current key lands, and the boundary stores the next epoch's outcome.
        let (threshold, _) = threshold_for(&config);
        let group_key = threshold.key.group_key();
        config.set_scheme(EpochScheme::Threshold(Arc::new(threshold)));
        assert_eq!(config.epoch_keys_status(EPOCH), ready(Some(group_key)));
        config.node_storage().save_dkg_outcome(EPOCH + 1, &DkgOutcome::Fallback).unwrap();
        assert_eq!(config.epoch_keys_status(EPOCH + 1), ready(None));

        // Under a fork that never activates nothing is pending, as before threshold certificates.
        let never = config_for(&fixture, EpochScheme::Aggregate, DealtEpochs::never(), lookahead);
        assert_eq!(never.epoch_keys_status(EPOCH - 1), ready(None));
        assert_eq!(never.epoch_keys_status(EPOCH), ready(None));
    }

    /// A real activation: the epoch before `EPOCH` does not deal and `EPOCH` does. `EPOCH` stays
    /// aggregate, the next epoch is pending until `EPOCH`'s boundary stores its key, and every
    /// epoch below a known non-dealing one is aggregate.
    #[test]
    fn epoch_keys_status_across_an_activation() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let keys = fixture.committee().bls_keys();
        let lookahead = CommitteeLookahead::from_entries(
            [EPOCH - 2, EPOCH - 1, EPOCH + 1].map(|epoch| (epoch, keys.clone())).to_vec(),
        );
        let dealt = DealtEpochs::from_known(BTreeMap::from([(EPOCH - 1, false), (EPOCH, true)]));
        let config = config_for(&fixture, EpochScheme::Aggregate, dealt, lookahead);
        let mut sorted = config.committee().bls_keys();
        sorted.sort();
        let ready =
            |group_key| EpochKeysStatus::Ready(EpochKeys { keys: sorted.clone(), group_key });

        assert_eq!(config.epoch_keys_status(EPOCH), ready(None), "the first dealing epoch");
        assert_eq!(config.epoch_keys_status(EPOCH - 1), ready(None));
        assert_eq!(config.epoch_keys_status(EPOCH - 2), ready(None), "inferred from EPOCH - 1");
        assert_eq!(config.epoch_keys_status(EPOCH + 1), EpochKeysStatus::Pending);

        // A node still in the epoch before knows the gate's answers up to its own epoch, all
        // non-dealing. It reads the answer of the epoch before the one it checks, so the first
        // dealing epoch is aggregate for it too, and the one after is pending.
        let ahead = CommitteeLookahead::from_entries(
            [EPOCH, EPOCH + 1].map(|epoch| (epoch, keys.clone())).to_vec(),
        );
        let before =
            DealtEpochs::from_known((EPOCH - 4..EPOCH).map(|epoch| (epoch, false)).collect());
        let behind = config_at(&fixture, EPOCH - 1, EpochScheme::Aggregate, before, ahead.clone());
        assert_eq!(behind.epoch_keys_status(EPOCH), ready(None));
        assert_eq!(behind.epoch_keys_status(EPOCH + 1), EpochKeysStatus::Pending);

        // A gate that cannot tell leaves the epoch pending: assuming aggregate would reject honest
        // threshold certificates.
        let unknown = DealtEpochs::from_known(BTreeMap::from([(EPOCH + 1, true)]));
        let later = config_at(&fixture, EPOCH + 1, EpochScheme::Aggregate, unknown, ahead);
        assert_eq!(later.epoch_keys_status(EPOCH), EpochKeysStatus::Pending);

        let (threshold, _) = threshold_for(&config);
        let group_key = threshold.key.group_key();
        config.node_storage().save_dkg_outcome(EPOCH + 1, &DkgOutcome::Key(threshold.key)).unwrap();
        assert_eq!(config.epoch_keys_status(EPOCH + 1), ready(Some(group_key)));
    }

    #[test]
    fn threshold_votes_must_be_signed_by_the_voters_own_share() {
        let s = threshold_setup();
        let mut aggregator =
            HeaderVotesAggregator::new(Arc::new(PrimaryMetrics::default()), &s.committee);

        // A vote under the voter's long-term key does not verify against its share key.
        let long_term = s.fixture.first_authority().vote(&s.header);
        assert!(matches!(
            aggregator.append(long_term, &s.committee, &s.header),
            Err(DagError::InvalidSignature)
        ));

        // Neither does another member's share, claimed under this voter's id.
        let mut misattributed = s.votes[0].clone();
        misattributed.author = s.votes[1].author.clone();
        assert!(matches!(
            aggregator.append(misattributed, &s.committee, &s.header),
            Err(DagError::InvalidSignature)
        ));

        // The voter's own share vote is accepted.
        assert!(aggregator.append(s.votes[1].clone(), &s.committee, &s.header).unwrap().is_none());
    }
}
