//! Participation projection over a stored `ConsensusHeader`: which authorities
//! contributed a certificate to the committed sub-dag, decoded without fully
//! deserializing each certificate's payload, parents, or signature bytes.
//!
//! [`ConsensusHeaderMeta`] `skip`s the `sub_dag.certificates` vector entirely
//! because it only needs the leader. This module reads the same bytes but
//! projects out each certificate's `header.author` instead of skipping it —
//! the participation signal for the hybrid reward tally
//! (`rayls_middleware_rewards::ConsensusRewardsCounter::tally_hybrid`) is
//! already on disk in every `ConsensusBlocks` entry, requiring no
//! consensus/networking change.

use crate::{
    bcs_layout::{BcsCursor, BcsLayoutError},
    AuthorityIdentifier, Certificate, Epoch, Round, B256,
};

/// Leader + participant projection of a stored `ConsensusHeader`.
#[derive(Debug, Clone)]
pub struct ConsensusHeaderParticipation {
    /// Identifier of the leader that committed the sub-dag.
    pub leader_author: AuthorityIdentifier,
    /// Leader certificate's round number.
    pub leader_round: Round,
    /// Leader certificate's epoch.
    pub leader_epoch: Epoch,
    /// Authors of every certificate included in the committed sub-dag, in
    /// on-disk order (may repeat an author across the flattened DAG history;
    /// callers wanting a per-round participant set should dedupe).
    pub participants: Vec<AuthorityIdentifier>,
}

impl ConsensusHeaderParticipation {
    /// Decode the leader + participants projection from a BCS-encoded
    /// `ConsensusHeader`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BcsLayoutError> {
        let mut c = BcsCursor::new(bytes);
        // parent_hash
        c.skip::<B256>()?;
        // sub_dag.certificates: Vec<Certificate>, author-projected per element.
        let n = c.read_len()?;
        let mut participants = Vec::with_capacity(n);
        for _ in 0..n {
            participants.push(read_certificate_author(&mut c)?);
        }
        // sub_dag.leader: read the first three header fields, stop early.
        let leader_author = c.read::<AuthorityIdentifier>()?;
        let leader_round = c.read::<Round>()?;
        let leader_epoch = c.read::<Epoch>()?;
        Ok(Self { leader_author, leader_round, leader_epoch, participants })
    }
}

/// Project a single BCS-encoded `Certificate` down to its `header.author`,
/// advancing the cursor exactly as far as `Certificate::skip` would.
///
/// Delegates to the ONE canonical [`Certificate::skip_with_header_span`] —
/// pinned by the `bcs_layout` skip-consumes-exact tests and the
/// `ConsensusHeaderChainMeta` digest round-trip tests — rather than
/// re-listing the `Header`/`Certificate` fields here. A field added to
/// either struct updates that canonical skip (and its pins) without any
/// change to this decode; a hand-rolled copy would silently desync and
/// corrupt the tally, which feeds both the state root (via `applyIncentives`)
/// and `withdrawals_root`. The author is the first field of the header, so it
/// sits at the start of the returned header span.
fn read_certificate_author(c: &mut BcsCursor<'_>) -> Result<AuthorityIdentifier, BcsLayoutError> {
    let header_span = Certificate::skip_with_header_span(c)?;
    BcsCursor::new(header_span).read::<AuthorityIdentifier>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CertificateDigest, CommittedSubDag, ConsensusHeader, HeaderBuilder, ReputationScores,
        WorkerId,
    };
    use alloy::primitives::BlockHash as AlloyBlockHash;
    use indexmap::IndexMap;
    use std::collections::BTreeSet;

    fn make_cert(
        author: u8,
        round: u32,
        epoch: u32,
        payload: Vec<(AlloyBlockHash, WorkerId)>,
        parents: usize,
    ) -> crate::Certificate {
        let mut p = IndexMap::<AlloyBlockHash, WorkerId>::new();
        for (d, w) in payload {
            p.insert(d, w);
        }
        let mut parents_set = BTreeSet::<CertificateDigest>::new();
        for i in 0..parents {
            parents_set.insert(CertificateDigest::new([i as u8; 32]));
        }
        let header = HeaderBuilder::default()
            .author(AuthorityIdentifier::dummy_for_test(author))
            .round(round)
            .epoch(epoch)
            .created_at(0)
            .payload(p)
            .parents(parents_set)
            .build();
        let mut cert = crate::Certificate::default();
        cert.update_header_for_test(header);
        cert
    }

    fn make_header(leader: crate::Certificate, certs: Vec<crate::Certificate>) -> ConsensusHeader {
        let sub_dag = CommittedSubDag::new(certs, leader, 0, ReputationScores::default(), None);
        ConsensusHeader {
            parent_hash: AlloyBlockHash::repeat_byte(0xAB),
            sub_dag,
            number: 100,
            extra: AlloyBlockHash::ZERO,
        }
    }

    #[test]
    fn extracts_leader_and_participants() {
        let leader = make_cert(0x77, 9, 3, vec![], 0);
        let certs = vec![
            make_cert(0x01, 8, 3, vec![(AlloyBlockHash::repeat_byte(2), 1)], 0),
            make_cert(0x02, 8, 3, vec![(AlloyBlockHash::repeat_byte(3), 2)], 1),
            make_cert(0x03, 8, 3, vec![], 4),
        ];
        let h = make_header(leader, certs);
        let bytes = crate::encode(&h);

        let m = ConsensusHeaderParticipation::from_bytes(&bytes).unwrap();
        assert_eq!(m.leader_author, AuthorityIdentifier::dummy_for_test(0x77));
        assert_eq!(m.leader_round, 9);
        assert_eq!(m.leader_epoch, 3);
        assert_eq!(
            m.participants,
            vec![
                AuthorityIdentifier::dummy_for_test(0x01),
                AuthorityIdentifier::dummy_for_test(0x02),
                AuthorityIdentifier::dummy_for_test(0x03),
            ]
        );
    }

    #[test]
    fn empty_sub_dag_has_no_participants() {
        let leader = make_cert(0xEE, 14, 1, vec![(AlloyBlockHash::repeat_byte(1), 0)], 2);
        let h = make_header(leader, vec![]);
        let bytes = crate::encode(&h);

        let m = ConsensusHeaderParticipation::from_bytes(&bytes).unwrap();
        assert_eq!(m.leader_author, AuthorityIdentifier::dummy_for_test(0xEE));
        assert!(m.participants.is_empty());
    }

    /// Same fixture as `ConsensusHeaderMeta`'s test of the same name: proves
    /// the two projections agree on the leader fields when reading identical
    /// bytes, one skipping the certs vector and one decoding it.
    #[test]
    fn agrees_with_leader_only_projection() {
        let leader = make_cert(0xEE, 14, 1, vec![(AlloyBlockHash::repeat_byte(1), 0)], 2);
        let certs = vec![
            make_cert(0x01, 13, 1, vec![(AlloyBlockHash::repeat_byte(2), 1)], 0),
            make_cert(0x02, 13, 1, vec![(AlloyBlockHash::repeat_byte(3), 2)], 1),
        ];
        let h = make_header(leader, certs);
        let bytes = crate::encode(&h);

        let leader_only = crate::ConsensusHeaderMeta::from_bytes(&bytes).unwrap();
        let with_participants = ConsensusHeaderParticipation::from_bytes(&bytes).unwrap();

        assert_eq!(leader_only.leader_author, with_participants.leader_author);
        assert_eq!(leader_only.leader_round, with_participants.leader_round);
        assert_eq!(leader_only.leader_epoch, with_participants.leader_epoch);
        assert_eq!(
            with_participants.participants,
            vec![
                AuthorityIdentifier::dummy_for_test(0x01),
                AuthorityIdentifier::dummy_for_test(0x02),
            ]
        );
    }
}
