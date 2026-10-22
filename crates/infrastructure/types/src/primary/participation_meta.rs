//! Participation projection over a stored `ConsensusHeader`: who took part in
//! the committed sub-dag, decoded without fully deserializing each
//! certificate's payload, parents, or signature bytes.
//!
//! [`ConsensusHeaderMeta`] `skip`s the `sub_dag.certificates` vector entirely
//! because it only needs the leader. This module reads the same bytes but
//! projects two things out of each certificate's header instead of skipping
//! it: its `author`, and (for a header written after the `HeaderParentSigners`
//! fork) the author's claim of who signed its own previous certificate. Both
//! feed the hybrid reward tally
//! (`rayls_middleware_rewards::ConsensusRewardsCounter::tally_hybrid`).
//!
//! The signer claims, not the certificates' own `signed_authorities` bitmaps,
//! are the participation signal: a certificate's signer set is outside every
//! commitment (the DAG keys certificates by header digest and the committed
//! sub-dag hashes those digests only), so two honest nodes can hold the same
//! certificate with different valid signer subsets, and crediting from them
//! would let one proposer split the state root. The claim sits inside the
//! signed header, which every node holds byte-identically.

use crate::{
    bcs_layout::{BcsCursor, BcsLayoutError},
    AuthorityIdentifier, Certificate, Epoch, Header, Round, B256,
};
use roaring::RoaringBitmap;

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
    /// Union of the own-parent signer claims carried by the sub-dag's
    /// certificate headers, as committee indices in BLS-key order (the index
    /// space of `Certificate::signed_authorities`). Empty when no header in the
    /// sub-dag carries a claim.
    pub attested_signers: RoaringBitmap,
    /// True when at least one certificate header in the sub-dag carries an
    /// own-parent signer claim, i.e. the sub-dag was committed with the
    /// `HeaderParentSigners` fork active. Distinguishes "nobody was attested"
    /// from "attestation did not exist yet".
    pub signers_attested: bool,
}

impl ConsensusHeaderParticipation {
    /// Decode the leader + participants projection from a BCS-encoded
    /// `ConsensusHeader`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BcsLayoutError> {
        let mut c = BcsCursor::new(bytes);
        // parent_hash
        c.skip::<B256>()?;
        // sub_dag.certificates: Vec<Certificate>, author- and claim-projected per element.
        let n = c.read_len()?;
        let mut participants = Vec::with_capacity(n);
        let mut attested_signers = RoaringBitmap::new();
        let mut signers_attested = false;
        for _ in 0..n {
            let header = project_certificate_header(&mut c)?;
            participants.push(header.author);
            if let Some(claim) = header.own_parent_signers {
                signers_attested = true;
                attested_signers |= claim;
            }
        }
        // sub_dag.leader: read the first three header fields, stop early.
        let leader_author = c.read::<AuthorityIdentifier>()?;
        let leader_round = c.read::<Round>()?;
        let (leader_epoch, _) = Header::split_wire_epoch(c.read::<Epoch>()?);
        Ok(Self {
            leader_author,
            leader_round,
            leader_epoch,
            participants,
            attested_signers,
            signers_attested,
        })
    }
}

/// Project a single BCS-encoded `Certificate` down to its header's `author`
/// and own-parent signer claim, advancing the cursor exactly as far as
/// `Certificate::skip` would.
///
/// Delegates to the ONE canonical [`Certificate::skip_with_header_span`] —
/// pinned by the `bcs_layout` skip-consumes-exact tests and the
/// `ConsensusHeaderChainMeta` digest round-trip tests — and to
/// [`Header::project_wire`] over the span it returns, rather than re-listing
/// the `Header`/`Certificate` fields here. A field added to either struct
/// updates those canonical walks (and their pins) without any change to this
/// decode; a hand-rolled copy would silently desync and corrupt the tally,
/// which feeds both the state root (via `applyIncentives`) and
/// `withdrawals_root`.
fn project_certificate_header(
    c: &mut BcsCursor<'_>,
) -> Result<crate::HeaderWireMeta, BcsLayoutError> {
    let header_span = Certificate::skip_with_header_span(c)?;
    Header::project_wire(header_span)
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
        make_cert_with_claim(author, round, epoch, payload, parents, None)
    }

    fn make_cert_with_claim(
        author: u8,
        round: u32,
        epoch: u32,
        payload: Vec<(AlloyBlockHash, WorkerId)>,
        parents: usize,
        own_parent_signers: Option<RoaringBitmap>,
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
            .own_parent_signers(own_parent_signers.map(crate::OwnParentSigners::unsigned_for_test))
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

    fn bitmap(indices: &[u32]) -> RoaringBitmap {
        RoaringBitmap::from_sorted_iter(indices.iter().copied()).unwrap()
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
        assert!(!m.signers_attested);
        assert!(m.attested_signers.is_empty());
    }

    #[test]
    fn empty_sub_dag_has_no_participants() {
        let leader = make_cert(0xEE, 14, 1, vec![(AlloyBlockHash::repeat_byte(1), 0)], 2);
        let h = make_header(leader, vec![]);
        let bytes = crate::encode(&h);

        let m = ConsensusHeaderParticipation::from_bytes(&bytes).unwrap();
        assert_eq!(m.leader_author, AuthorityIdentifier::dummy_for_test(0xEE));
        assert!(m.participants.is_empty());
        assert!(!m.signers_attested);
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

    /// Post-fork headers: the projection unions the own-parent signer claims
    /// across the sub-dag, keeps authorship intact, and the leader fields read
    /// through the flagged epoch unchanged.
    #[test]
    fn unions_own_parent_signer_claims_across_the_sub_dag() {
        let leader = make_cert_with_claim(0x77, 9, 3, vec![], 0, Some(bitmap(&[0, 1])));
        let certs = vec![
            make_cert_with_claim(
                0x01,
                8,
                3,
                vec![(AlloyBlockHash::repeat_byte(2), 1)],
                0,
                Some(bitmap(&[0, 1, 2])),
            ),
            make_cert_with_claim(0x02, 8, 3, vec![], 1, Some(bitmap(&[1, 3]))),
            // an empty claim: the author's previous certificate was not among its parents
            make_cert_with_claim(0x03, 8, 3, vec![], 4, Some(RoaringBitmap::new())),
        ];
        let h = make_header(leader, certs);
        let bytes = crate::encode(&h);

        let m = ConsensusHeaderParticipation::from_bytes(&bytes).unwrap();
        assert_eq!(m.leader_author, AuthorityIdentifier::dummy_for_test(0x77));
        assert_eq!(m.leader_round, 9);
        assert_eq!(m.leader_epoch, 3, "the wire flag must not leak into the epoch");
        assert_eq!(m.participants.len(), 3);
        assert!(m.signers_attested);
        assert_eq!(m.attested_signers, bitmap(&[0, 1, 2, 3]));

        // the leader-only projection reads the same flagged bytes
        let leader_only = crate::ConsensusHeaderMeta::from_bytes(&bytes).unwrap();
        assert_eq!(leader_only.leader_epoch, 3);
        let (epoch, _) = crate::leader_epoch_and_batch_digests(&bytes).unwrap();
        assert_eq!(epoch, 3);
    }

    /// A sub-dag mixing pre- and post-fork headers (only possible in tests;
    /// the fork switches per epoch) still reports attestation and the union.
    #[test]
    fn a_single_claim_marks_the_sub_dag_attested() {
        let leader = make_cert(0x77, 9, 3, vec![], 0);
        let certs = vec![
            make_cert(0x01, 8, 3, vec![], 0),
            make_cert_with_claim(0x02, 8, 3, vec![], 0, Some(bitmap(&[2]))),
        ];
        let bytes = crate::encode(&make_header(leader, certs));
        let m = ConsensusHeaderParticipation::from_bytes(&bytes).unwrap();
        assert!(m.signers_attested);
        assert_eq!(m.attested_signers, bitmap(&[2]));
    }
}
