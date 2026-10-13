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
    /// Committee indices of every authority that signed at least one certificate in the
    /// committed sub-dag, ascending and deduplicated.
    ///
    /// This is the availability signal the hybrid reward tally credits: an authority appears
    /// here when its vote reached a certificate that was committed, whether or not it authored
    /// a committed certificate itself. A latency-distant validator routinely fails to get its
    /// own certificate committed while still signing others', so authorship under-counts it.
    ///
    /// Indices address authorities **sorted by BLS public key** — see
    /// [`Certificate::skip_with_header_and_signers_span`] for why that is not
    /// `Committee::authorities()` order.
    pub signers: Vec<u32>,
}

impl ConsensusHeaderParticipation {
    /// Decode the leader + participants projection from a BCS-encoded
    /// `ConsensusHeader`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BcsLayoutError> {
        let mut c = BcsCursor::new(bytes);
        // parent_hash
        c.skip::<B256>()?;
        // sub_dag.certificates: Vec<Certificate>, author- and signer-projected per element.
        let n = c.read_len()?;
        let mut participants = Vec::with_capacity(n);
        let mut signers = RoaringBitmap::new();
        for _ in 0..n {
            let (author, cert_signers) = read_certificate_author_and_signers(&mut c)?;
            participants.push(author);
            signers |= cert_signers;
        }
        // sub_dag.leader: read the first three header fields, stop early.
        let leader_author = c.read::<AuthorityIdentifier>()?;
        let leader_round = c.read::<Round>()?;
        let leader_epoch = c.read::<Epoch>()?;
        Ok(Self {
            leader_author,
            leader_round,
            leader_epoch,
            participants,
            // ascending and deduplicated by construction: the union is a bitmap
            signers: signers.into_iter().collect(),
        })
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
fn read_certificate_author_and_signers(
    c: &mut BcsCursor<'_>,
) -> Result<(AuthorityIdentifier, RoaringBitmap), BcsLayoutError> {
    let (header_span, signers_span) = Certificate::skip_with_header_and_signers_span(c)?;
    let author = BcsCursor::new(header_span).read::<AuthorityIdentifier>()?;
    // `signed_authorities` is a serialized roaring bitmap; an undecodable one means the stored
    // certificate is corrupt, which must surface rather than silently crediting nobody.
    let signers = RoaringBitmap::deserialize_from(signers_span)
        .map_err(|_| BcsLayoutError::InvalidBitmap { type_name: "Certificate" })?;
    Ok((author, signers))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Address, BlsKeypair, BlsSignature, Committee, CommitteeBuilder};
    use crate::{
        CertificateDigest, CommittedSubDag, ConsensusHeader, HeaderBuilder, ReputationScores,
        WorkerId,
    };
    use alloy::primitives::BlockHash as AlloyBlockHash;
    use indexmap::IndexMap;
    use rand::{rngs::StdRng, SeedableRng};
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
        // No certificates means no signatures to union: the tally must credit nobody rather than
        // fall back to the leader, who signed a certificate the sub-dag does not carry.
        assert!(m.signers.is_empty());
    }

    /// Four authorities with random BLS keys, so identifier order and BLS-key order disagree.
    fn signing_committee() -> Committee {
        // Fixed seed: the permutation between the two orderings must not vary run to run, or a
        // genuine index-space regression would only fail intermittently.
        let mut rng = StdRng::seed_from_u64(0x246);
        let mut builder = CommitteeBuilder::new(3);
        for i in 0..4u8 {
            let keypair = BlsKeypair::generate(&mut rng);
            builder.add_authority(*keypair.public(), 1, Address::with_last_byte(i + 1));
        }
        builder.build()
    }

    fn make_signed_cert(
        committee: &Committee,
        author: AuthorityIdentifier,
        round: u32,
        signers: &[AuthorityIdentifier],
    ) -> crate::Certificate {
        let header = HeaderBuilder::default()
            .author(author)
            .round(round)
            .epoch(3)
            .created_at(0)
            .payload(IndexMap::new())
            .parents(BTreeSet::new())
            .build();
        let votes = signers.iter().map(|id| (id.clone(), BlsSignature::default())).collect();
        crate::Certificate::new_unsigned_for_test(committee, header, votes).unwrap()
    }

    /// Pins the signer projection against the production full-decode path, [`Certificate::signed_by`].
    ///
    /// The bitmap carries bare indices, so resolving it against the wrong committee ordering
    /// returns a plausible set of authorities rather than an error — the tally would silently pay
    /// the wrong validators. Comparing resolved identities (not index values) against the
    /// canonical decode is what makes that failure visible.
    #[test]
    fn signers_resolve_to_the_authorities_the_full_decode_reports() {
        let committee = signing_committee();
        let bls_keys = committee.bls_keys();
        let ids: Vec<AuthorityIdentifier> =
            committee.authorities().iter().map(|a| a.id()).collect();
        let ids_in_bls_order: Vec<AuthorityIdentifier> =
            bls_keys.iter().map(|k| committee.authority_by_key(k).unwrap().id()).collect();
        assert_ne!(
            ids, ids_in_bls_order,
            "fixture no longer exercises the permutation the projection has to get right",
        );

        // Disjoint-ish signer subsets, so the union is neither one certificate's set nor the
        // whole committee — both of which a broken resolve could produce by accident.
        let certs = vec![
            make_signed_cert(&committee, ids[0].clone(), 8, &[ids[0].clone(), ids[1].clone()]),
            make_signed_cert(&committee, ids[1].clone(), 8, &[ids[1].clone(), ids[2].clone()]),
        ];
        let leader =
            make_signed_cert(&committee, ids[2].clone(), 9, &[ids[2].clone(), ids[3].clone()]);

        let expected: BTreeSet<AuthorityIdentifier> = certs
            .iter()
            .flat_map(|c| c.signed_by(&bls_keys).1)
            .map(|key| committee.authority_by_key(&key).unwrap().id())
            .collect();

        let bytes = crate::encode(&make_header(leader, certs));
        let m = ConsensusHeaderParticipation::from_bytes(&bytes).unwrap();
        let decoded: BTreeSet<AuthorityIdentifier> = m
            .signers
            .iter()
            .map(|i| committee.authority_by_key(&bls_keys[*i as usize]).unwrap().id())
            .collect();

        assert_eq!(decoded, expected);
        assert_eq!(decoded, BTreeSet::from([ids[0].clone(), ids[1].clone(), ids[2].clone()]));
        // ids[3] signed only the leader certificate, which lives outside the `certificates`
        // vector this projection walks. In production the leader is also carried in that vector,
        // so this is a property of the fixture, not a gap in the signal.
        assert!(!decoded.contains(&ids[3]));
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
