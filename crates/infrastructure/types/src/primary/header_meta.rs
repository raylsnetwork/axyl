//! Lightweight metadata projections over a stored `ConsensusHeader`.

use crate::{
    bcs_layout::{BcsCursor, BcsLayoutError},
    crypto, AuthorityIdentifier, BlockHash, Certificate, Epoch, ReputationScores, Round,
    TimestampSec, WorkerId, B256,
};

/// Leader projection of a stored `ConsensusHeader`: who led the sub-dag and at
/// what `(round, epoch)`, the input `tally()` needs.
///
/// Skips `parent_hash` and the `sub_dag.certificates` vector, then reads only
/// the leader header's `author, round, epoch`; nothing else is parsed.
#[derive(Debug, Clone)]
pub struct ConsensusHeaderMeta {
    /// Identifier of the leader that committed the sub-dag.
    pub leader_author: AuthorityIdentifier,
    /// Leader certificate's round number.
    pub leader_round: Round,
    /// Leader certificate's epoch.
    pub leader_epoch: Epoch,
}

impl ConsensusHeaderMeta {
    /// Decode the leader-fields projection from a BCS-encoded `ConsensusHeader`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BcsLayoutError> {
        let mut c = BcsCursor::new(bytes);
        // parent_hash
        c.skip::<B256>()?;
        // sub_dag.certificates: Vec<Certificate>
        c.skip::<Vec<Certificate>>()?;
        // sub_dag.leader: read the first three header fields, stop early.
        let leader_author = c.read::<AuthorityIdentifier>()?;
        let leader_round = c.read::<Round>()?;
        let leader_epoch = c.read::<Epoch>()?;
        Ok(Self { leader_author, leader_round, leader_epoch })
    }
}

/// Chain-link projection of a stored `ConsensusHeader` (parent, number, digest) computed from the
/// raw BCS bytes.
///
/// Every digest reduces to hashing an embedded byte range located with cursor skips; nothing is
/// deserialized or allocated. A full `ConsensusHeader` decode would reconstruct each certificate's
/// aggregate `BlsSignature` (curve-point decompression + subgroup checks), which dominates a
/// whole-cache coverage sweep on cloud hardware; this path never touches the signature bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsensusHeaderChainMeta {
    /// Digest of the parent `ConsensusHeader`.
    pub parent_hash: B256,
    /// Consensus block number.
    pub number: u64,
    /// This header's digest, equal to [`crate::ConsensusHeader::digest`].
    pub digest: B256,
}

impl ConsensusHeaderChainMeta {
    /// Decode the chain-link projection from a BCS-encoded `ConsensusHeader`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BcsLayoutError> {
        let mut c = BcsCursor::new(bytes);
        let parent_hash = c.read::<B256>()?;
        // sub_dag digest: certificate digests + leader digest + encoded commit timestamp, with
        // reputation excluded (mirrors `CommittedSubDag::digest`).
        let mut sub_dag_hasher = crypto::DefaultHashFunction::new();
        let cert_count = c.read_len()?;
        for _ in 0..cert_count {
            sub_dag_hasher.update(embedded_certificate_digest(&mut c)?.as_slice());
        }
        sub_dag_hasher.update(embedded_certificate_digest(&mut c)?.as_slice()); // leader
        c.skip::<ReputationScores>()?; // excluded from the digest
        sub_dag_hasher.update(c.take_span::<u64>()?); // commit timestamp, hashed as raw 8 bytes
        let sub_dag_digest = sub_dag_hasher.finalize();
        let number = c.read::<u64>()?;

        // mirrors `ConsensusHeader::digest_from_parts`
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(parent_hash.as_slice());
        hasher.update(sub_dag_digest.as_bytes());
        hasher.update(number.to_le_bytes().as_ref());
        let digest = B256::from_slice(hasher.finalize().as_bytes());

        Ok(Self { parent_hash, number, digest })
    }
}

/// Returns one embedded certificate's digest (a hash over its header bytes) and advances the cursor
/// past the whole certificate.
fn embedded_certificate_digest(c: &mut BcsCursor<'_>) -> Result<B256, BcsLayoutError> {
    let header_bytes = Certificate::skip_with_header_span(c)?;
    let mut hasher = crypto::DefaultHashFunction::new();
    hasher.update(header_bytes);
    Ok(B256::from_slice(hasher.finalize().as_bytes()))
}

/// Projects the sub-dag's leader epoch and every batch digest its certificates reference, from a
/// BCS-encoded `ConsensusHeader`, without materializing the certificates.
///
/// The digests come out in the exact order a full walk yields them (each certificate's `payload`
/// keys in insertion order, certificates in vector order), so the cold archiver can group and
/// relocate batches straight from the projection instead of decoding the whole sub-dag. The
/// per-certificate walk mirrors `Certificate`/`Header`'s BCS layout, reading the payload keys and
/// skipping everything else; a field added to either type without updating it diverges from a full
/// decode and fails the pinning test below.
pub fn leader_epoch_and_batch_digests(
    bytes: &[u8],
) -> Result<(Epoch, Vec<BlockHash>), BcsLayoutError> {
    let mut c = BcsCursor::new(bytes);
    c.skip::<B256>()?; // parent_hash

    // sub_dag.certificates: Vec<Certificate>; collect each certificate's payload digests in order.
    let cert_count = c.read_len()?;
    let mut digests = Vec::new();
    for _ in 0..cert_count {
        read_certificate_payload_digests(&mut c, &mut digests)?;
    }

    // sub_dag.leader: Certificate; the committing epoch is its header's epoch, after author/round.
    c.skip::<AuthorityIdentifier>()?.skip::<Round>()?;
    let leader_epoch = c.read::<Epoch>()?;
    Ok((leader_epoch, digests))
}

/// Walks one BCS-encoded `Certificate`, pushing its header `payload` digests onto `out`.
///
/// Reuses [`Certificate::skip_with_header_span`] to advance past the whole certificate and
/// recover its `Header` bytes, so only the header prefix is walked here to reach the payload; the
/// certificate and header tails are skipped by the canonical impl rather than re-mirrored.
fn read_certificate_payload_digests(
    c: &mut BcsCursor<'_>,
    out: &mut Vec<BlockHash>,
) -> Result<(), BcsLayoutError> {
    let header = Certificate::skip_with_header_span(c)?;
    let mut h = BcsCursor::new(header);

    // Header prefix: author, round, epoch, created_at.
    h.skip::<AuthorityIdentifier>()?.skip::<Round>()?.skip::<Epoch>()?.skip::<TimestampSec>()?;

    // Header.payload: IndexMap<BlockHash, WorkerId>, on the wire as a length-prefixed sequence of
    // (digest, worker) pairs in insertion order. Keep the keys, skip the worker ids. Clamp the
    // pre-allocation to the remaining bytes so a corrupt length prefix cannot capacity-overflow and
    // abort the node (each pair is at least 33 bytes: a length-prefixed 32-byte digest).
    let entries = h.read_len()?;
    out.reserve(entries.min(h.len() / 33));
    for _ in 0..entries {
        out.push(h.read::<BlockHash>()?);
        h.skip::<WorkerId>()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CertificateDigest, CommittedSubDag, ConsensusHeader, HeaderBuilder, ReputationScores,
        WorkerId,
    };
    use alloy::primitives::BlockHash;
    use indexmap::IndexMap;
    use std::collections::BTreeSet;

    fn make_cert(
        author: u8,
        round: u32,
        epoch: u32,
        payload: Vec<(BlockHash, WorkerId)>,
        parents: usize,
    ) -> Certificate {
        let mut p = IndexMap::<BlockHash, WorkerId>::new();
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
        let mut cert = Certificate::default();
        cert.update_header_for_test(header);
        cert
    }

    fn make_header(leader: Certificate, certs: Vec<Certificate>) -> ConsensusHeader {
        let sub_dag = CommittedSubDag::new(certs, leader, 0, ReputationScores::default(), None);
        ConsensusHeader {
            parent_hash: BlockHash::repeat_byte(0xAB),
            sub_dag,
            number: 100,
            extra: BlockHash::ZERO,
        }
    }

    #[test]
    fn meta_extracts_leader_fields() {
        let leader = make_cert(0x77, 9, 3, vec![], 0);
        let h = make_header(leader, vec![]);
        let bytes = crate::encode(&h);
        let m = ConsensusHeaderMeta::from_bytes(&bytes).unwrap();
        assert_eq!(m.leader_author, AuthorityIdentifier::dummy_for_test(0x77));
        assert_eq!(m.leader_round, 9);
        assert_eq!(m.leader_epoch, 3);
    }

    #[test]
    fn chain_meta_matches_decoded_header() {
        let leader = make_cert(0xEE, 14, 1, vec![(BlockHash::repeat_byte(1), 0)], 2);
        let certs = vec![
            make_cert(0x01, 13, 1, vec![(BlockHash::repeat_byte(2), 1)], 0),
            make_cert(0x02, 13, 1, vec![(BlockHash::repeat_byte(3), 2)], 1),
        ];
        let h = make_header(leader, certs);
        let bytes = crate::encode(&h);
        let m = ConsensusHeaderChainMeta::from_bytes(&bytes).unwrap();
        assert_eq!(m.parent_hash, h.parent_hash);
        assert_eq!(m.number, h.number);
        assert_eq!(m.digest, h.digest(), "projection digest must match the decoded digest");
    }

    // Mirrors a production header: every committed sub-dag carries scores for the whole committee
    // and a non-zero commit timestamp, so the reputation skip must stay aligned for the raw-bytes
    // digest to match.
    #[test]
    fn chain_meta_matches_decoded_header_populated_reputation() {
        let mut payload = IndexMap::<BlockHash, WorkerId>::new();
        payload.insert(BlockHash::repeat_byte(1), 0);
        let mut parents = BTreeSet::<CertificateDigest>::new();
        parents.insert(CertificateDigest::new([7u8; 32]));
        parents.insert(CertificateDigest::new([8u8; 32]));
        // non-zero created_at yields a non-zero sub_dag commit_timestamp
        let leader_header = HeaderBuilder::default()
            .author(AuthorityIdentifier::dummy_for_test(0xEE))
            .round(14)
            .epoch(1)
            .created_at(1_700_000_000)
            .payload(payload)
            .parents(parents)
            .build();
        let mut leader = Certificate::default();
        leader.update_header_for_test(leader_header);

        let certs = vec![
            make_cert(0x01, 13, 1, vec![(BlockHash::repeat_byte(2), 1)], 0),
            make_cert(0x02, 13, 1, vec![(BlockHash::repeat_byte(3), 2)], 1),
        ];

        let mut scores = ReputationScores::default();
        for i in 1..=4u8 {
            scores.add_score(&AuthorityIdentifier::dummy_for_test(i), u64::from(i) * 10);
        }
        scores.final_of_schedule = true;

        let sub_dag = CommittedSubDag::new(certs, leader, 0, scores, None);
        assert_ne!(sub_dag.commit_timestamp(), 0);
        let h = ConsensusHeader {
            parent_hash: BlockHash::repeat_byte(0xCD),
            sub_dag,
            number: 4242,
            extra: BlockHash::ZERO,
        };

        let bytes = crate::encode(&h);
        let m = ConsensusHeaderChainMeta::from_bytes(&bytes).unwrap();
        assert_eq!(m.parent_hash, h.parent_hash);
        assert_eq!(m.number, h.number);
        assert_eq!(m.digest, h.digest(), "projection digest must match the decoded digest");
    }

    #[test]
    fn meta_skips_populated_certs() {
        let leader = make_cert(0xEE, 14, 1, vec![(BlockHash::repeat_byte(1), 0)], 2);
        let certs = vec![
            make_cert(0x01, 13, 1, vec![(BlockHash::repeat_byte(2), 1)], 0),
            make_cert(0x02, 13, 1, vec![(BlockHash::repeat_byte(3), 2)], 1),
        ];
        let h = make_header(leader, certs);
        let bytes = crate::encode(&h);
        let m = ConsensusHeaderMeta::from_bytes(&bytes).unwrap();
        assert_eq!(m.leader_author, AuthorityIdentifier::dummy_for_test(0xEE));
        assert_eq!(m.leader_round, 14);
        assert_eq!(m.leader_epoch, 1);
    }

    /// The digest projection must equal a full decode's `certificates -> payload keys` walk, in
    /// order, and recover the leader epoch. Multiple certs, multi-entry payloads, and populated
    /// parents/sig-state exercise every skip in the per-certificate walk.
    #[test]
    fn projection_matches_full_decode_walk() {
        let leader = make_cert(0xEE, 14, 7, vec![(BlockHash::repeat_byte(9), 0)], 2);
        let certs = vec![
            make_cert(
                0x01,
                13,
                7,
                vec![(BlockHash::repeat_byte(2), 1), (BlockHash::repeat_byte(5), 3)],
                0,
            ),
            make_cert(0x02, 13, 7, vec![], 3),
            make_cert(0x03, 13, 7, vec![(BlockHash::repeat_byte(8), 2)], 1),
        ];
        let h = make_header(leader, certs);
        let bytes = crate::encode(&h);

        let (epoch, digests) = leader_epoch_and_batch_digests(&bytes).unwrap();

        let expected: Vec<BlockHash> = h
            .sub_dag
            .certificates
            .iter()
            .flat_map(|c| c.header().payload().keys().copied())
            .collect();
        assert_eq!(epoch, h.sub_dag.leader_epoch(), "projected leader epoch");
        assert_eq!(digests, expected, "projected digests must match the full-decode walk");
    }

    /// A torn/corrupt header (the consensus env runs SafeNoSync) must surface as an `Err`, never a
    /// panic. In particular the payload-count pre-allocation must be bounded by the remaining
    /// bytes, so a bogus length prefix cannot capacity-overflow and abort the node.
    #[test]
    fn projection_rejects_corrupt_header_without_panicking() {
        let leader = make_cert(0xEE, 14, 7, vec![(BlockHash::repeat_byte(9), 0)], 2);
        let certs = vec![
            make_cert(
                0x01,
                13,
                7,
                vec![(BlockHash::repeat_byte(2), 1), (BlockHash::repeat_byte(5), 3)],
                1,
            ),
            make_cert(0x02, 13, 7, vec![(BlockHash::repeat_byte(8), 2)], 1),
        ];
        let bytes = crate::encode(&make_header(leader, certs));

        // Every cut inside the certificate region (the leader epoch the projection returns is read
        // last, near the end) must error rather than panic, whatever the truncation exposes as a
        // length prefix.
        for len in 1..bytes.len() / 2 {
            assert!(
                leader_epoch_and_batch_digests(&bytes[..len]).is_err(),
                "header truncated to {len} bytes must error, not panic",
            );
        }
        // The intact header still projects.
        assert!(leader_epoch_and_batch_digests(&bytes).is_ok());
    }

    /// Asserts the projection equals a full decode's `certificates -> payload keys` walk and
    /// recovers the leader epoch, for `h`.
    fn assert_projection_matches(h: &ConsensusHeader) {
        let bytes = crate::encode(h);
        let (epoch, digests) = leader_epoch_and_batch_digests(&bytes).unwrap();
        let expected: Vec<BlockHash> = h
            .sub_dag
            .certificates
            .iter()
            .flat_map(|c| c.header().payload().keys().copied())
            .collect();
        assert_eq!(epoch, h.sub_dag.leader_epoch(), "projected leader epoch");
        assert_eq!(digests, expected, "projected digests must match the full-decode walk");
    }

    /// The projection equivalence holds across header shapes, not just one handcrafted fixture:
    /// no certificates, an empty payload, a wide payload, a deep parent set, and a digest shared
    /// across certificates (which the walk must yield twice).
    #[test]
    fn projection_matches_full_decode_across_shapes() {
        let leader = || make_cert(0xEE, 14, 7, vec![(BlockHash::repeat_byte(9), 0)], 2);
        let wide: Vec<(BlockHash, WorkerId)> =
            (0..33u8).map(|i| (BlockHash::repeat_byte(i + 1), i as WorkerId)).collect();
        let shapes: Vec<Vec<Certificate>> = vec![
            vec![],
            vec![make_cert(1, 13, 7, vec![], 0)],
            vec![make_cert(1, 13, 7, wide, 0)],
            vec![make_cert(1, 13, 7, vec![(BlockHash::repeat_byte(2), 1)], 8)],
            vec![
                make_cert(1, 13, 7, vec![(BlockHash::repeat_byte(2), 1)], 1),
                make_cert(2, 13, 7, vec![(BlockHash::repeat_byte(2), 3)], 0),
            ],
        ];
        for certs in shapes {
            assert_projection_matches(&make_header(leader(), certs));
        }
    }

    /// Any single-byte corruption of a stored header must project to `Ok` or `Err`, never a
    /// panic: the consensus env runs SafeNoSync so a torn write can surface arbitrary bytes, and
    /// with panic=abort a projection panic is a node abort at boot or on a cold serve.
    #[test]
    fn projection_survives_single_byte_corruption() {
        let leader = make_cert(0xEE, 14, 7, vec![(BlockHash::repeat_byte(9), 0)], 2);
        let certs = vec![
            make_cert(
                0x01,
                13,
                7,
                vec![(BlockHash::repeat_byte(2), 1), (BlockHash::repeat_byte(5), 3)],
                1,
            ),
            make_cert(0x02, 13, 7, vec![(BlockHash::repeat_byte(8), 2)], 1),
        ];
        let bytes = crate::encode(&make_header(leader, certs));

        let mut mutated = bytes.clone();
        for pos in 0..bytes.len() {
            for flip in [0x01u8, 0x80, 0xFF] {
                mutated[pos] = bytes[pos] ^ flip;
                // Any outcome but a panic is acceptable; a flip may still parse validly.
                let _ = leader_epoch_and_batch_digests(&mutated);
                let _ = ConsensusHeaderMeta::from_bytes(&mutated);
                let _ = ConsensusHeaderChainMeta::from_bytes(&mutated);
            }
            mutated[pos] = bytes[pos];
        }
    }
}
