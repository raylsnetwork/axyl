//! Define the Epoch record, vote and certificate structs.
//!
//! These are used to form a signed "chain" of epoch records.  They are useful
//! for quickly determining a committee for a given epoch even if the executed
//! state is not available (i.e. when syncing).  They include a certificate so
//! can allow trustless syncing of meta-data (the consensus chain) in order to
//! execute with known correct consensus outputs.
use crate::{
    crypto, encode, serde::RoaringBitmapSerde, AuthorityIdentifier, BlockHash,
    BlsAggregateSignature, BlsPublicKey, BlsSignature, BlsSigner, Epoch, Intent, IntentMessage,
    IntentScope, ValidatorAggregateSignature as _, Votable, B256,
};
use alloy::eips::BlockNumHash;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

/// Serde helper that sorts `Vec<BlsPublicKey>` on both serialize and
/// deserialize, guaranteeing deterministic committee ordering everywhere:
/// `digest()`, DB persistence, network receive, and bitmap verification.
mod sorted_keys {
    use super::BlsPublicKey;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<S: Serializer>(keys: &[BlsPublicKey], s: S) -> Result<S::Ok, S::Error> {
        let mut sorted = keys.to_vec();
        sorted.sort_unstable();
        sorted.serialize(s)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Vec<BlsPublicKey>, D::Error> {
        let mut keys = Vec::<BlsPublicKey>::deserialize(d)?;
        keys.sort_unstable();
        Ok(keys)
    }
}

/// Record of an Epoch.  Will be created at epoch start for the previous epoch
/// and signed by that epochs committee members.
#[derive(PartialEq, Serialize, Deserialize, Clone, Debug, Default)]
pub struct EpochRecord {
    /// The epoch this record is for.
    pub epoch: Epoch,
    /// The active committee for this epoch.
    #[serde(with = "sorted_keys")]
    pub committee: Vec<BlsPublicKey>,
    /// The committee for the next epoch.
    /// This can be used for trustless syncing.
    #[serde(with = "sorted_keys")]
    pub next_committee: Vec<BlsPublicKey>,
    /// Hash of the previous EpochRecord.
    pub parent_hash: B256,
    /// The block number and hash of the last execution state of this epoch.
    /// Basically the execution genesis for the next epoch after this one.
    /// Also a signed checkpoint of execution state (with the certificate).
    pub parent_state: BlockNumHash,
    /// The hash of the last ['ConsensusHeader'] of this epoch.
    /// Can be used as a signed checkpoint for consensus (with the certificate).
    pub parent_consensus: B256,
}

impl EpochRecord {
    /// Return the digest for this ConsensusHeader.
    pub fn digest(&self) -> B256 {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(&encode(self));
        BlockHash::from_slice(hasher.finalize().as_bytes())
    }

    /// Use signer to generate an [`EpochVote`] for this EpochRecord.
    pub fn sign_vote<S: BlsSigner>(&self, signer: &S) -> EpochVote {
        let epoch_hash = self.digest();
        let intent =
            encode(&IntentMessage::new(Intent::consensus(IntentScope::EpochBoundary), epoch_hash));
        let signature = signer.request_signature_direct(&intent);

        EpochVote { epoch_hash, public_key: signer.public_key(), signature }
    }

    /// Return true if cert contains a quorum of committee signatures for this EpochRecord.
    pub fn verify_with_cert(&self, cert: &EpochCertificate) -> bool {
        if self.digest() != cert.epoch_hash {
            // Record and cert don't match.
            return false;
        }

        let auth_indexes = cert.signed_authorities.iter().collect::<Vec<_>>();
        let mut auth_iter = 0;
        let pks: Vec<BlsPublicKey> = self
            .committee
            .iter()
            .enumerate()
            .filter(|(i, _authority)| match auth_indexes.get(auth_iter) {
                Some(index) if *index == *i as u32 => {
                    auth_iter += 1;
                    true
                }
                _ => false,
            })
            .map(|(_, key)| *key)
            .collect();

        let aggregate_signature = BlsAggregateSignature::from_signature(&cert.signature);
        let intent =
            IntentMessage::new(Intent::consensus(IntentScope::EpochBoundary), cert.epoch_hash);
        if auth_iter < self.super_quorum() {
            false
        } else {
            aggregate_signature.verify_secure(&intent, &pks[..])
        }
    }

    /// Provide a super quorum, this is 2/3 of committee size plus one.
    /// With this many signers of an epoch record we are safe unless a
    /// super majority of validators are byzantine.
    pub fn super_quorum(&self) -> usize {
        ((self.committee.len() * 2) / 3) + 1
    }

    /// Verify the cert against `expected_committee` (which the record must declare exactly, both
    /// stored sorted) rather than against the committee embedded in the record.
    pub fn verify_against_committee(
        &self,
        expected_committee: &[BlsPublicKey],
        cert: &EpochCertificate,
    ) -> bool {
        if self.committee.as_slice() != expected_committee {
            return false;
        }
        self.verify_with_cert(cert)
    }
}

/// Vote for an ['EpochRecord'].
/// Each committee member should gossip this on epoch start and other nodes
/// should collect them and aggregate signatures.
/// Note this is gossipped by the outgoing (previous committee).
#[derive(PartialEq, Serialize, Deserialize, Copy, Clone, Debug, Default)]
pub struct EpochVote {
    /// The hash of the ['EpochRecord'].
    /// Store the hash not the record to keep gossip size down.
    /// Other nodes can request the record once vs recieving it many times.
    pub epoch_hash: B256,
    /// Public key of the committee member that signed this.
    /// This needs to be verified to be a committee member.
    pub public_key: BlsPublicKey,
    /// Signature of a committee member for the epoch.
    pub signature: BlsSignature,
}

impl Votable for EpochVote {
    fn voter_id(&self) -> AuthorityIdentifier {
        self.public_key.into()
    }
}

impl EpochVote {
    /// Verify a single signature of the cert.
    /// Used when receiving published "single" signer certs for agregation.
    pub fn check_signature(&self) -> bool {
        let intent = encode(&IntentMessage::new(
            Intent::consensus(IntentScope::EpochBoundary),
            self.epoch_hash,
        ));
        self.signature.verify_raw(&intent, &self.public_key)
    }
}

/// Certificate of an ['EpochRecord'].
/// Each committee member should gossip this on epoch start and other nodes
/// should collect them and aggregate signatures.
#[serde_as]
#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct EpochCertificate {
    /// The hash of the ['EpochRecord'].
    /// Store the hash not the record to keep gossip size down.
    /// Other nodes can request the record once vs recieving it many times.
    pub epoch_hash: B256,
    /// Signatures of a quorum of committee member for the epoch.
    pub signature: BlsSignature,
    /// Bitmap defining which committee members signed this certificate.
    #[serde_as(as = "RoaringBitmapSerde")]
    pub signed_authorities: roaring::RoaringBitmap,
}

impl EpochCertificate {
    /// Verify a groug of signatures against the cert.
    pub fn check_signatures(&self, signers: &[BlsPublicKey]) -> bool {
        let aggregate_signature = BlsAggregateSignature::from_signature(&self.signature);
        let intent =
            IntentMessage::new(Intent::consensus(IntentScope::EpochBoundary), self.epoch_hash);
        aggregate_signature.verify_secure(&intent, signers)
    }
}

/// Phases of the epoch transition state machine.
///
/// Each phase transition is persisted to the DB so that a crash
/// mid-transition can be recovered on restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EpochTransitionPhase {
    /// Epoch boundary subdag detected; transition initiated.
    BoundaryDetected,
    /// Subscriber drain requested; waiting for in-flight work to finish.
    Draining,
    /// Consensus tasks shut down; no new subdags will be produced.
    ConsensusShutdown,
    /// All pending execution payloads have been processed by the EVM.
    ExecutionComplete,
    /// Consensus tables cleared and channels reset for the next epoch.
    Cleared,
}

/// Persistent checkpoint written at each phase transition.
///
/// If the node crashes mid-transition, `recover_partial_transition()`
/// reads the last checkpoint and resumes from the completed phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochTransitionCheckpoint {
    /// The epoch being transitioned.
    pub epoch: crate::Epoch,
    /// The last phase that completed successfully.
    pub completed_phase: EpochTransitionPhase,
    /// Target hash from the epoch boundary subdag.
    pub target_hash: B256,
    /// Unix timestamp (seconds) when this checkpoint was written.
    pub timestamp: u64,
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use rand::{rngs::StdRng, CryptoRng, RngCore, SeedableRng as _};
    use roaring::RoaringBitmap;

    use crate::{BlsKeypair, Signer as _};

    use super::*;

    #[derive(Clone)]
    struct TestBlsKeypair(Arc<BlsKeypair>);

    impl TestBlsKeypair {
        fn new<R: CryptoRng + RngCore>(rng: &mut R) -> Self {
            Self(Arc::new(BlsKeypair::generate(rng)))
        }
    }

    impl BlsSigner for TestBlsKeypair {
        fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature {
            self.0.sign(msg)
        }

        fn public_key(&self) -> BlsPublicKey {
            *self.0.public()
        }
    }

    /// Build an EpochRecord with committee keys in the given order.
    fn make_record(keys: Vec<BlsPublicKey>) -> EpochRecord {
        EpochRecord {
            epoch: 1,
            committee: keys.clone(),
            next_committee: keys,
            parent_hash: B256::default(),
            parent_state: BlockNumHash::default(),
            parent_consensus: B256::default(),
        }
    }

    #[test]
    fn test_sorted_keys_serialize_sorts() {
        // Build sorted keys, then reverse to guarantee unsorted input.
        let mut rng = StdRng::from_os_rng();
        let k1 = TestBlsKeypair::new(&mut rng).public_key();
        let k2 = TestBlsKeypair::new(&mut rng).public_key();
        let k3 = TestBlsKeypair::new(&mut rng).public_key();
        let mut sorted = vec![k1, k2, k3];
        sorted.sort_unstable();
        let mut unsorted = sorted.clone();
        unsorted.reverse();
        assert_ne!(unsorted, sorted, "test requires unsorted input");

        let record = make_record(unsorted);
        // encode() calls bcs::to_bytes → Serialize → sorted_keys::serialize
        let bytes = encode(&record);
        let decoded: EpochRecord = bcs::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.committee, sorted);
        assert_eq!(decoded.next_committee, sorted);
    }

    #[test]
    fn test_sorted_keys_deserialize_sorts() {
        // Build sorted keys, then reverse to guarantee unsorted input.
        let mut rng = StdRng::from_os_rng();
        let k1 = TestBlsKeypair::new(&mut rng).public_key();
        let k2 = TestBlsKeypair::new(&mut rng).public_key();
        let k3 = TestBlsKeypair::new(&mut rng).public_key();
        let mut sorted = vec![k1, k2, k3];
        sorted.sort_unstable();
        let mut reversed = sorted.clone();
        reversed.reverse();
        assert_ne!(reversed, sorted, "test requires unsorted input");

        // encode() sorts via serialize, so the wire bytes are sorted.
        // Deserialize must also produce sorted output.
        let record = make_record(reversed);
        let bytes = encode(&record);
        let decoded: EpochRecord = bcs::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.committee, sorted);
        assert_eq!(decoded.next_committee, sorted);
    }

    #[test]
    fn test_digest_deterministic_regardless_of_input_order() {
        let mut rng = StdRng::from_os_rng();
        let k1 = TestBlsKeypair::new(&mut rng).public_key();
        let k2 = TestBlsKeypair::new(&mut rng).public_key();
        let k3 = TestBlsKeypair::new(&mut rng).public_key();

        let order_a = vec![k1, k2, k3];
        let order_b = vec![k3, k1, k2];
        let order_c = vec![k2, k3, k1];

        let digest_a = make_record(order_a).digest();
        let digest_b = make_record(order_b).digest();
        let digest_c = make_record(order_c).digest();

        assert_eq!(digest_a, digest_b, "different input order must produce same digest");
        assert_eq!(digest_b, digest_c, "different input order must produce same digest");
    }

    #[test]
    fn test_roundtrip_preserves_sorted_order() {
        let mut rng = StdRng::from_os_rng();
        let k1 = TestBlsKeypair::new(&mut rng).public_key();
        let k2 = TestBlsKeypair::new(&mut rng).public_key();
        let k3 = TestBlsKeypair::new(&mut rng).public_key();
        let unsorted = vec![k3, k2, k1];
        let mut sorted = unsorted.clone();
        sorted.sort_unstable();

        let record = make_record(unsorted);
        let bytes = encode(&record);
        let decoded: EpochRecord = bcs::from_bytes(&bytes).unwrap();
        // Re-encode the decoded record — bytes should be identical.
        let bytes2 = encode(&decoded);
        assert_eq!(bytes, bytes2, "roundtrip must produce identical bytes");
        assert_eq!(decoded.committee, sorted);
    }

    #[test]
    fn test_verify_with_cert_works_with_unsorted_construction() {
        // Build record with deliberately unsorted keys, roundtrip through
        // serde to get sorted in-memory form, sign, and verify.
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestBlsKeypair> = (0..3).map(|_| TestBlsKeypair::new(&mut rng)).collect();
        let mut sorted_keys: Vec<BlsPublicKey> = signers.iter().map(|s| s.public_key()).collect();
        sorted_keys.sort_unstable();

        // Reverse to guarantee unsorted input.
        let mut unsorted_keys = sorted_keys.clone();
        unsorted_keys.reverse();
        assert_ne!(unsorted_keys, sorted_keys, "test requires unsorted input");

        // Construct with unsorted keys, then roundtrip through serde to get
        // sorted in-memory committee (simulates DB read or network receive).
        let record = make_record(unsorted_keys);
        let decoded: EpochRecord = bcs::from_bytes(&encode(&record)).unwrap();
        assert_eq!(decoded.committee, sorted_keys, "serde must sort committee");

        // Sign votes against the decoded (sorted) record.
        let votes: Vec<EpochVote> = signers.iter().map(|s| decoded.sign_vote(s)).collect();
        for v in &votes {
            assert!(v.check_signature(), "vote sig check failed");
        }

        // Aggregate signatures in sorted-key order (matching bitmap positions).
        // verify_with_cert extracts keys at bitmap positions from committee,
        // so the aggregate must use the same order.
        let mut signer_by_pos: Vec<(usize, &TestBlsKeypair)> = signers
            .iter()
            .map(|s| {
                let pos = sorted_keys.iter().position(|k| *k == s.public_key()).unwrap();
                (pos, s)
            })
            .collect();
        signer_by_pos.sort_by_key(|(pos, _)| *pos);

        let ordered_sigs: Vec<BlsSignature> = signer_by_pos
            .iter()
            .map(|(_, s)| votes.iter().find(|v| v.public_key == s.public_key()).unwrap().signature)
            .collect();
        let agg = BlsAggregateSignature::aggregate(&ordered_sigs, true).unwrap();
        let signature = agg.to_signature();

        let mut signed_authorities = RoaringBitmap::new();
        for (pos, _) in &signer_by_pos {
            signed_authorities.push(*pos as u32);
        }

        let cert = EpochCertificate { epoch_hash: decoded.digest(), signature, signed_authorities };
        assert!(decoded.verify_with_cert(&cert), "cert verification failed on sorted record");
    }

    /// Build a valid certificate for `record` signed by super_quorum of `signers`.
    fn build_cert(record: &EpochRecord, signers: &[TestBlsKeypair]) -> EpochCertificate {
        let committee = &record.committee;
        let mut positioned: Vec<(usize, &TestBlsKeypair)> = signers
            .iter()
            .filter_map(|s| committee.iter().position(|k| *k == s.public_key()).map(|p| (p, s)))
            .collect();
        positioned.sort_by_key(|(p, _)| *p);
        let quorum = record.super_quorum();
        assert!(positioned.len() >= quorum, "need at least super_quorum signers in committee");
        let chosen = &positioned[..quorum];
        let sigs: Vec<BlsSignature> =
            chosen.iter().map(|(_, s)| record.sign_vote(*s).signature).collect();
        let agg = BlsAggregateSignature::aggregate(&sigs, true).unwrap();
        let mut bm = RoaringBitmap::new();
        for (p, _) in chosen {
            bm.push(*p as u32);
        }
        EpochCertificate {
            epoch_hash: record.digest(),
            signature: agg.to_signature(),
            signed_authorities: bm,
        }
    }

    #[test]
    fn test_verify_against_committee_rejects_forged_committee() {
        let mut rng = StdRng::from_os_rng();

        let genuine: Vec<TestBlsKeypair> = (0..4).map(|_| TestBlsKeypair::new(&mut rng)).collect();
        let mut genuine_keys: Vec<BlsPublicKey> = genuine.iter().map(|s| s.public_key()).collect();
        genuine_keys.sort_unstable();

        let attacker: Vec<TestBlsKeypair> = (0..4).map(|_| TestBlsKeypair::new(&mut rng)).collect();
        let attacker_keys: Vec<BlsPublicKey> = attacker.iter().map(|s| s.public_key()).collect();
        let forged: EpochRecord =
            bcs::from_bytes(&encode(&make_record(attacker_keys))).expect("decode forged");
        let forged_cert = build_cert(&forged, &attacker);

        assert!(
            forged.verify_with_cert(&forged_cert),
            "self-signed forged record should self-verify"
        );
        assert!(
            !forged.verify_against_committee(&genuine_keys, &forged_cert),
            "forged committee must be rejected when bound to the genuine committee",
        );

        let g_record: EpochRecord =
            bcs::from_bytes(&encode(&make_record(genuine_keys.clone()))).expect("decode genuine");
        let g_cert = build_cert(&g_record, &genuine);
        assert!(
            g_record.verify_against_committee(&genuine_keys, &g_cert),
            "genuine record must verify against its genuine committee",
        );
    }

    #[test]
    fn test_epoch_records() {
        let mut rng = StdRng::from_os_rng();
        let com1 = TestBlsKeypair::new(&mut rng);
        let com2 = TestBlsKeypair::new(&mut rng);
        let com3 = TestBlsKeypair::new(&mut rng);
        let record = EpochRecord {
            epoch: 0,
            committee: vec![com1.public_key(), com2.public_key(), com3.public_key()],
            next_committee: vec![com1.public_key(), com2.public_key(), com3.public_key()],
            parent_hash: B256::default(),
            parent_state: BlockNumHash::default(),
            parent_consensus: B256::default(),
        };
        let vote1 = record.sign_vote(&com1);
        let vote2 = record.sign_vote(&com2);
        let vote3 = record.sign_vote(&com3);
        assert_eq!(vote1.public_key, com1.public_key());
        assert_eq!(vote2.public_key, com2.public_key());
        assert_eq!(vote3.public_key, com3.public_key());
        assert!(vote1.check_signature(), "vote1 failed sig check");
        assert!(vote2.check_signature(), "vote2 failed sig check");
        assert!(vote3.check_signature(), "vote3 failed sig check");
        let sigs = [vote1.signature, vote2.signature, vote3.signature];
        match BlsAggregateSignature::aggregate(&sigs[..], true) {
            Ok(aggregated_signature) => {
                let signature: BlsSignature = aggregated_signature.to_signature();
                let mut signed_authorities = RoaringBitmap::new();
                signed_authorities.push(0);
                signed_authorities.push(1);
                signed_authorities.push(2);
                let cert =
                    EpochCertificate { epoch_hash: record.digest(), signature, signed_authorities };
                assert!(record.verify_with_cert(&cert), "record failed to verify");
                // leave out a sig.
                let mut signed_authorities = RoaringBitmap::new();
                signed_authorities.push(0);
                signed_authorities.push(2);
                let cert =
                    EpochCertificate { epoch_hash: record.digest(), signature, signed_authorities };
                assert!(!record.verify_with_cert(&cert), "record verified!");
            }
            Err(_) => {
                panic!("failed to aggregate epoch record signatures",);
            }
        }
    }
}
