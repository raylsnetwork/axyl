//! A consensus header hash that f+1 committee members signed as their committed output.

use crate::{
    crypto, serde::SignerBitmapSerde, to_intent_message, BlockHash, BlsAggregateSignature,
    BlsPublicKey, BlsSignature, Epoch, EpochCertificate, EpochRecord, Round,
    ValidatorAggregateSignature as _, B256,
};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

/// Proof that f+1 members of an epoch's committee committed the consensus header `hash` at
/// `number`: the aggregate of the signatures they gossip on every commit. At least one signer is
/// honest, so one peer can hand it to a node and the node can trust the head without asking
/// anyone else, then fetch the header by `hash` (#370, #312).
#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CertifiedTip {
    /// The epoch of the header's leader, whose committee signed.
    pub epoch: Epoch,
    /// The round of the header's leader.
    pub round: Round,
    /// The consensus header number.
    pub number: u64,
    /// The consensus header hash.
    pub hash: BlockHash,
    /// The aggregate of the signers' signatures over [`Self::digest`].
    pub signature: BlsSignature,
    /// The signers, as indexes into the epoch committee's keys sorted ascending.
    #[serde_as(as = "SignerBitmapSerde")]
    pub signed_authorities: RoaringBitmap,
}

impl CertifiedTip {
    /// The digest each committee member signs when it publishes a committed header.
    pub fn digest_data(epoch: Epoch, round: Round, number: u64, hash: BlockHash) -> B256 {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(&epoch.to_be_bytes());
        hasher.update(&round.to_be_bytes());
        hasher.update(&number.to_be_bytes());
        hasher.update(hash.as_ref());
        B256::from_slice(hasher.finalize().as_bytes())
    }

    /// The digest the signers signed.
    pub fn digest(&self) -> B256 {
        Self::digest_data(self.epoch, self.round, self.number, self.hash)
    }

    /// How many of a committee of `committee_size` must sign: f+1, so at least one is honest.
    pub fn quorum(committee_size: usize) -> usize {
        committee_size / 3 + 1
    }

    /// Aggregate `signatures` over this header into a certificate. Each signature must already be
    /// verified; keys outside `committee` and repeated keys are skipped. `None` below quorum.
    pub fn from_signatures(
        epoch: Epoch,
        round: Round,
        number: u64,
        hash: BlockHash,
        committee: &[BlsPublicKey],
        signatures: impl IntoIterator<Item = (BlsPublicKey, BlsSignature)>,
    ) -> Option<Self> {
        let committee = sorted(committee);
        let mut signed_authorities = RoaringBitmap::new();
        let mut sigs = Vec::new();
        for (key, signature) in signatures {
            let Ok(index) = committee.binary_search(&key) else { continue };
            if signed_authorities.insert(index as u32) {
                sigs.push(signature);
            }
        }
        if sigs.len() < Self::quorum(committee.len()) {
            return None;
        }
        // each signature was group-checked when it was verified on its own
        let signature = BlsAggregateSignature::aggregate(&sigs, false).ok()?.to_signature();
        Some(Self { epoch, round, number, hash, signature, signed_authorities })
    }

    /// Whether f+1 members of `committee`, the committee of [`Self::epoch`], signed this header.
    pub fn verify(&self, committee: &[BlsPublicKey]) -> bool {
        let committee = sorted(committee);
        let signers: Option<Vec<BlsPublicKey>> = self
            .signed_authorities
            .iter()
            .map(|index| committee.get(index as usize).copied())
            .collect();
        let Some(signers) = signers else { return false };
        signers.len() >= Self::quorum(committee.len())
            && BlsAggregateSignature::from_signature(&self.signature)
                .verify_secure(&to_intent_message(self.digest()), &signers)
    }
}

/// The head a node serves to a peer looking for where to catch up to.
///
/// Either a header f+1 members of its epoch's committee signed as they committed it, or the last
/// header of a closed epoch, which that epoch's certified record names. The record is signed by a
/// super-quorum of the same committee, so it proves that header at least as well as an f+1 quorum
/// would. It covers the header an epoch ends on even when no node counted an f+1 quorum for it:
/// the closing epoch's last results can land on either side of the epoch switch, or not at all.
///
/// Sent on the wire, where BCS encodes the variant index: append new variants, never reorder.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CertifiedHead {
    /// f+1 signatures over the consensus result of one header.
    Quorum(CertifiedTip),
    /// The last header of `record.epoch`: `record.parent_consensus`, with `certificate` over the
    /// record. `number` is the serving node's own, not signed: the hash is what the proof covers.
    EpochEnd {
        /// The number of the header the record names.
        number: u64,
        /// The record of the closed epoch.
        record: EpochRecord,
        /// The certificate over `record`.
        certificate: EpochCertificate,
    },
}

impl CertifiedHead {
    /// The epoch whose committee signed.
    pub fn epoch(&self) -> Epoch {
        match self {
            Self::Quorum(tip) => tip.epoch,
            Self::EpochEnd { record, .. } => record.epoch,
        }
    }

    /// The consensus header number. Unsigned for [`Self::EpochEnd`].
    pub fn number(&self) -> u64 {
        match self {
            Self::Quorum(tip) => tip.number,
            Self::EpochEnd { number, .. } => *number,
        }
    }

    /// The consensus header hash the proof covers.
    pub fn hash(&self) -> BlockHash {
        match self {
            Self::Quorum(tip) => tip.hash,
            Self::EpochEnd { record, .. } => record.parent_consensus,
        }
    }

    /// Order heads by: epoch first, so f+1 retired keys signing a high number cannot rank above a
    /// later epoch; then an epoch's end above any header of that epoch, which it is by definition
    /// and which does not trust its unsigned number; then the number.
    ///
    /// A peer can lie about an [`Self::EpochEnd`]'s number. That only reorders copies of the same
    /// hash, because `is_end` ranks above it, and callers act on [`Self::hash`] alone. Never act on
    /// such a number as if it were signed.
    pub fn rank(&self) -> (Epoch, bool, u64) {
        (self.epoch(), matches!(self, Self::EpochEnd { .. }), self.number())
    }

    /// Whether the proof holds against `committee`, the committee of [`Self::epoch`].
    pub fn verify(&self, committee: &[BlsPublicKey]) -> bool {
        match self {
            Self::Quorum(tip) => tip.verify(committee),
            Self::EpochEnd { record, certificate, .. } => {
                record.verify_against_committee(&sorted(committee), certificate)
            }
        }
    }
}

/// `committee` sorted ascending, the order the signer bitmap indexes.
fn sorted(committee: &[BlsPublicKey]) -> Vec<BlsPublicKey> {
    let mut keys = committee.to_vec();
    keys.sort_unstable();
    keys
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{encode, BlsKeypair, Signer as _};
    use rand::{rngs::StdRng, SeedableRng as _};

    const EPOCH: Epoch = 3;
    const ROUND: Round = 40;
    const NUMBER: u64 = 77;

    fn keypairs(n: usize) -> Vec<BlsKeypair> {
        let mut rng = StdRng::seed_from_u64(7);
        (0..n).map(|_| BlsKeypair::generate(&mut rng)).collect()
    }

    fn keys(keypairs: &[BlsKeypair]) -> Vec<BlsPublicKey> {
        keypairs.iter().map(|k| *k.public()).collect()
    }

    /// Each signer's signature as a committing member gossips it.
    fn signed(keypairs: &[BlsKeypair], hash: BlockHash) -> Vec<(BlsPublicKey, BlsSignature)> {
        let digest = CertifiedTip::digest_data(EPOCH, ROUND, NUMBER, hash);
        keypairs
            .iter()
            .map(|k| (*k.public(), k.sign(&encode(&to_intent_message(digest)))))
            .collect()
    }

    fn certify(keypairs: &[BlsKeypair], committee: &[BlsPublicKey]) -> Option<CertifiedTip> {
        let hash = B256::repeat_byte(9);
        CertifiedTip::from_signatures(EPOCH, ROUND, NUMBER, hash, committee, signed(keypairs, hash))
    }

    #[test]
    fn f_plus_one_signers_verify() {
        let all = keypairs(7);
        let committee = keys(&all);
        // f = 2 of 7, so 3 signers
        let tip = certify(&all[4..], &committee).expect("3 of 7 is f+1");
        assert_eq!(tip.signed_authorities.len(), 3);
        assert!(tip.verify(&committee));
        // the bitmap indexes the sorted committee, so committee order does not matter
        let mut reversed = committee.clone();
        reversed.reverse();
        assert!(tip.verify(&reversed));
    }

    #[test]
    fn below_quorum_is_not_a_certificate() {
        let all = keypairs(7);
        assert!(certify(&all[5..], &keys(&all)).is_none());
    }

    #[test]
    fn outsiders_and_repeats_do_not_count() {
        let all = keypairs(7);
        let committee = keys(&all[..4]);
        // two members (f+1 of 4), one repeated, and three outsiders
        let hash = B256::repeat_byte(9);
        let mut sigs = signed(&all[2..], hash);
        sigs.push(sigs[0]);
        let tip = CertifiedTip::from_signatures(EPOCH, ROUND, NUMBER, hash, &committee, sigs)
            .expect("two members");
        assert_eq!(tip.signed_authorities.len(), 2);
        assert!(tip.verify(&committee));
        let one_member = signed(&all[3..], hash);
        assert!(CertifiedTip::from_signatures(EPOCH, ROUND, NUMBER, hash, &committee, one_member)
            .is_none());
    }

    #[test]
    fn tampered_or_foreign_certificates_fail() {
        let all = keypairs(7);
        let committee = keys(&all);
        let tip = certify(&all[4..], &committee).expect("f+1");

        for tampered in [
            CertifiedTip { number: NUMBER + 1, ..tip.clone() },
            CertifiedTip { hash: B256::repeat_byte(1), ..tip.clone() },
            CertifiedTip { epoch: EPOCH + 1, ..tip.clone() },
            CertifiedTip { round: ROUND + 1, ..tip.clone() },
        ] {
            assert!(!tampered.verify(&committee));
        }

        // the bitmap naming fewer signers than signed, or an index past the committee
        let mut fewer = tip.clone();
        let first = fewer.signed_authorities.min().expect("signers");
        fewer.signed_authorities.remove(first);
        assert!(!fewer.verify(&committee));
        let mut past_end = tip.clone();
        past_end.signed_authorities.insert(committee.len() as u32);
        assert!(!past_end.verify(&committee));

        // another committee
        assert!(!tip.verify(&keys(&keypairs(7)[..0])));
        let other = {
            let mut rng = StdRng::seed_from_u64(99);
            (0..7).map(|_| *BlsKeypair::generate(&mut rng).public()).collect::<Vec<_>>()
        };
        assert!(!tip.verify(&other));
    }
}
