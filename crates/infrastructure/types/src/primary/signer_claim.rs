use crate::{
    bcs_layout::{BcsCursor, BcsLayout, BcsLayoutError},
    crypto::{to_intent_message, BlsAggregateSignature, BlsPublicKey, ValidatorAggregateSignature},
    BlsSignature, CertificateDigest, Committee, Epoch, Round, VotingPower,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

// Rounds a target must be behind the carrier before it may be claimed.
pub const SIGNER_CLAIM_LAG_ROUNDS: Round = 3;
// Claims per header, and the most a take may hand out: one constant, both sides.
pub const MAX_SIGNER_CLAIMS: usize = 8;
// Targets the collector keeps alive; eviction below the cap touches only `collecting`.
pub const MAX_TRACKED_TARGETS: usize = 2000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignerClaim {
    pub target_digest: CertificateDigest,
    pub target_epoch: Epoch,
    pub target_round: Round,
    // Ranks are positions in Committee::bls_keys(), never Committee::authorities().
    pub ranks: Vec<u32>,
    pub signature: BlsSignature,
}

#[derive(Debug, Error)]
pub enum ClaimError {
    #[error("claims region carries {0} claims, the bound is {MAX_SIGNER_CLAIMS}")]
    TooManyClaims(usize),
    #[error("claim names {ranks} signers, the committee has {committee}")]
    LengthTooLarge { ranks: usize, committee: usize },
    #[error("claim ranks are not strictly ascending")]
    RanksUnsorted,
    #[error("claim rank {rank} is outside the committee of {committee}")]
    RankOutOfRange { rank: u32, committee: usize },
    #[error("claim targets epoch {target}, the carrier is in epoch {carrier}")]
    WrongTargetEpoch { target: Epoch, carrier: Epoch },
    #[error(
        "claim targets round {target}, the carrier at round {carrier} needs it {lag} rounds back"
    )]
    TargetInFuture { target: Round, carrier: Round, lag: Round },
    #[error("claim signers weigh {weight}, a quorum is {threshold}")]
    BelowQuorum { weight: VotingPower, threshold: VotingPower },
    #[error("claim aggregate does not verify over its target digest")]
    BadAggregate,
}

impl SignerClaim {
    pub fn verify_claim(
        &self,
        carrier_epoch: Epoch,
        carrier_round: Round,
        committee: &Committee,
    ) -> Result<(), ClaimError> {
        if self.ranks.len() > committee.size() {
            return Err(ClaimError::LengthTooLarge {
                ranks: self.ranks.len(),
                committee: committee.size(),
            });
        }
        self.verify_with_keys(carrier_epoch, carrier_round, committee, &committee.bls_keys())
    }

    fn verify_with_keys(
        &self,
        carrier_epoch: Epoch,
        carrier_round: Round,
        committee: &Committee,
        keys: &[BlsPublicKey],
    ) -> Result<(), ClaimError> {
        if self.target_epoch != carrier_epoch {
            return Err(ClaimError::WrongTargetEpoch {
                target: self.target_epoch,
                carrier: carrier_epoch,
            });
        }

        // Age is measured against the carrier's own round: a node behind must still accept.
        let oldest_allowed = carrier_round.checked_sub(SIGNER_CLAIM_LAG_ROUNDS).ok_or(
            ClaimError::TargetInFuture {
                target: self.target_round,
                carrier: carrier_round,
                lag: SIGNER_CLAIM_LAG_ROUNDS,
            },
        )?;
        if self.target_round > oldest_allowed {
            return Err(ClaimError::TargetInFuture {
                target: self.target_round,
                carrier: carrier_round,
                lag: SIGNER_CLAIM_LAG_ROUNDS,
            });
        }

        // A repeated rank makes one signature look like a quorum, so distinctness is checked here.
        let mut previous: Option<u32> = None;
        for rank in &self.ranks {
            if let Some(previous) = previous {
                if *rank <= previous {
                    return Err(ClaimError::RanksUnsorted);
                }
            }
            if *rank as usize >= keys.len() {
                return Err(ClaimError::RankOutOfRange { rank: *rank, committee: keys.len() });
            }
            previous = Some(*rank);
        }

        let mut weight: VotingPower = 0;
        let mut pks = Vec::with_capacity(self.ranks.len());
        for rank in &self.ranks {
            let key = keys[*rank as usize];
            weight += committee.voting_power(&key);
            pks.push(key);
        }

        let threshold = committee.quorum_threshold();
        if weight < threshold {
            return Err(ClaimError::BelowQuorum { weight, threshold });
        }

        if !BlsAggregateSignature::from_signature(&self.signature)
            .verify_secure(&to_intent_message(self.target_digest), &pks[..])
        {
            return Err(ClaimError::BadAggregate);
        }

        Ok(())
    }
}

// The keys are resolved once for the whole region, not per rank per claim.
pub fn verify_signer_claims_region(
    claims: &[SignerClaim],
    carrier_epoch: Epoch,
    carrier_round: Round,
    committee: &Committee,
) -> Result<(), ClaimError> {
    if claims.len() > MAX_SIGNER_CLAIMS {
        return Err(ClaimError::TooManyClaims(claims.len()));
    }

    let committee_size = committee.size();
    for claim in claims {
        if claim.ranks.len() > committee_size {
            return Err(ClaimError::LengthTooLarge {
                ranks: claim.ranks.len(),
                committee: committee_size,
            });
        }
    }

    let keys = committee.bls_keys();
    for claim in claims {
        claim.verify_with_keys(carrier_epoch, carrier_round, committee, &keys)?;
    }
    Ok(())
}

impl BcsLayout for SignerClaim {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        c.skip::<CertificateDigest>()?
            .skip::<Epoch>()?
            .skip::<Round>()?
            .skip::<Vec<u32>>()?
            .skip::<BlsSignature>()?;
        Ok(())
    }
}

pub fn signer_claims_region_bytes(claims: &[SignerClaim]) -> Vec<u8> {
    let mut out = Vec::new();
    crate::encode_into_buffer(&mut out, claims).expect("claims serialize");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crypto::BlsKeypair, decode, Address, Authority, AuthorityIdentifier, BlsSignature,
        CertificateDigest, Committee, Vote,
    };
    use rand::{rngs::StdRng, SeedableRng};
    use std::collections::BTreeMap;

    fn keypair(rng: &mut StdRng) -> BlsKeypair {
        BlsKeypair::generate(rng)
    }

    fn committee_with_powers(powers: &[VotingPower]) -> (Committee, Vec<BlsKeypair>) {
        let mut rng = StdRng::from_seed([7; 32]);
        let keypairs: Vec<BlsKeypair> = powers.iter().map(|_| keypair(&mut rng)).collect();
        let authorities: BTreeMap<BlsPublicKey, Authority> = keypairs
            .iter()
            .zip(powers)
            .map(|(kp, power)| {
                let key = *kp.public();
                (key, Authority::new_for_test(key, *power, Address::random_with(&mut rng)))
            })
            .collect();
        (Committee::new_for_test(authorities, 9, BTreeMap::new()), keypairs)
    }

    fn rank_of(committee: &Committee, key: &BlsPublicKey) -> u32 {
        committee.bls_keys().iter().position(|k| k == key).expect("signer key is in bls_keys()")
            as u32
    }

    fn claim_from_signers(
        committee: &Committee,
        keypairs: &[BlsKeypair],
        signers: &[usize],
        target_round: Round,
    ) -> SignerClaim {
        let target = crate::HeaderBuilder::default()
            .author(AuthorityIdentifier::dummy_for_test(0xAA))
            .round(target_round)
            .epoch(committee.epoch())
            .created_at(1_700_000_000)
            .payload(Default::default())
            .parents(Default::default())
            .build();
        let digest = target.digest();
        let mut ranks: Vec<u32> =
            signers.iter().map(|i| rank_of(committee, keypairs[*i].public())).collect();
        ranks.sort_unstable();
        let sigs: Vec<BlsSignature> = signers
            .iter()
            .map(|i| {
                Vote::new_with_signer(&target, AuthorityIdentifier::default(), &keypairs[*i])
                    .signature
            })
            .collect();
        let signature =
            BlsAggregateSignature::aggregate(&sigs, true).expect("aggregate").to_signature();
        SignerClaim {
            target_digest: CertificateDigest::new(digest.into()),
            target_epoch: committee.epoch(),
            target_round,
            ranks,
            signature,
        }
    }

    #[test]
    fn test_region_encoding_is_deterministic() {
        // Region bytes are a pure function of the claims and their order: no map
        // iteration order, no clock, nothing local.
        let a = vec![claim(1), claim(2)];
        let b = vec![claim(1), claim(2)];
        assert_eq!(signer_claims_region_bytes(&a), signer_claims_region_bytes(&b));
        assert_eq!(signer_claims_region_bytes(&a), crate::encode(&a));
        assert_eq!(signer_claims_region_bytes(&[]), vec![0u8]);
    }

    #[test]
    fn test_claim_rank_resolves_through_bls_keys() {
        let (committee, keypairs) = committee_with_powers(&[1, 1, 1, 1]);
        let keys = committee.bls_keys();
        let by_id = committee.authorities();
        let signers = [0usize, 1usize, 2usize];

        // The rank must index the key-keyed order; the id-keyed order indexes other validators.
        let key_ranks: Vec<u32> =
            signers.iter().map(|i| rank_of(&committee, keypairs[*i].public())).collect();
        for (rank, signer) in key_ranks.iter().zip(signers.iter()) {
            assert_eq!(
                keys[*rank as usize],
                *keypairs[*signer].public(),
                "rank indexes bls_keys()"
            );
        }

        let mut id_ranks: Vec<u32> = signers
            .iter()
            .map(|i| {
                by_id
                    .iter()
                    .position(|a| a.protocol_key() == keypairs[*i].public())
                    .expect("authority is in the committee") as u32
            })
            .collect();
        id_ranks.sort_unstable();
        assert_ne!(
            key_ranks, id_ranks,
            "the same indexes in authorities() name different validators"
        );

        // Equal stakes keep the forged claim at the aggregate step instead of the weight step.
        let claim = claim_from_signers(&committee, &keypairs, &signers, 4);
        assert!(claim.verify_claim(committee.epoch(), 7, &committee).is_ok());

        let forged = SignerClaim { ranks: id_ranks, ..claim.clone() };
        assert!(
            matches!(
                forged.verify_claim(committee.epoch(), 7, &committee),
                Err(ClaimError::BadAggregate)
            ),
            "an id-order rank is a plausible number naming the wrong validator"
        );
    }

    #[test]
    fn test_verify_claim_rejects_duplicate_ranks() {
        let (committee, keypairs) = committee_with_powers(&[10, 10, 1, 1]);
        let mut claim = claim_from_signers(&committee, &keypairs, &[0, 1], 4);
        claim.ranks = vec![0, 1, 1];
        assert!(matches!(
            claim.verify_claim(committee.epoch(), 7, &committee),
            Err(ClaimError::RanksUnsorted)
        ));

        claim.ranks = vec![1, 0];
        assert!(matches!(
            claim.verify_claim(committee.epoch(), 7, &committee),
            Err(ClaimError::RanksUnsorted)
        ));
    }

    #[test]
    fn test_verify_claim_requires_weight_not_count() {
        let (committee, keypairs) = committee_with_powers(&[10, 10, 1, 1]);
        assert_eq!(committee.quorum_threshold(), 15, "2f+1 of 22 stake");
        assert!(committee.size() >= 3, "count-based quorum would accept three signers");

        // weight above quorum while the count is below 2f+1
        let heavy = claim_from_signers(&committee, &keypairs, &[0, 1], 4);
        assert_eq!(heavy.ranks.len(), 2, "two signers is fewer than 2f+1");
        assert!(heavy.verify_claim(committee.epoch(), 7, &committee).is_ok());

        // count at 2f+1 while the weight is below quorum
        let light = claim_from_signers(&committee, &keypairs, &[0, 2, 3], 4);
        assert_eq!(light.ranks.len(), 3, "three signers reaches 2f+1");
        assert!(matches!(
            light.verify_claim(committee.epoch(), 7, &committee),
            Err(ClaimError::BelowQuorum { .. })
        ));
    }

    #[test]
    fn test_verify_claim_rejects_cross_epoch_target() {
        let (committee, keypairs) = committee_with_powers(&[10, 10, 1, 1]);
        let mut claim = claim_from_signers(&committee, &keypairs, &[0, 1], 4);
        claim.target_epoch = committee.epoch() - 1;
        assert!(matches!(
            claim.verify_claim(committee.epoch(), 7, &committee),
            Err(ClaimError::WrongTargetEpoch { .. })
        ));
    }

    #[test]
    fn test_verify_claim_age_is_measured_against_the_carrier() {
        let (committee, keypairs) = committee_with_powers(&[10, 10, 1, 1]);
        let claim = claim_from_signers(&committee, &keypairs, &[0, 1], 5);
        assert!(claim.verify_claim(committee.epoch(), 8, &committee).is_ok());
        assert!(matches!(
            claim.verify_claim(committee.epoch(), 7, &committee),
            Err(ClaimError::TargetInFuture { .. })
        ));

        // A carrier too young to have a lag window rejects everything.
        assert!(matches!(
            claim.verify_claim(committee.epoch(), SIGNER_CLAIM_LAG_ROUNDS - 1, &committee),
            Err(ClaimError::TargetInFuture { .. })
        ));
    }

    #[test]
    fn test_verify_claim_accepts_empty_region() {
        let (committee, _) = committee_with_powers(&[10, 10, 1, 1]);
        assert!(verify_signer_claims_region(&[], committee.epoch(), 8, &committee).is_ok());
    }

    #[test]
    fn test_worst_case_region_still_decodes() {
        let (committee, keypairs) = committee_with_powers(&[10, 10, 1, 1]);
        let signers: Vec<usize> = (0..committee.size()).collect();
        let claims: Vec<SignerClaim> = (0..MAX_SIGNER_CLAIMS)
            .map(|i| claim_from_signers(&committee, &keypairs, &signers, 4 + i as Round))
            .collect();
        assert_eq!(claims.len(), MAX_SIGNER_CLAIMS);
        assert!(claims.iter().all(|c| c.ranks.len() == committee.size()));

        let bytes = signer_claims_region_bytes(&claims);
        let back: Vec<SignerClaim> = decode(&bytes);
        assert_eq!(back, claims, "the worst legitimate region round-trips");
        assert!(verify_signer_claims_region(&back, committee.epoch(), 14, &committee).is_ok());

        let mut buf = bytes.clone();
        buf.push(0xFF);
        let mut c = BcsCursor::new(&buf);
        let span = c.take_span::<Vec<SignerClaim>>().expect("span");
        assert_eq!(span, bytes.as_slice(), "the region does not swallow trailing input");
        assert_eq!(c.remaining(), &[0xFF]);
    }

    fn claim(i: u8) -> SignerClaim {
        SignerClaim {
            target_digest: CertificateDigest::new([i; 32]),
            target_epoch: 7,
            target_round: 123,
            ranks: vec![0, 2, 3],
            signature: BlsSignature::default(),
        }
    }
}
