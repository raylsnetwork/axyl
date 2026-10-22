//! Per-epoch key for threshold certificates, from publicly verifiable dealings.
//!
//! The committee of epoch `E-1` deals the key of epoch `E` to the committee of `E` through the
//! `DkgManager` system contract, which accepts a dealing only after the NI-DKG precompile has
//! verified it in full ([`super::nidkg`]). The key is the sum of every accepted dealing, and it
//! exists once at least [`dealers_needed`] of the dealing committee have dealt. Every node reads
//! the same accepted dealings from the chain, so every node derives the same [`EpochKey`]; each
//! recipient decrypts its own share from them, whenever it reads them, with no complaint and no
//! acknowledgement.

use super::{
    nidkg::{BabySteps, Dealing, DecryptionKey, NidkgError},
    threshold::{PublicPolynomial, SecretShare, ShareIndex, ThresholdError},
    BlsPublicKey,
};
use crate::{Address, Epoch};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

/// A validator's position in the committee's key order, the index space of signer bitmaps. The
/// recipient at position `p` holds share index `p + 1`.
pub type Position = u32;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DkgError {
    #[error("no dealings")]
    NoDealings,
    #[error("dealings and the key's dealers do not match")]
    DealerMismatch,
    #[error("the dealing of {dealer} does not open for this recipient: {source}")]
    BadDealing {
        dealer: Address,
        #[source]
        source: NidkgError,
    },
    #[error("the summed share does not match the key")]
    BadShare,
    #[error(transparent)]
    Threshold(#[from] ThresholdError),
}

/// What the DKG for an epoch produced, persisted by every node and keyed by the epoch the key is
/// for.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DkgOutcome {
    /// The epoch uses threshold certificates with this key.
    Key(EpochKey),
    /// The epoch uses aggregate certificates: it is before the `ThresholdCertificates` fork.
    Fallback,
}

/// Dealings needed for a key from a dealing committee of `dealer_count`: `ceil(n/3)`, at least
/// one. Under the usual bound of `f` faulty members in `3f+1`, at least one of them is honest,
/// and one honest dealing keeps the summed secret uniformly random. `DkgManager` applies the
/// same rule on-chain.
pub fn dealers_needed(dealer_count: usize) -> usize {
    dealer_count.div_ceil(3).max(1)
}

/// A node's material for the current epoch's threshold scheme: the epoch key, and this node's
/// share when it sits in the committee.
#[derive(Debug)]
pub struct EpochThreshold {
    pub key: EpochKey,
    pub share: Option<SecretShare>,
}

/// How the current epoch's certificates are signed, as far as this node knows.
#[derive(Clone, Debug, Default)]
pub enum EpochScheme {
    /// Aggregate certificates over the committee keys.
    #[default]
    Aggregate,
    /// Threshold certificates under the epoch key; this node votes with its share, if it has one.
    Threshold(Arc<EpochThreshold>),
    /// The epoch should use threshold certificates but this node has not computed the key yet:
    /// it neither votes nor verifies under the epoch's scheme until the key lands.
    Pending,
}

impl EpochScheme {
    /// The threshold material, when the epoch uses threshold certificates and the key is known.
    pub fn threshold(&self) -> Option<&Arc<EpochThreshold>> {
        match self {
            Self::Threshold(threshold) => Some(threshold),
            Self::Aggregate | Self::Pending => None,
        }
    }

    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }
}

/// The key for one epoch: the summed public polynomial of every accepted dealing, who dealt
/// them, in acceptance order, and the committee they were dealt to, in committee order. The
/// dealings themselves stay on-chain; a recipient reads them back to derive its share.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EpochKey {
    pub epoch: Epoch,
    pub public: PublicPolynomial,
    pub dealers: Vec<Address>,
    /// The committee the key was dealt to, sorted: the member at position `p` holds share index
    /// `p + 1`. The chain fixed it when the epoch before closed.
    pub members: Vec<BlsPublicKey>,
}

impl EpochKey {
    /// The key of `epoch` from its accepted dealings, in acceptance order, to the committee
    /// `members` in committee order. The dealings must have been verified (the chain accepts no
    /// other) and share one threshold.
    pub fn from_dealings(
        epoch: Epoch,
        members: Vec<BlsPublicKey>,
        dealings: &[(Address, Dealing)],
    ) -> Result<Self, DkgError> {
        let ((_, first), rest) = dealings.split_first().ok_or(DkgError::NoDealings)?;
        let public = rest
            .iter()
            .try_fold(first.public().clone(), |acc, (_, dealing)| acc.add(dealing.public()))?;
        let dealers = dealings.iter().map(|(dealer, _)| *dealer).collect();
        Ok(Self { epoch, public, dealers, members })
    }

    /// The position of the member with BLS key `key` in the committee the key was dealt to.
    pub fn position_of(&self, key: &BlsPublicKey) -> Option<Position> {
        self.members.iter().position(|member| member == key).map(|position| position as Position)
    }

    pub fn group_key(&self) -> BlsPublicKey {
        self.public.group_key()
    }

    pub fn threshold(&self) -> usize {
        self.public.threshold()
    }

    /// The key a recipient's partial signatures verify against.
    pub fn share_key(&self, recipient: Position) -> BlsPublicKey {
        self.public.share_key(ShareIndex::from_position(recipient))
    }

    /// The recipient's share: the sum of its share in every dealing, `dealings` being this key's
    /// dealings in its dealers' order.
    pub fn derive_share(
        &self,
        recipient: Position,
        dealings: &[(Address, Dealing)],
        key: &DecryptionKey,
        table: &BabySteps,
    ) -> Result<SecretShare, DkgError> {
        if dealings.len() != self.dealers.len()
            || dealings.iter().zip(&self.dealers).any(|((dealer, _), expected)| dealer != expected)
        {
            return Err(DkgError::DealerMismatch);
        }
        let mut total: Option<SecretShare> = None;
        for (dealer, dealing) in dealings {
            let share = dealing
                .decrypt(recipient, key, table)
                .map_err(|source| DkgError::BadDealing { dealer: *dealer, source })?;
            total = Some(match total {
                None => share,
                Some(acc) => acc.add(&share)?,
            });
        }
        let share = total.ok_or(DkgError::NoDealings)?;
        if share.public_key() != self.share_key(recipient) {
            return Err(DkgError::BadShare);
        }
        Ok(share)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::{
        nidkg::Recipient,
        threshold::{combine, SecretPolynomial},
        BlsKeypair, Signer,
    };
    use rand::{rngs::StdRng, SeedableRng};

    /// Two dealers deal to four recipients; every recipient derives a share of the summed key,
    /// and any three partial signatures combine into one signature under the group key.
    #[test]
    fn shares_of_summed_dealings_sign_under_the_group_key() {
        let mut rng = StdRng::seed_from_u64(1);
        let keys: Vec<DecryptionKey> = (0..4)
            .map(|_| {
                let keypair = BlsKeypair::generate(&mut rng);
                DecryptionKey::from_signature(&keypair.sign(b"derive")).unwrap()
            })
            .collect();
        let recipients: Vec<Recipient> = keys
            .iter()
            .enumerate()
            .map(|(position, key)| Recipient {
                position: position as u32,
                key: key.encryption_key(),
            })
            .collect();
        let dealings: Vec<(Address, Dealing)> = (1..=2u8)
            .map(|d| {
                let polynomial = SecretPolynomial::random(3, &mut rng).unwrap();
                let dealer = Address::repeat_byte(d);
                (dealer, Dealing::new(&[d], &polynomial, &recipients, &mut rng).unwrap())
            })
            .collect();
        let key = EpochKey::from_dealings(9, vec![], &dealings).unwrap();
        assert_eq!(key.dealers, vec![Address::repeat_byte(1), Address::repeat_byte(2)]);
        let table = BabySteps::new();
        let shares: Vec<SecretShare> = (0..4)
            .map(|position| {
                key.derive_share(position, &dealings, &keys[position as usize], &table).unwrap()
            })
            .collect();
        let msg = b"header digest";
        let partials: Vec<_> = shares[1..].iter().map(|s| s.partial_sign(msg)).collect();
        let signature = combine(3, &partials).unwrap();
        assert!(signature.verify_raw(msg, &key.group_key()));
    }

    #[test]
    fn derive_share_refuses_dealings_out_of_order() {
        let mut rng = StdRng::seed_from_u64(2);
        let secret =
            DecryptionKey::from_signature(&BlsKeypair::generate(&mut rng).sign(b"x")).unwrap();
        let recipients = vec![Recipient { position: 0, key: secret.encryption_key() }];
        let dealings: Vec<(Address, Dealing)> = (1..=2u8)
            .map(|d| {
                let polynomial = SecretPolynomial::random(1, &mut rng).unwrap();
                (
                    Address::repeat_byte(d),
                    Dealing::new(&[d], &polynomial, &recipients, &mut rng).unwrap(),
                )
            })
            .collect();
        let key = EpochKey::from_dealings(3, vec![], &dealings).unwrap();
        let reversed: Vec<_> = dealings.iter().rev().cloned().collect();
        assert_eq!(
            key.derive_share(0, &reversed, &secret, &BabySteps::new()).unwrap_err(),
            DkgError::DealerMismatch
        );
        assert_eq!(EpochKey::from_dealings(3, vec![], &[]).unwrap_err(), DkgError::NoDealings);
    }

    #[test]
    fn dealers_needed_is_a_third_rounded_up() {
        assert_eq!(dealers_needed(0), 1);
        assert_eq!(dealers_needed(1), 1);
        assert_eq!(dealers_needed(4), 2);
        assert_eq!(dealers_needed(7), 3);
        assert_eq!(dealers_needed(56), 19);
    }
}
