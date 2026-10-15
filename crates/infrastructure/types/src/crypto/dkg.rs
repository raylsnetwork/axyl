//! Per-epoch distributed key generation (DKG) for threshold certificates.
//!
//! The committee of epoch `E-1` deals shares of the key for epoch `E` to the committee of `E`.
//! Each dealer picks a random polynomial, commits to it (Feldman commitments in G2) and encrypts
//! one share per recipient to that recipient's on-chain BLS key. Deals and complaints travel in
//! headers, so the committed DAG gives every node the same ordered list of them, and
//! [`DkgState`] turns that list into the same epoch key on every node.
//!
//! Encryption is a pairing KEM. The dealer publishes `U = g2^r` once per deal and derives, per
//! recipient, `K_i = e(H(tag_i)^r, pk_i)`, where `tag_i` names the epoch, the dealer and the
//! recipient. The recipient derives the same `K_i = e(sigma_i, U)` from `sigma_i`, its BLS
//! signature over `tag_i`, which its signer produces without exposing the secret key. A recipient
//! whose share does not open or does not match the commitments reveals `sigma_i` in a complaint;
//! anyone can then decrypt that one ciphertext and confirm the dealer cheated. `sigma_i` is never
//! published otherwise, which is why the tag has its own intent scope.

use super::{
    threshold::{
        ConstantTermProof, PublicPolynomial, SecretPolynomial, SecretShare, ShareIndex,
        ThresholdError,
    },
    BlsPublicKey, BlsSignature, Intent, IntentMessage, IntentScope, Signer, DST_G1,
};
use crate::{encode, quorum_threshold, AuthorityIdentifier, ConsensusHeader, Epoch, Round};
use blst::{
    blst_bendian_from_fp12, blst_fp12,
    min_sig::{PublicKey, SecretKey, Signature},
};
use rand::{CryptoRng, RngCore};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Deref, sync::Arc};
use thiserror::Error;

/// A validator's position in the committee's key order, the index space of signer bitmaps.
pub type Position = u32;

/// Leader rounds after a deal's commit during which a complaint against it counts.
pub const COMPLAINT_WINDOW: Round = 20;

/// Big-endian size of a GT element.
const FP12_BYTES: usize = 576;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DkgError {
    #[error("deal is for epoch {got}, expected {expected}")]
    WrongEpoch { expected: Epoch, got: Epoch },
    #[error("deal has threshold {got}, expected {expected}")]
    WrongThreshold { expected: usize, got: usize },
    #[error("deal has {got} encrypted shares, expected {expected}")]
    WrongRecipientCount { expected: usize, got: usize },
    #[error("a deal point is not a valid group element")]
    BadPoint,
    #[error("deal's proof of knowledge of its constant term does not verify")]
    BadProof,
    #[error("position {0} is out of range")]
    BadPosition(Position),
    #[error("decryption key is not a signature over the share tag by the recipient")]
    BadDecryptionKey,
    #[error("share ciphertext failed authentication")]
    BadCiphertext,
    #[error("decrypted share does not match the dealer's commitments")]
    BadShare,
    #[error("complaint targets a dealer with no committed deal")]
    UnknownDealer,
    #[error("complaint is false: the share decrypts and matches the commitments")]
    FalseComplaint,
    #[error("complaint was committed after the dealer's complaint window")]
    LateComplaint,
    #[error("no qualified deals")]
    NoDeals,
    #[error("the share from dealer {dealer} does not open or does not match its commitments")]
    BadShareFrom { dealer: Position },
    #[error(transparent)]
    Threshold(#[from] ThresholdError),
}

/// Names one share ciphertext. The recipient's signature over it is its decryption key.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShareTag {
    pub epoch: Epoch,
    pub dealer: Position,
    pub recipient: Position,
}

impl ShareTag {
    /// The bytes the recipient signs: an intent message in the `DkgShare` scope, so no other
    /// protocol path ever produces this signature.
    pub fn message(&self) -> Vec<u8> {
        encode(&IntentMessage::new(Intent::consensus(IntentScope::DkgShare), *self))
    }

    /// The recipient's decryption key for this share. Keep it private unless complaining.
    pub fn decryption_key(&self, signer: &dyn Signer) -> BlsSignature {
        signer.sign(&self.message())
    }
}

/// One share, encrypted under the KEM secret shared with its recipient.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptedShare {
    ciphertext: [u8; 32],
    mac: [u8; 32],
}

impl EncryptedShare {
    fn seal(key: &[u8; 32], share: &[u8; 32]) -> Self {
        let pad = blake3::derive_key("rayls dkg share pad", key);
        let mut ciphertext = [0u8; 32];
        for (out, (byte, mask)) in ciphertext.iter_mut().zip(share.iter().zip(pad.iter())) {
            *out = byte ^ mask;
        }
        let mac = blake3::keyed_hash(&blake3::derive_key("rayls dkg share mac", key), &ciphertext);
        Self { ciphertext, mac: *mac.as_bytes() }
    }

    fn open(&self, key: &[u8; 32]) -> Option<[u8; 32]> {
        let mac =
            blake3::keyed_hash(&blake3::derive_key("rayls dkg share mac", key), &self.ciphertext);
        if mac != self.mac {
            return None;
        }
        let pad = blake3::derive_key("rayls dkg share pad", key);
        let mut share = [0u8; 32];
        for (out, (byte, mask)) in share.iter_mut().zip(self.ciphertext.iter().zip(pad.iter())) {
            *out = byte ^ mask;
        }
        Some(share)
    }
}

/// Shared secret of the pairing KEM, `KDF(e(p, q))`. The dealer calls it with `(H(tag)^r, pk_i)`
/// and the recipient with `(sigma_i, U)`; both are `e(H(tag), g2)^(r * x_i)`.
fn kem(p: &Signature, q: &PublicKey) -> [u8; 32] {
    let gt = blst_fp12::miller_loop(q.into(), p.into()).final_exp();
    let mut bytes = [0u8; FP12_BYTES];
    // SAFETY: `bytes` holds the 576 bytes blst writes for a GT element.
    unsafe { blst_bendian_from_fp12(bytes.as_mut_ptr(), &gt) };
    blake3::derive_key("rayls dkg share kem", &bytes)
}

/// A dealer's contribution to an epoch key: commitments to its polynomial, a proof that it
/// knows the constant term, and one encrypted share per recipient in the recipient committee's
/// key order.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Deal {
    pub epoch: Epoch,
    pub commitments: PublicPolynomial,
    /// Binds the commitments to this dealer and epoch, so a dealer cannot derive them from
    /// other dealers' commitments.
    pub proof: ConstantTermProof,
    /// `U = g2^r`, the KEM's ephemeral value shared by every ciphertext in the deal.
    pub ephemeral: BlsPublicKey,
    pub shares: Vec<EncryptedShare>,
}

/// What a deal's proof of knowledge is bound to.
fn proof_context(epoch: Epoch, dealer: Position) -> Vec<u8> {
    encode(&(epoch, dealer))
}

impl Deal {
    /// Deal shares of a fresh random polynomial to `recipients`. The polynomial and the KEM
    /// randomness are dropped on return; the dealer recovers its own share like any recipient.
    pub fn new<R: CryptoRng + RngCore>(
        epoch: Epoch,
        dealer: Position,
        threshold: usize,
        recipients: &[BlsPublicKey],
        rng: &mut R,
    ) -> Result<Self, DkgError> {
        let polynomial = SecretPolynomial::random(threshold, rng)?;
        let commitments = polynomial.commit()?;
        let proof = polynomial.prove_constant_term(&proof_context(epoch, dealer), rng)?;
        let mut ikm = [0u8; 32];
        rng.fill_bytes(&mut ikm);
        let r = SecretKey::key_gen(&ikm, &[]).expect("32 bytes of ikm is enough");
        let ephemeral: BlsPublicKey = r.sk_to_pk().into();
        let shares = recipients
            .iter()
            .enumerate()
            .map(|(position, recipient_key)| {
                let recipient = position as Position;
                let tag = ShareTag { epoch, dealer, recipient };
                // `H(tag)^r` is a signature with key `r` over the tag bytes.
                let h_r = r.sign(&tag.message(), DST_G1, &[]);
                let key = kem(&h_r, recipient_key);
                let share = polynomial.share(ShareIndex::from_position(recipient))?;
                Ok(EncryptedShare::seal(&key, &share.to_bytes()))
            })
            .collect::<Result<Vec<_>, DkgError>>()?;
        Ok(Self { epoch, commitments, proof, ephemeral, shares })
    }

    /// Checks a voter runs before voting for a header by `dealer` that carries this deal.
    pub fn validate(
        &self,
        dealer: Position,
        epoch: Epoch,
        threshold: usize,
        recipient_count: usize,
    ) -> Result<(), DkgError> {
        if self.epoch != epoch {
            return Err(DkgError::WrongEpoch { expected: epoch, got: self.epoch });
        }
        if self.commitments.threshold() != threshold {
            return Err(DkgError::WrongThreshold {
                expected: threshold,
                got: self.commitments.threshold(),
            });
        }
        if self.shares.len() != recipient_count {
            return Err(DkgError::WrongRecipientCount {
                expected: recipient_count,
                got: self.shares.len(),
            });
        }
        for key in self.commitments.coefficients() {
            key.validate().map_err(|_| DkgError::BadPoint)?;
        }
        self.ephemeral.validate().map_err(|_| DkgError::BadPoint)?;
        if !self.commitments.verify_constant_term_proof(&proof_context(epoch, dealer), &self.proof)
        {
            return Err(DkgError::BadProof);
        }
        Ok(())
    }

    /// The share for `recipient`, opened with its decryption key for this deal and checked
    /// against the commitments.
    pub fn decrypt_share(
        &self,
        recipient: Position,
        decryption_key: &BlsSignature,
    ) -> Result<SecretShare, DkgError> {
        let encrypted =
            self.shares.get(recipient as usize).ok_or(DkgError::BadPosition(recipient))?;
        let key = kem(decryption_key.deref(), self.ephemeral.deref());
        let bytes = encrypted.open(&key).ok_or(DkgError::BadCiphertext)?;
        let index = ShareIndex::from_position(recipient);
        let share = SecretShare::from_bytes(index, &bytes).map_err(|_| DkgError::BadShare)?;
        if share.public_key() != self.commitments.share_key(index) {
            return Err(DkgError::BadShare);
        }
        Ok(share)
    }
}

/// A recipient's proof that a dealer sent it a bad share: the decryption key for that one
/// ciphertext, so everyone can open it and check it against the commitments.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Complaint {
    pub tag: ShareTag,
    pub decryption_key: BlsSignature,
}

impl Complaint {
    pub fn new(tag: ShareTag, signer: &dyn Signer) -> Self {
        Self { tag, decryption_key: tag.decryption_key(signer) }
    }

    /// Check the complaint against the dealer's committed deal. `Ok` means the dealer cheated;
    /// `FalseComplaint` means the share is fine and the complainer is the one at fault.
    pub fn validate(&self, deal: &Deal, recipient_key: &BlsPublicKey) -> Result<(), DkgError> {
        if deal.epoch != self.tag.epoch {
            return Err(DkgError::WrongEpoch { expected: deal.epoch, got: self.tag.epoch });
        }
        if !self.decryption_key.verify_raw(&self.tag.message(), recipient_key) {
            return Err(DkgError::BadDecryptionKey);
        }
        match deal.decrypt_share(self.tag.recipient, &self.decryption_key) {
            Ok(_) => Err(DkgError::FalseComplaint),
            Err(DkgError::BadCiphertext | DkgError::BadShare) => Ok(()),
            Err(other) => Err(other),
        }
    }
}

/// What a header carries for the DKG: the author's deal, if any, and the complaints it relays.
/// Empty on every header before the fork and once the author's deal has been committed.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DkgPayload {
    pub deal: Option<Deal>,
    pub complaints: Vec<Complaint>,
}

impl DkgPayload {
    pub fn is_empty(&self) -> bool {
        self.deal.is_none() && self.complaints.is_empty()
    }
}

/// Parameters every node agrees on before the DKG for an epoch starts.
#[derive(Clone, Debug)]
pub struct DkgConfig {
    /// The epoch the key is for.
    pub epoch: Epoch,
    /// Shares needed to sign: the recipient committee's quorum.
    pub threshold: usize,
    /// The recipient committee's keys in bitmap order.
    pub recipients: Vec<BlsPublicKey>,
    /// Size of the dealing committee.
    pub dealer_count: usize,
    /// Distinct qualified dealers needed: the dealing committee's validity threshold, so at
    /// least one of them is honest.
    pub dealers_needed: usize,
    /// Leader rounds after a deal's commit during which a complaint against it counts.
    pub complaint_window: Round,
}

#[derive(Clone, Debug)]
struct Candidate {
    dealer: Position,
    round: Round,
    deal: Deal,
    disqualified: bool,
}

/// Deterministic consumer of the committed deal and complaint stream. Fed in commit order with
/// the leader round each item was committed at, it yields the same outcome on every node.
#[derive(Clone, Debug)]
pub struct DkgState {
    config: DkgConfig,
    candidates: Vec<Candidate>,
}

impl DkgState {
    pub fn new(config: DkgConfig) -> Self {
        Self { config, candidates: Vec::new() }
    }

    pub fn config(&self) -> &DkgConfig {
        &self.config
    }

    /// A deal by `dealer` committed at leader `round`. Only a dealer's first deal counts; the
    /// return value says whether this one was it.
    pub fn on_deal(
        &mut self,
        dealer: Position,
        round: Round,
        deal: Deal,
    ) -> Result<bool, DkgError> {
        if dealer as usize >= self.config.dealer_count {
            return Err(DkgError::BadPosition(dealer));
        }
        deal.validate(
            dealer,
            self.config.epoch,
            self.config.threshold,
            self.config.recipients.len(),
        )?;
        if self.candidates.iter().any(|c| c.dealer == dealer) {
            return Ok(false);
        }
        self.candidates.push(Candidate { dealer, round, deal, disqualified: false });
        Ok(true)
    }

    /// A complaint committed at leader `round`. `Ok(true)` when it newly disqualified a dealer.
    pub fn on_complaint(&mut self, round: Round, complaint: &Complaint) -> Result<bool, DkgError> {
        let recipient_key = self
            .config
            .recipients
            .get(complaint.tag.recipient as usize)
            .ok_or(DkgError::BadPosition(complaint.tag.recipient))?;
        let window = self.config.complaint_window;
        let candidate = self
            .candidates
            .iter_mut()
            .find(|c| c.dealer == complaint.tag.dealer)
            .ok_or(DkgError::UnknownDealer)?;
        if round > candidate.round.saturating_add(window) {
            return Err(DkgError::LateComplaint);
        }
        complaint.validate(&candidate.deal, recipient_key)?;
        let newly = !candidate.disqualified;
        candidate.disqualified = true;
        Ok(newly)
    }

    /// The outcome once nothing more can be committed before `final_round`, the epoch's last
    /// leader round. A dealer qualifies when it was not disqualified and its complaint window
    /// closed before `final_round`; the first `dealers_needed` of them define the key.
    pub fn outcome(&self, final_round: Round) -> Option<EpochKey> {
        let window = self.config.complaint_window;
        let qualified: Vec<&Candidate> = self
            .candidates
            .iter()
            .filter(|c| !c.disqualified && c.round.saturating_add(window) < final_round)
            .take(self.config.dealers_needed)
            .collect();
        if qualified.len() < self.config.dealers_needed {
            return None;
        }
        let public = qualified[1..]
            .iter()
            .try_fold(qualified[0].deal.commitments.clone(), |acc, c| acc.add(&c.deal.commitments))
            .expect("validated deals share one threshold");
        Some(EpochKey {
            epoch: self.config.epoch,
            public,
            deals: qualified.iter().map(|c| (c.dealer, c.deal.clone())).collect(),
        })
    }
}

/// What the DKG for an epoch produced, persisted by every node that processed the dealing epoch
/// and keyed by the epoch the key is for.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DkgOutcome {
    /// The epoch uses threshold certificates with this key.
    Key(EpochKey),
    /// Too few dealers qualified: the epoch falls back to aggregate certificates.
    Fallback,
}

/// Parameters a dealing committee shares for the next epoch's key: what the proposer deals and
/// what voters validate deals against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DealingParams {
    /// The epoch the key is for.
    pub epoch: Epoch,
    /// The recipient committee's quorum.
    pub threshold: usize,
    /// The recipient committee's keys in bitmap order.
    pub recipients: Vec<BlsPublicKey>,
    /// Leader rounds after a deal's commit during which complaints count.
    pub complaint_window: Round,
}

impl DealingParams {
    /// Parameters for dealing the key of `epoch` to `recipients` (bitmap order): their quorum
    /// signs, and complaints count for [`COMPLAINT_WINDOW`] leader rounds.
    pub fn new(epoch: Epoch, recipients: Vec<BlsPublicKey>) -> Self {
        Self {
            epoch,
            threshold: quorum_threshold(recipients.len() as u64) as usize,
            recipients,
            complaint_window: COMPLAINT_WINDOW,
        }
    }

    /// The DKG configuration for a dealing committee of `dealer_count` members whose validity
    /// threshold is `dealers_needed`.
    pub fn config(&self, dealer_count: usize, dealers_needed: usize) -> DkgConfig {
        DkgConfig {
            epoch: self.epoch,
            threshold: self.threshold,
            recipients: self.recipients.clone(),
            dealer_count,
            dealers_needed,
            complaint_window: self.complaint_window,
        }
    }
}

/// Dealers that must qualify: the dealing committee's validity threshold, so at least one of them
/// is honest.
pub fn dealers_needed(dealer_count: usize) -> usize {
    dealer_count.div_ceil(3).max(1)
}

/// Replays a dealing epoch's committed headers through [`DkgState`]. Fed every header of the
/// epoch in commit order, up to and including the boundary, it gives the same [`DkgOutcome`] on
/// every node: every input is committed consensus data, and invalid deals or complaints are
/// skipped the same way everywhere.
#[derive(Debug)]
pub struct DealingReplay {
    state: DkgState,
    /// Dealer positions in the dealing committee's key order, by authority.
    positions: HashMap<AuthorityIdentifier, Position>,
    /// Headers fed so far.
    pub headers: usize,
    /// Deals that became candidates.
    pub deals: usize,
    /// Complaints that disqualified a dealer.
    pub complaints: usize,
}

impl DealingReplay {
    /// A replay of the DKG for `epoch`, dealt by `dealers` to `recipients` (both in key order).
    pub fn new(epoch: Epoch, dealers: &[BlsPublicKey], recipients: Vec<BlsPublicKey>) -> Self {
        let positions = dealers
            .iter()
            .enumerate()
            .map(|(position, key)| (AuthorityIdentifier::from(*key), position as Position))
            .collect();
        let config = DealingParams::new(epoch, recipients)
            .config(dealers.len(), dealers_needed(dealers.len()));
        Self { state: DkgState::new(config), positions, headers: 0, deals: 0, complaints: 0 }
    }

    /// Feed the next committed header. Its certificates' DKG items count at its leader round.
    pub fn feed(&mut self, header: &ConsensusHeader) {
        self.headers += 1;
        let round = header.sub_dag.leader_round();
        for cert in &header.sub_dag.certificates {
            let payload = cert.header().dkg();
            if payload.is_empty() {
                continue;
            }
            let Some(&dealer) = self.positions.get(cert.header().author()) else {
                continue;
            };
            if let Some(deal) = &payload.deal {
                // Errors mean an invalid deal, ignored identically on every node.
                if let Ok(true) = self.state.on_deal(dealer, round, deal.clone()) {
                    self.deals += 1;
                }
            }
            for complaint in &payload.complaints {
                if let Ok(true) = self.state.on_complaint(round, complaint) {
                    self.complaints += 1;
                }
            }
        }
    }

    /// The outcome once the boundary, committed at leader round `final_round`, was fed.
    pub fn outcome(&self, final_round: Round) -> DkgOutcome {
        match self.state.outcome(final_round) {
            Some(key) => DkgOutcome::Key(key),
            None => DkgOutcome::Fallback,
        }
    }
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

/// The key for one epoch: the summed public polynomial and the qualified deals a recipient
/// opens to derive its share.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EpochKey {
    pub epoch: Epoch,
    pub public: PublicPolynomial,
    pub deals: Vec<(Position, Deal)>,
}

impl EpochKey {
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

    /// The recipient's share: the sum of its shares from every qualified deal, each opened with
    /// the decryption key its signer derives for that deal.
    pub fn derive_share(
        &self,
        recipient: Position,
        signer: &dyn Signer,
    ) -> Result<SecretShare, DkgError> {
        let mut total: Option<SecretShare> = None;
        for (dealer, deal) in &self.deals {
            let tag = ShareTag { epoch: self.epoch, dealer: *dealer, recipient };
            let share = deal.decrypt_share(recipient, &tag.decryption_key(signer)).map_err(
                |e| match e {
                    DkgError::BadCiphertext | DkgError::BadShare => {
                        DkgError::BadShareFrom { dealer: *dealer }
                    }
                    other => other,
                },
            )?;
            total = Some(match total {
                None => share,
                Some(acc) => acc.add(&share)?,
            });
        }
        let share = total.ok_or(DkgError::NoDeals)?;
        if share.public_key() != self.share_key(recipient) {
            return Err(DkgError::BadShare);
        }
        Ok(share)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crypto::{threshold::combine, BlsKeypair},
        try_decode,
    };

    const EPOCH: Epoch = 7;
    const WINDOW: Round = 10;

    struct Setup {
        recipients: Vec<BlsKeypair>,
        config: DkgConfig,
    }

    fn setup(dealers: usize, recipients: usize) -> Setup {
        let mut rng = rand::rng();
        let recipients: Vec<BlsKeypair> =
            (0..recipients).map(|_| BlsKeypair::generate(&mut rng)).collect();
        let config = DkgConfig {
            epoch: EPOCH,
            threshold: recipients.len() * 2 / 3 + 1,
            recipients: recipients.iter().map(|k| *k.public()).collect(),
            dealer_count: dealers,
            dealers_needed: dealers.div_ceil(3),
            complaint_window: WINDOW,
        };
        Setup { recipients, config }
    }

    fn honest_deal(s: &Setup, dealer: Position) -> Deal {
        Deal::new(EPOCH, dealer, s.config.threshold, &s.config.recipients, &mut rand::rng())
            .unwrap()
    }

    /// A deal whose share for `victim` is a different, well-formed secret under a valid MAC.
    fn cheating_deal(s: &Setup, dealer: Position, victim: Position) -> Deal {
        let mut deal = honest_deal(s, dealer);
        let tag = ShareTag { epoch: EPOCH, dealer, recipient: victim };
        let sigma = tag.decryption_key(&s.recipients[victim as usize]);
        let key = kem(sigma.deref(), deal.ephemeral.deref());
        let wrong = BlsKeypair::generate(&mut rand::rng()).to_bytes();
        deal.shares[victim as usize] = EncryptedShare::seal(&key, &wrong);
        deal
    }

    #[test]
    fn honest_dealers_produce_a_working_epoch_key() {
        let s = setup(4, 4);
        let mut state = DkgState::new(s.config.clone());
        for dealer in 0..4 {
            assert!(state.on_deal(dealer, 1, honest_deal(&s, dealer)).unwrap());
        }
        assert!(state.outcome(WINDOW + 1).is_none(), "windows are still open");
        let key = state.outcome(WINDOW + 2).unwrap();
        assert_eq!(key.deals.len(), s.config.dealers_needed);
        assert_eq!(key.threshold(), s.config.threshold);

        let msg = b"header digest";
        let partials: Vec<_> = s
            .recipients
            .iter()
            .enumerate()
            .map(|(position, keypair)| {
                let share = key.derive_share(position as Position, keypair).unwrap();
                assert_eq!(share.public_key(), key.share_key(position as Position));
                let partial = share.partial_sign(msg);
                assert!(partial.verify(&key.public, msg));
                partial
            })
            .collect();
        let combined = combine(key.threshold(), &partials).unwrap();
        assert!(combined.verify_raw(msg, &key.group_key()));
        assert_eq!(
            combined.to_bytes(),
            combine(key.threshold(), &partials[1..]).unwrap().to_bytes()
        );
    }

    #[test]
    fn only_a_dealers_first_deal_counts_and_late_deals_do_not_qualify() {
        let s = setup(4, 4);
        let mut state = DkgState::new(s.config.clone());
        assert!(state.on_deal(0, 1, honest_deal(&s, 0)).unwrap());
        assert!(!state.on_deal(0, 2, honest_deal(&s, 0)).unwrap());
        assert!(state.on_deal(1, 50, honest_deal(&s, 1)).unwrap());
        // Dealer 1's window is still open at the final round, so only dealer 0 qualifies, and
        // two are needed.
        assert!(state.outcome(55).is_none());
        assert_eq!(state.outcome(100).unwrap().deals.len(), 2);
        assert_eq!(state.on_deal(9, 1, honest_deal(&s, 9)), Err(DkgError::BadPosition(9)));
    }

    #[test]
    fn deals_are_validated_structurally() {
        let s = setup(4, 4);
        let deal = honest_deal(&s, 0);
        assert_eq!(deal.validate(0, EPOCH, s.config.threshold, 4), Ok(()));
        assert_eq!(
            deal.validate(0, EPOCH + 1, s.config.threshold, 4),
            Err(DkgError::WrongEpoch { expected: EPOCH + 1, got: EPOCH })
        );
        assert_eq!(
            deal.validate(0, EPOCH, 2, 4),
            Err(DkgError::WrongThreshold { expected: 2, got: 3 })
        );
        assert_eq!(
            deal.validate(0, EPOCH, 3, 5),
            Err(DkgError::WrongRecipientCount { expected: 5, got: 4 })
        );
    }

    #[test]
    fn a_deal_must_prove_knowledge_of_its_own_constant_term() {
        let s = setup(4, 4);
        let mine = honest_deal(&s, 0);
        // The proof is bound to the dealer: the same deal under another author fails.
        assert_eq!(mine.validate(1, EPOCH, 3, 4), Err(DkgError::BadProof));
        // Commitments copied (or derived) from someone else's deal fail: the dealer does not know
        // their constant term.
        let theirs = honest_deal(&s, 1);
        let rogue = Deal { commitments: theirs.commitments.clone(), ..mine.clone() };
        assert_eq!(rogue.validate(0, EPOCH, 3, 4), Err(DkgError::BadProof));
        let mut state = DkgState::new(s.config.clone());
        assert_eq!(state.on_deal(0, 1, rogue), Err(DkgError::BadProof));
        assert!(state.on_deal(0, 1, mine).unwrap());
    }

    #[test]
    fn a_valid_complaint_disqualifies_the_dealer() {
        let s = setup(4, 4);
        let victim: Position = 2;
        let mut state = DkgState::new(s.config.clone());
        let bad = cheating_deal(&s, 0, victim);
        assert!(state.on_deal(0, 1, bad.clone()).unwrap());
        for dealer in 1..4 {
            assert!(state.on_deal(dealer, 1, honest_deal(&s, dealer)).unwrap());
        }
        let tag = ShareTag { epoch: EPOCH, dealer: 0, recipient: victim };
        let sigma = tag.decryption_key(&s.recipients[victim as usize]);
        assert_eq!(bad.decrypt_share(victim, &sigma), Err(DkgError::BadShare));
        // Other recipients' shares from the same deal are fine.
        let other = ShareTag { epoch: EPOCH, dealer: 0, recipient: 1 };
        assert!(bad.decrypt_share(1, &other.decryption_key(&s.recipients[1])).is_ok());

        let complaint = Complaint::new(tag, &s.recipients[victim as usize]);
        assert!(state.on_complaint(5, &complaint).unwrap());
        assert!(!state.on_complaint(5, &complaint).unwrap(), "already disqualified");

        let key = state.outcome(WINDOW + 2).unwrap();
        assert_eq!(key.deals.iter().map(|(d, _)| *d).collect::<Vec<_>>(), vec![1, 2]);
        for (position, keypair) in s.recipients.iter().enumerate() {
            assert!(key.derive_share(position as Position, keypair).is_ok());
        }
    }

    #[test]
    fn a_tampered_ciphertext_is_also_a_valid_complaint() {
        let s = setup(4, 4);
        let mut bad = honest_deal(&s, 0);
        bad.shares[3].ciphertext[0] ^= 1;
        let tag = ShareTag { epoch: EPOCH, dealer: 0, recipient: 3 };
        let sigma = tag.decryption_key(&s.recipients[3]);
        assert_eq!(bad.decrypt_share(3, &sigma), Err(DkgError::BadCiphertext));
        let complaint = Complaint::new(tag, &s.recipients[3]);
        assert_eq!(complaint.validate(&bad, s.recipients[3].public()), Ok(()));
    }

    #[test]
    fn false_and_forged_complaints_are_rejected() {
        let s = setup(4, 4);
        let mut state = DkgState::new(s.config.clone());
        let deal = honest_deal(&s, 0);
        assert!(state.on_deal(0, 1, deal.clone()).unwrap());

        let tag = ShareTag { epoch: EPOCH, dealer: 0, recipient: 1 };
        let honest_complaint = Complaint::new(tag, &s.recipients[1]);
        assert_eq!(state.on_complaint(2, &honest_complaint), Err(DkgError::FalseComplaint));

        // A complaint carrying another recipient's key, or a key over another tag, is rejected
        // before any decryption happens.
        let forged = Complaint { tag, decryption_key: tag.decryption_key(&s.recipients[2]) };
        assert_eq!(state.on_complaint(2, &forged), Err(DkgError::BadDecryptionKey));
        let other_tag = ShareTag { epoch: EPOCH, dealer: 3, recipient: 1 };
        let wrong_tag =
            Complaint { tag, decryption_key: other_tag.decryption_key(&s.recipients[1]) };
        assert_eq!(state.on_complaint(2, &wrong_tag), Err(DkgError::BadDecryptionKey));

        let unknown =
            Complaint::new(ShareTag { epoch: EPOCH, dealer: 2, recipient: 1 }, &s.recipients[1]);
        assert_eq!(state.on_complaint(2, &unknown), Err(DkgError::UnknownDealer));

        // A valid complaint that lands after the window does not count.
        let mut late_state = DkgState::new(s.config.clone());
        let bad = cheating_deal(&s, 0, 1);
        assert!(late_state.on_deal(0, 1, bad).unwrap());
        let late = Complaint::new(tag, &s.recipients[1]);
        assert_eq!(late_state.on_complaint(1 + WINDOW + 1, &late), Err(DkgError::LateComplaint));
        assert!(late_state.on_complaint(1 + WINDOW, &late).unwrap());
    }

    #[test]
    fn a_complaint_for_another_epoch_cannot_disqualify_a_dealer() {
        let s = setup(4, 4);
        let mut state = DkgState::new(s.config.clone());
        let bad = cheating_deal(&s, 0, 1);
        assert!(state.on_deal(0, 1, bad.clone()).unwrap());
        assert!(state.on_deal(1, 1, honest_deal(&s, 1)).unwrap());
        let foreign = Complaint::new(
            ShareTag { epoch: EPOCH + 1, dealer: 0, recipient: 1 },
            &s.recipients[1],
        );
        assert_eq!(
            foreign.validate(&bad, s.recipients[1].public()),
            Err(DkgError::WrongEpoch { expected: EPOCH, got: EPOCH + 1 })
        );
        assert!(state.on_complaint(2, &foreign).is_err());
        let key = state.outcome(100).unwrap();
        assert_eq!(key.deals.iter().map(|(d, _)| *d).collect::<Vec<_>>(), vec![0, 1]);
        // The victim cannot derive a share and is told which dealer is at fault.
        assert_eq!(
            key.derive_share(1, &s.recipients[1]).map(|_| ()),
            Err(DkgError::BadShareFrom { dealer: 0 })
        );
    }

    #[test]
    fn a_dealers_second_deal_does_not_replace_its_first() {
        let s = setup(4, 4);
        let mut state = DkgState::new(s.config.clone());
        let first = honest_deal(&s, 0);
        assert!(state.on_deal(0, 1, first.clone()).unwrap());
        assert!(!state.on_deal(0, 2, honest_deal(&s, 0)).unwrap());
        assert!(state.on_deal(1, 1, honest_deal(&s, 1)).unwrap());
        let key = state.outcome(100).unwrap();
        assert_eq!(key.deals[0], (0, first));
        assert!(key.derive_share(1, &s.recipients[1]).is_ok());
    }

    #[test]
    fn too_few_qualified_dealers_means_no_key() {
        let s = setup(7, 4);
        assert_eq!(s.config.dealers_needed, 3);
        let mut state = DkgState::new(s.config.clone());
        for dealer in 0..2 {
            assert!(state.on_deal(dealer, 1, honest_deal(&s, dealer)).unwrap());
        }
        assert!(state.outcome(100).is_none());
        assert!(state.on_deal(2, 1, honest_deal(&s, 2)).unwrap());
        assert!(state.outcome(100).is_some());
    }

    #[test]
    fn a_wrong_decryption_key_cannot_open_a_share() {
        let s = setup(4, 4);
        let deal = honest_deal(&s, 0);
        let other_tag = ShareTag { epoch: EPOCH + 1, dealer: 0, recipient: 1 };
        let key = other_tag.decryption_key(&s.recipients[1]);
        assert_eq!(deal.decrypt_share(1, &key), Err(DkgError::BadCiphertext));
        assert_eq!(deal.decrypt_share(9, &key), Err(DkgError::BadPosition(9)));
    }

    #[test]
    fn deals_complaints_and_keys_round_trip_through_bcs() {
        let s = setup(4, 4);
        let deal = honest_deal(&s, 0);
        let decoded: Deal = try_decode(&encode(&deal)).unwrap();
        assert_eq!(decoded, deal);
        let complaint =
            Complaint::new(ShareTag { epoch: EPOCH, dealer: 0, recipient: 1 }, &s.recipients[1]);
        let decoded: Complaint = try_decode(&encode(&complaint)).unwrap();
        assert_eq!(decoded, complaint);
        let mut state = DkgState::new(s.config.clone());
        for dealer in 0..4 {
            state.on_deal(dealer, 1, honest_deal(&s, dealer)).unwrap();
        }
        let key = state.outcome(100).unwrap();
        let decoded: EpochKey = try_decode(&encode(&key)).unwrap();
        assert_eq!(decoded, key);
    }
}
