//! Precompile that verifies publicly verifiable DKG dealings and encryption-key proofs natively,
//! for the `DkgManager` system contract. Active from the `ThresholdCertificates` fork.
//!
//! The checks are `rayls_infrastructure_types::nidkg`'s, run by every executing node, so the trust
//! model is that of execution itself. Doing the same in Solidity over the BLS12-381 precompiles
//! would cost several million gas per dealing for the same answer.
//!
//! Before activation the address is not in the precompile map at all, so a call to it behaves as
//! a call to an empty account, as it always has.

use alloy::{
    primitives::{address, Address, Bytes},
    sol,
    sol_types::{SolCall, SolValue},
};
use alloy_evm::precompiles::{DynPrecompile, PrecompileInput};
use rayls_infrastructure_types::{
    nidkg::{
        dealing_context, dealing_message, encryption_key_context, key_registration_message,
        Dealing, EncryptionKey, EncryptionKeyProof, Recipient,
    },
    try_decode, BlsPublicKey, BlsSignature,
};
use reth_chainspec::ForkCondition;
use reth_revm::precompile::{PrecompileError, PrecompileId, PrecompileOutput, PrecompileResult};
use std::sync::OnceLock;

/// Address of the precompile, next to the native ERC-20 one at `0x0400`.
pub const NIDKG_PRECOMPILE_ADDRESS: Address = address!("0000000000000000000000000000000000000401");

const NIDKG_PRECOMPILE_ID: PrecompileId =
    PrecompileId::Custom(std::borrow::Cow::Borrowed("NidkgVerifier"));

/// Gas for `verifyDealing`: a base (including one BLS signature check), a cost per committee
/// member for the proofs, and a cost per byte of the dealing for decoding it, every point of which
/// is decompressed and checked for subgroup membership. Priced from measured native times at the
/// rate of `ecrecover` (about 70 gas a microsecond): at 56 members a dealing is 67 kB, decodes in
/// about 106 ms and verifies in about 15 ms.
pub const DEALING_BASE_GAS: u64 = 160_000;
/// Gas per committee member of a dealing.
pub const DEALING_GAS_PER_RECIPIENT: u64 = 25_000;
/// Gas per byte of a dealing, for decoding it.
pub const DEALING_GAS_PER_BYTE: u64 = 110;
/// Gas for `verifyEncryptionKey`: one BLS signature check and two G1 multiplications.
pub const ENCRYPTION_KEY_GAS: u64 = 80_000;
/// Gas charged for a call that does not decode, so a malformed call is never free.
const MALFORMED_GAS: u64 = 3_000;

sol! {
    /// Verify a dealing for the key of `epoch` by the validator `dealer`, whose BLS public key
    /// (compressed) is `dealerKey` and who signed the dealing with it (`signature`).
    /// `recipientBlsKeys[i]` and `recipients[i]` are the compressed BLS key and encryption key of
    /// one member of the epoch's committee, in any order, the encryption key empty for a member
    /// that gets no share. The committee is sorted by BLS key bytes, the protocol's committee
    /// order, where the member at position `p` holds share index `p + 1`, and the dealing is
    /// checked against the members with a key. `threshold` is the full committee's. Returns false
    /// for any dealing that does not verify, including one that does not decode.
    function verifyDealing(
        uint64 chainId,
        uint32 epoch,
        address dealer,
        bytes dealerKey,
        bytes signature,
        uint32 threshold,
        bytes[] recipientBlsKeys,
        bytes[] recipients,
        bytes dealing
    ) external view returns (bool);

    /// Verify a validator's registration of a compressed encryption key: its BLS signature over
    /// the registration and the proof of knowledge of the key.
    function verifyEncryptionKey(
        uint64 chainId,
        bytes blsPublicKey,
        bytes encryptionKey,
        bytes proof,
        bytes signature
    ) external view returns (bool);
}

/// When the precompile is active, set once at startup from the node's chain spec.
static ACTIVATION: OnceLock<ForkCondition> = OnceLock::new();

/// Record the `ThresholdCertificates` activation. The first call wins: a process runs one chain.
pub fn initialize_nidkg_precompile(activation: ForkCondition) {
    let _ = ACTIVATION.set(activation);
}

/// Whether the precompile is in the map for a block at `number`.
pub(crate) fn nidkg_precompile_active_at(number: u64) -> bool {
    ACTIVATION.get().is_some_and(|activation| activation.active_at_block(number))
}

pub(crate) fn create_nidkg_dyn_precompile() -> DynPrecompile {
    DynPrecompile::new(NIDKG_PRECOMPILE_ID, |input: PrecompileInput<'_>| run(input.data, input.gas))
}

fn charge(gas_limit: u64, cost: u64) -> Result<(), PrecompileError> {
    if cost > gas_limit {
        return Err(PrecompileError::OutOfGas);
    }
    Ok(())
}

fn answer(gas: u64, valid: bool) -> PrecompileResult {
    Ok(PrecompileOutput::new(gas, Bytes::from(valid.abi_encode())))
}

fn run(data: &[u8], gas_limit: u64) -> PrecompileResult {
    let selector: Option<[u8; 4]> = data.get(..4).and_then(|s| s.try_into().ok());
    match selector {
        Some(verifyDealingCall::SELECTOR) => {
            let Ok(call) = verifyDealingCall::abi_decode(data) else {
                charge(gas_limit, MALFORMED_GAS)?;
                return answer(MALFORMED_GAS, false);
            };
            let gas = dealing_gas(call.recipientBlsKeys.len(), call.dealing.len());
            charge(gas_limit, gas)?;
            answer(gas, verify_dealing(&call))
        }
        Some(verifyEncryptionKeyCall::SELECTOR) => {
            let Ok(call) = verifyEncryptionKeyCall::abi_decode(data) else {
                charge(gas_limit, MALFORMED_GAS)?;
                return answer(MALFORMED_GAS, false);
            };
            charge(gas_limit, ENCRYPTION_KEY_GAS)?;
            answer(ENCRYPTION_KEY_GAS, verify_encryption_key(&call))
        }
        _ => {
            charge(gas_limit, MALFORMED_GAS)?;
            Err(PrecompileError::other("unknown selector"))
        }
    }
}

/// Gas `verifyDealing` charges for a committee of `members` and a dealing of `bytes`.
pub fn dealing_gas(members: usize, bytes: usize) -> u64 {
    DEALING_BASE_GAS
        .saturating_add(DEALING_GAS_PER_RECIPIENT.saturating_mul(members as u64))
        .saturating_add(DEALING_GAS_PER_BYTE.saturating_mul(bytes as u64))
}

fn verify_dealing(call: &verifyDealingCall) -> bool {
    let Ok(dealer_key) = BlsPublicKey::from_literal_bytes(&call.dealerKey) else {
        return false;
    };
    let Ok(signature) = BlsSignature::from_bytes(&call.signature) else {
        return false;
    };
    let message = dealing_message(call.chainId, call.epoch, call.dealer, &call.dealing);
    if !signature.verify_raw(&message, &dealer_key) {
        return false;
    }
    // Anyone can call this precompile, and its gas does not depend on the threshold, so the shape is
    // bounded before anything is sized by it: the only threshold a key can have is its committee's
    // quorum, and no committee above the cap gets a key.
    let members = call.recipientBlsKeys.len();
    if members == 0
        || members > crate::reth_env::dkg_manager::MAX_KEYED_COMMITTEE
        || u64::from(call.threshold) != rayls_infrastructure_types::quorum_threshold(members as u64)
    {
        return false;
    }
    let Some(recipients) = recipients_in_committee_order(&call.recipientBlsKeys, &call.recipients)
    else {
        return false;
    };
    // the size follows from the shape: refuse a mis-sized dealing before decoding anything
    if call.dealing.len() != Dealing::encoded_len_for(recipients.len(), call.threshold as usize) {
        return false;
    }
    let Ok(dealing) = try_decode::<Dealing>(&call.dealing) else {
        return false;
    };
    let context = dealing_context(call.chainId, call.epoch, call.dealer);
    dealing.verify(&context, call.threshold as usize, &recipients).is_ok()
}

/// The committee members with an encryption key, at their positions in the committee sorted by
/// BLS key bytes, the order of the protocol's committee (`Committee::bls_keys`). `None` for
/// mismatched lists, duplicate BLS keys or an encryption key that does not decode.
fn recipients_in_committee_order(
    bls_keys: &[Bytes],
    encryption_keys: &[Bytes],
) -> Option<Vec<Recipient>> {
    if bls_keys.len() != encryption_keys.len() {
        return None;
    }
    let mut members: Vec<(&[u8], &[u8])> = bls_keys
        .iter()
        .zip(encryption_keys)
        .map(|(bls, key)| (bls.as_ref(), key.as_ref()))
        .collect();
    members.sort_by(|a, b| a.0.cmp(b.0));
    if members.windows(2).any(|w| w[0].0 == w[1].0) {
        return None;
    }
    members
        .into_iter()
        .enumerate()
        .filter(|(_, (_, key))| !key.is_empty())
        .map(|(position, (_, key))| {
            EncryptionKey::from_bytes(key).map(|key| Recipient { position: position as u32, key })
        })
        .collect()
}

fn verify_encryption_key(call: &verifyEncryptionKeyCall) -> bool {
    let Ok(owner) = BlsPublicKey::from_literal_bytes(&call.blsPublicKey) else {
        return false;
    };
    let Some(key) = EncryptionKey::from_bytes(&call.encryptionKey) else {
        return false;
    };
    let Ok(proof) = try_decode::<EncryptionKeyProof>(&call.proof) else {
        return false;
    };
    let Ok(signature) = BlsSignature::from_bytes(&call.signature) else {
        return false;
    };
    signature.verify_raw(&key_registration_message(call.chainId, &key), &owner)
        && key.verify(&owner, &encryption_key_context(), &proof)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};
    use rayls_infrastructure_types::{
        encode, nidkg::DecryptionKey, threshold::SecretPolynomial, BlsKeypair, Signer,
    };

    fn decode_bool(output: &PrecompileOutput) -> bool {
        bool::abi_decode(&output.bytes).unwrap()
    }

    struct Fixture {
        recipient_bls_keys: Vec<Bytes>,
        recipients: Vec<Bytes>,
        dealing: Bytes,
        dealer: Address,
        dealer_keypair: BlsKeypair,
    }

    fn fixture(rng: &mut StdRng) -> Fixture {
        fixture_without(rng, None)
    }

    /// Four members; the one at committee position `keyless`, if any, has no encryption key and
    /// gets no share.
    fn fixture_without(rng: &mut StdRng, keyless: Option<usize>) -> Fixture {
        // recipients in BLS key order, as the protocol's committee orders them
        let mut members: Vec<(BlsPublicKey, EncryptionKey)> = (0..4)
            .map(|_| {
                let keypair = BlsKeypair::generate(rng);
                let key = DecryptionKey::from_signature(&keypair.sign(b"enc")).unwrap();
                (*keypair.public(), key.encryption_key())
            })
            .collect();
        members.sort_by(|a, b| a.0.cmp(&b.0));
        let recipients: Vec<Recipient> = members
            .iter()
            .enumerate()
            .filter(|(position, _)| Some(*position) != keyless)
            .map(|(position, (_, key))| Recipient { position: position as u32, key: *key })
            .collect();
        let dealer = Address::repeat_byte(7);
        let polynomial = SecretPolynomial::random(3, rng).unwrap();
        let dealing =
            Dealing::new(&dealing_context(42, 5, dealer), &polynomial, &recipients, rng).unwrap();
        let encryption_keys: Vec<Bytes> = members
            .iter()
            .enumerate()
            .map(|(position, (_, key))| match Some(position) == keyless {
                true => Bytes::new(),
                false => Bytes::from(key.to_bytes().to_vec()),
            })
            .collect();
        // submitted in another order: the precompile sorts them back
        let mut submitted: Vec<(Bytes, Bytes)> = members
            .iter()
            .map(|(bls, _)| Bytes::from(bls.as_ref().to_vec()))
            .zip(encryption_keys)
            .collect();
        submitted.reverse();
        Fixture {
            recipient_bls_keys: submitted.iter().map(|(bls, _)| bls.clone()).collect(),
            recipients: submitted.iter().map(|(_, key)| key.clone()).collect(),
            dealing: Bytes::from(encode(&dealing)),
            dealer,
            dealer_keypair: BlsKeypair::generate(rng),
        }
    }

    /// A call for `epoch` and `dealer`, signed by the fixture's dealer key for the dealing's own
    /// epoch and dealer.
    fn call(f: &Fixture, epoch: u32, dealer: Address) -> Vec<u8> {
        let signature = f.dealer_keypair.sign(&dealing_message(42, 5, f.dealer, &f.dealing));
        verifyDealingCall {
            chainId: 42,
            epoch,
            dealer,
            dealerKey: Bytes::from(f.dealer_keypair.public().as_ref().to_vec()),
            signature: Bytes::from(signature.to_bytes().to_vec()),
            threshold: 3,
            recipientBlsKeys: f.recipient_bls_keys.clone(),
            recipients: f.recipients.clone(),
            dealing: f.dealing.clone(),
        }
        .abi_encode()
    }

    #[test]
    fn verifies_a_dealing_only_for_its_own_epoch_and_dealer() {
        let mut rng = StdRng::seed_from_u64(1);
        let f = fixture(&mut rng);
        let ok = run(&call(&f, 5, f.dealer), u64::MAX).unwrap();
        assert!(decode_bool(&ok));
        assert_eq!(ok.gas_used, dealing_gas(4, f.dealing.len()));
        assert!(!decode_bool(&run(&call(&f, 6, f.dealer), u64::MAX).unwrap()));
        assert!(!decode_bool(&run(&call(&f, 5, Address::repeat_byte(8)), u64::MAX).unwrap()));
    }

    /// A member without a key gets no share; the dealing verifies against the others at their
    /// committee positions, and not once that member's key is filled in.
    #[test]
    fn verifies_a_dealing_that_skips_a_member_without_a_key() {
        let mut rng = StdRng::seed_from_u64(5);
        let mut f = fixture_without(&mut rng, Some(1));
        assert!(decode_bool(&run(&call(&f, 5, f.dealer), u64::MAX).unwrap()));
        let empty = f.recipients.iter().position(|key| key.is_empty()).unwrap();
        let late = DecryptionKey::from_signature(&BlsKeypair::generate(&mut rng).sign(b"enc"))
            .unwrap()
            .encryption_key();
        f.recipients[empty] = Bytes::from(late.to_bytes().to_vec());
        assert!(!decode_bool(&run(&call(&f, 5, f.dealer), u64::MAX).unwrap()));
    }

    /// A dealing signed by a key other than the one the caller names is refused.
    #[test]
    fn refuses_a_dealing_signed_by_another_key() {
        let mut rng = StdRng::seed_from_u64(4);
        let mut f = fixture(&mut rng);
        let honest = call(&f, 5, f.dealer);
        f.dealer_keypair = BlsKeypair::generate(&mut rng);
        let mut forged = verifyDealingCall::abi_decode(&call(&f, 5, f.dealer)).unwrap();
        forged.dealerKey = verifyDealingCall::abi_decode(&honest).unwrap().dealerKey;
        assert!(!decode_bool(&run(&forged.abi_encode(), u64::MAX).unwrap()));
    }

    #[test]
    fn garbage_is_false_and_short_gas_is_out_of_gas() {
        let mut rng = StdRng::seed_from_u64(2);
        let mut f = fixture(&mut rng);
        assert_eq!(
            run(&call(&f, 5, f.dealer), DEALING_BASE_GAS).unwrap_err(),
            PrecompileError::OutOfGas
        );
        // a dealing one byte too long is refused without being decoded
        let mut long = f.dealing.to_vec();
        long.push(0);
        let honest = f.dealing.clone();
        f.dealing = Bytes::from(long);
        assert!(!decode_bool(&run(&call(&f, 5, f.dealer), u64::MAX).unwrap()));
        f.dealing = honest;
        f.dealing = Bytes::from(vec![1, 2, 3]);
        assert!(!decode_bool(&run(&call(&f, 5, f.dealer), u64::MAX).unwrap()));
        assert!(run(&[0u8; 3], u64::MAX).is_err());
    }

    #[test]
    fn verifies_an_encryption_key_proof_for_its_owner() {
        let mut rng = StdRng::seed_from_u64(3);
        let keypair = BlsKeypair::generate(&mut rng);
        let key = DecryptionKey::from_signature(&keypair.sign(b"enc")).unwrap();
        let proof = key.prove(keypair.public(), &encryption_key_context(), &mut rng).unwrap();
        let signature = keypair.sign(&key_registration_message(42, &key.encryption_key()));
        let call_for = |owner: &BlsPublicKey| {
            verifyEncryptionKeyCall {
                chainId: 42,
                blsPublicKey: Bytes::from(owner.as_ref().to_vec()),
                encryptionKey: Bytes::from(key.encryption_key().to_bytes().to_vec()),
                proof: Bytes::from(encode(&proof)),
                signature: Bytes::from(signature.to_bytes().to_vec()),
            }
            .abi_encode()
        };
        assert!(decode_bool(&run(&call_for(keypair.public()), u64::MAX).unwrap()));
        let other = BlsKeypair::generate(&mut rng);
        assert!(!decode_bool(&run(&call_for(other.public()), u64::MAX).unwrap()));
    }
}
