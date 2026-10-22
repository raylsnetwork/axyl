//! Reads of the `DkgManager` system contract, which collects each epoch's publicly verifiable
//! dealings, and the transactions a validator sends to it.
//!
//! A validator node holds no execution key of its own, so it signs its transactions with a key
//! derived from its BLS key (see [`DkgSubmitter`]). That address needs funds for one transaction:
//! the contract refunds the gas of every accepted registration and dealing. The dealer's
//! authority over a dealing comes from its BLS signature, checked on-chain, not from the sender.

use crate::{reth_env::RethEnv, system_calls::SYSTEM_ADDRESS};
use alloy::{
    consensus::{SignableTransaction, TxEip1559},
    primitives::{keccak256, Bytes, TxKind},
    signers::{local::PrivateKeySigner, SignerSync},
    sol,
    sol_types::{SolCall, SolEvent},
};
use alloy_evm::Evm;
use rayls_infrastructure_config::DKG_MANAGER_ADDRESS;
use rayls_infrastructure_types::{
    nidkg::{EncryptionKey, Recipient},
    Address, BlsPublicKey, BlsSignature, Epoch, SealedHeader, TransactionSigned, B256, U256,
};
use reth_evm::{ConfigureEvm, EvmFactory};
use reth_provider::{
    DatabaseProviderFactory, LatestStateProvider, ReceiptProvider, StateProviderBox,
};
use reth_revm::{context::result::ExecutionResult, database::StateProviderDatabase, State};

sol! {
    /// The `DkgManager` system contract (`rayls-contracts/src/consensus/DkgManager.sol`).
    contract DkgManager {
        event DealingAccepted(
            uint32 indexed epoch,
            address indexed dealer,
            bytes32 digest,
            bytes32 dealingHash,
            bytes dealing
        );
        function registerEncryptionKey(
            address validator,
            bytes key,
            bytes proof,
            bytes signature
        ) external;
        function submitDealing(
            uint32 epoch,
            address dealer,
            bytes signature,
            bytes dealing
        ) external;
        function encryptionKeys(address validator) external view returns (bytes);
        function dealers(uint32 epoch) external view returns (address[]);
        function dealingBlock(uint32 epoch, address dealer) external view returns (uint64);
        function dealingHash(uint32 epoch, address dealer) external view returns (bytes32);
        function finalize(uint32 epoch) external;
        function finals(uint32 epoch)
            external
            view
            returns (bool finalized, bool keyed, bytes32 digest);
        function finalMembers(uint32 epoch) external view returns (address[] memory);
        function genesisDealings() external view returns (bytes32);
        function isReady(uint32 epoch) external view returns (bool);
        function hasKey(uint32 epoch) external view returns (bool);
        function recipients(uint32 epoch)
            external
            view
            returns (address[] memory members, bytes[] memory keys);
    }
}

/// Largest committee that gets a threshold key, `DkgManager.MAX_KEYED_COMMITTEE`: a larger one's
/// dealings would not fit the transaction size a default node pool accepts.
pub const MAX_KEYED_COMMITTEE: usize = 100;

/// Gas limit for an encryption-key registration: the precompile, a registry read and storage.
pub const REGISTRATION_GAS_LIMIT: u64 = 400_000;
/// Gas limit for a dealing of `bytes` to a committee of `members`: the precompile's charge, and
/// calldata, the event carrying the dealing and the registry reads, which grow with the dealing
/// and the committee. The sender's balance must cover the whole limit at the fee cap.
pub fn dealing_gas_limit(members: usize, bytes: usize) -> u64 {
    600_000
        + 80_000 * members as u64
        + 40 * bytes as u64
        + crate::nidkg_precompile::dealing_gas(members, bytes)
}

impl RethEnv {
    /// Call a view function of `DkgManager` at the canonical tip.
    fn call_dkg_manager<C: SolCall>(&self, call: C) -> eyre::Result<C::Return> {
        self.call_view(DKG_MANAGER_ADDRESS, call)
    }

    /// Whether the registry knows `validator` (any status but `Undefined`): only such a validator
    /// can register an encryption key.
    pub fn dkg_is_known_validator(&self, validator: Address) -> eyre::Result<bool> {
        let info = self.call_view(
            crate::system_calls::CONSENSUS_REGISTRY_ADDRESS,
            crate::system_calls::ConsensusRegistry::getValidatorCall {
                validatorAddress: validator,
            },
        )?;
        Ok(info.currentStatus != crate::system_calls::ConsensusRegistry::ValidatorStatus::Undefined)
    }

    /// Call a view function of `contract` at the canonical tip. The state is the tip's own,
    /// blocks not yet persisted included: the persisted state lags the tip by a varying number of
    /// blocks, and a read that two nodes must answer alike (the close gate's) cannot depend on
    /// how far each node's persistence got.
    fn call_view<C: SolCall>(&self, contract: Address, call: C) -> eyre::Result<C::Return> {
        self.call_view_at(&self.canonical_tip(), contract, call)
    }

    /// Call a view function of `contract` in the state after `block`, a canonical block.
    fn call_view_at<C: SolCall>(
        &self,
        canonical_tip: &SealedHeader,
        contract: Address,
        call: C,
    ) -> eyre::Result<C::Return> {
        let db_provider = self.blockchain_provider.database_provider_ro()?;
        let persisted: StateProviderBox = Box::new(LatestStateProvider::new(db_provider));
        let state_provider: StateProviderBox = Box::new(
            self.canonical_in_memory_state().state_provider(canonical_tip.hash(), persisted),
        );
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder()
            .with_database(state)
            .with_bundle_update()
            .without_state_clear()
            .build();
        let mut evm = self
            .evm_config
            .evm_factory()
            .create_evm(&mut db, self.evm_config.evm_env(canonical_tip)?);
        let result = evm
            .transact_system_call(SYSTEM_ADDRESS, contract, call.abi_encode().into())
            .map_err(|e| eyre::eyre!("read of {contract} failed: {e}"))?;
        match result.result {
            ExecutionResult::Success { output, .. } => Ok(C::abi_decode_returns(output.data())?),
            other => Err(eyre::eyre!("read of {contract} reverted: {other:?}")),
        }
    }

    /// The encryption key `validator` registered, if any.
    pub fn dkg_encryption_key(&self, validator: Address) -> eyre::Result<Option<EncryptionKey>> {
        let bytes = self.call_dkg_manager(DkgManager::encryptionKeysCall { validator })?;
        if bytes.is_empty() {
            return Ok(None);
        }
        EncryptionKey::from_bytes(&bytes)
            .map(Some)
            .ok_or_else(|| eyre::eyre!("DkgManager holds an undecodable key for {validator}"))
    }

    /// Dealers whose dealing for the key of `epoch` was accepted, in acceptance order.
    pub fn dkg_dealers(&self, epoch: Epoch) -> eyre::Result<Vec<Address>> {
        self.call_dkg_manager(DkgManager::dealersCall { epoch })
    }

    /// Whether the key of `epoch` is settled at the canonical tip: finalized, dealt by enough
    /// dealers, or not to exist at all (see [`Self::dkg_has_key`]).
    pub fn dkg_is_ready(&self, epoch: Epoch) -> eyre::Result<bool> {
        if !self.dkg_manager_deployed()? {
            return Ok(true);
        }
        self.call_dkg_manager(DkgManager::isReadyCall { epoch })
    }

    /// Whether this chain has `DkgManager` (code at its address in the canonical tip's state).
    /// A chain without it never deals: every epoch uses aggregate certificates.
    pub fn dkg_manager_deployed(&self) -> eyre::Result<bool> {
        let tip = self.canonical_tip();
        let db_provider = self.blockchain_provider.database_provider_ro()?;
        let persisted: StateProviderBox = Box::new(LatestStateProvider::new(db_provider));
        let state = self.canonical_in_memory_state().state_provider(tip.hash(), persisted);
        use reth_provider::StateProvider as _;
        Ok(state.account_code(&DKG_MANAGER_ADDRESS)?.is_some_and(|code| !code.is_empty()))
    }

    /// Whether `epoch` gets a threshold key: as finalized, or while dealing is open, whether every
    /// member of its committee had an encryption key before the dealing epoch began. Otherwise
    /// the epoch uses aggregate certificates.
    pub fn dkg_has_key(&self, epoch: Epoch) -> eyre::Result<bool> {
        self.call_dkg_manager(DkgManager::hasKeyCall { epoch })
    }

    /// The key of `epoch` as `DkgManager.finalize` recorded it in the block that closed the epoch
    /// before: `None` before that.
    pub fn dkg_final(&self, epoch: Epoch) -> eyre::Result<Option<DkgFinal>> {
        let answer = self.call_dkg_manager(DkgManager::finalsCall { epoch })?;
        if !answer.finalized {
            return Ok(None);
        }
        let members = self.call_dkg_manager(DkgManager::finalMembersCall { epoch })?;
        let mut keys = Vec::with_capacity(members.len());
        for member in &members {
            keys.push(self.dkg_bls_key(*member)?);
        }
        keys.sort();
        Ok(Some(DkgFinal { keyed: answer.keyed, members: keys }))
    }

    /// The commitment to the genesis dealings of epoch 0's key, when genesis gave epoch 0 one.
    pub fn dkg_genesis_dealings(&self) -> eyre::Result<Option<B256>> {
        let commitment = self.call_dkg_manager(DkgManager::genesisDealingsCall {})?;
        Ok((!commitment.is_zero()).then_some(commitment))
    }

    /// The BLS keys of `epoch`'s committee, in the protocol's committee order.
    pub fn dkg_committee_keys(&self, epoch: Epoch) -> eyre::Result<Vec<BlsPublicKey>> {
        let mut keys = self
            .validators_for_epoch(epoch)?
            .iter()
            .map(|validator| {
                BlsPublicKey::from_literal_bytes(&validator.blsPubkey).map_err(|e| {
                    eyre::eyre!("bad BLS key for {}: {e:?}", validator.validatorAddress)
                })
            })
            .collect::<eyre::Result<Vec<_>>>()?;
        keys.sort();
        Ok(keys)
    }

    /// `validator`'s BLS key, from the registry, which keeps every validator ever staked.
    fn dkg_bls_key(&self, validator: Address) -> eyre::Result<BlsPublicKey> {
        let info = self.call_view(
            crate::system_calls::CONSENSUS_REGISTRY_ADDRESS,
            crate::system_calls::ConsensusRegistry::getValidatorCall {
                validatorAddress: validator,
            },
        )?;
        BlsPublicKey::from_literal_bytes(&info.blsPubkey)
            .map_err(|e| eyre::eyre!("bad BLS key for {validator}: {e:?}"))
    }

    /// The recipients of the key of `epoch` while dealing is open, as `DkgManager.recipients`
    /// fixes them: the committee in the protocol's committee order (BLS key bytes), and the
    /// members with a key in time at their positions in that order.
    pub fn dkg_recipients(&self, epoch: Epoch) -> eyre::Result<DkgRecipients> {
        let answer = self.call_dkg_manager(DkgManager::recipientsCall { epoch })?;
        let mut members = Vec::with_capacity(answer.members.len());
        for (address, key) in answer.members.iter().zip(&answer.keys) {
            let bls_key = self.dkg_bls_key(*address)?;
            let encryption_key = match key.is_empty() {
                true => None,
                false => Some(EncryptionKey::from_bytes(key).ok_or_else(|| {
                    eyre::eyre!("DkgManager holds an undecodable key for {address}")
                })?),
            };
            members.push((bls_key, encryption_key));
        }
        members.sort_by(|a, b| a.0.cmp(&b.0));
        let recipients = members
            .iter()
            .enumerate()
            .filter_map(|(position, (_, key))| {
                key.map(|key| Recipient { position: position as u32, key })
            })
            .collect();
        Ok(DkgRecipients { committee_size: members.len(), recipients })
    }

    /// Next nonce of `account` at the canonical tip.
    pub fn account_nonce(&self, account: Address) -> eyre::Result<u64> {
        Ok(self.latest()?.account_nonce(&account)?.unwrap_or_default())
    }

    /// The chain id dealings and registrations are bound to.
    pub fn dkg_chain_id(&self) -> u64 {
        self.chainspec().chain_id()
    }

    /// Hash of `dealer`'s accepted dealing for the key of `epoch` to its active recipient set;
    /// `None` if none was accepted.
    pub fn dkg_dealing_hash(&self, epoch: Epoch, dealer: Address) -> eyre::Result<Option<B256>> {
        let hash = self.call_dkg_manager(DkgManager::dealingHashCall { epoch, dealer })?;
        Ok((!hash.is_zero()).then_some(hash))
    }

    /// Every dealing accepted in canonical blocks `from..=to`, from their `DealingAccepted`
    /// events. A block whose logs bloom rules out such an event is skipped without reading its
    /// receipts, so a scan of blocks without dealings reads headers only. A block that should be
    /// read but whose receipts this node no longer has is skipped with a warning.
    pub fn dkg_dealings_accepted_in(
        &self,
        from: u64,
        to: u64,
    ) -> eyre::Result<Vec<AcceptedDealing>> {
        use alloy::primitives::BloomInput;
        use reth_provider::HeaderProvider as _;
        let mut found = Vec::new();
        for number in from..=to {
            let Some(header) = self.blockchain_provider.header_by_number(number)? else {
                return Err(eyre::eyre!("no canonical block {number}"));
            };
            let bloom = header.logs_bloom;
            if !bloom.contains_input(BloomInput::Raw(DKG_MANAGER_ADDRESS.as_slice()))
                || !bloom.contains_input(BloomInput::Raw(
                    DkgManager::DealingAccepted::SIGNATURE_HASH.as_slice(),
                ))
            {
                continue;
            }
            // a pruned block's receipts read back as an empty list, and a block whose bloom
            // holds a log has at least one receipt
            let receipts =
                self.blockchain_provider.receipts_by_block(number.into())?.unwrap_or_default();
            if receipts.is_empty() {
                tracing::warn!(target: "engine", block = number, "receipts of a block with dkg dealings are pruned: its dealings are not kept");
                continue;
            }
            for log in receipts.iter().flat_map(|receipt| receipt.logs.iter()) {
                if log.address != DKG_MANAGER_ADDRESS
                    || log.topics().first() != Some(&DkgManager::DealingAccepted::SIGNATURE_HASH)
                {
                    continue;
                }
                let event = DkgManager::DealingAccepted::decode_log_data(&log.data)?;
                if keccak256(&event.dealing) != event.dealingHash {
                    return Err(eyre::eyre!("dealing event in block {number} is corrupt"));
                }
                found.push(AcceptedDealing {
                    epoch: event.epoch,
                    dealer: event.dealer,
                    hash: event.dealingHash,
                    dealing: event.dealing,
                });
            }
        }
        Ok(found)
    }

    /// The accepted dealing of `dealer` for the key of `epoch` to its active recipient set, from
    /// the `DealingAccepted` event of the block that accepted it, checked against the hash the
    /// contract stored. `None` when none was accepted or this node does not have that block's
    /// receipts (a pruned block's receipts read back empty).
    pub fn dkg_dealing(&self, epoch: Epoch, dealer: Address) -> eyre::Result<Option<Bytes>> {
        let block = self.call_dkg_manager(DkgManager::dealingBlockCall { epoch, dealer })?;
        if block == 0 {
            return Ok(None);
        }
        let stored = self.call_dkg_manager(DkgManager::dealingHashCall { epoch, dealer })?;
        let Some(receipts) = self.blockchain_provider.receipts_by_block(block.into())? else {
            return Ok(None);
        };
        let epoch_topic = B256::from(U256::from(epoch));
        let dealer_topic = dealer.into_word();
        for log in receipts.iter().flat_map(|receipt| receipt.logs.iter()) {
            let topics = log.topics();
            if log.address != DKG_MANAGER_ADDRESS
                || topics.first() != Some(&DkgManager::DealingAccepted::SIGNATURE_HASH)
                || topics.get(1) != Some(&epoch_topic)
                || topics.get(2) != Some(&dealer_topic)
            {
                continue;
            }
            let event = DkgManager::DealingAccepted::decode_log_data(&log.data)?;
            // the block can also hold a dealing for an earlier recipient set of the epoch
            if event.dealingHash != stored {
                continue;
            }
            if keccak256(&event.dealing) != stored {
                return Err(eyre::eyre!("dealing event of {dealer} for epoch {epoch} is corrupt"));
            }
            return Ok(Some(event.dealing));
        }
        Ok(None)
    }
}

/// One `DealingAccepted` event: a dealing the chain accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedDealing {
    /// The epoch whose key it deals.
    pub epoch: Epoch,
    pub dealer: Address,
    /// Its hash, as `DkgManager` stores it.
    pub hash: B256,
    pub dealing: Bytes,
}

/// One epoch's key as `DkgManager.finalize` recorded it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DkgFinal {
    /// The epoch has a threshold key.
    pub keyed: bool,
    /// The BLS keys of the committee the key was dealt to, in the protocol's committee order:
    /// the member at position `p` holds share index `p + 1`.
    pub members: Vec<BlsPublicKey>,
}

/// The recipients of one epoch's key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DkgRecipients {
    /// Members of the epoch's committee; the key's threshold is their quorum.
    pub committee_size: usize,
    /// The members with an encryption key in time, at their committee positions.
    pub recipients: Vec<Recipient>,
}

/// The account a validator node sends its `DkgManager` transactions from. Its key is derived
/// from the validator's BLS signature over a message in the never-published `DkgShare` scope
/// ([`submitter_key_message`]), so there is no new secret to back up.
#[derive(Debug, Clone)]
pub struct DkgSubmitter {
    signer: PrivateKeySigner,
}

/// What a validator signs, in the never-published `DkgShare` scope, to derive its submitter key.
/// It names no chain, so the keytool can derive the address before a genesis exists and genesis
/// can fund it.
pub fn submitter_key_message() -> Vec<u8> {
    use rayls_infrastructure_types::{encode, Intent, IntentMessage, IntentScope};
    encode(&IntentMessage::new(Intent::consensus(IntentScope::DkgShare), "nidkg submitter key"))
}

/// Native balance genesis gives each genesis validator's submitter (100 units). A dealing's
/// gas is refunded, so this is a float: the account must cover one transaction's gas limit at
/// its fee cap (under one unit at 56 members and 50 gwei), and lasts even when refunds stop.
pub const GENESIS_SUBMITTER_BALANCE: u128 = 100_000_000_000_000_000_000;

/// Native balance genesis gives `DkgManager` for gas refunds (100,000 units): at 56 members and
/// 50 gwei every member's dealing costs about 13 units an epoch.
pub const GENESIS_DKG_MANAGER_BALANCE: u128 = 100_000_000_000_000_000_000_000;

impl DkgSubmitter {
    /// Derive the submitter from the validator's signature over [`submitter_key_message`].
    pub fn from_signature(signature: &BlsSignature) -> eyre::Result<Self> {
        // a uniformly random 32-byte string is a valid secp256k1 key except with negligible
        // probability; hash again in that case
        let mut seed = keccak256(signature.to_bytes());
        loop {
            if let Ok(signer) = PrivateKeySigner::from_bytes(&seed) {
                return Ok(Self { signer });
            }
            seed = keccak256(seed);
        }
    }

    /// The address that pays for the transactions.
    pub fn address(&self) -> Address {
        self.signer.address()
    }

    fn sign(
        &self,
        chain_id: u64,
        nonce: u64,
        max_fee_per_gas: u128,
        gas_limit: u64,
        input: Vec<u8>,
    ) -> eyre::Result<TransactionSigned> {
        let tx = TxEip1559 {
            chain_id,
            nonce,
            max_priority_fee_per_gas: 0,
            max_fee_per_gas,
            gas_limit,
            to: TxKind::Call(DKG_MANAGER_ADDRESS),
            value: U256::ZERO,
            input: input.into(),
            access_list: Default::default(),
        };
        let signature = self.signer.sign_hash_sync(&tx.signature_hash())?;
        Ok(TransactionSigned::new_unhashed(tx.into(), signature))
    }

    /// The transaction registering `validator`'s encryption key.
    #[allow(clippy::too_many_arguments)]
    pub fn registration(
        &self,
        chain_id: u64,
        nonce: u64,
        max_fee_per_gas: u128,
        validator: Address,
        key: &EncryptionKey,
        proof: Vec<u8>,
        signature: &BlsSignature,
    ) -> eyre::Result<TransactionSigned> {
        let input = DkgManager::registerEncryptionKeyCall {
            validator,
            key: key.to_bytes().to_vec().into(),
            proof: proof.into(),
            signature: signature.to_bytes().to_vec().into(),
        }
        .abi_encode();
        self.sign(chain_id, nonce, max_fee_per_gas, REGISTRATION_GAS_LIMIT, input)
    }

    /// The transaction submitting `dealer`'s encoded `dealing` for the key of `epoch`.
    #[allow(clippy::too_many_arguments)]
    pub fn dealing(
        &self,
        chain_id: u64,
        nonce: u64,
        max_fee_per_gas: u128,
        epoch: Epoch,
        dealer: Address,
        signature: &BlsSignature,
        dealing: Vec<u8>,
        members: usize,
    ) -> eyre::Result<TransactionSigned> {
        let gas_limit = dealing_gas_limit(members, dealing.len());
        let input = DkgManager::submitDealingCall {
            epoch,
            dealer,
            signature: signature.to_bytes().to_vec().into(),
            dealing: dealing.into(),
        }
        .abi_encode();
        self.sign(chain_id, nonce, max_fee_per_gas, gas_limit, input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_infrastructure_types::{nidkg::Dealing, quorum_threshold, Encodable2718 as _};
    use reth_transaction_pool::validate::DEFAULT_MAX_TX_INPUT_BYTES;

    /// A dealing to the largest keyed committee fits a default node's pool, and its gas limit is
    /// refunded in full.
    #[test]
    fn dealing_at_the_committee_cap_fits_one_transaction() {
        let members = MAX_KEYED_COMMITTEE;
        let bytes = Dealing::encoded_len_for(members, quorum_threshold(members as u64) as usize);
        let submitter = DkgSubmitter::from_signature(&BlsSignature::default()).unwrap();
        let tx = submitter
            .dealing(1, 0, 1, 1, Address::ZERO, &BlsSignature::default(), vec![1; bytes], members)
            .unwrap();
        let size = tx.encoded_2718().len();
        assert!(size <= DEFAULT_MAX_TX_INPUT_BYTES, "{size} bytes at {members} members");
        let gas = dealing_gas_limit(members, bytes);
        assert!(gas <= 30_000_000, "gas limit {gas} at {members} members");
    }
}
