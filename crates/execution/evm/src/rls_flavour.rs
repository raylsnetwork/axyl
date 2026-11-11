//! RLS-flavour transactions (spike).
//!
//! A transaction signed for [`rls_chain_id`] instead of the chain's own id runs as an "RLS
//! flavour" transaction: gas is charged in RLS instead of native USDr, and the transaction's
//! `value` moves RLS instead of USDr. Inside it, CHAINID returns the RLS chain id, so EIP-712
//! signatures made for that chain id verify.
//!
//! RLS stays an ordinary ERC-20 contract. The node moves RLS by writing its OpenZeppelin
//! `ERC20Upgradeable` balance slots directly, the same slots the `RlsStorage` hardfork writes.
//!
//! Spike constants: a production version reads the RLS gas price and the fee recipient from
//! onchain configuration instead.

use rayls_infrastructure_types::{address, Address, U256};
use reth_revm::primitives::{b256, keccak256, Log, LogData, B256};

/// The RLS ERC-20 proxy.
pub const RLS_TOKEN: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17ea");

/// Receives the RLS paid as gas: the RLSAccumulator, which feeds staker rewards.
pub const RLS_FEE_RECIPIENT: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17ec");

/// RLS per USDr used to price gas in RLS. Governance-set in a production version.
pub const RLS_PER_USDR: u128 = 552;

/// ERC-7201 base of `openzeppelin.storage.ERC20`; `_balances` is the struct's first field.
const ERC20_BASE: B256 = b256!("52c63247e1f47db19d5ce0460030c497f067ca4cebf71ba98eeadabe20bace00");

/// `Transfer(address,address,uint256)`.
const TRANSFER_TOPIC: B256 =
    b256!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

/// The chain id that selects the RLS flavour on a chain with id `chain_id`.
pub const fn rls_chain_id(chain_id: u64) -> u64 {
    chain_id * 10 + 1
}

/// Whether a transaction signed with `tx_chain_id` is an RLS-flavour transaction on a chain with
/// id `chain_id`. Pre-EIP-155 transactions (no chain id) are always the USDr flavour.
pub const fn is_rls(tx_chain_id: Option<u64>, chain_id: u64) -> bool {
    matches!(tx_chain_id, Some(id) if id == rls_chain_id(chain_id))
}

/// Precompiles (Ethereum's and Rayls' own, such as the USDr precompile at `0x0400`) all sit below
/// `0x10000`. They have no code hash, but a call to them can still fail, and RLS value moves before
/// execution and is not reverted, so they cannot receive RLS value.
pub fn is_precompile_range(address: Address) -> bool {
    address.as_slice()[..18].iter().all(|byte| *byte == 0)
}

/// The RLS base fee per gas for a block whose USDr base fee is `usdr_base_fee`.
pub const fn rls_base_fee(usdr_base_fee: u64) -> u128 {
    usdr_base_fee as u128 * RLS_PER_USDR
}

/// Storage slot of `_balances[owner]` in the RLS contract.
pub fn balance_slot(owner: Address) -> U256 {
    let mut preimage = [0u8; 64];
    preimage[12..32].copy_from_slice(owner.as_slice());
    preimage[32..].copy_from_slice(ERC20_BASE.as_slice());
    keccak256(preimage).into()
}

/// The ERC-20 `Transfer` event the RLS contract would emit for this move.
pub fn transfer_log(from: Address, to: Address, amount: U256) -> Log {
    Log {
        address: RLS_TOKEN,
        data: LogData::new_unchecked(
            vec![TRANSFER_TOPIC, from.into_word(), to.into_word()],
            amount.to_be_bytes_vec().into(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balance_slot_matches_the_rls_storage_hardfork() {
        // `_balances[ADMIN]` as written by hardforks/rls_storage.rs
        let admin = address!("91ec7A2be07A79D2eAB99135553b26F706099e9D");
        let expected = U256::from_be_bytes(
            b256!("bec4543cf8dffc230fdc1a0e7ebd59a8e1295dd40c8379a775cb0ce08caa64d0").0,
        );
        assert_eq!(balance_slot(admin), expected);
    }

    #[test]
    fn rls_chain_id_is_distinct_and_fixed() {
        assert_eq!(rls_chain_id(487), 4871);
        assert_eq!(rls_chain_id(72957), 729571);
        assert_ne!(rls_chain_id(72957), 72957);
    }

    #[test]
    fn flavour_comes_from_the_signed_chain_id() {
        assert!(is_rls(Some(4871), 487));
        assert!(!is_rls(Some(487), 487));
        assert!(!is_rls(None, 487));
    }

    #[test]
    fn precompile_range_covers_low_addresses_only() {
        assert!(is_precompile_range(address!("0000000000000000000000000000000000000001")));
        assert!(is_precompile_range(address!("0000000000000000000000000000000000000400")));
        assert!(is_precompile_range(address!("000000000000000000000000000000000000ffff")));
        assert!(!is_precompile_range(address!("0000000000000000000000000000000000010000")));
        assert!(!is_precompile_range(address!("3C44CdDdB6a900fa2b585dd299e03d12FA4293BC")));
    }

    #[test]
    fn rls_base_fee_scales_the_usdr_base_fee() {
        assert_eq!(rls_base_fee(48_000_000_000), 48_000_000_000 * RLS_PER_USDR);
        assert_eq!(rls_base_fee(0), 0);
    }

    #[test]
    fn transfer_log_is_an_erc20_transfer() {
        let from = address!("0000000000000000000000000000000000000001");
        let to = address!("0000000000000000000000000000000000000002");
        let log = transfer_log(from, to, U256::from(5));
        assert_eq!(log.address, RLS_TOKEN);
        assert_eq!(log.data.topics(), &[TRANSFER_TOPIC, from.into_word(), to.into_word()]);
        assert_eq!(U256::from_be_slice(&log.data.data), U256::from(5));
    }
}
