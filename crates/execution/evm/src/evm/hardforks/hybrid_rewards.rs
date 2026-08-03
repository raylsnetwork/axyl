//! HybridRewards hardfork: swap the deployed `ConsensusRegistry` to the hybrid-reward
//! (participation + anchor + stake) bytecode.
//!
//! ## Why a byte-splice and not a fresh `CREATE`
//!
//! `ConsensusRegistry` links the external `BlsG1` library at a **per-network** address
//! (`owner.create(0)`, filled in at genesis) and bakes a per-network `_rls` immutable into
//! its runtime code. The checked-in `deployedBytecode` therefore cannot be installed
//! directly — it carries dead (zero) library links and a zero `_rls`, so `stake()` /
//! `safeTransferFrom` would revert.
//!
//! Genesis solves this with a synthetic `CREATE` that runs the constructor. Re-running a
//! `CREATE` inside a block-execution migration would require threading the EVM factory into
//! the migration dispatcher. Instead we do the equivalent, deterministically and without any
//! EVM execution: read the per-network values straight out of the **currently deployed**
//! runtime code and splice them into the new hybrid runtime code.
//!
//! What must be carried over from the live contract:
//! - **BlsG1 library address** (8 link sites) — identical at every site.
//! - **`_rls` immutable** (the RLS proxy address) — identical at every site.
//! - **The 5 Solady EIP-712 cached immutables.** Their Solady AST ids are stable across the
//!   pre-hybrid and hybrid compilations (only `ConsensusRegistry`'s own `_rls` AST id shifted),
//!   so they map 1:1 by index. `_cachedNameHash`/`_cachedVersionHash` are constants of the
//!   (unchanged) EIP-712 domain, so the live value is already correct; `_cachedThis` /
//!   `_cachedChainId` / `_cachedDomainSeparator` self-heal at call time (Solady recomputes the
//!   separator whenever `address(this)`/`block.chainid` disagree with the cached values, which
//!   they always do because the cached `_cachedThis` was the genesis CREATE address).
//!
//! Everything else in the runtime — the actual hybrid `applyIncentives` logic — comes from the
//! checked-in fixture. Storage, nonce and balance are untouched (the dispatcher carries them
//! over), so live validator/epoch state survives the swap.
//!
//! The offsets below are extracted from the two checked-in artifacts
//! (`rayls-contracts/artifacts/ConsensusRegistry.json` = pre-hybrid/live,
//! `rayls-contracts/out/ConsensusRegistry.sol/ConsensusRegistry.json` = hybrid); see the
//! `offsets_match_live_artifact` test, which re-derives the old offsets from the checked-in
//! artifact and fails if they drift.

use rayls_infrastructure_config::CONSENSUS_REGISTRY_ADDRESS;
use rayls_infrastructure_types::Address;
use reth_errors::BlockExecutionError;
use reth_revm::{
    bytecode::Bytecode, primitives::HashMap, state::Account as RevmAccount, Database, State,
};

use super::account_with_code;

/// Length of the pre-hybrid (currently deployed) `ConsensusRegistry` runtime code.
const OLD_DEPLOYED_LEN: usize = 21_929;
/// Length of the hybrid `ConsensusRegistry` runtime code (the fixture below).
const NEW_DEPLOYED_LEN: usize = 23_042;
/// Solidity library link placeholder width (an address).
const LINK_LEN: usize = 20;
/// Solidity immutable slot width.
const IMM_LEN: usize = 32;

/// BlsG1 link sites in the pre-hybrid runtime code (all hold the same address).
const OLD_BLSG1_OFFSETS: [usize; 8] = [6700, 6836, 7802, 7911, 8051, 9448, 9557, 9697];
/// BlsG1 link sites in the hybrid runtime code.
const NEW_BLSG1_OFFSETS: [usize; 8] = [7291, 7427, 8393, 8502, 8642, 10155, 10264, 10404];

/// `_rls` immutable sites in the pre-hybrid runtime code (all hold the same value).
const OLD_RLS_OFFSETS: [usize; 7] = [749, 1437, 4637, 8617, 9928, 12786, 16246];
/// `_rls` immutable sites in the hybrid runtime code.
const NEW_RLS_OFFSETS: [usize; 7] = [801, 1489, 5228, 9208, 10635, 13787, 17247];

/// Solady EIP-712 cached immutables, index-aligned by stable AST id
/// (55781, 55783, 55785, 55787, 55789) between the pre-hybrid and hybrid compilations.
const OLD_EIP712_OFFSETS: [usize; 5] = [14651, 14686, 14766, 14804, 14618];
/// Same immutables in the hybrid runtime code, in the same AST-id order.
const NEW_EIP712_OFFSETS: [usize; 5] = [15652, 15687, 15767, 15805, 15619];

/// Hybrid `ConsensusRegistry` runtime code with BlsG1 link sites and all immutables zeroed;
/// the per-network values are spliced in from the live contract at activation.
const NEW_DEPLOYED_BYTECODE: &[u8] =
    include_bytes!("bytecodes/hybrid_rewards/consensus_registry_deployed.bin");

/// Produce the hybrid runtime code for this network by carrying the per-network BlsG1 address,
/// `_rls` immutable and EIP-712 cached immutables from `live` (the currently deployed runtime
/// code) into the hybrid fixture.
///
/// Fails loudly if `live` is not the expected pre-hybrid contract (wrong length, or link/`_rls`
/// sites that disagree with each other) rather than installing corrupt code — a mismatch means
/// the target network does not carry the contract this migration was built against.
fn splice_hybrid_registry_code(live: &[u8]) -> Result<Vec<u8>, BlockExecutionError> {
    if live.len() != OLD_DEPLOYED_LEN {
        return Err(BlockExecutionError::msg(format!(
            "HybridRewards: live ConsensusRegistry runtime code is {} bytes, expected {} \
             (pre-hybrid artifact); refusing to migrate an unexpected contract",
            live.len(),
            OLD_DEPLOYED_LEN
        )));
    }

    // Per-network BlsG1 library address — must agree at every link site.
    let blsg1 = &live[OLD_BLSG1_OFFSETS[0]..OLD_BLSG1_OFFSETS[0] + LINK_LEN];
    if OLD_BLSG1_OFFSETS.iter().any(|&o| &live[o..o + LINK_LEN] != blsg1) {
        return Err(BlockExecutionError::msg(
            "HybridRewards: BlsG1 link sites disagree in live code; unexpected contract layout"
                .to_string(),
        ));
    }

    // Per-network `_rls` immutable — must agree at every site.
    let rls = &live[OLD_RLS_OFFSETS[0]..OLD_RLS_OFFSETS[0] + IMM_LEN];
    if OLD_RLS_OFFSETS.iter().any(|&o| &live[o..o + IMM_LEN] != rls) {
        return Err(BlockExecutionError::msg(
            "HybridRewards: `_rls` immutable sites disagree in live code; unexpected contract layout"
                .to_string(),
        ));
    }

    let mut code = NEW_DEPLOYED_BYTECODE.to_vec();
    if code.len() != NEW_DEPLOYED_LEN {
        return Err(BlockExecutionError::msg(format!(
            "HybridRewards: hybrid bytecode fixture is {} bytes, expected {}",
            code.len(),
            NEW_DEPLOYED_LEN
        )));
    }

    for &o in &NEW_BLSG1_OFFSETS {
        code[o..o + LINK_LEN].copy_from_slice(blsg1);
    }
    for &o in &NEW_RLS_OFFSETS {
        code[o..o + IMM_LEN].copy_from_slice(rls);
    }
    for i in 0..OLD_EIP712_OFFSETS.len() {
        let v = &live[OLD_EIP712_OFFSETS[i]..OLD_EIP712_OFFSETS[i] + IMM_LEN];
        let o = NEW_EIP712_OFFSETS[i];
        code[o..o + IMM_LEN].copy_from_slice(v);
    }

    Ok(code)
}

/// Build the state delta for the `HybridRewards` hardfork: replace the `ConsensusRegistry`
/// runtime code with the per-network hybrid bytecode. Reads the live code (hence `&mut
/// State<DB>`, like [`super::usdr_supply_correction`]); storage/nonce/balance are preserved by
/// the dispatcher's code-replacement branch.
pub(crate) fn hybrid_rewards_state<DB: alloy_evm::Database>(
    db: &mut State<DB>,
) -> Result<HashMap<Address, RevmAccount>, BlockExecutionError>
where
    DB::Error: core::fmt::Display,
{
    let info = db
        .basic(CONSENSUS_REGISTRY_ADDRESS)
        .map_err(|e| {
            BlockExecutionError::msg(format!("HybridRewards: read ConsensusRegistry account: {e}"))
        })?
        .ok_or_else(|| {
            BlockExecutionError::msg("HybridRewards: ConsensusRegistry account is missing".to_string())
        })?;

    let live_code: Bytecode = match info.code {
        Some(code) => code,
        None => db.code_by_hash(info.code_hash).map_err(|e| {
            BlockExecutionError::msg(format!("HybridRewards: load ConsensusRegistry code: {e}"))
        })?,
    };

    let new_code = splice_hybrid_registry_code(live_code.original_byte_slice())?;

    let mut state = HashMap::default();
    state.insert(CONSENSUS_REGISTRY_ADDRESS, account_with_code(&new_code));
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn synthetic_old_code(blsg1: [u8; 20], rls: [u8; 32], eip: [[u8; 32]; 5]) -> Vec<u8> {
        let mut c = vec![0u8; OLD_DEPLOYED_LEN];
        for &o in &OLD_BLSG1_OFFSETS {
            c[o..o + LINK_LEN].copy_from_slice(&blsg1);
        }
        for &o in &OLD_RLS_OFFSETS {
            c[o..o + IMM_LEN].copy_from_slice(&rls);
        }
        for i in 0..5 {
            c[OLD_EIP712_OFFSETS[i]..OLD_EIP712_OFFSETS[i] + IMM_LEN].copy_from_slice(&eip[i]);
        }
        c
    }

    #[test]
    fn fixture_has_expected_length_and_zeroed_link_sites() {
        assert_eq!(NEW_DEPLOYED_BYTECODE.len(), NEW_DEPLOYED_LEN);
        for &o in &NEW_BLSG1_OFFSETS {
            assert_eq!(
                &NEW_DEPLOYED_BYTECODE[o..o + LINK_LEN],
                &[0u8; LINK_LEN],
                "fixture link site {o} must be zeroed so splicing fully overwrites it"
            );
        }
    }

    #[test]
    fn splice_carries_per_network_values_to_hybrid_offsets() {
        let blsg1 = [0xABu8; 20];
        let rls = [0xCDu8; 32];
        let eip = [[1u8; 32], [2u8; 32], [3u8; 32], [4u8; 32], [5u8; 32]];
        let live = synthetic_old_code(blsg1, rls, eip);

        let out = splice_hybrid_registry_code(&live).expect("splice must succeed");
        assert_eq!(out.len(), NEW_DEPLOYED_LEN);

        for &o in &NEW_BLSG1_OFFSETS {
            assert_eq!(&out[o..o + LINK_LEN], &blsg1, "BlsG1 not written at {o}");
        }
        for &o in &NEW_RLS_OFFSETS {
            assert_eq!(&out[o..o + IMM_LEN], &rls, "_rls not written at {o}");
        }
        for i in 0..5 {
            let o = NEW_EIP712_OFFSETS[i];
            assert_eq!(&out[o..o + IMM_LEN], &eip[i], "EIP-712 immutable {i} not written at {o}");
        }
    }

    #[test]
    fn splice_leaves_non_spliced_bytes_equal_to_fixture() {
        let live = synthetic_old_code([0xAB; 20], [0xCD; 32], [[7u8; 32]; 5]);
        let out = splice_hybrid_registry_code(&live).unwrap();

        // Every byte the migration does not touch must equal the fixture verbatim.
        let mut spliced = vec![false; NEW_DEPLOYED_LEN];
        for &o in &NEW_BLSG1_OFFSETS {
            spliced[o..o + LINK_LEN].fill(true);
        }
        for &o in &NEW_RLS_OFFSETS {
            spliced[o..o + IMM_LEN].fill(true);
        }
        for &o in &NEW_EIP712_OFFSETS {
            spliced[o..o + IMM_LEN].fill(true);
        }
        for i in 0..NEW_DEPLOYED_LEN {
            if !spliced[i] {
                assert_eq!(out[i], NEW_DEPLOYED_BYTECODE[i], "unspliced byte {i} changed");
            }
        }
    }

    #[test]
    fn splice_rejects_wrong_length_code() {
        let err = splice_hybrid_registry_code(&[0u8; 100]).expect_err("must reject short code");
        assert!(format!("{err}").contains("refusing to migrate"), "got: {err}");
    }

    /// Re-derive the old offsets from the checked-in pre-hybrid artifact and fail if the
    /// hardcoded constants drift from it (the live code is produced from this artifact).
    #[test]
    fn offsets_match_live_artifact() {
        let art: serde_json::Value =
            serde_json::from_str(rayls_infrastructure_config::CONSENSUS_REGISTRY_JSON)
                .expect("valid artifact json");
        let db = &art["deployedBytecode"];

        let mut links: Vec<usize> = db["linkReferences"]["src/consensus/BlsG1.sol"]["BlsG1"]
            .as_array()
            .expect("BlsG1 link refs")
            .iter()
            .map(|s| s["start"].as_u64().unwrap() as usize)
            .collect();
        links.sort_unstable();
        let mut expected = OLD_BLSG1_OFFSETS.to_vec();
        expected.sort_unstable();
        assert_eq!(links, expected, "pre-hybrid BlsG1 link offsets drifted from artifact");

        // `_rls` is the one immutable whose site count is 7 (the EIP-712 cached values are single-site).
        let imms = db["immutableReferences"].as_object().expect("immutable refs");
        let mut rls: Vec<usize> = imms
            .values()
            .find(|sites| sites.as_array().map(|a| a.len()) == Some(OLD_RLS_OFFSETS.len()))
            .expect("_rls immutable (7 sites)")
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["start"].as_u64().unwrap() as usize)
            .collect();
        rls.sort_unstable();
        let mut expected_rls = OLD_RLS_OFFSETS.to_vec();
        expected_rls.sort_unstable();
        assert_eq!(rls, expected_rls, "pre-hybrid _rls immutable offsets drifted from artifact");
    }
}
