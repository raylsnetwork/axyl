# RLS-flavour transactions (proof of concept)

The node accepts a second chain id, the **RLS flavour** (`chain_id * 10 + 1`: 487 → 4871 locally,
72957 → 729571 on mainnet). A transaction signed for it pays gas in RLS, and its `value` moves RLS.
USDr stays the only native token; a transaction signed for the chain's own id works exactly as
today. Plain MetaMask on the RLS chain id shows RLS as the native coin, its Send button moves RLS,
dapps see the user's own address, and nothing needs a relayer, approval or gas station.

**Status: proof of concept, not for merge.** Built on axyl `main` (`1970747`). Every check below ran
on a fresh local 4-validator chain built from this branch, with the real rate of ~552 RLS per USDr.

![MetaMask key screens](demo/key-screens.png)

Video: [`demo/metamask-rls-gas.mp4`](demo/metamask-rls-gas.mp4). Real MetaMask 13.56.0 pays gas
in USDr on chain 487, switches to chain 4871 and pays gas in RLS from the same account.

## How it works

| File | Change |
|---|---|
| `crates/execution/evm/src/rls_flavour.rs` (new) | RLS chain id, RLS base fee (USDr base fee × `RLS_PER_USDR`), RLS balance slots, `Transfer` log, unit tests |
| `crates/execution/evm/src/evm/mod.rs` | `transact_raw`: a tx signed for the RLS chain id runs with gas price and value 0. CHAINID = RLS chain id, base fee 0 for that tx |
| `crates/execution/evm/src/evm/handler.rs` | revm `Handler` hooks: take max fee + value in RLS before execution (checked arithmetic), refund unused gas, pay gas to the RLSAccumulator |
| `crates/execution/evm/src/txn_pool.rs` | See below |
| `etc/rls-flavour/rpc-proxy.mjs` | Wallet-facing RPC for the RLS chain id (balance and gas prices in RLS). Production would serve this from the node |

`RlsFlavourValidator` in `txn_pool.rs` wraps Reth's validator via `TransactionValidationTaskExecutor::map`:
- RLS-chain-id transactions are checked against the sender's RLS: nonce, gas, a fee at or above the
  current RLS base fee, and RLS covering this transaction plus the sender's other waiting RLS
  transactions.
- Their USDr `cost` is set to 0.
- A same-nonce replacement that switches flavour is refused.
- A task after each block (on the blocking pool) drops waiting RLS transactions that can no longer
  execute: unpayable, fee cap under the RLS base fee, or a value recipient that has gained code.
  Nonces the sender has already used are skipped, both there and in the admission sum.

Further points:
- **No Reth fork and no second native token.** All changes use extension points Reth and revm already
  provide.
- **RLS stays a Solidity contract.** The node writes its OpenZeppelin `_balances` slots, as the
  `RlsStorage` hardfork does.
- **The flavour is part of what the user signs.** Every path that re-executes a transaction (sync,
  replay, tracing) sees it.

## Run it

```sh
cargo build --release --bin rayls-network
cd etc/test-network
cp .env.example .env && sed -i '' 's/^RAYLS_NETWORK="devnet"/RAYLS_NETWORK="local"/' .env
cp ../rls-flavour/rls-accounts.yaml .   # Anvil account #1: 1,000 RLS, no USDr
./local-testnet.sh --start --dev-funds 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
UPSTREAM=http://127.0.0.1:8543 node ../rls-flavour/rpc-proxy.mjs &   # RLS chain id on :8645
cd ../rls-flavour
NODE=http://127.0.0.1:8543 ./spike-test.sh && NODE=http://127.0.0.1:8543 ./stress-test.sh && NODE=http://127.0.0.1:8543 ./stress-test-2.sh
```

The genesis is chain 487, so `RAYLS_NETWORK` must be `local`. The keys in the scripts are Anvil's
public dev keys. `demo/record-metamask.mjs` drives MetaMask with Playwright. The extension path and
`demo/test-dapp.html` (served on `127.0.0.1:5199`) are set inside it.

## Results (2026-11-11, this branch)

| Area | Check | Result |
|---|---|---|
| Basics | Native send of 1 RLS from an account with 0 USDr | Recipient +1 RLS; sender paid 21,000 × 26,496 gwei = 0.556 RLS; USDr untouched; two RLS `Transfer` events: the value, and the gas paid to the fee recipient |
| | Dapp call (`approve`), contract deploy, legacy type-0 tx from the RLS chain id | Work; `msg.sender` is the user; gas in RLS |
| | USDr transactions | Unchanged |
| EIP-712 | CHAINID | 4871 in RLS-flavour txs, 487 in USDr txs |
| | RLS permit signed for 4871 | Works in an RLS-flavour tx, reverts in a USDr tx |
| EIP-7702 | Type-4 tx from the RLS chain id, authorization for 4871 | Account delegated (`0xef0100…`); gas in RLS |
| | Calling the delegated account in an RLS-flavour tx | Delegate code runs; CHAINID 4871 |
| | Authorization for 487 inside an RLS tx, or for 4871 inside a USDr tx | Ignored, as for any wrong-chain authorization |
| | Authorization for chain 0 | Works in either flavour (undelegate verified) |
| Nonce | Same nonce in both flavours, either order | Second is refused: "a transaction with this nonce is waiting on the other Rayls network…" |
| | MetaMask (see below) | Takes nonces in order across both networks and handles one network's confirmations at a time |
| Pool | Two RLS sends that each fit but together exceed the balance | Second refused at admission |
| | A waiting RLS send made unpayable by an ERC-20 transfer | Dropped from the pool after the block (no stuck tx) |
| | Fee under the RLS base fee; sender without RLS; RLS value to a contract or a precompile (`0x0400`) | Refused |
| | 21,000 gas with 2 KB of calldata (intrinsic gas) | Refused at the pool, by the same stateless checks reth runs (size, init code, gas caps, tip ≤ fee cap, min tip, 7702 auth list, intrinsic gas) |
| | A waiting RLS tx whose fee cap falls under the RLS base fee | Dropped after the block (same task as the balance re-check) |
| | A waiting RLS value tx whose recipient gets an EIP-7702 delegation | Dropped after the block |
| | `value = 2^256-1` | Refused (overflow checked) |
| Consensus | 120 txs from 3 senders, alternating flavours, sent at once | 120/120 mined, 0 reverted; 4 of 5 blocks held both flavours |
| | State roots | Identical on 4 validators + observer |
| | Observer wiped and re-synced from genesis | All 71 non-empty blocks identical (58 with RLS txs, 157 RLS txs incl. 7 type-4, 8 mixed blocks) |
| Tracing | `debug_traceTransaction` of an RLS-flavour tx | Same gas as the receipt (34,182) |
| | `eth_estimateGas` vs actual (`approve`) | 34,541 vs 34,182 |
| Tests | `cargo test --release -p rayls-execution-evm --lib` | 224 passed (218 existing + 6 new) |
| | `review-checks.sh` (review fixes on #386, both rounds) | 9/9 |

### Real MetaMask

Official MetaMask 13.56.0, in a disposable headless Chromium driven by Playwright. MetaMask created
a fresh wallet itself, funded on the devnet with RLS (and USDr for the switching test). No key was
imported.

| Step | Result |
|---|---|
| Add chain 4871 (`wallet_addEthereumChain`, ticker RLS) | Plain "Add network" prompt, no warnings |
| Home | "100 RLS" as the native balance |
| **Main Send button**, 1 RLS | Confirm screen "Sending 1 RLS · Network fee 0.5564 RLS"; mined; matches the chain |
| Send **Max** | 95.31 RLS (balance − fee); balance exactly 0 |
| Dapp `approve` | "Spending cap request · Network fee 1.3695 RLS"; allowance set from the MetaMask account |
| `eth_signTypedData_v4` permit for 4871 | Signed; works relayed in an RLS-flavour tx |
| Typed data for 487 while on 4871 | MetaMask refuses ("must match the active chainId") |
| Both networks added | Separate entries; USDr fee 0.001 USDr on 487, RLS fee 0.5564 RLS on 4871; nonces 2 then 3 |
| Queue sends on both networks at once | MetaMask warns "Switching networks will cancel all pending confirmations"; one at a time |
| RLS value to a contract | Blocked before signing: "Network fee Unavailable" + "Review alert" |
| `wallet_watchAsset` for RLS on 4871 | Allowed, then **RLS shows twice**. Integrator docs must say not to add it |

## What this does not do yet

1. **Fork gate.** It's active from genesis. Gate it behind a `RaylsHardFork`.
2. **Gas price inside the EVM.** BASEFEE (and GASPRICE) read 0 inside RLS-flavour txs. Divide the
   signed fee fields by the rate before revm instead, so they keep their USDr meaning.
3. **Rate and fee recipient are constants.** Read the rate from onchain config, governance-set and
   whole-number ≥ 1. Pay the fee to FeeAggregator / validators with the RLS split.
4. **Receipts and gas suggestions.** The node's `effectiveGasPrice` for RLS txs uses the USDr base
   fee, and RLS tips appear in USDr `eth_feeHistory`. Both need to filter or convert by chain id in
   the node RPC; the proxy fixes receipts only.
5. **Writes to RLS storage.** These bypass the token's `paused` flag and hooks. Add the
   implementation/pause guard, and coordinate RLS upgrades with node releases.
6. **USDr precompile (`0x0400`).** Its EIP-712 domain is fixed at node start, so USDr EIP-3009
   signatures made on the RLS chain id don't verify.
7. **Pool ordering.** Tips in RLS units sort against tips in USDr units.
8. **Explorer.** Blockscout and indexers must read the tx `chainId` to label RLS sends and fees.
   chainlist must register the RLS chain id.
9. **Nonce guard race.** The cross-flavour nonce guard is check-then-insert. Two transactions
   validated at the same moment can both pass, so it belongs in the pool's insert path.
10. **RLS chain id.** It's derived (`chain_id * 10 + 1`, unchecked, and it can collide with another
    network's id). It should be a configured constant, registered on chainlist and checked at
    startup against the chain's own id.
11. **Failed transactions and indexers.** A reverted or halted RLS tx still pays its gas, and its
    `value` has already moved before execution, but a failed receipt keeps no logs, so neither
    `Transfer` is visible. Indexers that rebuild RLS balances from events drift. Fix: emit the logs
    outside the reverted frame, or have indexers read balances.
12. **Gas differs slightly between flavours.** The handler touches the sender's RLS balance slot
    before execution, so a contract that then reads RLS balances finds that slot warm and pays less
    gas than the same call in a USDr tx. Deterministic on every node, but `eth_estimateGas` from the
    other flavour is off by up to ~2,000 gas per slot.
13. **Re-check cost.** The after-block task scans every pending and queued RLS tx. Limit it to
    senders touched by the block, or keep an index of RLS txs, before mainnet load.
14. **Execution fallback.** The pool re-check catches the known ways an RLS tx becomes
    unexecutable. A tx that still fails the handler's checks at execution is skipped without paying
    and waits until it's dropped. Dropping it in `execute_batch` would close this generally.
15. **Not measured.** State-root cost at mainnet load; MetaMask Mobile.
