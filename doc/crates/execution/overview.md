# Overview of the execution crates.

The execution layer lives in [`crates/execution/`](../../../crates/execution/) and consists of
four crates:

| Crate | Package name | Purpose |
|-------|-------------|---------|
| [`evm/`](../../../crates/execution/evm/) | `rayls-execution-evm` | EVM execution engine, built on `reth` |
| [`rpc/`](../../../crates/execution/rpc/) | `rayls-execution-rpc` | Rayls-specific `rayls_*` RPC namespace |
| [`faucet/`](../../../crates/execution/faucet/) | `rayls-execution-faucet` | Testnet token faucet (must not be enabled on mainnet) |
| [`execution-metrics/`](../../../crates/execution/execution-metrics/) | `rayls-execution-metrics` | Prometheus metrics for the execution layer |

The sections below focus on `rayls-execution-evm`, since that is where the interesting
machinery lives. The `rpc` and `faucet` crates are described briefly at the end.

---

## How transactions arrive from consensus

When Bullshark commits a sub-dag a `ConsensusOutput` value is produced. The middleware
processor receives it and calls `execute_consensus_output` in
[`crates/middleware/processor/src/execution/orchestrator.rs`](../../../crates/middleware/processor/src/execution/orchestrator.rs)
(method at `:62`, free function at `:445`).

That function:

1. Calls `output.flatten_batches()` to extract an ordered list of `(cert_idx, batch_idx_in_cert)`
   pairs — one entry per batch in the committed sub-dag.
2. Iterates the list and, for each batch, constructs an `RLPayload` that describes the block to
   produce (parent header, beneficiary, gas limit, batch/output digests, timestamp, …).
3. Calls `execute_payload` (in
   [`crates/middleware/processor/src/execution/block.rs`](../../../crates/middleware/processor/src/execution/block.rs))
   → `RethEnv::build_block_from_batch_payload` (in
   [`evm/src/reth_env/execution.rs`](../../../crates/execution/evm/src/reth_env/execution.rs))
   to produce one EVM block per batch.
4. Out-of-order batches (by per-authority sequence number) are parked in `BatchOrderingState` and
   executed once their predecessors arrive; all parked batches are drained at epoch boundaries.
5. If the output contains no batches at all, one empty block is produced to advance the canonical
   tip (with the leader's address as beneficiary).
6. After all blocks for the round have been built, calls `reth_env.finish_executing_output` then
   `reth_env.finalize_block`.

One `ConsensusOutput` → one or more EVM blocks (one per batch), or exactly one empty block.

---

## The transaction execution loop

`RethEnv::build_block_from_batch_payload`
([`evm/src/reth_env/execution.rs`](../../../crates/execution/evm/src/reth_env/execution.rs))
is the innermost loop that takes a single batch's raw transaction bytes and produces an
`ExecutedBlock`.

Steps, in order:

1. **Recover transactions** — `reth_recover_raw_transactions` decodes and signature-recovers all
   raw bytes in the batch.
2. **Prewarm** — recovered transactions are passed to `spawn_sparse_trie_task` (see [state root
   section](#how-canonical-blocks-are-persisted)) which starts parallel pre-warming of the sparse
   trie using reth's `PayloadProcessor`.
3. **Create state** — a layered `StateProviderDatabase` is built from `CanonicalInMemoryState` +
   the MDBX `BlockchainProvider`. A revm `State` with bundle tracking is wrapped around it.
4. **Create EVM** — `evm_config.create_evm_with_native_erc20_only` creates the EVM with the
   `NativeErc20Inspector` attached (see [native ERC-20 section](#how-the-native-erc20-is-connected-to-the-evm)).
5. **Pre-execution changes** — `builder.apply_pre_execution_changes()` runs EIP-4788 / EIP-2935
   system calls.
6. **Per-transaction loop** — for each recovered transaction:
   - If decoding or signature recovery failed earlier, the transaction is skipped (counted as
     `validation_counts.other`).
   - `builder.execute_transaction(recovered)` is called.
   - `NonceTooLow` → skip, count.
   - `NonceTooHigh` → skip, count with sender + nonce details for diagnostics.
   - Any other `InvalidTx` error → skip, count as `other`.
   - Fatal `BlockExecutionError` variants → propagate as an error, aborting the batch.
   - Successful transactions are added to `committed_txs` and their priority fees accumulated.
7. **Post-execution** — `executor.finish()` runs closing-epoch system calls (calls
   `ConsensusRegistry.concludeEpoch` and related contracts) when the `close_epoch` flag is set
   in the payload.
8. **State root** — see [state storage section](#how-state-is-stored).
9. **Block assembly** — `RaylsBlockAssembler.assemble_block` seals the block.
10. **Insert into CIM** — the completed `ExecutedBlock` is inserted into `CanonicalInMemoryState`
    immediately so the _next_ batch in the same round can read this block's state as its parent.

---

## How state is stored

State lives in two places: an in-memory overlay and a persistent MDBX database.

**In-memory state — `CanonicalInMemoryState` (CIM)**

Reth's `CanonicalInMemoryState` is the single source of truth during normal operation. After each
batch is executed, `update_chain(NewCanonicalChain::Commit { new: [block] })` inserts the block
into CIM. This makes state visible to subsequent blocks in the same round without waiting for disk
I/O.

CIM also acts as the read-side for state root computation: `spawn_sparse_trie_task` derives a
`LazyOverlay` from CIM's in-memory trie data plus any accumulated `TrieInput` from ancestors to
feed the parallel sparse trie computation.

**On-disk state — MDBX via reth's `PersistenceService`**

A dedicated OS thread runs reth's `PersistenceService` and is accessed through a
`PersistenceHandle`. Blocks are flushed from CIM to MDBX asynchronously in the background.

New execution databases are created with a 16 KiB MDBX page size unless `--db.page-size` is
given (`RethEnv::new_database`); existing databases keep the page size they were created with.

`PersistenceState`
([`evm/src/reth_env/persistence.rs`](../../../crates/execution/evm/src/reth_env/persistence.rs))
tracks when to trigger a flush:

- `should_persist(canonical_head_number)` returns `true` when
  `canonical_head - last_persisted_block > persistence_threshold`.
- `finalize_block` (called once per consensus round, after all blocks for that round are built)
  checks `should_persist` and, if true, sends the range of blocks to
  `PersistenceHandle::save_blocks`.
- A `crossbeam` receiver is stored in `PersistenceState::pending_rx`; the result is polled on the
  next call to `check_persistence_completion`.
- `flush_persistence` forces an immediate synchronous flush of everything in CIM.

**State root computation**

Two tiers are attempted:

1. **Sparse/parallel (Tier 1)** — `spawn_sparse_trie_task` creates a `PayloadProcessor` task that
   computes the state root in parallel while the EVM executes transactions. A `state_hook` is
   attached to the block executor to stream per-transaction state diffs into the task. After
   `executor.finish()` drops the hook (signaling completion), the closure returned by
   `spawn_sparse_trie_task` blocks until the result is ready.
2. **Serial fallback (Tier 2)** — if the sparse trie task fails or panics,
   `state_provider.state_root_with_updates(hashed_state)` recomputes the root serially.

After the state root is known, trie changesets are computed and inserted into `changeset_cache`
for use by subsequent blocks' overlay factories.

---

## How the native ERC-20 is connected to the EVM

The native token (USDr) is exposed as a standard ERC-20 at the precompile address `0x0400`
(referred to in code as `USDR_PRECOMPILE_ADDRESS`) via
[`evm/src/native_erc20/`](../../../crates/execution/evm/src/native_erc20/).

There are two paths, chosen depending on context:

**Inspector path (real transactions)**

`NativeErc20Inspector` intercepts EVM calls to `0x0400` during transaction execution and applies
state changes to the revm journal — giving them the same atomicity guarantees as any other EVM
state change. The inspector is wrapped in `CompositeInspector` (which allows chaining multiple
inspectors), and that composite inspector is passed to `create_evm_with_native_erc20_only` in
[`evm/src/evm/factory.rs`](../../../crates/execution/evm/src/evm/factory.rs).

Also, `NativeErc20Inspector` automatically emits ERC-20 `Transfer` events for all native ETH
transfers, keeping on-chain logs consistent with ERC-20 tooling expectations.

**DynPrecompile path (`eth_call` simulations)**

For read-only RPC calls (`eth_call`, `eth_estimateGas`) the inspector path is not active. Instead,
`create_erc20_dyn_precompile` registers a `DynPrecompile` at `0x0400` that reads from the global
`ERC20_PRECOMPILE_INSTANCE` (an `Arc<RwLock<Erc20Precompile>>`). State changes made through this
path are not committed.

**Storage layout**

ERC-20 state (allowances, authorization nonces, total supply) is stored in deterministic EVM
storage slots computed by the helper functions in
[`evm/src/native_erc20/storage.rs`](../../../crates/execution/evm/src/native_erc20/storage.rs):
`allowance_slot`, `authorization_nonce_slot`, `total_supply_slot`. These are read and written
directly by both the inspector and the precompile paths.

---

## How transactions are validated

Validation happens at two points in the pipeline:

**1. Transaction pool ingress (`BypassableValidator`)**

When a transaction is submitted via RPC it goes through reth's `TransactionValidationTaskExecutor`
wrapped by `BypassableValidator`
([`evm/src/bypass_validator.rs`](../../../crates/execution/evm/src/bypass_validator.rs)). The
wrapper holds an optional `HashMap<TxHash, SenderState>` of pre-validated transaction hashes;
when populated (at epoch boundaries, for orphan transactions being re-introduced after already
having been validated in the previous epoch) any matching tx short-circuits to a `Valid`
outcome, skipping the per-transaction state lookup and MPSC channel hop. Non-orphan
transactions, and any tx not present in the map, are delegated to the inner validator and go
through reth's full Ethereum pool validation.

**2. Execution-time validation inside `build_block_from_batch_payload`**

Once a batch reaches the execution loop, raw transactions are decoded and signature-recovered
before execution. Errors at this stage are handled gracefully rather than aborting the batch:

| Error | Handling |
|-------|----------|
| Decode / signature failure | Skip; `validation_counts.other += 1` |
| `NonceTooLow` | Skip; `validation_counts.nonce_too_low += 1` |
| `NonceTooHigh` | Skip; `validation_counts.nonce_too_high += 1`; sender + nonce details recorded for diagnostics |
| Other `InvalidTx` | Skip; `validation_counts.other += 1` |
| Fatal `BlockExecutionError` | Propagated as an error; batch execution aborts |

Failed transactions are reported via `FailedTxNotification` so subscribers (e.g. the batch
builder's in-flight tracker) can remove them proactively rather than waiting for stale cleanup.

---

## How the EVM is created

`RaylsEvmConfig`
([`evm/src/evm/config.rs`](../../../crates/execution/evm/src/evm/config.rs)) is the top-level
EVM configuration. It is constructed once at node startup via `RaylsEvmConfig::new(chain_spec,
rewards_counter)` and holds:

- `RaylsBlockExecutorFactory` — creates per-block executors.
- `RaylsEvmFactory` — creates individual EVM instances.
- `RaylsBlockAssembler` — seals the completed block from execution output.

For block production, `RaylsEvmFactory::create_evm_with_native_erc20_only`
([`evm/src/evm/factory.rs`](../../../crates/execution/evm/src/evm/factory.rs)) builds the
`RaylsEvm` instance:

1. Fetches the global `ERC20_PRECOMPILE_INSTANCE`.
2. Builds the active `Precompiles` set dynamically from the current `SpecId` and registers the
   ERC-20 precompile as a `DynPrecompile` at `0x0400` (for `eth_call` support inside the same
   EVM).
3. Wraps a `NativeErc20Inspector` in a `CompositeInspector` so the inspector path handles
   real transaction calls to `0x0400`.
4. Assembles a `RevmEvm` with `RaylsEvmContext`, `EthInstructions`, and the `RaylsEvmHandler`.

`RaylsEvmHandler`
([`evm/src/evm/handler.rs`](../../../crates/execution/evm/src/evm/handler.rs)) overrides
the default `reward_beneficiary` logic: the priority fee is sent to the block's coinbase as usual,
but the base fee portion is sent to `BASEFEE_ADDRESS` (a `OnceLock<Address>` set once at startup;
defaults to `FEE_AGGREGATOR_ADDRESS` so base fees flow into fee distribution) instead of being
burned.

`RaylsChainSpec`
([`evm/src/chainspec.rs`](../../../crates/execution/evm/src/chainspec.rs)) overrides
`next_block_base_fee`. When EIP-1559 is active for the next block, it computes a standard
EIP-1559 base-fee adjustment (`compute_next_base_fee`, `chainspec.rs:418`) and clamps the
result to a configurable `min_base_fee` floor (defaulting to `MIN_RAYLS_PROTOCOL_BASE_FEE`).
Only when EIP-1559 is not yet active does it return the static `MIN_PROTOCOL_BASE_FEE`. See
[`gasless-mode.md`](../../gasless-mode.md) for how the `min_base_fee` floor is configured to
zero on gasless chains.

---

## How canonical blocks are persisted

Persistence is decoupled from execution to keep the consensus round loop fast.

**Per-batch: insert into CIM**

`build_block_from_batch_payload` calls `canonical_in_memory_state.update_chain(...)` immediately
after building each block. This makes the block available as a parent for the next batch in the
same round without any disk I/O.

**Per-round: update canonical head**

`finish_executing_output` is called once after all batches in a round are executed. It:
- Calls `NewCanonicalChain::Commit { new: blocks }.to_chain_notification()` to broadcast a
  `CanonStateNotification` to subscribers (the tx pool, the `rpc` crate, etc.).
- Updates the canonical head pointer on `BlockchainProvider`.

**Deferred MDBX persistence**

`finalize_block` is called after `finish_executing_output`. It:
1. Updates the finalized and safe block numbers on `BlockchainProvider` and sends them to the
   `PersistenceService`.
2. Checks `PersistenceState::should_persist(canonical_head)`.
3. If true (and no flush is already in-flight), calls `get_canonical_blocks_to_persist` to walk
   the CIM canonical chain from `last_persisted_block` to the current head, then dispatches those
   blocks to `PersistenceHandle::save_blocks`.
4. The `crossbeam` receiver is stored in `pending_rx`; `check_persistence_completion` polls it
   on subsequent rounds, updating `last_persisted_block` when the write completes.
5. `flush_persistence` can be called to trigger an immediate, synchronous flush of all in-memory
   blocks to MDBX (used at node shutdown and in tests).

---

## `rayls-execution-rpc`

[`crates/execution/rpc/`](../../../crates/execution/rpc/)

Adds a `rayls` JSON-RPC namespace with Rayls-specific endpoints not covered by `eth_*`. The
`EngineToPrimary` trait decouples the RPC layer from the consensus crates.

| Method | Description |
|--------|-------------|
| `rayls_latestHeader` | Latest `ConsensusHeader` |
| `rayls_genesis` | Chain genesis |
| `rayls_epochRecord` | `EpochRecord` + `EpochCertificate` by epoch number |
| `rayls_epochRecordByHash` | `EpochRecord` + `EpochCertificate` by epoch hash |

---

## `rayls-execution-faucet`

[`crates/execution/faucet/`](../../../crates/execution/faucet/)

> **Warning:** must not be enabled on mainnet.

A testnet faucet that extends the worker's RPC server. Drip requests are rate-limited per address
via a time-based LRU cache. Signing is delegated to Google Cloud KMS so the private key is never
in process memory. Configurable via `FaucetArgs` (wait period, drip amount, contract address, KMS key).
