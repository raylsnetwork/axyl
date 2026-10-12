# Axyl — System Overview

This document provides a high-level map of the Axyl codebase: how a transaction is processed
end-to-end, an architectural diagram, the custom database tables that sit on top of Ethereum's
standard storage, and a summary of the node's RPC and faucet interfaces.

For per-crate detail see the [crate index](crates/index.md).

---

## How a transaction flows through the system

```
  Client / dApp
       │  eth_sendRawTransaction
       ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  WorkerTxPool  (reth EthTransactionPool)                    │
  │  Standard Ethereum RPC server (eth_*)                       │
  └──────────────────────┬──────────────────────────────────────┘
                         │ pending transactions
                         ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  Worker  (crates/consensus/worker)                          │
  │  BatchBuilder: select best txs, record nonce ranges,        │
  │  seal batch (list of raw tx bytes + digest)                 │
  └──────────────────────┬──────────────────────────────────────┘
                         │ broadcast sealed batch to peer workers
                         ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  Peer Workers  — gossip + validate                          │
  │  QuorumWaiter: wait for 2f+1 acknowledgements               │
  └──────────────────────┬──────────────────────────────────────┘
                         │ batch digest forwarded to Primary
                         ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  Primary  (crates/consensus/primary)                        │
  │  Proposer: bundle parent certs + batch digests → Header     │
  │  Certifier: broadcast header, collect votes → Certificate   │
  │  DAG: track certificates round by round                     │
  └──────────────────────┬──────────────────────────────────────┘
                         │ committed sub-DAG (ConsensusOutput)
                         ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  Bridge  (crates/middleware/bridge)                         │
  │  Subscriber: validate + fetch missing batches, dedup,       │
  │  forward ConsensusOutput to execution engine channel        │
  └──────────────────────┬──────────────────────────────────────┘
                         │ mpsc channel  (max 100 queued outputs)
                         ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  ExecutorEngine  (crates/middleware/processor)              │
  │  Polls stream, queues ConsensusOutputs, executes one at a   │
  │  time on a blocking thread via execute_consensus_output     │
  └──────────────────────┬──────────────────────────────────────┘
                         │ one RLPayload per batch in the sub-DAG
                         ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  RethEnv::build_block_from_batch_payload                    │
  │  (crates/execution/evm)                                     │
  │   1. Recover raw tx bytes → TransactionSigned               │
  │   2. Spawn parallel sparse-trie pre-warm task               │
  │   3. Build layered state: CanonicalInMemoryState + MDBX     │
  │   4. Create RaylsEvm (revm + NativeErc20Inspector)          │
  │   5. Apply EIP-4788 / EIP-2935 pre-execution system calls   │
  │   6. Execute each transaction; skip invalid, record errors  │
  │   7. finish(): run epoch-closing system calls if last batch │
  │      └─ ConsensusRegistry.applyIncentives + concludeEpoch   │
  │   8. Compute state root (parallel sparse trie)              │
  │   9. RaylsBlockAssembler: seal ExecutedBlock                │
  │  10. Insert into CanonicalInMemoryState                     │
  └──────────────────────┬──────────────────────────────────────┘
                         │ ExecutedBlock
                         ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  PersistenceService  (reth, background thread)              │
  │  Flush CanonicalInMemoryState → MDBX on disk                │
  │  Broadcast CanonStateNotification → RPC consumers           │
  └─────────────────────────────────────────────────────────────┘
                         │
                         ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  WorkerTxPool: remove mined transactions from pool          │
  │  Remaining (un-batched) transactions stay in pool           │
  └─────────────────────────────────────────────────────────────┘
```

### Key invariants

- Transactions are **not validated or executed** until the consensus layer has committed them.
  The worker's batch builder checks validity at batch-sealing time, but the EVM treats any
  invalid transaction as a skip (no revert, no fee charge).
- One `ConsensusOutput` (committed sub-DAG) maps to **one EVM block per batch** within the
  sub-DAG, or a **single empty block** if the output carries no batches.
- Blocks within the same consensus round are inserted into `CanonicalInMemoryState` one at a
  time; each block reads the state produced by the previous block in the same round.
- Base fees are constant within an epoch and only adjusted at epoch boundaries.
- The mining reward (priority fees from batch transactions) is credited to the batch author's
  `coinbase` address; the block reward for `concludeEpoch` is handled by
  `ConsensusRegistry.applyIncentives` via system call.

---

## Architectural diagram

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │  External                                                           │
  │  eth_sendRawTransaction   eth_*   rayls_*   faucet_transfer         │
  └──────────┬───────────────────┬──────────┬──────────────────────────┘
             │                   │          │
  ┌──────────▼───────────────────▼──────────▼──────────────────────────┐
  │  Execution Layer  (crates/execution/)                               │
  │                                                                     │
  │  ┌─────────────────┐  ┌───────────────────┐  ┌──────────────────┐  │
  │  │  WorkerTxPool   │  │  rayls-execution  │  │ rayls-execution  │  │
  │  │  + eth_* RPC    │  │  -rpc             │  │ -faucet          │  │
  │  │  (reth)         │  │  (rayls_* NS)     │  │ (faucet_* NS)    │  │
  │  └────────┬────────┘  └─────────┬─────────┘  └────────┬─────────┘  │
  │           │                     │ EngineToPrimary       │ WorkerTxPool│
  │  ┌────────▼─────────────────────▼───────────────────────┘           │
  │  │  RethEnv  (rayls-execution-evm)                                  │
  │  │  build_block_from_batch_payload                                  │
  │  │  CanonicalInMemoryState + PersistenceService (MDBX)              │
  │  └────────────────────────┬────────────────────────────────────────┘│
  └───────────────────────────┼─────────────────────────────────────────┘
                              │ ExecutedBlock / ConsensusOutput
  ┌───────────────────────────▼─────────────────────────────────────────┐
  │  Middleware Layer  (crates/middleware/)                              │
  │                                                                     │
  │  ┌──────────────────────────────────────────────────────────────┐   │
  │  │  Orchestrator  (rayls-middleware-orchestrator)               │   │
  │  │  launch_node → EpochManager → epoch transition state machine │   │
  │  │  EngineToPrimaryRpc: bridges ConsensusBus + DB to rayls_*    │   │
  │  └───┬─────────────────────────────────┬────────────────────────┘   │
  │      │ spawns                          │ spawns                      │
  │  ┌───▼──────────────────┐  ┌───────────▼────────────────────────┐   │
  │  │  Bridge              │  │  Processor (ExecutorEngine)        │   │
  │  │  (rayls-middleware   │  │  (rayls-middleware-processor)      │   │
  │  │  -bridge)            │  │  Future polling consensus stream,  │   │
  │  │  Subscriber          │  │  one blocking execution task       │   │
  │  │  → mpsc channel      │  │  at a time                        │   │
  │  └──────────────────────┘  └────────────────────────────────────┘   │
  └──────────────────────────────────────────────────────────────────────┘
                              │ ConsensusOutput committed
  ┌───────────────────────────▼─────────────────────────────────────────┐
  │  Consensus Layer  (crates/consensus/)                               │
  │                                                                     │
  │  ┌─────────────────────────────────────────────────────────────┐    │
  │  │  Primary  (rayls-consensus-primary)                         │    │
  │  │  Proposer → Certifier → DAG → Bullshark commit              │    │
  │  │  ConsensusBus: broadcasts mode, output, headers             │    │
  │  │  StateSynchronizer: catch-up for lagging peers              │    │
  │  └─────────────────────────────────────────────────────────────┘    │
  │  ┌─────────────────────────────────────────────────────────────┐    │
  │  │  Worker  (rayls-consensus-worker)                           │    │
  │  │  BatchBuilder → QuorumWaiter → tx removal from pool         │    │
  │  └─────────────────────────────────────────────────────────────┘    │
  │  ┌──────────────────────────┐  ┌──────────────────────────────┐     │
  │  │  Network (libp2p)        │  │  State Sync                  │     │
  │  │  rayls-consensus-network │  │  rayls-consensus-state-sync  │     │
  │  └──────────────────────────┘  └──────────────────────────────┘     │
  └─────────────────────────────────────────────────────────────────────┘
                              │ shared types, storage, config, CLI
  ┌───────────────────────────▼─────────────────────────────────────────┐
  │  Infrastructure Layer  (crates/infrastructure/)                     │
  │  types · storage (redb / MDBX / mem) · config · network-cli        │
  │  network-types · utils                                              │
  └─────────────────────────────────────────────────────────────────────┘
```

---

## Custom database tables

Axyl uses two separate storage engines that sit alongside Ethereum's standard MDBX tables:

- **Consensus DB** (MDBX by default, redb optional) — stores all DAG and epoch state.
  Managed by `rayls-infrastructure-storage` (`crates/infrastructure/storage/Cargo.toml`
  default features select MDBX via `reth-libmdbx`; the `redb` feature flag switches the
  backend at build time).
- **Execution DB** (reth MDBX) — standard Ethereum execution state (accounts, storage,
  receipts, etc.). Managed by reth's built-in persistence layer.

The tables listed here are the **Axyl-specific additions**.
Standard reth tables (accounts, bytecode, transactions, receipts, etc.) are unchanged.

### Consensus DB tables

These tables are defined in `crates/infrastructure/storage/src/lib.rs` and accessed via the
typed store wrappers in `src/stores/`.

#### DAG data

| Table | Key | Value | Purpose |
|---|---|---|---|
| `certificates` | `CertificateDigest` | `Certificate` | Full certificates, indexed by their digest |
| `certificate_digest_by_round` | `(Round, AuthorityIdentifier)` | `CertificateDigest` | Look up a certificate for a specific authority in a specific round |
| `certificate_digest_by_origin` | `(AuthorityIdentifier, Round)` | `CertificateDigest` | Look up all rounds a specific authority participated in |
| `last_proposed` | `ProposerKey` (`u32`) | `Header` | The last header proposed by each primary (for equivocation detection) |
| `votes` | `AuthorityIdentifier` | `VoteInfo` | Votes collected for the current round |

#### Batch / payload data

| Table | Key | Value | Purpose |
|---|---|---|---|
| `payload` | `(BlockHash, WorkerId)` | `PayloadToken` | Acknowledges that a worker batch has been received and can be referenced in a header |
| `batches` | `BlockHash` | `Batch` | Raw batch contents indexed by digest |
| `node_batches_cache` | `BlockHash` | `Batch` | Short-lived node-local batch cache (avoids re-fetching recently seen batches) |
| `batch_seq_counter` | `WorkerId` | `u64` | Monotonically increasing sequence counter per worker; enforces ordering |

#### Consensus block data

| Table | Key | Value | Purpose |
|---|---|---|---|
| `consensus_block` | `u64` (block number) | `ConsensusHeader` | The consensus chain, one entry per committed sub-DAG |
| `consensus_block_number_by_digest` | `B256` (header hash) | `u64` (block number) | Reverse index: look up a consensus block number by its hash |
| `consensus_block_cache` | `u64` (block number) | `ConsensusHeader` | In-memory cache for recent consensus headers (verified but not yet processed); cleared once the header is promoted to `consensus_block` |

#### Epoch data

| Table | Key | Value | Purpose |
|---|---|---|---|
| `epoch_record_by_number` | `u64` (epoch) | `EpochRecord` | Signed committee snapshot at each epoch boundary; the trust anchor for light sync |
| `epoch_cert_by_number` | `u64` (epoch) | `EpochCertificate` | Aggregate BLS certificate proving the `EpochRecord` is authoritative |
| `epoch_records_index` | `B256` (record hash) | `u64` (epoch) | Reverse index: look up an epoch number by its record hash |
| `epoch_transition_checkpoints` | `u8` (phase enum) | `EpochTransitionCheckpoint` | Persists in-progress epoch transition phase so resumption is safe after a crash |

#### Kademlia DHT data

| Table | Key | Value | Purpose |
|---|---|---|---|
| `kad_record` | `BlockHash` | `Vec<u8>` (encoded libp2p record) | Primary Kademlia DHT records (node discovery) |
| `kad_provider_record` | `BlockHash` | `Vec<u8>` | Primary Kademlia provider records |
| `kad_worker_record` | `BlockHash` | `Vec<u8>` | Worker Kademlia DHT records |
| `kad_worker_provider_record` | `BlockHash` | `Vec<u8>` | Worker Kademlia provider records |

### Execution block header — non-standard field encoding

Rather than adding new tables, Axyl encodes consensus metadata into standard Ethereum block header
fields that are otherwise unused or repurposed.
All standard reth tables are unmodified; the meaning of these fields is extended.

| EVM header field | Axyl encoding | Decoded as |
|---|---|---|
| `parent_beacon_block_root` | `consensus_header_digest` — `keccak256(ConsensusHeader)` | Links each EVM block back to the consensus round that produced it |
| `mix_hash` (`prev_randao`) | `output_digest XOR batch_digest` | Recovers the batch digest as `mix_hash XOR parent_beacon_block_root` |
| `nonce` | `(epoch << 32) \| round` | Encodes the epoch and consensus round number that produced this block |
| `ommers_hash` / `requests_hash` | `batch_digest` (hardfork-gated, see below) | Keccak of the raw batch |
| `difficulty` | `(batch_index << 16) \| worker_id` | Position of the batch within the sub-DAG and the worker that created it |
| `coinbase` | Batch authority ECDSA address | The validator whose batch this block represents; receives priority fees |
| `extra_data` | `keccak(leader_bls_signatures)` on epoch-boundary blocks, otherwise empty | Randomness seed for the new committee shuffle; identifies epoch boundaries |

#### `batch_digest` placement — hardfork-gated

Before the `BatchDigestV2` hardfork, the batch digest is carried in `ommers_hash` and
`requests_hash` is set to `EMPTY_REQUESTS_HASH`. After the hardfork (current behaviour on
networks that have activated it), `ommers_hash` is set to `EMPTY_OMMER_ROOT_HASH` for
go-ethereum compatibility and the digest is carried in `requests_hash` instead. The selection
is performed by `RaylsEvmConfig::resolve_batch_digest_fields`
([`evm/src/evm/config.rs:67`](../../crates/execution/evm/src/evm/config.rs)) and applied in
the header builder ([`evm/src/evm/block.rs:60`](../../crates/execution/evm/src/evm/block.rs)).

---

## RPC interface

### Standard Ethereum RPC (`eth_*`)

Every node runs the full reth `EthApi` server over HTTP, WebSocket, and IPC.
All standard `eth_*` methods are available.

### Rayls namespace (`rayls_*`)

Seven additional endpoints expose consensus state that is not covered by the standard Ethereum API.
Provided by `rayls-execution-rpc`; details in [crates/execution/rpc.md](crates/execution/rpc.md).

| Endpoint | Parameters | Returns | Description |
|---|---|---|---|
| `rayls_latestHeader` | — | `ConsensusHeader` | The consensus chain tip: parent hash, committed sub-DAG, block number |
| `rayls_consensusHeaderByNumber` | `number: u64` | `ConsensusHeader` | The consensus header at that number, if the node holds it |
| `rayls_consensusHeaderByHash` | `hash: B256` | `ConsensusHeader` | The consensus header with that digest, if the node holds it |
| `rayls_genesis` | — | `Genesis` | Full chain genesis configuration (chain id, alloc, config) |
| `rayls_epochRecord` | `epoch: u64` | `(EpochRecord, EpochCertificate)` | Committee snapshot and BLS certificate for the given epoch number |
| `rayls_epochRecordByHash` | `hash: B256` | `(EpochRecord, EpochCertificate)` | Same lookup keyed by the record's hash instead of epoch number |
| `rayls_nodeStatus` | — | `NodeStatus` | The node's role (active or inactive validator, observer), whether it is caught up, its epoch, DAG rounds and last canonical block |

**`EpochRecord`** is the trust anchor for light clients and syncing nodes. It records the active
committee, the next committee, the last execution block of the epoch, and the last consensus header
of the epoch. The paired **`EpochCertificate`** is an aggregate BLS signature over the record
produced by ≥ ⌊2n/3⌋ + 1 committee members.

### System contract addresses

These contracts are deployed at genesis and driven by EVM system calls at epoch boundaries.

| Contract | Address | Purpose |
|---|---|---|
| `ConsensusRegistry` | `0x07E1…7e1` | Validator registration, staking, committee management, epoch rewards |
| `DelegationPool` | `0x07E1…7e2` | Multi-delegator staking pools |
| `FeeAggregator` | `0x07E1…7e3` | Collects EIP-1559 base fees, swaps to RLS, distributes each epoch |
| `RewardDistributor` | `0x07E1…7e5` | RLS staking reward distribution |

---

## Faucet

> **Warning:** the faucet must not be enabled on mainnet.

The faucet is an optional `faucet_*` RPC namespace provided by `rayls-execution-faucet`.
It is activated by passing `--google-kms` on the command line.
Details in [crates/execution/faucet.md](crates/execution/faucet.md).

### `faucet_transfer`

| Parameter | Type | Required | Description |
|---|---|---|---|
| `address` | `Address` | yes | Recipient address |
| `contract` | `Address \| null` | no | Stablecoin contract to drip; `null` / `address(0)` for native RLS |

Returns the `TxHash` of the submitted transfer transaction.

### How it works

1. Rate-limit check — each `(address, contract)` pair may only receive one drip per `--wait-period`
   (default 24 hours) and only one drip may be pending at a time.
2. An EIP-1559 transaction calling `drip(address, contract)` (selector `0xeb3839a7`) is
   constructed with the next pending nonce.
3. The transaction hash is sent to **Google Cloud KMS** for signing; the private key never
   enters the process.
4. The signed transaction is submitted directly to the `WorkerTxPool`.
5. When the transaction is mined, the address is added to the success cache.

### Configuration summary

| CLI flag | Default | Description |
|---|---|---|
| `--wait-period` | `86400` (24 h) | Minimum seconds between drips per address |
| `--faucet-contract` | `0x0…0` | On-chain faucet contract |
| `--public-key` | test key | Faucet wallet public key (hex or PEM) |
| `--google-kms` | off | Enable the faucet and KMS signing |
| `--project-id` | — | GCP project ID |
| `--key-locations` | — | KMS key location |
| `--key-rings` | — | KMS key ring name |
| `--crypto-keys` | — | KMS key name |
| `--crypto-key-versions` | — | KMS key version |
