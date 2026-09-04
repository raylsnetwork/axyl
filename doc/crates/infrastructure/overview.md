# Infrastructure Crates

The infrastructure layer provides the shared foundation used by every other part of the codebase: primitive types, key management, configuration, persistent storage, network message interfaces, and the CLI entry point.

| Crate | Name | Responsibility |
|---|---|---|
| `crates/infrastructure/types` | `rayls-infrastructure-types` | Core types and traits re-exported across the entire workspace |
| `crates/infrastructure/storage` | `rayls-infrastructure-storage` | Persistent consensus storage (redb, MDBX, in-memory) |
| `crates/infrastructure/config` | `rayls-infrastructure-config` | Node configuration: keys, consensus parameters, genesis, networking |
| `crates/infrastructure/network-cli` | `rayls-network-cli` | CLI entry point: `node`, `genesis`, `keytool` commands |
| `crates/infrastructure/network-types` | `rayls-infrastructure-network-types` | Worker↔Primary network message traits and mock implementations |
| `crates/infrastructure/utils` | `rayls-infrastructure-utils` | Small shared utilities (`NotifyRead`) |

---

## `rayls-infrastructure-types`

The types crate is the lowest-level dependency in the workspace.
Almost every other crate imports from it directly.
It owns the canonical definitions of consensus data structures and provides
the async runtime primitives that the rest of the node is built on.

### Re-exports

The crate re-exports the types most frequently needed elsewhere so that consumers
do not need to track individual upstream library versions:

| Source | Selected exports |
|---|---|
| `alloy` | `Address`, `B256`, `U256`, `BlockHash`, `BlockNumHash`, `Transaction`, `TxEip1559`, `Bytes`, `keccak256`, `Genesis`, `GenesisAccount`, `Withdrawals`, `AccessList`, `Signature`, … |
| `reth` | `Block`, `BlockBody`, `SealedBlock`, `SealedHeader`, `TransactionSigned`, `Receipt`, `RecoveredBlock`, `EthPrimitives`, … |
| `libp2p` | `Multiaddr`, `Protocol` |

### Consensus data structures

The `primary/`, `worker/`, `committee.rs`, and `genesis.rs` modules define the
core objects that flow through the Narwhal/Bullshark DAG:

| Type | Description |
|---|---|
| `Committee` / `CommitteeBuilder` | The set of staked validators for an epoch; used for quorum checks |
| `AuthorityIdentifier` | Unique identity of a validator within a committee |
| `Certificate` / `CertificateDigest` | A quorum-signed header; the unit of DAG progress |
| `Header` / `HeaderBuilder` | An unsigned header proposed by a primary |
| `Batch` / `CertifiedBatch` | A worker's collection of raw transactions |
| `ConsensusOutput` / `ConsensusHeader` | The committed subDAG output forwarded to execution |
| `EpochRecord` / `EpochCertificate` / `EpochVote` | Epoch boundary bookkeeping |
| `Round`, `Epoch`, `TimestampSec` | Core numeric newtypes |
| `WorkerId` | Identifies a worker within a validator node |

### Database traits

`database_traits.rs` defines the generic storage interfaces implemented by the
storage crate's backends:

| Trait | Purpose |
|---|---|
| `Table` | Associates a name, key type, and value type for a database column |
| `DbTx` | Read-only transaction: `get`, `iter`, `skip_to`, `reverse_iter`, `last_record` |
| `DbTxMut: DbTx` | Read-write transaction: `insert`, `remove`, `clear_table`, `commit` |
| `Database` | Top-level handle; opens read and write transactions |

### Task management

`TaskManager` / `TaskSpawner` wrap the Tokio runtime with structured task lifecycle:

| Type / Concept | Description |
|---|---|
| `TaskManager` | Holds all spawned `JoinHandle`s; detects crashes of critical tasks |
| `TaskSpawner` | Cheaply-cloned handle for spawning tasks on the shared manager |
| `TaskKind::Drainable` | Task can be drained with a timeout (e.g., subscriber) |
| `TaskKind::Doomed` | Task must be aborted immediately at epoch boundary |
| `TaskKind::Cancel` | Task has no drainable state; cancel immediately |
| `TaskJoinError` | Returned when a critical task exits unexpectedly |

### Shutdown signalling

`Notifier` / `Noticer` provide a `watch`-channel-based one-shot shutdown signal:

- `Notifier` is owned by the top-level manager and triggers shutdown by dropping or calling `notify()`.
- `Noticer` is a cheap clone distributed to every task; tasks `select!` on it.

### Other types

| Module | Key exports |
|---|---|
| `crypto` | `BlsSigner` trait, BLS/Ed25519 key wrappers, `to_intent_message`, `encode` |
| `gas_accumulator` | `GasAccumulator`, `RewardsCounter` — tracks per-epoch gas and block rewards |
| `batch_tracker` | `BatchTracker` — lifecycle metrics for worker batches |
| `notifier` | `Noticer`, `Notifier` |
| `sync` | `RaylsSender` / `RaylsReceiver` channel traits (`sync.rs:163` / `:151`) and helper utilities |
| `genesis` | `test_chain_spec_arc` free function (`genesis.rs:82`) for building a default test `ChainSpec` |

---

## `rayls-infrastructure-storage`

The storage crate provides all persistent consensus state.
Execution state (the EVM chain) is stored separately by Reth.

### Backends

| Backend | When used |
|---|---|
| `ReDB` (`redb::database`) | Default; always built. Stores consensus data as a key-value store backed by [redb](https://github.com/cberner/redb). |
| `MdbxDatabase` (`mdbx::database`) | Optional; enabled by the `reth-libmdbx` feature. Uses LMDB-derived MDBX. |
| `MemDb` (`mem_db`) | In-memory; used in tests. |
| `LayeredDatabase` (`layered_db`) | Wraps two databases; reads fall through from the first to the second. Used for cache-over-persistent patterns. |

### Store abstractions

The `stores/` module provides typed wrappers around the generic `Database` trait:

| Store | Purpose |
|---|---|
| `CertificateStore` | Certificates indexed by digest, round, and origin |
| `CheckpointStore` | Epoch transition checkpoints (phase-based state machine persistence) |
| `ConsensusStore` | Consensus blocks, number-by-digest index, block cache |
| `EpochStore` | Epoch records (`EpochRecord`) and epoch certificates indexed by number |
| `PayloadStore` | Batch payload tokens acknowledging worker batches |
| `ProposerStore` | Last proposed header per authority (`LastProposed`) |
| `VoteDigestStore` | Cast votes keyed by `AuthorityIdentifier`, valued as `VoteInfo` |

### Tables (column families)

The full set of named tables managed by the storage crate (see
[`storage/src/lib.rs:41-63`](../../../crates/infrastructure/storage/src/lib.rs)):

```
last_proposed                last_proposed_by_authority   votes
certificates                 certificate_digest_by_round  certificate_digest_by_origin
payload                      batches                      consensus_block
consensus_block_number_by_digest                          consensus_block_cache
node_batches_cache           epoch_record_by_number       epoch_cert_by_number
epoch_records_index          epoch_transition_checkpoints batch_seq_counter
kad_record                   kad_provider_record          kad_worker_record
kad_worker_provider_record   node_identity                batch_ordering_state
```

The certificate-keyed tables (`votes`, `certificates`, `certificate_digest_by_round`,
`certificate_digest_by_origin`) are cleared every epoch rather than garbage-collected by a
fixed round window (`storage/src/lib.rs:87-92`); there is no `ROUNDS_TO_KEEP` constant.

### Factory helpers

| Function | Purpose |
|---|---|
| `open_db` (`storage/src/lib.rs:135`) | Open a database at a path with default column families |
| `open_db_with_consensus_config` (`storage/src/lib.rs:142`) | Open or create a database from a `ConsensusConfig`, materialising every required column family |

---

## `rayls-infrastructure-config`

The config crate holds every configuration type that a running node needs.
It is the authority on how files are read from and written to disk.

### Configuration types

#### `ConsensusConfig<DB>`

The primary runtime configuration for all consensus-layer components.
Holds the `Committee`, `WorkerCache`, `KeyConfig`, `NetworkConfig`,
the consensus `DB`, and the `LocalNetwork` (in-process worker↔primary channel).
Distributed to the primary, workers, and subscriber at the start of each epoch.

#### `KeyConfig`

The most security-critical config.
Manages the three keys required for node operation:

| Key | Algorithm | Use |
|---|---|---|
| Primary BLS12-381 | BLS | Signs headers, votes, and epoch certificates; must be unique on `ConsensusRegistry` |
| Primary Ed25519 (network key) | Ed25519 | Authenticated p2p communication via libp2p |
| Worker Ed25519 (network key) | Ed25519 | Authenticated p2p communication for the worker process |

BLS private keys are stored on the filesystem encrypted with AES-GCM-SIV.
Network keys are deterministically derived from a BLS signature of a seed string so
that they never need to be stored separately.
The `BlsSigner` trait provides signatures without exposing the private key outside
`KeyConfig`.

#### `NetworkGenesis`

Used only at network genesis (`config/src/genesis.rs:88`). No `GenesisConfig` type exists.
The initial committee is assembled from validator `node-info.yaml` files collected into a shared
directory by a "Master of Ceremony" (typically a VCS). Each entry contains a proof-of-possession
that is verified during genesis.

#### `NetworkConfig`

Holds libp2p behaviour parameters and peer manager policy.
Controls banned-peer lists, peer scoring, and connection limits.

#### `Config` (Node Config)

Narwhal/Bullshark protocol parameters and information broadcast to peers
(listen addresses, timeouts, round durations, etc.).

#### `RetryConfig`

Retry policy for state-sync requests sent during batch validation.
Only used in `consensus/primary/src/header_validator.rs`.

### File I/O traits

`ConfigTrait` / `ConfigFmt` — generic traits for reading/writing configuration
files to/from YAML or JSON on the filesystem. All config structs implement these.

`RaylsDirs` (`config/src/traits.rs:126`) is a trait of path accessors
(`node_keys_path`, `genesis_path`, `committee_path`, `consensus_db_path`, `reth_db_path`, …)
blanket-implemented for every `T: AsRef<Path>`. Each method joins a fixed relative name onto the
caller-supplied base path. There is no XDG / `Library/Application Support` / `%APPDATA%`
resolution — the base path is whatever the caller passes (typically the `--datadir` flag).

---

## `rayls-network-cli`

The CLI crate is the library used by `bin/rayls-network` to parse arguments and
dispatch to the appropriate node subsystem.

### Command tree

```
rayls-network
├── node          Start the validator/observer node
├── genesis       Run the genesis ceremony to create a new network
└── keytool
    ├── generate  Generate BLS + network keys for a validator or observer
    └── stake-calldata  Produce ABI-encoded calldata for the staking transaction
```

### `node` command (`NodeCommand`)

The main entry point for running a node. Key flags:

| Flag | Purpose |
|---|---|
| `--config-file <PATH>` | Load the chain-id and hardfork schedule of the selected subnet from an external per-client YAML file (replaces the baked-in values); requires `--subnet` |
| `--subnet <NAME>` | Select the subnet entry inside `--config-file`; requires `--config-file` |
| `--network <NAME>` | Override the Rayls hardfork profile (`devnet`, `testnet`, `mainnet`, `local`) from the baked-in schedule; refused if combined with `--config-file` |
| `--observer` | Start as a non-validating observer node |
| `--instance <N>` | Offset ports by instance number (max 200) to run multiple nodes on one host |
| `--with-unused-ports` | Let the OS assign random free ports (testing) |
| `--metrics <SOCKET>` | Enable Prometheus endpoint |
| `--healthcheck <PORT>` | Enable TCP health check endpoint |
| `--datadir <PATH>` | Override the data directory |
| `--bls-passphrase-source` | How to obtain the BLS key passphrase: `env`, `stdin`, `ask`, `no-passphrase` |

The `node` command delegates to `launch_node` (orchestrator crate) after building
the `RaylsBuilder` / `RethConfig` from the parsed CLI arguments.

#### External hardfork configuration (`--config-file` / `--subnet`)

A node can run the chain-id and hardfork schedule of the selected subnet from
an external per-client YAML file instead of the values baked into the binary.
One file holds every subnet a client operates (the number of subnets is not
fixed); see `examples/client1.yaml` and `examples/new-client.template.yaml` in
the repository root. With `--config-file`:

- the selected subnet's `chain_id` and `hardforks` section are the single
  source of truth (installed in a process-wide profile the execution layer
  reads); fork names are case-insensitive, an absent fork stays `never`, every
  given fork name must be a real one, and `chain_id` is required (all validated
  before startup);
- everything else — genesis, parameters, committee, node identity — comes from
  the datadir, exactly as without the file, so the datadir must be fully
  provisioned as before. At boot the datadir's genesis chain-id is verified
  against the subnet's `chain_id`, and the node refuses to start on a mismatch
  (the datadir belongs to a different network or client);
- the datadir's `parameters.network` is ignored while the file is active, and the
  flag cannot be combined with `--network`.

Without `--config-file` the schedule is as before (the baked-in schedule
selected by `parameters.network`, or `--network`), but the datadir's genesis
chain-id is still verified against the effective network's baked-in chain-id
(mainnet is `487`, testnet/devnet/local are `2017`) and the node refuses to
start on a mismatch.

### `genesis` command (`GenesisArgs`)

Runs the genesis ceremony.
Reads `node-info.yaml` files from the validators directory, validates
proofs-of-possession, deploys the `ConsensusRegistry` contract, and writes
the genesis block and initial committee YAML files.

Key parameters:
- `--consensus-registry-owner` — governance multisig address
- `--basefee-address` — `FeeAggregator` contract address
- Initial staking amounts and funded accounts

### `keytool` command (`KeyArgs`)

- **`generate validator`** — generates BLS12-381 + Ed25519 keys, writes
  encrypted key files and a `node-info.yaml` suitable for the genesis ceremony.
- **`generate observer`** — generates keys for a non-validating observer.
- **`stake-calldata`** — reads existing keys and produces the ABI-encoded
  `ConsensusRegistry.stake(...)` calldata for on-chain staking.

---

## `rayls-infrastructure-network-types`

This crate defines the async message traits used for local (in-process)
communication between the worker and primary processes.

### Traits

#### `WorkerToPrimaryClient`

| Method | Description |
|---|---|
| `report_own_batch(WorkerOwnBatchMessage)` | Worker notifies primary of a batch it produced |
| `report_others_batch(WorkerOthersBatchMessage)` | Worker notifies primary of a batch received from a peer |

#### `PrimaryToWorkerClient`

| Method | Description |
|---|---|
| `synchronize(WorkerSynchronizeMessage)` | Primary instructs worker to fetch a missing batch |
| `fetch_batches(HashSet<BlockHash>)` | Primary requests batch payloads by digest |

### Mock implementations

| Type | Behaviour |
|---|---|
| `MockWorkerToPrimary` | Returns `Ok(())` immediately on all calls |
| `MockWorkerToPrimaryHang` | Pends forever; used to test timeout behaviour |
| `MockPrimaryToWorkerClient` | Returns pre-loaded batches from `HashMap`; returns empty otherwise |

`LocalNetwork` (in `local.rs`) is the production in-process implementation that
routes messages directly between the primary and worker tasks without a network hop.

---

## `rayls-infrastructure-utils`

A minimal utility crate containing `NotifyRead` (`notify_read.rs`): an async wrapper
that allows waiting for a value to appear in a store, built on `tokio::sync::watch`.
It is used in several places where tasks need to wait for consensus data to be
written by another task before proceeding.
