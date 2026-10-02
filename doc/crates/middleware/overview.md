# Middleware Crates

The middleware layer sits between the consensus engine and the execution engine.
It is responsible for three concerns:

| Crate | Name | Responsibility |
|---|---|---|
| `crates/middleware/bridge` | `rayls-middleware-bridge` | Subscribes to consensus output and forwards it to the execution engine |
| `crates/middleware/orchestrator` | `rayls-middleware-orchestrator` | Manages the full node lifecycle: startup, epoch transitions, node mode, health checks |
| `crates/middleware/processor` | `rayls-middleware-processor` | Executes `ConsensusOutput` values and produces canonical EVM blocks |

---

## `rayls-middleware-bridge`

The bridge is the adaptor between the consensus layer and the execution engine.
Its sole job is to subscribe to committed consensus output and forward it to the processor.

### Architecture

```
ConsensusBus (sequenced output)
        │
        ▼
   Subscriber
  ┌──────────────────────────────────────────────────────────┐
  │  mode = CvvActive   → run consensus subscriber           │
  │  mode = CvvInactive → catch-up + rejoin subscriber       │
  │  mode = Observer    → stream-only subscriber             │
  └──────────────────────────────────────────────────────────┘
        │
        ▼
  tx_consensus_output (mpsc channel)
        │
        ▼
  ExecutorEngine (processor crate)
```

### Entry point: `Executor::spawn`

`Executor::spawn` calls `spawn_subscriber`, which selects the correct subscriber
mode based on the current `NodeMode` observed on the `ConsensusBus`:

| `NodeMode` | Subscriber behaviour |
|---|---|
| `CvvActive` | Participates in consensus; receives output from the primary directly |
| `CvvInactive` | Runs a catch-up loop (fetches missing consensus from peers), then rejoins as active |
| `Observer` | Streams consensus output from a validator without participating |

The `Subscriber<DB>` struct
([`subscriber.rs:35-44`](../../../crates/middleware/bridge/src/subscriber.rs)) holds four
direct fields, most of the state living inside the `Arc<Inner>`:

| Field | Type | Purpose |
|---|---|---|
| `consensus_bus` | `ConsensusBus` | Subscribes to node mode changes and the sequenced output stream |
| `config` | `ConsensusConfig<DB>` | Committee, key material, and the consensus DB |
| `network_handle` | `PrimaryNetworkHandle` | p2p handle used to fetch missing batches |
| `inner` | `Arc<Inner>` | Shared state: authority id, committee, worker client, dedup set, … |

Before forwarding an output the subscriber validates batch signatures, fetches any
missing worker batches over the network, and deduplicates against already-executed
digests stored in the consensus DB.

---

## `rayls-middleware-orchestrator`

The orchestrator is the top-level entry point for a running node.
It owns the long-lived resources that span multiple epochs (network handles, databases,
`ConsensusBus`) and drives the epoch state machine.

### Node modes

A node can be in one of three modes at any given time:

| Mode | Meaning |
|---|---|
| `CvvActive` | Actively participating in consensus (proposing and voting) |
| `CvvInactive` | A CVV that has fallen behind or just started; catching up before rejoining |
| `Observer` | Non-validator that tracks chain state without participating in consensus |

### Entry point: `launch_node`

```
launch_node(builder, rayls_datadir, passphrase)
    │
    ├── open Reth DB (execution state)
    ├── open consensus DB (MDBX)
    ├── create ConsensusBus
    ├── create EpochManager
    └── epoch_manager.run().await  ← blocks for the lifetime of the process
```

`launch_node` creates a Tokio multi-thread runtime, opens both databases once for the
lifetime of the process, and hands everything to `EpochManager::run`.

### `EpochManager`

`EpochManager<P, DB>`
([`epoch_manager/types.rs:29`](../../../crates/middleware/orchestrator/src/epoch_manager/types.rs))
is the long-running type that oversees epoch transitions. It holds:

- `builder: RaylsBuilder` – node configuration factory
- `rayls_datadir: P` – path-typed handle to the configured data directory
- `primary_network_handle` / `worker_network_handle` – long-lived p2p handles (`Option`-wrapped during transitions)
- `key_config: KeyConfig` – loaded once; shared across epochs
- `node_shutdown: Notifier` – shuts down the entire node
- `epoch_boundary: TimestampSec` – current epoch close time (updated each epoch)
- `reth_db: RethDb` / `consensus_db: DB` – kept open for the process lifetime
- `consensus_bus: ConsensusBus` – shared bus; carries node mode, sequenced output, latest header, etc.
- `worker_event_stream: QueChannel<NetworkEvent<…>>` – persistent worker p2p event stream
- `epoch_record: Option<EpochRecord>` – record from the just-completed epoch
- `initial_epoch: bool` – `true` only during the first epoch after process start

#### Epoch transition state machine

Each call to `run_epoch()` runs a `select!` loop that produces one of four outcomes:

| Outcome | Meaning |
|---|---|
| `NodeShutdown` | Graceful SIGTERM; drain the subscriber and exit |
| `EpochBoundary(hash, output)` | Leader timestamp ≥ epoch close time; flush and advance to next epoch |
| `ModeTransition(mode)` | A mode change was detected (e.g., catch-up complete → `CvvActive`) |
| `TaskCrash(err)` | A consensus subtask panicked; attempt recovery or shutdown |

A `TransitionCtx` bundles the resources needed across sequential transition phases
(`ExecutionNode`, `PrimaryNode`, the `to_engine` channel, `GasAccumulator`, and task
managers) so that phase code does not need to pass them individually.

### Task managers

The orchestrator uses three named task managers:

| Name | Scope |
|---|---|
| `"Node Task Manager"` | Long-lived; holds network handles and health check |
| `"Epoch Task Manager"` | Per-epoch; holds the primary, workers, and subscriber tasks |
| `"Engine Task Manager"` | Per-epoch; holds the execution engine (`ExecutorEngine`) |

### `EngineToPrimaryRpc`

`EngineToPrimaryRpc<DB>` implements the `EngineToPrimary` trait and bridges the
execution RPC layer to consensus data:

| Method | Source |
|---|---|
| `get_latest_consensus_block` | `ConsensusBus::local_consensus_tip` (in-memory; the subscriber publishes each header it saves and re-seeds it from the canonical tip at every epoch start) |
| `consensus_block_by_number` | consensus `DB` (historical lookup) |
| `consensus_block_by_hash` | consensus `DB` (historical lookup) |
| `epoch` (by number or hash) | consensus `DB` |
| `node_status` | `ConsensusBus` only: the local tip for `epoch` and the observer's `is_caught_up`, the watches for the round watermarks |

### Supporting subtasks

| Module | Role |
|---|---|
| `epoch_manager/primary.rs` | Spawns and owns the `Primary` consensus node for one epoch |
| `epoch_manager/worker.rs` | Spawns and owns `Worker` nodes; one per configured worker |
| `types/health.rs` | HTTP healthcheck server; responds to liveness probes |
| `engine/` | `ExecutionNode` wrapper; creates and owns `ExecutorEngine` for one epoch |
| `types/epoch_transition.rs` | Phase types and `TransitionCtx`; pure data, no async logic |

---

## `rayls-middleware-processor`

The processor ("Engine") receives `ConsensusOutput` values from the bridge and
executes them against the EVM state to produce canonical EVM blocks.

### Architecture

```
mpsc::Receiver<ConsensusOutput>
        │
        ▼
  ExecutorEngine  (Future, polled by Tokio runtime)
  ┌─────────────────────────────────────────────────────────┐
  │  queued: VecDeque<ConsensusOutput>  (max 100)           │
  │  pending_task: Option<PendingExecutionTask>             │
  └─────────────────────────────────────────────────────────┘
        │ spawn_blocking_task (one at a time)
        ▼
  execute_consensus_output  (execution/orchestrator.rs)
        │
        ▼
  Processor<DB>  →  RethEnv  (writes canonical EVM blocks)
```

### `ExecutorEngine`

`ExecutorEngine` is a hand-implemented `Future`.
The runtime polls it, and it drives both intake and execution:

1. **Intake** – drain `consensus_output_stream` into `queued` (back-pressure limit: 100 outputs).
2. **Execute** – if no task is pending, pop the front of `queued`, spawn a blocking task via
   `spawn_blocking_task`, and store the `oneshot::Receiver` in `pending_task`.
3. **Complete** – when the blocking task sends its result, update `parent_header`, then loop
   back to step 2.

Key fields
([`processor/src/lib.rs:53-74`](../../../crates/middleware/processor/src/lib.rs)):

| Field | Purpose |
|---|---|
| `queued` | `VecDeque<ConsensusOutput>` buffering up to `MAX_QUEUED_OUTPUTS = 100` |
| `pending_task` | `Option<PendingExecutionTask>` for the single in-flight blocking task |
| `max_round` | Optional ceiling for testing/debugging — engine shuts down once reached |
| `consensus_output_stream` | `ReceiverStream<ConsensusOutput>` consuming the bridge channel |
| `parent_header` | Most recent `SealedHeader`; updated after each successful execution |
| `rx_shutdown` | `Noticer` shutdown receiver |
| `task_spawner` | `TaskSpawner` used to dispatch blocking execution work |
| `last_seen_output_number` | Sequence number of the last received output; used for ordering checks |
| `last_seen_epoch` | Epoch of the last received output; detects cross-epoch gaps |
| `processor: Processor<DB>` | Shared execution services bundle (see below) |

`Processor<DB>`
([`execution/orchestrator.rs:25-32`](../../../crates/middleware/processor/src/execution/orchestrator.rs))
owns the per-process execution services that used to live directly on `ExecutorEngine`:

| Field | Purpose |
|---|---|
| `reth_env` | Handle to the Reth execution environment |
| `gas_accumulator` | Tracks per-worker gas usage and base fee across the epoch |
| `batch_tracker` | Optional tracker for batch lifecycle metrics |
| `executed_batch_registry` | `ExecutedBatchRegistry` dedup set; prevents double-execution |
| `batch_ordering` | `BatchOrdering<DB>` (`batch/ordering.rs:28`); enforces per-authority sequence ordering |
| `gas_limit` | Per-epoch gas limit propagated into each `RLPayload` |

### Deduplication / restart safety

On construction, `ExecutorEngine` calls `reconstruct_batch_digests`, which scans the
last `DIGEST_RECONSTRUCTION_DEPTH = 1000` canonical blocks and recovers previously
executed batch digests from the header fields:

```
batch_digest = header.mix_hash XOR header.parent_beacon_block_root
```

This allows the engine to skip batches that were already executed in a previous run
without requiring a separate persistence layer.

### `execute_consensus_output` (`execution/orchestrator.rs`)

The blocking task calls `execute_consensus_output`
([`execution/orchestrator.rs:75`](../../../crates/middleware/processor/src/execution/orchestrator.rs)
for the method, `:496` for the free function) with a `BuildArguments` struct:

- **Per-batch block** – each batch in the subDAG becomes a separate EVM block
  (distinct block environment, block number, timestamp, etc.).
- **Empty output** – an output with no batches produces a single empty EVM block to
  advance the chain.
- **Block reward** – the mining reward (`coinbase` credit) is applied only to the
  leader's block, not to worker batch blocks.
- After all blocks are sealed they are written to the canonical chain via `RethEnv`.

### Catch-up on epoch rejoin

When a node rejoins mid-epoch, `catchup_accumulator`
([`epoch_manager/utils.rs:20`](../../../crates/middleware/orchestrator/src/epoch_manager/utils.rs))
reconstructs the `GasAccumulator` and leader counts by replaying execution block headers from
the persisted chain before `ExecutorEngine` starts processing new outputs. This ensures that
per-epoch gas statistics and withdrawal roots remain consistent with a continuously running
node.
