# Rayls RPC (`rayls_*` namespace)

The `rayls-execution-rpc` crate
([`crates/execution/rpc/`](../../../crates/execution/rpc/)) adds a `rayls` JSON-RPC namespace
alongside the standard `eth_*` endpoints. It exposes Rayls-specific data — consensus headers,
epoch records, and the chain genesis — that is not covered by the Ethereum JSON-RPC API.

## Architecture

The crate defines an `EngineToPrimary` trait
([`src/lib.rs`](../../../crates/execution/rpc/src/lib.rs)) through which the RPC handlers reach
back into the execution engine to fetch consensus state without taking a direct dependency on the
consensus crates. The concrete implementation is provided by the middleware layer when the node
is assembled.

`RaylsNetworkRpcExt` ([`src/rpc_ext.rs`](../../../crates/execution/rpc/src/rpc_ext.rs)) is the
jsonrpsee server struct that implements the trait. It holds a `ChainSpec` (for genesis) and an
`N: EngineToPrimary` (for live consensus data).

All methods are async and return `RaylsNetworkRpcResult<T>`. The public error type is
`RaylsRpcError` ([`rpc/src/error.rs:13-19`](../../../crates/execution/rpc/src/error.rs)) with two
variants:

- `RaylsRpcError::NotFound` — the requested item does not exist (HTTP-style code 401).
- `RaylsRpcError::InvalidProofOfPossession` — returned by the handshake path when a client
  provides an invalid signature for the network key or genesis (HTTP-style code 401).

## Endpoints

### `rayls_latestHeader`

Returns the `ConsensusHeader` at the tip of the consensus chain: the highest header this node
has saved to its consensus database, on every role. Served from memory; the node publishes the
tip as it saves each header and re-seeds it from the canonical chain tip at every epoch start.

**Parameters:** none

**Returns:** `ConsensusHeader`

| Field | Type | Description |
|-------|------|-------------|
| `parent_hash` | `B256` | Keccak-256 hash of the previous `ConsensusHeader` |
| `sub_dag` | `CommittedSubDag` | The committed sub-dag that was used to extend the execution chain for this block |
| `number` | `u64` | Consensus chain block number (genesis = 0) |
| `extra` | `B256` | Reserved extra-data field (currently unused) |

The `ConsensusHeader` digest is `keccak256(parent_hash ‖ sub_dag.digest() ‖ number)` and is
stored in the `parent_beacon_block_root` field of every EVM block produced from this header.

> **Note:** identifiers inside the header (authority ids, BLS keys, digests) are base58
> strings in JSON and raw bytes in the binary (BCS/bincode) encodings the network and the
> database use.

---

### `rayls_nodeStatus`

Returns a snapshot of the local node's role and sync state. Every field describes **this** node;
nothing here is a network-wide view.

**Parameters:** none

**Returns:** `NodeStatus`

| Field | Type | Description |
|-------|------|-------------|
| `role` | `"active_cvv" \| "inactive_cvv" \| "observer"` | This node's role in the current committee |
| `is_caught_up` | `bool` | Whether the node has finished catching up (see below) |
| `epoch` | `u32` | The consensus epoch this node is operating in |
| `committed_round` | `u32` | Last round committed by the local DAG |
| `primary_round` | `u32` | Current primary round of the local DAG |
| `gc_round` | `u32` | Garbage-collection round boundary of the local DAG |
| `last_canonical_block` | `u64` | Number of the latest executed (EVM) block |

`is_caught_up` is decided per role: an `active_cvv` is caught up by construction, because the
promotion gate is what made it active; an `inactive_cvv` is catching up by definition, so it is
always `false`; an `observer` is caught up once its highest canonical consensus header reaches the
highest header gossiped by the network. An observer that has *seen* a header but not yet processed
it is not caught up.

`epoch` is the leader epoch of the same saved tip header that `rayls_latestHeader` returns, so the
two methods always agree and a syncing node reports the epoch it has actually reached, not the
network's.

One consequence worth knowing before alerting on this field: through an epoch transition the tip
header stays in the closing epoch until the new epoch's first commit lands, a few rounds, while
`committed_round`, `primary_round` and `gc_round` come from in-memory state that resets at the
boundary. A single response can therefore pair the old `epoch` with new-epoch rounds for a few
seconds.

---

### `rayls_genesis`

Returns the chain genesis configuration.

**Parameters:** none

**Returns:** `Genesis` — standard Alloy genesis type (chain id, alloc, config, etc.)

---

### `rayls_epochRecord`

Returns the `EpochRecord` and `EpochCertificate` for a given epoch number.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `epoch` | `u64` | The epoch number to look up |

**Returns:** `(EpochRecord, EpochCertificate)` — see type descriptions below.

**Errors:** `NotFound` if no record exists for the requested epoch.

---

### `rayls_epochRecordByHash`

Returns the `EpochRecord` and `EpochCertificate` identified by the hash of the record.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `hash` | `B256` | `EpochRecord` digest (`keccak256(encode(record))`) |

**Returns:** `(EpochRecord, EpochCertificate)` — see type descriptions below.

**Errors:** `NotFound` if no record matches the hash.

---

## Return types

### `EpochRecord`

An `EpochRecord` is produced at the start of each epoch and signed by the outgoing committee. It
acts as a signed checkpoint that allows a node to:

- Determine the active committee for any epoch without replaying execution state.
- Trustlessly sync the consensus chain (with the paired `EpochCertificate` as proof).

| Field | Type | Description |
|-------|------|-------------|
| `epoch` | `u64` | The epoch this record covers |
| `committee` | `Vec<BlsPublicKey>` | BLS public keys of the active committee for this epoch (sorted) |
| `next_committee` | `Vec<BlsPublicKey>` | BLS public keys of the committee for the following epoch (sorted) |
| `parent_hash` | `B256` | Hash of the previous `EpochRecord`, forming a chain |
| `parent_state` | `BlockNumHash` | Block number and hash of the last execution block of this epoch — the execution genesis for the next epoch |
| `parent_consensus` | `B256` | Hash of the last `ConsensusHeader` of this epoch — a signed consensus checkpoint |

### `EpochCertificate`

An `EpochCertificate` is an aggregate BLS signature over an `EpochRecord` produced by a quorum
(≥ ⌊2n/3⌋ + 1) of the outgoing committee. It proves that the paired `EpochRecord` is
authoritative.

| Field | Type | Description |
|-------|------|-------------|
| `epoch_hash` | `B256` | Digest of the `EpochRecord` being certified |
| `signature` | `BlsSignature` | Aggregate BLS signature over `IntentMessage(EpochBoundary, epoch_hash)` |
| `signed_authorities` | `RoaringBitmap` | Bitmap indicating which committee members contributed to the aggregate signature |

To verify: reconstruct `aggregate = BlsAggregateSignature::from_signature(cert.signature)` then
call `record.verify_with_cert(&cert)`, which checks the digest matches and that the bitmap
references a super-quorum of the committee.
