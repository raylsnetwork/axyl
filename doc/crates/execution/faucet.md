# Rayls Faucet (`faucet_*` namespace)

> **Warning:** the faucet must not be enabled on mainnet.

The `rayls-execution-faucet` crate
([`crates/execution/faucet/`](../../../crates/execution/faucet/)) adds a `faucet` JSON-RPC
namespace to the worker's RPC server. It transfers native RLS (and optionally stablecoins) to
requesting addresses, subject to a per-address rate limit.

## Architecture

```
HTTP client
    │
    ▼
FaucetRpcExt  (jsonrpsee server, faucet namespace)
    │   faucet_transfer(address, contract)
    ▼
Faucet        (async front-end, mpsc channel)
    │
    ▼
FaucetService (background task, rate limiting, tx construction)
    │
    ├─ LruCache (success_cache)   — address seen? reject until wait_period expires
    ├─ LruCache (pending_cache)   — short-lived dedup while tx reaches consensus (~10 s)
    │
    ├─ Google Cloud KMS           — remote signing (private key never in process memory)
    │
    └─ WorkerTxPool               — submit signed EIP-1559 tx directly to the pool
```

`FaucetRpcExt` ([`src/rpc_ext.rs`](../../../crates/execution/faucet/src/rpc_ext.rs)) is the thin
jsonrpsee server struct. It owns a `Faucet` handle that forwards requests over an async channel
(bounded at 1 024 entries to limit DoS exposure).

`FaucetService` ([`src/service.rs`](../../../crates/execution/faucet/src/service.rs)) is a
long-running `Future` spawned as a critical task on the node's task executor. It owns both LRU
caches and the wallet configuration, processes requests sequentially (to keep nonce tracking
simple), and spawns a sub-task per request for the KMS round-trip.

`FaucetArgs` ([`src/cli_ext.rs`](../../../crates/execution/faucet/src/cli_ext.rs)) extends the
node's CLI. All faucet configuration is passed on the command line or via environment variables.

## Endpoint

### `faucet_transfer`

Requests a token transfer from the faucet wallet to the given address.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `address` | `Address` | yes | Recipient address |
| `contract` | `Address \| null` | no | Stablecoin contract to drip. Pass `null` or `address(0)` to receive native RLS instead |

**Returns:** `TxHash` — the hash of the submitted transaction.

**Errors:** standard `eth` RPC error object. Common cases:

| Condition | Error |
|-----------|-------|
| Address received a drip within the wait period | `InvalidParams` with a message stating when the wait period ends |
| Address has a pending (unconfirmed) drip | `InvalidParams` indicating a drip is already pending |
| Faucet service task not running | `InvalidParams("faucet service unavailable")` |
| KMS signing failure | Internal error logged server-side; no response sent to client |

### Request flow

1. `FaucetRpcExt::transfer` sends `(address, contract, oneshot_tx)` over the bounded channel to
   `FaucetService`.
2. `FaucetService::poll`
   ([`service.rs:452-493`](../../../crates/execution/faucet/src/service.rs)) inspects the
   **pending cache** first. If an entry exists for `(address, contract)`, the wait remainder is
   reported back through the `oneshot_tx` and the request is dropped.
3. If the pending cache misses, `poll` then peeks the **success cache** (`peek` is used so the
   LRU timer is not refreshed). If found, the wait remainder is reported and the request is
   dropped.
4. On a miss in both caches, `poll` inserts a fresh entry into the **pending cache** and calls
   `process_transfer_request`. This function does no cache lookups — it is purely the
   sign + submit path:
   - `create_transaction_to_sign` builds an EIP-1559 transaction calling `drip(address, contract)`
     on the configured faucet contract (selector `0xeb3839a7`). The nonce is read from the pool
     (highest pending) or, if none, from the database.
   - A sub-task is spawned to send the `signature_hash` to Google Cloud KMS and await the
     DER-encoded signature. The signature is converted to Ethereum `(r, s, v)` form by trying
     both recovery IDs until the recovered address matches the wallet address.
   - The signed `TransactionSigned` is submitted to `WorkerTxPool`.
5. `WorkerTxPool` emits a `TransactionEvent::Mined` notification. On receipt,
   `(address, contract)` is forwarded to the **success cache** via a separate bounded channel
   (capacity 256). Only mined transactions enter the success cache — failed or dropped
   transactions stay in the short-lived pending cache and clear when it evicts.
6. `TxHash` is sent back through the oneshot channel to the waiting RPC handler and returned to
   the caller.

## Configuration

The faucet is configured entirely through `FaucetArgs`. Passing `--google-kms` activates it; if
the flag is absent the `faucet` RPC namespace is not registered.

| CLI flag | Env var | Default | Description |
|----------|---------|---------|-------------|
| `--wait-period` | — | `86400` (24 h) | Seconds a recipient must wait between drips |
| `--faucet-contract` | — | `0x0…0` | On-chain faucet contract managing drip amounts and enabled tokens |
| `--public-key` | `FAUCET_PUBLIC_KEY` | (test key) | Faucet wallet public key — hex or PEM format |
| `--google-kms` | — | off | Enable faucet and use Google KMS for signing |
| `--project-id` | `PROJECT_ID` | — | GCP project ID |
| `--key-locations` | `KMS_KEY_LOCATIONS` | — | KMS key location (e.g. `global`) |
| `--key-rings` | `KMS_KEY_RINGS` | — | KMS key ring name |
| `--crypto-keys` | `KMS_CRYPTO_KEYS` | — | KMS key name |
| `--crypto-key-versions` | `KMS_CRYPTO_KEY_VERSIONS` | — | KMS key version |

The full KMS key name passed to the API is:
```
projects/{project_id}/locations/{key_locations}/keyRings/{key_rings}/cryptoKeys/{crypto_keys}/cryptoKeyVersions/{crypto_key_versions}
```

## Signing via Google Cloud KMS

The faucet wallet private key is never loaded into the process. Instead, `FaucetService` sends the
transaction's `signature_hash` (a `keccak256` digest) to the KMS `AsymmetricSign` API, which
returns a DER-encoded secp256k1 signature. The service then:

1. Parses `(r, s)` from the DER bytes.
2. Normalises `s` to the lower half of the curve (required for Ethereum).
3. Tries recovery IDs 0 and 1 until `SECP256K1.recover_ecdsa(hash, sig)` returns a public key
   matching the configured wallet public key.
4. Computes `v` from the recovery ID using EIP-155: `v = recovery_id + chain_id * 2 + 35`.
5. Constructs an `EthSignature { r, s, y_odd_parity }` and attaches it to the transaction.
