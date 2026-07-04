# Glossary

This page exists because a term can mean different things in different parts of Axyl —
a word used casually in a design doc, a Rust crate name, and a Solidity concept can all be
spelled the same way and mean three unrelated things. That ambiguity is expensive: it showed
up during the networking-layer redesign discussion, where "networking layer" was read as both
"the `crates/consensus/network` crate" and "the whole node" depending on who was talking.

This is **not** a place to redefine every identifier in the codebase — code is the source of
truth for that, and a glossary that tries to mirror it will rot immediately. It exists only for
terms that are **overloaded across spheres** (code / on-chain / networking / operations /
product) badly enough that two people can use the same word and mean different things.

## How to use this page

1. About to write "the X layer" / "X service" / "the X" in a design doc, PR description, or
   Slack thread, and X could plausibly mean more than one thing? Check here first.
2. If X is listed below, use the **canonical name** column instead of the bare word.
3. If X *isn't* listed but you just got confused (or confused someone else) about which X was
   meant, add it — see [Adding a term](#adding-a-term).

## Ambiguous terms

Each entry lists every sense the term is actually used in across this repo, tagged by sphere,
with a suggested canonical name to say/write instead of the bare term.

### "Network" / "Networking layer"

The single word "network" resolves to at least four different things in this codebase alone:

| # | Sense | Sphere | Canonical name | Where it lives |
|---|---|---|---|---|
| 1 | The entire node binary (consensus + execution + infra, wired together) | Code / ops | **the node** / `rayls-network` (binary) | [`bin/rayls-network`](../bin/rayls-network), [doc](crates/rayls-network.md) |
| 2 | The libp2p transport used by consensus (gossipsub, request/response, Kademlia discovery, peer scoring) | Code (consensus) | **consensus network layer** / `rayls-consensus-network` | [`crates/consensus/network`](../crates/consensus/network), [doc](crates/consensus/network.md) |
| 3 | Shared Worker↔Primary message traits and mocks (no transport logic) | Code (infra) | **network message types** / `rayls-infrastructure-network-types` | [`crates/infrastructure/network-types`](../crates/infrastructure/network-types) |
| 4 | CLI commands (`node`, `genesis`, `keytool`) used to run/configure a node | Code (infra) / ops | **network CLI** / `rayls-network-cli` | [`crates/infrastructure/network-cli`](../crates/infrastructure/network-cli) |

**Don't confuse with:** when someone says "the networking layer" in a design discussion, ask
whether they mean sense 2 (the actual libp2p transport, which is what most redesign
conversations are about) or sense 1 (the whole node, which is how it's colloquially used outside
the consensus team).

### "Whitelist" / "Allowlist"

At least three unrelated allow-lists exist, gating different things at different layers:

| # | Sense | Sphere | Canonical name | Where it lives |
|---|---|---|---|---|
| 1 | On-chain admission of a validator address so it can stake/activate | On-chain (Solidity) | **validator allowlist** (`ConsensusRegistry.allowlistValidator`) | [`etc/validator/README.md`](../etc/validator/README.md), `rayls-contracts` |
| 2 | Access control on who may mint/burn a native ERC20 stablecoin | Execution / on-chain semantics | **mint/burn whitelist** (`check_whitelist`, `MINTING_MODULE_ADDRESS`) | [`crates/execution/evm/src/native_erc20`](../crates/execution/evm/src/native_erc20/precompile.rs) |
| 3 | Per-topic set of peers a consensus node will accept gossip from (`update_authorized_publishers`) | Networking (consensus) | **authorized publishers** | [`crates/consensus/network`](../crates/consensus/network/src/tests/network_tests.rs) |

**Don't confuse with:** "allowlist the validator" (on-chain admission, sense 1) is a completely
different operation from "whitelisted for mint/burn" (sense 2) or a peer being removed from
gossip's authorized-publisher set (sense 3). The repo already uses "allowlist" for sense 1 and
"whitelist" for senses 2–3 — worth deciding whether to standardize on one spelling repo-wide.

### Validator / Observer / Node

"Node" is the generic term — any running `rayls-network` binary. Validator and Observer are the
two roles a node can have, and the distinction is about committee membership, not about running
different software:

| Term | Definition | Source |
|---|---|---|
| **Node** | Any running instance of the node binary (see "Network" sense 1 above), validator or observer. | — |
| **Validator** | A node whose authority has been admitted on-chain (`ConsensusRegistry.allowlistValidator` → stake → activate) and can sign/vote on consensus — i.e. a CVV, at least while its `NodeMode` is `CvvActive` (see below). | [`etc/validator/README.md`](../etc/validator/README.md) |
| **Observer** | A node that syncs consensus and execution but never signs or votes, and exposes the same RPC surface as a validator. Per `NodeMode::Observer`: "follower not in the committee (**staked or unstaked**)" — an observer can hold stake without being in the committee; what makes it an Observer is not being admitted, not its stake. | [`crates/consensus/primary/src/consensus_bus.rs:151`](../crates/consensus/primary/src/consensus_bus.rs), [`etc/observer/README.md`](../etc/observer/README.md) |

**Don't confuse with:** a validator that's temporarily catching up (`NodeMode::CvvInactive`, see
below) is still a Validator — it's just not currently signing/voting. It is not an Observer; the
role (admitted to the committee) hasn't changed, only its current sync/voting state.

### Round

`pub type Round = u32` — the DAG round number that Primary/Bullshark consensus advances through.
Primaries propose one header per round and need 2f+1 certificates to advance; leaders are elected
on even rounds only.

- Type: [`crates/infrastructure/types/src/primary/mod.rs:38`](../crates/infrastructure/types/src/primary/mod.rs)
- Leader election on even rounds: [`crates/consensus/primary/src/consensus/bullshark.rs:133`](../crates/consensus/primary/src/consensus/bullshark.rs)
- Encoded as the **low** 32 bits of the EVM block `nonce` field (epoch is the high 32 bits) —
  see the header-field-encoding table in [`index.md`](index.md).

### Epoch

`pub type Epoch = u32` — the period during which the committee (the set of validators authorized
to sign/vote) is fixed. Epoch boundaries are marked by a signed `EpochRecord` plus an aggregate
BLS `EpochCertificate` (the trust anchor light/syncing nodes bootstrap from), and closed on-chain
by a `ConsensusRegistry.concludeEpoch` system call that applies incentives and rotates the
committee.

- Type: [`crates/infrastructure/types/src/committee.rs:19`](../crates/infrastructure/types/src/committee.rs)
- Trust-anchor detail: [`index.md`](index.md#rpc-interface), [`../SYNC.md`](../SYNC.md)
- Encoded as the **high** 32 bits of the EVM block `nonce` field (round is the low 32 bits).

Unlike Network/Whitelist above, this isn't a case of two competing meanings — consensus code and
the on-chain registry mean the same thing by "epoch." It was flagged as a candidate purely
because it gets discussed in both off-chain and on-chain language, and it's easy to transpose
with Round (remember: epoch = high bits, round = low bits).

### Batch

`pub struct Batch` — a Worker-sealed list of raw transaction bytes (`transactions: Vec<Vec<u8>>`)
plus the `epoch` it belongs to, `beneficiary`, `base_fee_per_gas`, `worker_id`, and a per-worker
monotonic `seq`. Hashing a `Batch` produces a `SealedBatch` (`Batch::seal_slow`); `BatchBuilder`
in `crates/consensus/worker` is what assembles and seals one. One committed sub-DAG maps to one
EVM block per batch it contains (see [`index.md`](index.md)).

- Struct: [`crates/infrastructure/types/src/worker/sealed_batch.rs:63`](../crates/infrastructure/types/src/worker/sealed_batch.rs)

**Don't confuse with:** casually calling a group of RPC calls or DB writes "a batch" — unrelated
to the consensus `Batch` type.

### Bridge

`crates/middleware/bridge` (package `rayls-middleware-bridge`). Its `Subscriber` receives
certificates sequenced by consensus, waits until every batch they reference has been downloaded,
dedups, and forwards each `ConsensusOutput` to the execution engine (`ExecutorEngine`) over an
mpsc channel. Purely an internal consensus→execution handoff — nothing cross-chain.

- [`crates/middleware/bridge/src/subscriber.rs:43`](../crates/middleware/bridge/src/subscriber.rs)

**Don't confuse with:** any cross-chain/interop "bridge" (moving assets or messages between two
chains). If a design doc means that, say "interop bridge" or name the actual mechanism — this
crate has nothing to do with it.

### CVV states (`NodeMode`)

CVV = **Committee-voting Validator**. `NodeMode` is the enum that tracks whether a node is
currently admitted to the committee and, if so, whether it's actually voting right now:

| `NodeMode` | Meaning | `is_cvv()` | `is_active_cvv()` |
|---|---|---|---|
| `CvvActive` | Full CVV actively voting in the current committee. | ✓ | ✓ |
| `CvvInactive` | Staked CVV **catching up** — allowed to sync past the GC window so it can rejoin. Still a validator, just not currently signing/voting. | ✓ | ✗ |
| `Observer` | Follower not in the committee (staked or unstaked). Never votes. | ✗ | ✗ |

- Enum + doc comments: [`crates/consensus/primary/src/consensus_bus.rs:146`](../crates/consensus/primary/src/consensus_bus.rs)
- `is_batch_producing()` is `CvvActive | Observer` — deliberately **excludes** `CvvInactive`: a
  catching-up node must not run the batch builder, or a sealed batch wedges the worker on
  `report_own_batch` with no proposer draining it, stalling epoch-transition drain.

**Verified:** this confirms the read that CVV-active means admitted to the committee and
currently signing/voting, and CVV-inactive means still staked/admitted but in a catch-up state —
not yet back to signing/voting. The precise trigger for `CvvInactive` is "syncing past the GC
window to rejoin," not sync status in general.

**Don't confuse with:** `NVV` appears only as informal test-variable shorthand in
[`crates/consensus/network/src/tests/network_tests.rs`](../crates/consensus/network/src/tests/network_tests.rs)
(paired with `cvv`), likely "non-voting validator" — it is **not** a `NodeMode` variant. Worth
confirming with whoever wrote those tests before treating it as canonical.

## Candidate terms — not yet fleshed out

These come up often enough, and touch enough spheres (consensus code, on-chain contracts,
operator-facing docs), that they're likely candidates for the next entries. Flagging them here
rather than guessing at definitions — whoever resolves the ambiguity should fill in the row.

- **Committee** — on-chain `ConsensusRegistry` concept vs. the in-memory committee consensus
  code operates over per round.

## Adding a term

A term belongs here when **two people, in the same conversation, used the same word to mean
different things** — not because it's jargon or unfamiliar to newcomers (that's what per-crate
docs under [`doc/crates/`](crates/index.md) are for).

1. Add a row (or a new `###` section for a new term) following the table format above:
   sense, sphere, canonical name, and a link to where that sense actually lives in the repo.
2. Link real code/docs, not a description you're inventing — if you can't point at the file,
   it's not grounded enough to disambiguate anything.
3. If the ambiguity is bad enough that the repo uses inconsistent spelling/naming for the *same*
   sense (like "whitelist" vs. "allowlist" above), say so explicitly rather than silently picking
   one — that's a signal for a future rename, not something this page should paper over.
4. Prefer the canonical name in new design docs, PR descriptions, and code comments once a term
   has an entry here.
