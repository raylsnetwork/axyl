# Network command order on a fresh node

Commands a fresh node issues **per swarm** on first-epoch launch. The orchestrator
(epoch-manager) drives both the primary and worker swarms with the same shape.
Source: `crates/middleware/orchestrator/src/epoch_manager/primary.rs:86-155`
(worker is analogous in `worker.rs`). Commands are `NetworkCommand` values pushed
into the mpsc channel drained by `crates/consensus/network/src/consensus/runtime.rs::run()`.

| # | Command | Issued by | Purpose | Fires another command? |
|---|---------|-----------|---------|------------------------|
| 1 | `AddBootstrapPeers` | orchestrator | register committee bootstrap servers (bls → `P2pNode`) | **Yes** → if any addr is `/dnsaddr`, spawns off-loop `dnsaddr-relay-discovery` → `RegisterRelays` (command.rs:112) |
| 2 | `RegisterRelays` | the discovery task (async) | exempt resolved relay peer-ids from banning | No (terminal; on-loop, fire-and-forget) |
| 3 | `NewEpoch` | orchestrator | set the epoch's committee on the behaviour | No |
| 4 | `StartListening` (×N, **initial epoch only**) | orchestrator via `start_swarm_listeners` | bind the advertised address + one per relay reservation | No (reservation retries are timer-driven in `runtime.rs`, not commands) |
| 5 | `DialBls` (per other committee member) | orchestrator (`dial_peer_bls`) | connect to each committee peer by bls key | **Yes** → if the peer's addr is `/dnsaddr`, spawns `dial-resolve-dnsaddr` → `DialResolved` (command.rs:162) |
| 6 | `DialResolved` | the resolve task (async) | dial the peer on its resolved `/p2p-circuit` addrs | No (calls `peer_manager.dial_peer` internally) |
| 7 | `Subscribe` / `UpdateAuthorizedPublishers` | orchestrator (`subscribe_with_publishers`) | join the gossip topic + set authorized publishers | No |

## Command → command chains

Only two hops chain; everything else is terminal:

- `AddBootstrapPeers` → `RegisterRelays` (via off-loop DNS resolve)
- `DialBls` → `DialResolved` (via off-loop DNS resolve)

Both hops are deliberately async **off** the swarm loop — the DNS `txt_lookup` must
not block the loop, or relayed (yamux-over-circuit) connections get reset by peers
(they have no transport-level keep-alive). See the `RegisterRelays` doc and
`command.rs:152`.

## Background (non-startup) follow-on

When connected-peer count drops low, the peer manager emits `PeerEvent::Discovery`
→ kad query → `PeerEvent::MissingAuthorities` → internal redials. These are
peer-manager *events*, not `NetworkCommand`s, but they are the runtime's
self-healing dial path after startup.

## Notes

- Steps 1, 3, 5, 7 run every epoch; step 4 (`StartListening`) only on the initial
  epoch. For a fresh node (initial epoch) all of them fire.
- The command channel has one receiver per swarm (the `ConsensusNetwork` runtime
  loop); the sender is a cloneable `NetworkHandle`. The orchestrator sends the
  lifecycle commands above; consensus tasks (certifier, proposer, fetchers,
  state-sync on the primary; quorum-waiter, batch-builder/fetcher on the worker)
  later send the data-plane commands (`Publish`, `SendRequest*`, `SendResponse`).
