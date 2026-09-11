# Networking state machines, events, and the heartbeat

How the consensus networking layer fits together: one central loop feeding two
per-peer state machines, with the heartbeat as the clock that supplies every
time-based transition the event stream can't.

Source anchors:
- `crates/consensus/network/src/consensus/runtime.rs` (central loop)
- `crates/consensus/network/src/consensus/command.rs` (process_command)
- `crates/consensus/network/src/consensus/peer_events.rs` (PeerEvent handling)
- `crates/consensus/network/src/peers/manager.rs` (peer manager + heartbeat)
- `crates/consensus/network/src/peers/status.rs` (ConnectionStatus FSM)
- `crates/consensus/network/src/peers/score.rs` (Reputation/Score FSM)

## The central loop (runtime.rs::run)

```
   one per swarm — primary & worker run the same path:

                         ┌─────────────────────────────────────────┐
                         │        ConsensusNetwork::run loop        │
                         │              tokio::select!              │
   orchestrator ───────► │  1. commands.recv()  → process_command   │  "do X"      (intent)
   consensus tasks ────► │                                          │
                         │  2. swarm event      → process_event ────┼─► 2.1 peer_manager.poll() (see 2.1 below)
   libp2p swarm ───────► │                                          │  "X happened"(reality)
                         │                                          │
   15s timer ──────────► │  3. relay_retry.tick → re-reserve relays │  liveness
                         └─────────────────────────────────────────┘

   2.1 peer_manager.poll() runs, in order:
     1. heartbeat / penalties     ─push─► self.events
     2. drain self.events         ─► GenerateEvent
            └► runtime re-issues some of these as commands (→ arm 1):
               RedialCommittee → DialBls,  MissingAuthorities → kad
     3. drain self.dial_requests  ─► ToSwarm::Dial
```

One receiver per swarm (primary + worker). The sender is a cloneable `NetworkHandle`.

## Two planes feed the peer state

- **Command plane (intent):** orchestrator + consensus tasks push `NetworkCommand`s
  (`AddBootstrapPeers`, `DialBls`, `StartListening`, `Publish`, `Subscribe`, ...).
- **Event plane (reality):** libp2p emits `ConnectionEstablished/Closed`, dial
  failures, gossip, identify. The peer manager turns these into:
  - `PeerAction`: `Ban` / `Disconnect` / `DisconnectWithPX` / `Unban` / `NoAction`
  - `PeerEvent`: `PeerConnected/Disconnected`, `Banned/Unbanned`,
    `MissingAuthorities`, `Discovery`, `RedialCommittee`

## Per-peer state machine 1: ConnectionStatus (status.rs)

```
        Unknown
          │ dial issued
          ▼
        Dialing ──────────── dial failed / timeout ─────────► Disconnected
          │ ConnectionEstablished                                 ▲
          ▼                                                        │
        Connected ── excess / penalized / closed ──► Disconnecting ┘
          │                                                        
          │ score ≤ ban threshold (reputation verdict)            
          ▼                                                        
        Banned ──── score decays back above threshold ──► Unbanned → Disconnected
```

Inputs are `NewConnectionStatus` values, driven by swarm events (establish/close)
AND by reputation verdicts. Disconnects carry a reason: `ExcessPeers` / `Penalized` / `Banned`.

## Per-peer state machine 2: Reputation / Score (score.rs)

```
   penalties (EVENT-driven, push down)        decay (TIME-driven, pull toward 0)
   Mild -1 / Medium -5 / Severe -10 / Fatal        rayls_score *= 0.5^(elapsed/halflife)
            │                                             │
            ▼                                             ▼
        aggregate_score ─────────────► Reputation:
            > disconnect threshold  → Trusted
            ≤ disconnect threshold  → Disconnected
            ≤ ban threshold         → Banned
```

The asymmetry is the whole point: **penalties are event-driven** (something arrived),
**decay is time-driven** (nothing arrived). No event ever says "enough time passed to
forgive this peer" — that transition can only come from a periodic sweep.

## The heartbeat: the clock that closes the gaps (manager.rs::heartbeat, ~30s default)

The command/event planes only react to discrete arrivals. But the important failure
modes produce SILENCE, not events: a dead relay, a partitioned peer, a ban that should
have decayed. "Nothing happened for a while" is itself a condition needing action, and
there is no event for it. Each heartbeat sweeps and supplies those transitions:

| Heartbeat step | Supplies the transition that has no event |
|---|---|
| `heartbeat_maintenance` | recompute decayed scores → `Unban` when a banned peer recovers (the ONLY un-ban path for a score-ban) |
| `unban_temp_banned_peers` | expire excess-peer temporary bans (TTL lapsed) |
| `redial_missing_committee` | re-dial every committee member not connected/dialing → heal mid-epoch without waiting for an event or the epoch boundary |
| `prune_connected_peers` | enforce connection limits |
| `discovery_heartbeat` | seed kad queries when peer counts are low |

`redial_missing_committee` is validator-only (`is_peer_validator(local_peer_id)`), one
attempt per member per heartbeat, and committee members are ban-exempt so retries never
escalate. It routes through `RedialCommittee → DialBls`, which re-resolves `/dnsaddr`
at dial time — so a recovered relay/DNS is picked up on the next heartbeat.

## Other timer-driven complements (not the heartbeat)

- **relay-retry (15s, runtime.rs):** re-`listen_on` a lost reservation so a returning
  relay restores inbound reachability.
- **per-dial backoff (`dial_peer_bls`, orchestrator):** exponential retry of one dial
  (1→2→...→120s cap), re-resolving `/dnsaddr` each attempt; gives up only if
  `retries > 10 && peers > 0` (a 0-peer node retries forever).
- **epoch boundary (`NewEpoch`):** coarse re-sync — re-runs `AddBootstrapPeers` +
  the committee dial loop + `Subscribe`.
- **cleanup (1000 events / 10s, runtime.rs):** request-map GC.

## The unifying idea

**Event-driven for immediacy, timer-driven for liveness.** Commands + swarm events
handle instantaneous transitions the moment something arrives; the heartbeat and the
other timers guarantee the system keeps decaying state and re-attempting connectivity
even when nothing arrives — because a dead relay, a decayed ban, and a healed-but-not-
yet-redialed committee member all look like silence to the event plane. The heartbeat
converts the passage of time and the absence of connections into concrete `PeerAction`s
and `RedialCommittee`/`Unban` events.

See also: `startup-command-order.md` (the command sequence on a fresh node).
