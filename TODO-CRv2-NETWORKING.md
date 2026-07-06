# TODO / follow-ups — circuit-relay-v2 networking

## Faster relay failover (re-dial committee peers before the next epoch)

### Status
Not implemented — needs a design decision first. The `/dnsaddr` + dual-relay setup
(committed on `ba-circuit-relay-v2-poc`) already **fails over correctly**, but not quickly.

### Problem
Committee members are dialed only at **epoch start** (`dial_peer_bls` in
`crates/middleware/orchestrator/src/epoch_manager/{primary,worker}.rs` →
`network.rs::dial_peer_bls`). That task dials once and **exits on success**. So if a
peer's connection drops mid-epoch (e.g. its primary relay dies), nothing re-dials it
until the next epoch boundary. Epochs are ~120s, so worst-case failover latency is ~an
epoch. Reachability is fine (the node stays reservable via its backup relay); it's the
*reconnect* that's slow.

Verified: killing a validator's primary relay keeps consensus running (peers eventually
reconnect via the backup), but the reconnection is not prompt.

### Proposed approach (option "B" — not yet agreed)
Two complementary parts:

1. **Immediate re-dial on disconnect** (network layer,
   `crates/consensus/network/src/consensus/peer_events.rs`, `PeerEvent::PeerDisconnected`):
   if the dropped peer is a committee member (`bls` is `Some` *and*
   `peer_manager.peer_is_important(peer_id)`), spawn a task that sends
   `NetworkCommand::DialBls { bls_key }` back to the network. `DialBls` re-resolves
   `/dnsaddr` fresh, so it lands on whatever relay is currently live (the backup). Fast
   path; fires once per disconnect. `PeerCondition::NotDialing` makes it a no-op if
   something already reconnected. (Requires adding `NetworkCommand` to the module imports.)

2. **Periodic reconnect maintainer** (epoch layer, `network.rs::dial_peer_bls`): change
   the one-shot dial into a loop that keeps the peer connected for the whole epoch — dial,
   then on success re-check every ~15s and re-dial if the link dropped; on failure retry
   with capped backoff (don't give up — a committee member is needed for quorum). This is
   the robust backstop: if the immediate re-dial in (1) can't connect at that instant, the
   maintainer retries on its next tick instead of waiting for the epoch.
   - `dial_by_bls` is idempotent (returns `AlreadyConnected`/`AlreadyDialing` when the link
     is up), so a healthy peer just costs one cheap check per interval.
   - The task is spawned via the epoch task spawner as `TaskKind::Doomed` (shutdown-
     cancellable), so an infinite loop is safe: it's aborted at epoch end and respawned for
     the then-current committee. No accumulation across epochs. (Confirmed in
     `crates/infrastructure/types/src/task_manager.rs`.)

### Open questions to resolve before implementing
- Is the network layer (peer_events) the right place for (1), or should reconnect be
  driven entirely from the epoch/orchestrator layer where committee membership lives?
- Is the ~15s maintainer tick the right cadence? Should it derive from config instead of a
  constant?
- Interaction between (1) and (2): both can dial the same peer; confirm `NotDialing`
  dedup + `AlreadyConnected` fully prevent duplicate connections / churn under load.
- Optional optimization: guard `DialBls` to skip `/dnsaddr` re-resolution when already
  connected (avoids a per-tick DNS lookup for healthy peers). Needs a `pub(crate)`
  "is connected/dialing" accessor on `PeerManager` (currently `pub(super)`).

### Also deferred
- `add-relay-node.sh`: give an added node **2 relays + `/dnsaddr`** (currently single
  relay, concrete circuit). Requires the running dnsmasq to serve the new node's TXT
  records → switch `local-testnet.sh`'s dnsmasq to a reloadable conf file + SIGHUP so the
  add script can append records. Only meaningful for a node that peers actually dial (a
  future committee validator); an observer is dial-out-only and doesn't need it.
