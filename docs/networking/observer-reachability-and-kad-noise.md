# Observer reachability, kad dial noise, and the promotion hazard

Notes from diagnosing "failed to dial" churn in a mixed setup, and why there is no
clean static rule for which addresses a node should advertise. The core lesson:
**reachability is per-vantage (relative to the dialer), not a global property** — so
every static publish/mode rule breaks at least one legitimate case.

## The setup that surfaced it

- **hostA:** 4 validators + 1 observer + their relays (all co-located, mutually
  reachable on `127.0.0.1`).
- **hostB:** relay-node-6 (a dynamically-added, relay-fronted validator).

Symptom: on hostB, `relay-node-6.log` shows repeated

```
failed to dial peer  peer_id=<observer primary/worker>
  error=Transport([(/ip4/127.0.0.1/udp/59591/quic-v1/p2p/<observer>, Timeout)])
```

## What is actually happening

- The observer's single configured **`external_addr`** is a loopback address
  (`/ip4/127.0.0.1/...`). This node publishes **exactly one address** — that
  `external_addr` — as a custom `NodeRecord` value keyed on its BLS pubkey (it does
  **not** enumerate libp2p listen addresses into the record; see "What actually gets
  published" below). It runs kademlia in **Server** mode (forced for all nodes in
  `runtime.rs`), so that record (carrying `127.0.0.1`) is put into the DHT and provided.
- node-6 (hostB) learns that record via the DHT and **kad dials** it for routing
  maintenance — kad-initiated dials are explicitly allowed (`handle_pending_outbound_connection`),
  bypassing the committee-only dial gating (`redial_missing_committee`, the epoch dial loop).
- From hostB, `127.0.0.1` is **hostB's own loopback**, not the observer → perpetual timeout.
- The **4 validators do NOT show this** because node-6 reaches them via their **relay
  circuits** (from `committee.yaml`/`/dnsaddr`) and **stays connected**, so kad reuses the
  existing connection and never falls back to their `127.0.0.1`. The observer is the
  outlier: node-6 can **never** connect to it, so kad keeps retrying its only (loopback)
  address.

Why the timing looks random: it's **kad routing maintenance** (random-target bucket
refresh `get_closest_peers(PeerId::random())`, jittered timers, discovery bursts), not
the fixed-cadence `redial_missing_committee`.

## What actually gets published (only one address)

This repo does **not** publish libp2p listen addresses to kad. It publishes a **custom
application record** — `NodeRecord::build(network_pubkey, external_addr, …)`
(`constructor.rs`) — stored under the node's BLS pubkey via `put_record` /
`start_providing` (`kad.rs::provide_our_data`). That record carries **exactly one**
address: the single `external_addr` passed into the constructor. `swarm.add_external_address`
also registers only that one.

Consequence: **no matter how many addresses a node listens on, it publishes exactly one.**
If `external_addr` is `/dnsaddr/<relay-name>/p2p/<peer>`, that single dnsaddr (→ relay
circuit) is the only thing published; listing extra listen addresses (`127.0.0.1`,
`10.10.0.10`, a routable IP) does **not** add them to the record. The whole reachability
question therefore reduces to **"what is `external_addr` set to"**, not "which listen
addresses leak."

The one other address a peer can learn is the address a connection was actually
**established on** (kad stores it in its routing table) — but that is reachable by
definition, so it is not a loopback-noise source.

## The two harms (they need different fixes)

1. **kad dial churn** — the node published an unreachable address that others dial and
   fail. A network-layer concern.
2. **Unreachability → quorum/liveness** — if such a node is *staked/promoted*, the
   committee **counts** it but **can't reach** it (the "counted-but-unreachable member",
   zero-fault-tolerance wedge). Not fixable at the network layer — it's counted on-chain.

## The promotion hazard

Someone can start a node **without `--observer`** (a would-be validator). Unstaked, it's
effectively an observer and needs no relay. If it's then **staked**, it's promoted to a
counted committee member — but it may be **relay-less and behind NAT**, having already
published an **internal** `external_addr` to the DHT. Result: a counted member that is
unreachable cross-host and whose one published address is undialable. Staking is on-chain
and **decoupled** from network setup, so nothing stops this.

## Why there is no clean static rule (per-vantage reachability)

A node publishes one `external_addr`, but the *right* value for it depends on who dials —
the same address can be simultaneously good and bad:

- **Uniform local:** a `127.0.0.1` `external_addr` is reachable by everyone → publish direct, no relays.
- **Uniform behind relays:** publish the relay circuit (`/dnsaddr/.../p2p-circuit`).
- **Mixed (this setup):** a validator's `127.0.0.1` is reachable *from hostA* and not
  *from hostB*. No single choice of `external_addr` is correct for all dialers, and even
  AutoNAT gives a *global* verdict, which can't express "reachable from hostA, not hostB."

## Approaches and tradeoffs

| Approach | What it solves | What it breaks / misses | Verdict |
|---|---|---|---|
| `LISTEN_HOST=<routable-ip>` (sets `external_addr` to a routable IP) | Publishes one routable address instead of loopback, if that IP reaches all dialers | Useless behind NAT; recurs on promotion; can't express per-vantage | Deployment band-aid |
| kad `Client` while Observer | The pure-observer noise fully — never published → never dialed; zero cost; safe for local | Keys on role, not reachability → NAT'd *validator* (Server) still leaks internal; promotion recurs | Good for observers only |
| Reject a loopback/private `external_addr` | Would stop an internal address being the published one | Breaks pure-local (there `127.0.0.1` is the *correct* `external_addr`); "internal" isn't per-vantage | Rejected |
| `PeerCondition::Disconnected` (skip if connected) | Redundant dials to already-connected peers (dual-relay 2nd circuit) | No help for the observer (never connected); slightly cuts relay failover | Minor; part of the model below |
| Observer holds connections to all | Masks others' dial-backs while connected (small setup) | Doesn't fix the cause; fragile; O(observers×committee) load; pesters committee | Stopgap for tiny local only |
| AutoNAT + kad `auto` (verified reachability) | In a flat public internet: advertise/serve only confirmed-reachable addrs | Skips loopback/private by default; oscillates on conflicting per-vantage answers; mis-verdicts relayed nodes as "Private" (doesn't test circuits) → DHT collapses to all-Client. Assumes global reachability + global IPs | Doesn't fit this topology |
| `external_addr` = the relay circuit | Server nodes publish nothing internal; reachable *if the relay is reachable* | Breaks pure-local-direct (no relays → no circuit to publish); relay can itself be NAT'd; recursion bottoms at relays | Only for uniformly-relayed deployments w/ public relays |
| Choose `external_addr` per deployment (the current model) | Each deployment publishes the one address that works from its dialers' vantage (direct IP local; circuit behind relays) | Only one address, so no built-in fallback: if it's wrong for a dialer, that peer can't reach this node (the observer case); relies on the operator picking correctly | The realistic model; keep it |
| Registry / counting reachability gate | The counted-but-unreachable quorum hazard from staking a relay-less/NAT'd node | Architectural (on-chain address / attestation / reachability check before counting) | The real fix for the promotion hazard |

## Synthesis / recommendations

- There is **no static publish/mode rule** right for all cases — reachability is relative
  to the dialer, and a single published `external_addr` can't express that.
- **Baseline model: one `external_addr`, chosen per deployment.** Each node publishes exactly
  one address (direct IP for pure-local; a relay circuit where one exists). There is no
  multi-address "advertise everything and let peers pick" — so the operator must pick the
  value that works from its dialers' vantage. A wrong choice = that node is unreachable for
  the affected dialers (the observer's loopback case), which shows up as their repeated dial
  failures. Mitigated by **skip-if-connected** and by keeping unreachable-only nodes (pure
  observers) out of the DHT entirely.
- **One clean win: kad `Client` for pure observers.** They have only an out-of-scope
  address, no fallback, and nobody needs to reach them — so keeping them out of the DHT
  removes their churn without touching anything else. It does **not** fix NAT'd validators
  (role ≠ reachability).
- **AutoNAT is out** for this network — it assumes global reachability and global IPs;
  this topology has neither.
- The **promotion hazard is not a publishing problem** — a staked, relay-less, NAT'd node
  is genuinely unreachable cross-host no matter what it advertises. Durable fixes are
  **operational** (require a relay/routable path before staking — the `add-relay-node`
  model) and **architectural** (gate *counting* on established reachability). The network
  layer's job is only to **not turn that misconfig into dial churn**.
- **Relays are the reachability anchors.** The reachability requirement doesn't disappear;
  it concentrates onto a small set of operator-run, publicly-reachable relays so everything
  else can sit behind NAT. Push NAT all the way down and there is nothing to anchor on —
  that's the irreducible core of relay-based NAT traversal, not a bug to code around.

## Recommended solution: flag-driven kad mode (operator-declared reachability)

Since a node **cannot reliably auto-detect its own reachability** (it is per-vantage, and
AutoNAT does not fit this topology), make reachability an **explicit operator declaration**
via the existing `--observer` flag, and gate kad mode + address advertisement on it. This
turns the "kad `Client` while Observer" idea into a complete, static contract:

| Deployment | kad mode | Advertises? (`provide` + routing) | Promotable? | Meaning |
|---|---|---|---|---|
| **`--observer`** | `Client` | No (skip `provide`) | No (sticky Observer) | "I'm a follower, not reachable — don't route to me" |
| **no flag** | `Server` | Yes | Yes (on staking) | "I'm a full, reachable participant" (operator's responsibility) |
| **no flag + relay** (relay branch) | `Server` | Yes (circuit) | Yes | reachable via relay; operator ensures relay-or-node reachable |

Why this works:
- **Mode is static per flag — no dynamic switching.** `--observer` is a sticky Observer
  that is never promoted, so it is `Client` forever; a no-flag node is `Server` from the
  start (advertising even while unstaked), so promotion on staking needs no mode flip.
- **Reachable read-replicas are still supported** — deploy **without** `--observer` and just
  do not stake it: it is a `Server` (discoverable, serves batches/records, offloads the
  committee) that never votes.
- A `Client` `--observer` is not "cannot serve" — it can still answer batch/req-res requests
  over connections it is **already on** (req/res is not kad); it just is not *discoverable*
  by unconnected peers. That is the intended "not a discoverable read-replica" semantics.

**Scope: this fixes observers, not validators.** A flag can only describe the node it runs
on, and a validator is `Server` **by necessity** — you cannot tell a counted committee member
"don't be routable," because being routable is its job. So a NAT'd validator (no `--observer`,
staked) still advertises its internal addresses and still produces the same dial churn +
counted-but-unreachable hazard. The `--observer` flag cleanly wins only for the observer,
where "not reachable, don't route to me, never promoted" are all true at once.

What it does not fix by itself (the flag is a contract, not enforcement):
- A **no-flag node the operator wrongly believes is reachable** (actually NAT'd) still
  advertises undialable addresses → churn, and → counted-but-unreachable if staked. This is
  the **validator case above** — no kad-mode flag can address it. Back it with two guardrails:
  - a **loud startup log** on no-flag nodes ("running as a reachable Server — ensure this
    node is dialable; if it is not, use `--observer`"), and
  - the **counting gate** (do not seat a staked member until reachability is established) as
    the real backstop for the quorum hazard — no flag can *prevent* staking a mis-declared node.

Implementation: gate both `kademlia.set_mode(...)` **and** `provide_our_data()` on
`observer_flag` (`Client` + no-provide when set; `Server` + provide otherwise), instead of
forcing `Server` unconditionally in `runtime.rs`. The only plumbing is getting `observer_flag`
(already known to the orchestrator) down to the `ConsensusNetwork`.

## DCUtR — why it does not solve this

DCUtR (Direct Connection Upgrade through Relay) is a **relay-load optimization**, not a fix
for this issue:

- **It needs relays and doesn't apply to `main`.** DCUtR upgrades an existing **relayed**
  connection to a direct one by coordinating a simultaneous hole punch over the relay. With
  no relay there is nothing to upgrade from — it's inherently a relay-branch concept.
- **It doesn't touch the root.** The churn here is nodes **publishing unreachable addresses
  to the DHT** and kad **dialing** them. DCUtR changes neither the `NodeRecord` addresses nor
  kad's dialing; it uses each peer's *observed external* address on a separate path. So the
  `failed to dial …127.0.0.1` noise is untouched.
- **It doesn't guarantee reachability.** Hole punching works only for punchable (cone) NATs;
  **symmetric NATs / strict firewalls can't be punched** → it falls back to the relay. So it
  can't make an arbitrary NAT'd node reachable, and the **counted-but-unreachable** hazard
  remains.
- **It still needs a relay as the rendezvous anchor** — same "someone must be reachable"
  property; DCUtR just tries to get *off* the relay after meeting through it.

Where DCUtR *does* help (a different problem, relay branch only): it **cuts relay bandwidth /
slot load** by moving connections direct after establishment, and could collapse the
dual-relay redundancy and make circuits transient (used only for the punch) — easing the
circuit-exhaustion and stale-circuit churn on the relay path. Worth pursuing there as an
efficiency improvement, but orthogonal to this issue's root (kad advertising/dialing
unreachable addresses, and counting unreachable members).

## Observing it live: the `*_peer_addr` metrics

Several metrics expose this off the existing metrics port; all carry a `peer_addr` token so one
grep catches them:

```
curl -s localhost:<metrics_port>/metrics | grep peer_addr
```

Per swarm (`_primary` / `_worker`), refreshed every 15s:

- `kad_known_peer_addr_*` — the kademlia routing table (`kbuckets`): peers this node is
  **connected** to and the resolved address in use. Populated only on a successful connect, so
  unreachable peers never appear here.
- `advertised_peer_addr_*` — the peer-manager's `known_peers`: addresses peers **advertised**
  (DHT record / committee) that this node will redial, incl. peers it never connected to.
- `discovery_peer_addr_*` — the peer-manager's `discovery_peers`: candidates learned from
  other nodes' routing tables via `get_closest_peers`, dialed on the heartbeat. Note this map is
  drained as it dials, so a fast-churning entry is often absent at snapshot time.

And a counter (labelled `swarm`):

- `dial_peer_addr_failures{peer_id,multiaddr,swarm}` — increments once per attempted address on
  every failed outbound dial. This is the **reliable churn signal**: a climbing count for an
  unreachable address (e.g. a cross-host `127.0.0.1`) shows up regardless of which path issued
  the dial — kad iterative query, discovery heartbeat, or committee redial — including
  kad-internal dials that never sit in an app-side map for the gauges to catch.

The gauge-diff "trying to dial, never connected" is `advertised_peer_addr_worker unless
on(peer_id) kad_known_peer_addr_worker`; for the full picture (incl. discovery / kad-internal
churn) watch `rate(dial_peer_addr_failures[5m])` by `multiaddr`.

And this node's own identity/addresses (same `peer_addr` grep):

- `node_peer_addr_self{peer_id, authority, swarm}` — own peer id + BLS authority (set once); maps
  any peer id seen elsewhere back to a node.
- `node_peer_addr_external{multiaddr, swarm}` — the address(es) this node publishes.
- `node_peer_addr_listen_{primary,worker}` — current listen addresses.
- `node_peer_addr_reservation_{primary,worker}` — desired relay reservations, `1` = active,
  `0` = desired but currently down.

A relay-down alert falls straight out of the reservation gauge — it fires exactly when a relay is
wanted but gone, which the listen set can't express:

```
node_peer_addr_reservation_primary == 0
```

## Source anchors

- `crates/consensus/network/src/consensus/runtime.rs` — `kademlia.set_mode(Some(Mode::Server))` (forced for all)
- `crates/consensus/network/src/peers/behavior.rs` — `handle_pending_outbound_connection` (kad dials allowed); `DialFailure` log (now info, prints attempted addrs)
- `crates/consensus/network/src/peers/manager.rs` — `redial_missing_committee` (committee-only, per-heartbeat)
- `crates/consensus/network/src/consensus/peer_events.rs` — `Discovery` → `get_closest_peers(PeerId::random())`
