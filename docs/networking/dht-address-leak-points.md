# DHT address "leak" points

How un-reachable-from-here addresses (loopback, cross-vantage private, dead
circuits, bare `/p2p/<peer>`) enter this node's dial candidates.

## The root cause

A peer's `NodeRecord` addresses are **author-controlled** and are only ever
**cryptographically validated** (`peer_record_valid` checks the signature and
timestamp) — **never reachability-validated**. So whatever a node advertises
propagates verbatim through the DHT into every other node's dial candidates,
regardless of whether *this* node, from *its* vantage, can actually reach those
addresses.

That is why a static IP-class filter at ingestion is wrong: a `10.x` / `192.168.x`
/ `127.0.0.1` address is legitimately reachable from a co-located / same-LAN peer
and unreachable from another. Only the **dial layer** knows the truth for this
vantage, so the correct fix (an empirical per-address failure backoff) lives
there, not at ingestion.

## Three roles in any Kademlia lookup

- **Querier** — the node that calls `get_record` / `get_closest_peers`. This is
  *us*, the node whose `known_peers` / `discovery_peers` fills up. It is the node
  with the leak.
- **Queried (responders)** — the peers whose ids sit closest to the key in the
  keyspace. We contact them (dialing if needed); they answer.
- **Subject** — whoever's addresses come back in the answer. **Not necessarily a
  peer we talked to.**

`QueryResult` (inside `kad::Event::OutboundQueryProgressed`) is always the outcome
of a query **we** started — so in the outbound leaks (1, 2, 5) this node is the
querier. `kad::Event::InboundRequest` (leak 4) is the reverse: someone queries us.

Everything funnels through one handler: `process_kad_event`
(`crates/consensus/network/src/consensus/kad.rs:71`).

## The leak list

| # | Trigger (libp2p event) | Path | Lands in | Who queries |
|---|------------------------|------|----------|-------------|
| 1 | `OutboundQueryProgressed { GetRecord }` `kad.rs:109` | `process_kad_query_result` `kad.rs:413` → `close_kad_query` `kad.rs:468` → `add_known_peer` `manager.rs:806` | `known_peers` (dialed by `redial_missing_committee`/`DialBls`) | us |
| 2 | `OutboundQueryProgressed { GetClosestPeers }` `kad.rs:181` | `process_peers_for_discovery` `manager.rs:999` | `discovery_peers` (dialed on the heartbeat) | us |
| 3 | startup preload | `load_known_peers_from_kad_store` `kad.rs:485` (called `runtime.rs:30`) → `add_known_peer` | `known_peers` | us (from disk) |
| 4 | `InboundRequest::PutRecord` `kad.rs:85` | `process_kad_put_request` `kad.rs:228` | kad store → later `known_peers` | **them** |
| 5 | relayed inbound send-back `/p2p/<src>` (no IP — `behavior.rs:71-74`) | `kademlia.add_address` on connect → kbucket → FIND_NODE | discoverer's dial → bare `/p2p/<peer>` (**FIXED: `add_address` now requires a transport**) | us (discovery walk) |

Only reachability-relevant validation on the way in: `eligible_for_discovery` →
`has_valid_unbanned_ips` drops **banned** IPs only — it does **not** filter by
reachability class, so private/loopback pass straight through.

## It all reduces to two kad writes

The five leaks above are surface symptoms; every propagatable address enters
kademlia through exactly **two** writes, and everything else is a delete, a read, a
query, or an app-level side effect. Enumerating every `kademlia.` call in the crate:

| Write | Store | Carries | Propagates via |
|-------|-------|---------|----------------|
| `add_address` (sole writer; `BucketInserts::Manual` disables auto-insert) | routing table (kbuckets) | the **connection** address (inbound send-back / outbound dialed) | FIND_NODE responses (**pull only**) |
| `put_record` / `put_record_to` (ours) **+** `store_mut().put(record)` (inbound `PutRecord` from a peer) | record store | the peer's **advertised** `NodeRecord` address (its `network_address` from keygen/node-info) | `get_record` responses (pull) **+** `put_record` active push to the K-closest |

Not address sources:
- `remove_peer`, `remove_record` — deletions.
- `kbuckets()`, `store_mut().records()` — reads (metrics, startup load).
- `get_record`, `get_closest_peers` — **queries** (reads); their *responses* populate
  the **app-level** `known_peers` / `discovery_peers` maps. Those maps are effects,
  not sources — they hold what *this* node ingested from *other* nodes'
  `add_address` / `put_record`, and they feed the dialer (`DialBls` / discovery).
- `start_providing` — a provider record for *our own* key only.
- `set_mode` — client/server mode.

Two consequences worth holding onto:
1. The two writes carry **different** addresses — `add_address` leaks the
   *connection* view (fixed by the transport-less gate, leak 5); `put_record` leaks
   the *advertised* view (an outbound-only node's loopback `network_address`). A
   complete fix must consider both. The advertised view is addressed by advertising an
   **identity-only `network_address`** (see below): an outbound-only node sets its
   `network_address` to a bare `/p2p/<peer-id>` (keytool `--advertise-identity-only`)
   and binds its listen socket separately via `*_LISTENER_MULTIADDR`, so its published
   record still carries identity but is undialable. Note it MUST still publish (a node
   that skips publishing becomes unidentifiable -- see "The record is identity, not just
   an address" below).
2. Routing-table entries are **pull-only** (they reach another node solely when it
   queries you and the entry is among the K-closest to its key) — there is no
   broadcast or periodic sync. Records, by contrast, are actively **pushed** by
   `put_record` to the K-closest nodes. "Committee-only kad" clamps *both* by gating
   on membership: only committee peers get `add_address`'d, and only committee records
   are accepted/served.

## What's in a `NodeRecord` (the `put_record` payload)

`put_record` publishes the record built by `get_peer_record` (`kad.rs:522`):

```rust
kad::Record {
    key:       RecordKey::new(&primary_public_key()),  // your BLS pubkey — the index others look up
    value:     encode(&node_record),                   // BCS-encoded NodeRecord (below)
    publisher: Some(local_peer_id),
    expires:   None,                                   // never expires
}
```

The `value` is the `NodeRecord` (`types.rs:800`), which is just signed network info:

```rust
NodeRecord {
    info: NetworkInfo {
        pubkey:     <network public key>,   // the libp2p key your peer id derives from
        multiaddrs: vec![ <network_address> ],// ONE entry — your advertised network_address
        timestamp:  <now>,                  // used to keep the newest record
    },
    signature: <BLS signature over encode(&info)>,   // lets ingesters authenticate it
}
```

Key facts:
- It is keyed on the **BLS authority key**; the **network pubkey** inside yields the
  **peer id** others dial; `multiaddrs` holds exactly **one** advertised address —
  `node-info.yaml`'s **`network_address`**: a concrete `/p2p-circuit` under `--relay`, a
  `/dnsaddr` under `--relay-dns`, an identity-only `/p2p/<peer-id>` for an observer
  (`--advertise-identity-only`), or `127.0.0.1` for the bare default. See
  `NodeRecord::build` (`types.rs`).
- **Validation on ingest is signature + timestamp + peer-id match only** — never
  reachability. A valid record's `info` is written straight into the ingester's
  `known_peers` via `add_known_peer`, i.e. `bls_key -> (peer_id, [network_address])`,
  which `DialBls` later dials.
- So the record propagates **your advertised address, verbatim, to everyone**. If
  `network_address` is loopback, every ingester stores and dials that loopback — this is
  the `put_record` (advertised-view) half of the leak.

## Concrete examples

Topology for the examples: committee `v1..v15`, non-committee `node-16..node-30`,
plus observers, several co-located on one host (so their real listen address is a
`127.0.0.1:<port>`), others behind relays.

### Leak 1 — `GetRecord` (one subject: the record owner)

The observer is missing committee member **v7** (it has v7's BLS key from the
committee list but no record yet), so `MissingAuthorities`
(`peer_events.rs:234`) fires `get_record(v7_bls)`.

1. The observer contacts **v1** and **v2** — the peers closest to v7's key that it
   already knows — dialing them if needed.
2. **v1** holds v7's `NodeRecord` and returns it. The record advertises, say,
   `/ip4/10.10.0.7/udp/50007/quic-v1/.../p2p-circuit/.../v7`.
3. `close_kad_query` → `add_known_peer(v7_bls, ...)` stores `10.10.0.7` in the
   observer's `known_peers`. The observer will now redial it via `DialBls`.

- **Querier** = observer. **Queried** = v1, v2. **Subject** = **v7**.
- The leak: the observer stores and dials `10.10.0.7` **even though it never
  talked to v7**, and even if `10.10.0.7` is not routable from the observer's
  vantage → repeated dial failures.

### Leak 2 — `GetClosestPeers` (many subjects, none asked for by identity)

The observer has 4 peers but wants `target_num_peers = 30`, so every 30s heartbeat
`discovery_heartbeat` sees `discovery_peers` low and pushes `PeerEvent::Discovery`
(`manager.rs:1120`), which runs `get_closest_peers(PeerId::random())`
(`peer_events.rs:247`). Call the random key **R**.

1. The observer contacts the peers in its routing table closest to **R** — **v1,
   v2, v3**.
2. Each responder replies with **the peers from its own kbuckets nearest R, plus
   the addresses it holds for them**. Crucially, the observer did **not** ask for
   any of these peers by identity — it asked for "closest to R", and got back a
   *firehose of third-party entries*.
3. Suppose **v1** learned **node-16** back when node-16 connected to v1. On a
   single-host testnet v1 recorded node-16 at its co-located listen address
   `/ip4/127.0.0.1/udp/41016/quic-v1/.../node-16`. v1 returns exactly that.
4. `process_peers_for_discovery` inserts `node-16 → 127.0.0.1:41016` into the
   observer's `discovery_peers`. Next heartbeat the observer dials it, hits **its
   own loopback**, and fails. Every heartbeat re-discovers it → churn.

- **Querier** = observer. **Queried** = v1, v2, v3. **Subjects** = node-16 (and
  every other kbucket neighbor the responders name).
- Contrast with Leak 1: `GetRecord` returns exactly **one** subject — the record
  owner whose key we asked for. `GetClosestPeers` returns **many** subjects that we
  never named; we asked only for *proximity to a random key*. That is why it is the
  broadest firehose of unreachable addresses, and why a co-located peer's
  `127.0.0.1` reaches us via a **third** node rather than from the peer itself.

### Leak 3 — persistent store preload (a stale subject, no query at all)

The observer restarts. Before any live query runs,
`load_known_peers_from_kad_store` (`runtime.rs:30`) walks **every** record
persisted in the kad store and calls `add_known_peer` for each.

1. A record for **node-22** persisted from a previous run advertises
   `/ip4/10.0.0.22/...`, but node-22 has since moved / that address is no longer
   reachable from here.
2. The stale `10.0.0.22` is back in `known_peers` at boot and gets dialed
   immediately — the unreachable address survives restarts without anyone
   re-advertising it.

- **Querier** = us, from disk (no network query). **Subject** = **node-22** (a past
  record).
- The leak: restarts re-introduce stale addresses before the network can correct
  them.

### Leak 4 — inbound `PutRecord` (they query us)

A peer pushes its own record to us rather than us pulling it.

1. **node-19** does a Kademlia `PUT_RECORD` of its `NodeRecord` toward the peers
   closest to its key, and this node is one of them.
2. `kad::InboundRequest::PutRecord` (`kad.rs:85`) → `process_kad_put_request`
   (`kad.rs:228`) writes node-19's record into our store; it later surfaces into
   `known_peers`.
3. If node-19 advertises `/ip4/192.168.5.19/...` that we cannot route to, we now
   hold and will dial it.

- **Querier** = **node-19** (the initiator). We are the responder/store.
  **Subject** = node-19 (itself).
- The leak: we accept and later dial an address a peer *pushed* at us, again with
  no reachability check. (`AddProvider`, `kad.rs:81`, is the provider-side analog.)

### Leak 5 — the transport-less `/p2p/<peer>` send-back (FIXED at source)

This is the bare `/p2p/<peer>` churn, and its source is precise: a **relayed inbound
connection's send-back address**.

1. A relay-only peer (e.g. an observer, or any node behind a relay) dials the
   committee **through a relay** — outbound for it, **inbound** for each committee
   member. At the destination, `handle_pending_inbound_connection` documents that
   the send-back address for a relayed inbound is **just `/p2p/<src>` with no IP**
   (`behavior.rs:71-74`).
2. On `PeerEvent::PeerConnected` the destination calls
   `kademlia.add_address(peer, /p2p/<src>)` (`peer_events.rs`), seeding its kbucket
   with a **transport-less** entry. `add_address` is the sole populator of the
   kbuckets (`BucketInserts::Manual`), so this is *the* way the entry gets in.
3. FIND_NODE then hands that entry to any discoverer (a `get_closest_peers` walk).
   The discoverer has a peer id and no dialable address, so the swarm dials the peer
   id expressed as a multiaddr — `/p2p/<peer>` — which no transport can dial →
   `MultiaddrNotSupported`, every 30s.

- **Source** = the destination `add_address`ing a relayed inbound's bare send-back.
  **Propagation** = FIND_NODE. **Subject** = the relay-only peer.
- Unlike the other leaks this is **not vantage-dependent**: a `/p2p/<peer>` with no
  transport is undialable for *everyone*, always. So it is fixable structurally, at
  the source.

**Fix (implemented):** gate `add_address` on the address carrying a real transport
(`/ip4`, `/ip6`, or `/dns*`); skip a bare `/p2p/<src>` send-back
(`peer_events.rs`). This keeps kbuckets — and therefore FIND_NODE responses — free
of transport-less entries network-wide, so a relay-only peer's id never propagates
address-less and no one ever dials `/p2p/<peer>`. Nothing is lost: a relay-only
peer is reached via its published record (`get_record` -> `DialBls` -> circuit), not
this send-back. (The one use a relayed address normally has is DCUtR relay->direct
upgrade, but there is no `dcutr` behaviour here, and DCUtR would drive off the live
relayed connection, not this kad entry.)

Note this filters only the KAD store (`kademlia.add_address`), which is what
propagates. The swarm's local address book (`swarm.add_peer_address`) still records
the send-back, but that is node-local and never enters FIND_NODE.

## Note: advertising `0.0.0.0` behaves like loopback (and is not what you want)

The advertised address comes from `node-info.yaml`'s `network_address`
(`p2p_info.primary/worker.network_address`), read verbatim at startup
(`epoch_manager/network.rs:116`) and baked into the signed `NodeRecord`. It is also
the node's *listen* bind — unless `PRIMARY/WORKER_LISTENER_MULTIADDR` overrides it
(`parse_listener_address_for_swarm`), which is how you make what you *advertise*
differ from what you *bind* (e.g. advertise a public ip, bind `0.0.0.0`).

If `node-info.yaml` sets `network_address` to `0.0.0.0`, e.g.

```
network_address: /ip4/0.0.0.0/udp/37907/quic-v1/p2p/12D3KooWH7iu...
```

then that is exactly what is published to the DHT — **verbatim, no expansion**. It
is *not* translated to the node's concrete interface IPs (that expansion only
happens for `0.0.0.0` *listen* binds, via `NewListenAddr`, never for the advertised
`network_address`). So `node_peer_addr_external` shows `/ip4/0.0.0.0/udp/37907/...`,
and nothing on the host actually listens on `37907` (it is a keygen-time ephemeral
port — `get_available_udp_port` opened `127.0.0.1:0`, read the number, and closed
it; the real sockets are the `*_LISTENER_MULTIADDR` ports). `37907` is therefore a
**phantom port**: advertised, bound by nobody, absent from `ss`.

`0.0.0.0` is a *bind* wildcard, not a routable *destination*. When a remote peer
dials `/ip4/0.0.0.0/udp/37907/quic-v1/p2p/<key>`, on Linux `connect()` to `0.0.0.0`
is mapped to **localhost**, so the dialer ends up attempting **its own**
`127.0.0.1:37907` — where nothing useful listens. This is identical in outcome to
advertising `/ip4/127.0.0.1/...` directly: both make every remote dialer bang on
its own loopback and fail.

The failure is harmless — no ban, no penalty. A dial to a phantom/own port either
gets `connection refused`/timeout, or, if some unrelated process happens to hold
that port, is rejected at the libp2p secure handshake with `WrongPeerId` (the
remote's key can never match the expected `/p2p/<key>`, which lives only on the
advertising node). libp2p verifies identity **before** stream-mux and protocol
negotiation, so a misdial never reaches gossip and never triggers the
gossip-authorization ban path. And `on_dial_failure` (`behavior.rs:313`) applies no
penalty. So the net effect of advertising `0.0.0.0` (or `127.0.0.1`) is **pure
churn**: repeated, harmless, self-directed loopback dials — the same waste this
document is about, just self-inflicted via a bad advertise address.

Takeaway: `0.0.0.0` is the right value for the *listen* bind (all interfaces) and
the wrong value for the *advertise* address. For a node that must be dialable, set
`network_address` to a concrete routable IP (or a relay / `/dnsaddr`) and, if it binds
a different socket, point `*_LISTENER_MULTIADDR` at the bind (e.g. `0.0.0.0`). For an
observer (which nothing dials), set `network_address` to an **identity-only bare
`/p2p/<peer-id>`** (keytool `--advertise-identity-only`) and bind via
`*_LISTENER_MULTIADDR` — do **not** try to make `0.0.0.0` (or skipping publish
entirely) do that job. `/p2p/<peer-id>` is undialable (every dial filter rejects a
transport-less address) yet still publishable, so the observer stays identifiable.

## Where each leak is fixed — two layers

The leaks split into two kinds, and each has its own correct fix:

**Structurally-undialable (leak 5): fix at the source.** A bare `/p2p/<peer>` has no
transport — it is undialable from *every* vantage, always. That is not a
reachability guess, so it can be filtered deterministically where it enters the DHT:
gate `add_address` on the address carrying a transport, so a transport-less
send-back never reaches a kbucket and never propagates via FIND_NODE. **Implemented**
(see leak 5). This is vantage-independent and needs no probing.

**Vantage-dependent (leaks 1-4): fix at the dial layer.** The rest deposit an
address that *has* a transport but may or may not be reachable *from here* — a
subject's own record (1, 3, 4) or a third-party kbucket entry (2). The **same
address class** (`127.0.0.1`, a `10.x`/`192.168.x` private IP, a `/p2p-circuit`
through a relay) is reachable for a co-located / same-VPC peer and unreachable for a
remote one, so no static rule at ingestion is correct. Only the dial layer observes
actual reachability, so an **empirical per-address failure backoff** (increment on
dial failure, reset on a successful connect, skip during exponential cooldown with a
cap and half-open re-probe; committee members exempt to protect consensus liveness)
is the correct, vantage-aware fix. **Not yet implemented.**

A source-side handling for the observer / outbound-only case — an
**identity-only `network_address`**: such a node sets its `network_address` to a bare
`/p2p/<peer-id>` (keytool `--advertise-identity-only`). It advertises identity-only —
undialable (leak-5 filter rejects it everywhere) yet still published, so peers can
identify it. It must NOT skip publishing (see below). Because that `network_address`
is not listenable, the node binds its listen socket via `*_LISTENER_MULTIADDR`
(`parse_listener_address_for_swarm` errors if it is unset), and dials out normally.
The backoff remains the catch-all for anything that still leaks.

## The record is identity, not just an address

A tempting but wrong "fix" is to have an outbound-only node **skip publishing** its
record (if nothing dials it, why advertise?). This breaks it: the published record is
also the node's **identity**. When it sends a request-response message (e.g. an
observer fetching batches), the responder does `peer_to_bls(peer_id)` — populated by
`add_known_peer` from that node's **published record** — to decide whether to serve
it. No record ⇒ `peer_to_bls` is `None` ⇒ the responder rejects with
`"requesting peer unknown"` (`reqres.rs`) ⇒ the node can never fetch batches and
falls out of sync. Identity comes from the record's **`pubkey`**, independent of the
`multiaddrs`, so the address can be an undialable `/p2p/<peer-id>` and identity still
works. Hence: **advertise `/p2p/<peer-id>` (undialable) but keep publishing the record
(identity).** Do not skip publishing for any node that participates in
request-response.
