# Banning: `ba-circuit-relay-v2-poc` vs `main`

How banning differs on the circuit-relay-v2 branch compared to `main`. The changes
are about **not scoring relays for merely being relays** — on a relay-routed network a
ban there tears down the reservation and every circuit behind it (and, for co-located
relays, IP-cascades onto real peers). Scoring stays behaviour-based: a relay that
misbehaves is banned like any other peer.

Baseline on `main`: banning is a flat per-peer reputation system. A peer that does
not support gossipsub is Fatal-banned (`gossip.rs`: `process_penalty(peer_id,
Penalty::Fatal)`), and there is no concept of a relay.

| Aspect | `main` | `ba-circuit-relay-v2-poc` |
|---|---|---|
| Peer that doesn't speak gossipsub (`GossipsubNotSupported`) | Fatal-banned immediately | No penalty (this is the one signal an honest relay emits by nature); dropped from the DHT. Confers no privilege — the peer stays subject to every other penalty and to pruning. A committee validator hitting it is only warned about (protocol/version fault) |
| Concept of a relay peer | none — every peer scored/banned the same | New `relay_peers: HashSet<PeerId>` populated only from `/p2p-circuit` hops (`register_relays_from_addrs`); `is_relay()` = prune-exempt + kept out of kad |
| Penalizing a relay | possible → score drops → ban → drops the reservation + all its circuits | Unchanged mechanics — relays are **not** penalty-exempt. No remaining penalty fires for lacking a protocol, so any penalty on a relay is behaviour (authoring gossip, sending requests, delivering kad records) and it is banned like any other peer; `warn "penalty resulted in ban"` names the trigger |
| Request/response failures | outbound `UnsupportedProtocols` → Severe on the target; outbound `Io` → Medium; inbound `UnsupportedProtocols` → Fatal | Behaviour-based: outbound `UnsupportedProtocols` → **no penalty** (we picked the target — `SendRequestAny` round-robins over every connected peer, incl. relays and misdirected worker identities; this arm banned them, see issue #143); outbound `Io` → **Mild** (ambiguous between a garbage response and a link reset, routine on keep-alive-less circuits); inbound `UnsupportedProtocols` → **Severe** (the peer's own stream, but a stray one is usually misconfiguration/version skew; 5 hits ban, and the ban decays once the peer is fixed) |
| Pruning a relay | possible (no relay concept) | `relay_peers` are skipped by `prune_connected_peers`: the direct leg to a relay carries our reservation and every inbound circuit |
| How relays are learned | n/a | From `/p2p-circuit` addresses only: `StartListening` (relays we reserve on), `dial_peer`/`add_known_peer` (relays we dial peers through), and `/dnsaddr` resolution (entries anchored to the looked-up `/p2p/<id>`, capped at 16 per name) via the `RegisterRelays` command. Never inferred from `GossipsubNotSupported` |
| Kademlia treatment of relays | added/published like any peer → others dial them → they penalize→ban the relay | `is_relay` skips kad add/publish for relays, so nobody treats a relay as a DHT peer |
| Inbound relayed (`/p2p-circuit`) connections | no relay transport exists; all inbound is direct and IP-sanitized | Accepted without IP sanitization — a circuit has no peer IP to validate/ban, and banning would reset the relay's STOP stream so the circuit never completes |
| IP-level ban cascade (co-located relay, e.g. 127.0.0.1) | an IP-ban on a "peer" that is really a relay knocks out every real peer behind that IP | an honest relay is never scored, so it never contributes to the per-IP ban count; a *misbehaving* relay can still be banned and, with `BANNED_PEERS_PER_IP_THRESHOLD = 1`, can tip its host IP into a block together with one other banned peer |
| Ban observability | bare "peer banned" event | adds `warn "penalty resulted in ban"` naming the triggering penalty, plus connection-close-cause logging |
| Committee/validator ban-exemption (`is_peer_validator`) | present | present — unchanged (not a difference) |

## Through-line

`main` = flat per-peer reputation. Relays run none of the consensus protocols
(gossip/kad/req-res), so on `main` scoring would instantly ban them for
`GossipsubNotSupported`, which on a relay-routed network tears down the reservation and
every circuit behind it. This branch removes that one *protocol-absence* penalty and
keeps every *behavioural* penalty, so relays are banned exactly when they misbehave.
What relays do get is a **prune** exemption and exclusion from kad (the leg to a relay
is infrastructure we depend on), plus IP sanitization is skipped on circuit
connections (no peer IP to validate). Membership in `relay_peers` comes only from
`/p2p-circuit` hops we actually use, never from a peer's behaviour, so a planted peer
id gains at most a connection slot — never immunity. Validator/committee exemption is
the same on both branches.

## Key source anchors (branch)

- `peers/manager.rs`: `relay_peers`, `is_relay`, `register_relays_from_addrs`,
  `should_skip_gossip_penalty`, `process_penalty` (no relay carve-out; "penalty resulted
  in ban" log), `prune_connected_peers` relay skip
- `consensus/gossip.rs`: `GossipsubNotSupported` → no penalty + kad removal (was `Penalty::Fatal`)
- `consensus/command.rs`: `RegisterRelays` command (on-loop registration);
  `resolve_relay_circuits` anchor + cap
- `consensus/reqres.rs` / `constructor.rs`: skip IP sanitization for `/p2p-circuit` inbound
- `peers/behavior.rs` / `consensus/mod.rs`: `is_relay` skips kad add/publish
