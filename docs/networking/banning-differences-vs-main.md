# Banning: `ba-circuit-relay-v2-poc` vs `main`

How banning differs on the circuit-relay-v2 branch compared to `main`. The changes
are almost entirely about **not banning relays or relayed connections** — on a
relay-routed network a ban there tears down the reservation and every circuit behind
it (and, for co-located relays, IP-cascades onto real peers).

Baseline on `main`: banning is a flat per-peer reputation system. A peer that does
not support gossipsub is Fatal-banned (`gossip.rs`: `process_penalty(peer_id,
Penalty::Fatal)`), and there is no concept of a relay.

| Aspect | `main` | `ba-circuit-relay-v2-poc` |
|---|---|---|
| Peer that doesn't speak gossipsub (`GossipsubNotSupported`) | Fatal-banned immediately | Recorded as relay infrastructure via `mark_relay_peer` — penalty-exempt, dropped from the DHT, not banned (banning it would sever every circuit routed through it and strand relayed nodes) |
| Concept of a relay peer | none — every peer scored/banned the same | New `relay_peers: HashSet<PeerId>`; `is_relay()` / `mark_relay_peer()` / `register_relays_from_addrs()` API |
| Penalizing a relay | possible → score drops → ban → drops the reservation + all its circuits | Short-circuited: `process_penalty` early-returns for relay peers ("ignoring penalty for relay peer") — exempt from penalties and pruning |
| How relays are learned | n/a | From `/p2p-circuit` addresses at dial time (`register_relays_from_addrs`, incl. `/dnsaddr` failover) and authoritatively from `GossipsubNotSupported` (`mark_relay_peer`); registered on-loop via the `RegisterRelays` command |
| Kademlia treatment of relays | added/published like any peer → others dial them → they penalize→ban the relay | `is_relay` skips kad add/publish for relays, so nobody treats a relay as a DHT peer |
| Inbound relayed (`/p2p-circuit`) connections | no relay transport exists; all inbound is direct and IP-sanitized | Accepted without IP sanitization — a circuit has no peer IP to validate/ban, and banning would reset the relay's STOP stream so the circuit never completes |
| IP-level ban cascade (co-located relay, e.g. 127.0.0.1) | an IP-ban on a "peer" that is really a relay knocks out every real peer behind that IP | avoided by the relay penalty-exemption + kad exclusion above |
| Ban observability | bare "peer banned" event | adds `warn "penalty resulted in ban"` naming the triggering penalty, plus connection-close-cause logging |
| Committee/validator ban-exemption (`is_peer_validator`) | present | present — unchanged (not a difference) |

## Through-line

`main` = flat per-peer reputation. This branch adds a carve-out for **relays and
relayed connections**: they run none of the consensus protocols (gossip/kad/req-res),
so ordinary scoring would instantly ban them, which on a relay-routed network tears
down the reservation and every circuit behind it. The branch therefore makes relays
first-class, penalty-exempt infrastructure and skips IP-based banning on circuit
connections. Validator/committee exemption is the same on both branches.

## Key source anchors (branch)

- `peers/manager.rs`: `relay_peers`, `is_relay`, `mark_relay_peer`,
  `register_relays_from_addrs`, `process_penalty` early-return, "penalty resulted in ban" log
- `consensus/gossip.rs`: `GossipsubNotSupported` → `mark_relay_peer` (was `Penalty::Fatal`)
- `consensus/command.rs`: `RegisterRelays` command (on-loop registration)
- `consensus/reqres.rs` / `constructor.rs`: skip IP sanitization for `/p2p-circuit` inbound
- `peers/behavior.rs` / `consensus/mod.rs`: `is_relay` skips kad add/publish
