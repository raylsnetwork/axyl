# Relay isolation testnet - changes and rationale

Everything added or changed to build `etc/relay-network` (2026-07-13), and why each piece has the
shape it has. The harness answers one question in practice: can the committee run when every
validator is locked in its own private network and the only way traffic reaches it is a
circuit-relay-v2 relay? (Answer: yes - see README for the verification and chaos procedures.)

## Files touched

| File | Change |
|---|---|
| `etc/relay-network/compose.yaml` | New. The topology: 4 isolated validators, 4 DMZ relays, 4 NAT routers, dnsmasq, 2 observers. |
| `etc/relay-network/Dockerfile` | New. `etc/docker-network`'s Dockerfile plus the `rayls-relay` binary and runtime tools (curl, iproute2, iptables, dnsmasq). |
| `etc/relay-network/genesis.sh` | New. `etc/docker-network/genesis.sh` plus observer keygen, made idempotent. |
| `etc/relay-network/README.md` | New. Topology, run/verify instructions, DNS guidance, tx-flow notes, chaos tests. |
| `.dockerignore` (repo root) | New. Excludes `target/`, `.git`, `etc/test-network/local-validators` from build contexts. Safe: every Dockerfile in the repo COPYs specific directories only. Without it the context upload includes multi-GB build artifacts. |
| `Makefile` | `relay-up` / `relay-down` targets + help text, mirroring `up`/`down`. |
| (reused unchanged) | `etc/docker-network/setup_validator.sh`, mounted as-is - keytool reads the relay address from the `RL_RELAY_ADDR` env var, so no fork of the script was needed. |

## Decisions, in the order the problems forced them

### 1. Committee creation mirrors `local-testnet.sh --relay` exactly

Each validator's advertised primary/worker address is a concrete
`/ip4/<relay>/udp/4001/quic-v1/p2p/<relay-id>/p2p-circuit/p2p/<node-key>` circuit baked at keygen
(`RL_RELAY_ADDR`, the env form of `--relay`), relays run the fixed identities from
`etc/test-network/RELAY_KEYS.md`, and each validator held a backup reservation on a neighbor
relay via `PRIMARY/WORKER_RELAY_MULTIADDRS` (since removed - see 11). This is the battle-tested
path.

The first iteration instead advertised `/dns4/<alias>` circuit addresses resolved by Docker's
embedded DNS. It failed fatally at boot: every reservation listener closed within milliseconds,
each swarm hit `AllListenersClosed`, and all validators exited with "can not connect to enough
peers". The root cause was pinned down later (see 8): it was never DNS resolution.

### 2. NAT router per VPC instead of multi-homed relays (superseded by 11)

Concrete `/ip4` committee addresses need each relay reachable at ONE stable IP from every VPC. A
Docker network can't give one container the same IP on several networks, so reachability has to be
routed, not multi-homed: each `vpcN` is `internal: true` and its only egress is a NAT-gateway
container (iptables MASQUERADE, `ip_forward=1`); the validator's default route points at it. This
is also the honest model of the target deployment - a private subnet whose instances reach the
internet through a NAT gateway and accept nothing inbound.

### 3. Relays are dual-homed into their validator's VPC (DMZ)

Requirement: the relay should sit in the DMZ - publicly reachable, able to talk to its validator
directly, without exposing anything about the validator. Each relay has a public leg (static
`10.20.0.1N`, the only address the committee ever advertises) and a leg inside its own validator's
VPC (`10.10.N.3`). Circuit-relay-v2 gives the no-exposure property for free: reservations carry
only the relay's addresses, and peers never learn the validator's internal IP.

### 4. Isolation is enforced at the validator itself, not only by Docker

`internal: true` plus Docker's inter-bridge isolation SHOULD make validators unreachable, but that
guarantee is backend-specific: OrbStack's engine routes freely between bridge networks (verified:
cross-VPC HTTP probes returned responses). Each validator therefore drops every inbound flow that
is not loopback or a reply to its own outbound connections (iptables INPUT policy). This is safe
precisely because circuit-relay-v2 needs no inbound flows at all - every circuit rides the
validator's outbound reservation connection. Verified: probes from the public net, from another
validator, and even a new flow from the relay's own DMZ leg all fail, while consensus runs.

### 5. Observers override their QUIC listeners to `0.0.0.0`

libp2p-quic reuses the listening socket for outbound dials (`port_use: Reuse`). The keytool
default gives observers a `/ip4/127.0.0.1` listener, so every outbound dial left from a
loopback-bound socket and silently timed out - no error, no swarm event. On `local-testnet.sh`
everything lives on 127.0.0.1, which is why this never surfaced there. Validators are immune: in
relay mode they have no direct QUIC listener, so dials use ephemeral sockets.
`PRIMARY/WORKER_LISTENER_MULTIADDR=/ip4/0.0.0.0/...` fixes it (same mechanism `etc/docker-network`
uses).

### 6. local-testnet's large txpool limits, especially on the observers

Observers are the network's tx ingestion points (the only host-exposed RPCs). reth's default pool
allows 16 slots per sender: a single-sender load test gets `-32003: txpool is full` on the 17th
in-flight tx (verified: exactly 16 of a 60-tx burst accepted), and every rejected tx is a
permanent nonce hole that clogs the sender - the "tps-checker clogs immediately" symptom. All
nodes now carry `local-testnet.sh`'s 1M-count limits; after the fix the same 60-parallel burst
mined 60/60.

Context that matters for load tests: with execution-layer devp2p fully disabled
(`reth_env/config.rs`), the only path from an observer's pool to the committee is the worker
gossip disbursement (`disburse_txns` -> `WorkerGossip::Txn`; one validator absorbs each message by
slot digest). Disbursement is fire-and-forget - txs leave the observer pool on publish with no
delivery ack - so a lost message is also a nonce hole. Prefer multi-sender load profiles.

### 7. `RAYLS_NETWORK=local`

Hardfork profile with all mainnet forks active, the intended profile for local networks (`devnet`
was inherited from `etc/docker-network`). Consensus-relevant, so it was switched on all nodes
together with a fresh chain.

### 8. DNS: a dnsmasq service + `RAYLS_DNS_SERVER`, and where names are safe

Docker's embedded DNS is network-scoped - a validator can never resolve a name for a foreign
relay it shares no network with - so the harness runs its own dnsmasq (`10.20.0.53`, public net)
as the stand-in for public DNS, and nodes resolve against it via `RAYLS_DNS_SERVER` (the exact
mechanism `local-testnet.sh --relay-dns` uses).

Where names are safe was established empirically, and it explains the failure in (1):

- `/dns4` in addresses that are DIALED (circuit hops, peer addresses) works - verified end to end
  (dnsmasq logged the node's query, the QUIC connection established).
- `/dns4` in a RESERVATION LISTEN address is broken whenever the same relay is concurrently
  reachable under another address form. The libp2p relay client binds a pending reservation to
  the specific dial it issues; a concurrent dial to the same relay peer (e.g. an `/ip4` circuit
  hop to a peer behind it) wins the swarm's dial arbitration and the reservation listener is torn
  down, reason Ok. Verified: 11/11 losses on a `/dns4` listener while the `/ip4` listener on the
  identical code path held; the relay never received a single reservation request despite
  connections establishing. In the all-`/dns4` design of (1), every listener lost that boot race,
  which is fatal (`AllListenersClosed`).
- The correct split for DNS-based addressing is the branch's `/dnsaddr` mode: advertise a name
  (peers resolve `_dnsaddr` TXT records to relay circuits at dial time - this is what buys
  peer-visible relay failover), reserve on concrete `/ip4` relays. The dns service is ready to
  serve those TXT records.

### 9. Reservations ride the relay's DMZ leg, not the NAT path

The validator's reservation connection to its own relay carries 100% of its consensus traffic in
both directions, and dialing the relay's public leg sends all of it through the NAT router for no
benefit - the relay has a leg inside the VPC precisely so it can talk to its validator directly.
Each validator therefore reserves on `/ip4/10.10.N.3/...` (first `*_RELAY_MULTIADDRS` entry) and
skips listening on its advertised public form: circuit-relay-v2 matches inbound circuits to
reservations by destination peer id, so peers dialing `10.20.0.1N` still reach it, while
double-reserving the same relay under two address forms would both waste a reservation slot and
race the relay client's pending-reservation arbitration (the same failure mode as (8)'s `/dns4`
listener). The skip lives in the node (`advertised_relay_covered`): an advertised relay covered by
an explicit reservation entry is reserve-only-once. The two-address-form hazard stays dormant
because a validator never *dials* its own relay's public form (no committee peer advertises
through it).

### 10. Two observers behind DNS (see 11 for the validator side)

`observer1`/`observer2` (static `10.20.0.21`/`.22`, host RPC 7545/7544) with A records
`observerN.rayls.test` and a round-robin `rpc.rayls.test` spanning both. Verified: both follow the
chain through the relays only, and RPC calls through the round-robin name answer.

## Branch-level findings (not harness issues)

Recorded in `TODO-CRv2-NETWORKING.md`: the relay-client reservation/dial arbitration limitation,
the fatal `AllListenersClosed` at boot when all relay listeners fail transiently (the retry loop
could recover but never gets the chance), the fire-and-forget tx disbursement, and the observer
loopback-listener dial trap. Fixes belong on the branch with failing-first tests, per repo
discipline.

### 11. The relay is the validator's only network neighbor (gateway), kernel-enforced

Requirement: a validator may connect to its own relay and nothing else. Circuit-relay-v2 makes
the literal L4 form of this impossible with per-validator relays - a v1<->v2 link needs a relay
BOTH sides are connected to (the dialer must reach the relay holding the destination's
reservation), and CRv2 cannot chain relays - so the restriction is enforced at the pipe level
instead: the relay replaced the NAT router as the validator's default gateway (the routers are
gone), meaning every packet a validator sends physically enters its own relay box first. Circuit
first hops still NAME foreign relay IPs, but they transit - and are filtered by - the own relay.

Enforcement is default-drop on both ends, so the property is a kernel invariant rather than an
observation: validator OUTPUT DROP (allowed: loopback, own DMZ leg, relay-public udp/4001),
validator INPUT DROP (allowed: loopback, replies to own outbound flows), relay FORWARD DROP
(allowed: the validator's udp/4001 to relay public legs, MASQUERADEd, plus established replies).
`verify-topology.sh` asserts the policies and the zero OTHER-egress ledger.

Backup reservations on neighbor relays were dropped: they would violate the restriction, peers
could never re-reach a node through them anyway (that needs `/dnsaddr` advertising), and their
one real job - keeping the swarm's last listener alive - disappeared when zero listeners with a
desired reservation became a retried state instead of a shutdown. Consequence accepted by
design: own-relay loss makes the validator fully dark until the relay returns (it survives and
re-reserves). Also foreclosed by construction: validator-side DNS (port 53 blocked; a `/dnsaddr`
mode would need the gateway to forward udp/53).
