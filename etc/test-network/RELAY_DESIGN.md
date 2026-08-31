# Relay routing — design notes & security tradeoffs

This documents the circuit-relay-v2 setup added for the local testnet (`--relay` in
`local-testnet.sh`, the `rayls-relay` binary, and the client-side changes in
`crates/consensus/network`). It is a **proof of concept / NAT-traversal convenience**, not a
hardened production topology. Read this before relying on it for anything beyond local testing.

## Topology

- Every validator advertises **only** a `<relay>/p2p-circuit/p2p/<node>` address (baked into
  `node-info.yaml` by `keytool generate --relay`), and its **only listener is the relay
  reservation** — it never opens a direct QUIC listen socket.
- Each node reserves on its **own** relay; peers reach it through that relay. Nodes never connect
  directly to each other — the only direct connections a node makes are *to relays* (its own
  reservation + the first hop when dialing a peer through the peer's relay).
- Result: **all** validator↔validator traffic transits a relay, in both directions.

## What is NOT weakened

- **Confidentiality & integrity are intact.** A relayed connection is end-to-end encrypted and
  authenticated: the two endpoints run their own noise handshake *through* the relay tunnel, keyed
  to their peer ids. The relay forwards ciphertext only — it **cannot read, modify, or forge**
  consensus messages, nor impersonate a validator.

## Tradeoffs / risks

- **Mandatory chokepoint & single point of failure.** A node's connectivity depends entirely on
  its relay, with no direct-listener fallback. If a node's relay is down or seized, that validator
  is fully partitioned. Taking down more than `f` validators' relays can halt consensus (liveness
  attack) — relays are softer, higher-value targets than the validators themselves.
- **Eclipse / censorship.** A malicious or coerced relay cannot forge messages, but it can
  **selectively drop, delay, or reorder** them — eclipsing a validator, biasing timing, or breaking
  liveness/fairness. The relay sits in the ideal position to do this since all traffic transits it.
- **Metadata exposure.** Even with encrypted payloads, each relay sees the full communication graph,
  timing, volumes, and **every connected node's real (egress) IP**. Peers do not see each other's
  IPs, but relays see all of them. NAT/VPN only changes *which* public IP the relay observes; it
  does not hide you from the relay (you dial out to it directly).
- **Open reservations.** `rayls-relay` grants reservations to anyone and raises the default
  circuit/reservation limits, so a reachable relay is abusable by third parties (resource
  exhaustion). Fine for a local test; not for a public deployment.
- **Trust concentration.** Whoever operates the relays is a privileged observer and gatekeeper for
  the nodes that depend on them.

## Mitigations (if this needs to be robust)

- **Multiple relays per node** for redundancy, so one relay failure ≠ partition.
- **dcutr hole-punching**: use the relay only to *establish* the connection, then upgrade to a
  direct QUIC link (add `dcutr` + `autonat`). This removes the permanent chokepoint and most
  metadata exposure — the standard libp2p pattern and the direct counter to "always relayed".
- **Authorized relays**: only let committee peers reserve/use the relay; run the relays yourself.
- **Keep a direct listener** as a fallback rather than being relay-only.

See `RELAY_KEYS.md` for the fixed test relay identities and how to run them.
