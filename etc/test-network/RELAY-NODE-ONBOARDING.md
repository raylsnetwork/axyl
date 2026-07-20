# Relay testnet + onboarding a new validator

A reproducible harness for the circuit-relay-v2 topology and the **dynamic
validator onboarding** flow (observer → staked → committee validator). Use it to
deep-dive the actual onboarding problems (see [Known problems](#known-problems)).

For the plain direct-QUIC testnet see [`README.md`](README.md); this doc is the
relay + staking path.

## Prerequisites

- Built binaries: `rayls-network`, `rayls-relay` (the scripts build them).
- `dnsmasq` on `PATH` (used by `--relay-dns`).
- Foundry `cast` on `PATH` (used by `stake-relay-node.sh`).
- A **dev-funds account you hold the private key to** — it becomes the
  ConsensusRegistry **owner** and the RLS **minter** (governance). Do not use a
  random address.

## One-shot sequence

```bash
DEV_FUNDS=0x57b9D26eF4a6d4738E17932AC4d0191EfE6dBc88   # owner+minter; YOU must hold its key
DEV_FUNDS_KEY=0x<private-key-of-DEV_FUNDS>

# 1. bring up the relay-fronted 4-validator mesh (inside=direct, outside=relay)
MULTI_LISTEN=1 ./etc/test-network/local-testnet.sh --start --dev-funds "$DEV_FUNDS" --relay-dns

# 2. add node 6 as a relayed OUTSIDER (resolves the committee via the public/relay DNS view)
DNSMASQ_PORT=5354 ./etc/test-network/add-relay-node.sh 6

# 3. stake it into the committee (waits for the chain to be ready, then mint→allowlist→approve→stake→activate)
ADMIN_PRIVATE_KEY="$DEV_FUNDS_KEY" ./etc/test-network/stake-relay-node.sh 6

# --- stopping ---

# stop just the added node (+ its relay):
./etc/test-network/stop-relay-node.sh 6
# stop just a base validator (+ its two relays), seq 0-based (1 = validator-2):
./etc/test-network/local-testnet.sh --stop-validator 1
# bring the whole network down:
killall rayls-network rayls-relay dnsmasq
```

After step 3, node-6 promotes `Observer → CVV` at the **next epoch boundary**
(epoch duration is ~60s). See [Stopping, restarting & chaos-testing](#stopping-restarting--chaos-testing)
for restarting a single node and the chaos loop.

## What each step does (and the gotchas)

**1. `local-testnet.sh --start --dev-funds … --relay-dns` (+ `MULTI_LISTEN=1`)**
- Generates genesis (owner = RLS minter = `--dev-funds`), starts 4 validators, a
  per-validator relay (primary + backup), and a split-horizon dnsmasq:
  - **inside/private view** on `:5353` → **direct** `127.0.0.1` records (base validators mesh directly)
  - **public view** on `:5354` → **relay circuit** records (how an outsider reaches the committee)
- `MULTI_LISTEN=1` makes each validator additionally open a **direct listener on
  `0.0.0.0`** (primary `40000+i`, worker `41000+i`) alongside its relay reservation.
- **Gotcha — genesis is created only once.** If `local-validators/` already
  exists the script *skips* config and **ignores `--dev-funds`**, reusing the old
  owner. To change owner/regenerate: `killall rayls-network rayls-relay dnsmasq;
  rm -rf etc/test-network/local-validators`, then re-run.

**2. `DNSMASQ_PORT=5354 add-relay-node.sh 6`**
- Starts relay-6, keygens node-6 with a **deterministic operator address** derived
  from the index (`OPERATOR_KEY = 0x(1000+index)`, address via `cast`) baked into
  its proof-of-possession, copies genesis, and starts the node pointed at the
  **public DNS view** (`:5354`) so it reaches the committee over relays.
- It **does not stake** — the node just follows as an **observer** (`not-in-committee`).
- Restart-safe: re-running reuses the datadir (no re-keygen, no re-stake).

**3. `stake-relay-node.sh 6`**
- **Readiness gate** first: polls until the RLS proxy is wired (ERC-1967 impl slot
  ≠ 0) **and** the ConsensusRegistry has an owner — right after `--start` these
  aren't live yet, which caused the confusing early reverts.
- Then, on-chain: fund native gas → **mint 5e24 RLS** to the operator (admin holds
  `MINTER_ROLE`) → **allowlist** the operator (owner-only) → operator **approves**
  the registry → **stake** → **activate**.
- `ADMIN_PRIVATE_KEY` must be the key of the `--dev-funds` account (owner+minter).

## Stopping, restarting & chaos-testing

**Whole network down:** `killall rayls-network rayls-relay dnsmasq` (add
`rm -rf etc/test-network/local-validators` to wipe state for a fresh genesis).

**Stop / restart a single node.** Two node kinds, two toolchains — but both are
env-self-contained, so a restart never loses the relay/DNS env (a hand-restarted
node instead resolves committee `/dnsaddr` via the system/public resolver, gets
NXDomain, and can't rejoin):

| | base (genesis) validator | dynamically-added node |
|---|---|---|
| stop | `local-testnet.sh --stop-validator <SEQ>` | `stop-relay-node.sh <N>` |
| start | `local-testnet.sh --start-validator <SEQ> [flags]` | `add-relay-node.sh <N>` |
| index | `SEQ` 0-based (`1`=validator-2) | `N` = the add-relay index (`6`) |
| scope | validator **+ its two relays** | node **+ its relay** |

- `--stop-validator` / `--start-validator` also **manage that validator's relays**
  (scrap on stop, revive on start). `--start-validator` rebuilds the *same*
  `RAYLS_DNS_SERVER` + relay reservations the `--start` loop used — but you must
  pass the **same mode flags** the network was started with, else the env comes out
  empty:
  ```bash
  MULTI_LISTEN=1 ./etc/test-network/local-testnet.sh --start-validator 1 --relay-dns
  ```
- `add-relay-node.sh` is restart-safe (reuses the datadir, revives the relay, no
  re-keygen/re-stake) and sets its own DNS env; pass `DNSMASQ_PORT=5354` as on the
  first add.
- **Shutdown semantics:** a consensus node is stopped with SIGTERM and **waited on
  indefinitely — no `kill -9`**, so a hung graceful shutdown blocks (and is caught)
  instead of being silently masked. Relays are stateless, so they get SIGTERM then
  `kill -9` if they linger.

**Chaos-test rejoin** with `fork_test_configs/bounce-node.sh` — it waits until the
node reports `is_caught_up`, then loops stop → restart, exercising the
catch-up/rejoin path:
```bash
# base validator (pass the net's mode flags):
RELAY_DNS=1 MULTI_LISTEN=1 ./fork_test_configs/bounce-node.sh 1
# dynamically-added node:
ADDED=1 ./fork_test_configs/bounce-node.sh 6
```
If it parks on `still shutting down after Ns…`, that's a real hung shutdown — look
at the node's log; it won't force-kill.

## Ports (node N; base validators use 9100+i etc.)

`INSTANCE = 100+N`; RPC `= 8545-(INSTANCE-1)`, WS `= 18556-(N-1)`, consensus
metrics `= 19100+(N-1)`, relay `= 50000+(N-1)`.

| node | RPC (http) | WS | consensus metrics | relay |
|---|---|---|---|---|
| validator-1..4 | 8545..8542 | 8556..8553 | 9100..9103 | 50000..50003 |
| relay-node-6 | 8440 | 18551 | 19105 | 50005 |

## Observing the mesh

No dedicated peers RPC yet — use the consensus Prometheus metrics:
```bash
curl -s http://127.0.0.1:19105/metrics \
  | grep -E '^(connected_peers|connections_by_path|peer_scores)'
```
- `connected_peers{peer_id,kad_type}` — live connections (gauge), lists **relay**
  and **validator** peer ids together.
- `connections_by_path{path,kad_type}` — **cumulative counter** by transport
  (`circuit` / `relay_direct` / `direct_nonrelay`); not a live count.

A relayed member holds ~`2·(committee size)` connections (a circuit to each peer
**plus** a direct leg to that peer's relay) + its own relay reservation, so counts
look high vs a direct peer — expected for the relay path.

## Troubleshooting

- **`identify_node_mode: … mode=Observer reason=not-in-committee`** — expected
  until the node is staked. Not an error.
- **`stake` reverts `ERC20InsufficientBalance` / `InsufficientAllowance`, or
  `mint`/`allowlist` fail** right after `--start` — the genesis system contracts
  weren't live yet (RLS proxy impl `0x0`). The readiness gate now waits; if you
  bypass it, just retry after ~a few seconds / the first epoch.
- **`OwnableUnauthorizedAccount(...)` on allowlist**, or `mint` no-op — the admin
  key isn't the current chain's owner/minter. It must equal `--dev-funds`; if you
  changed `--dev-funds` without wiping `local-validators/`, the old owner is still
  in effect (see step-1 gotcha).

## Known problems

The onboarding path *works* end-to-end here, but the dynamic-committee design has
open questions worth investigating:
- A newly-staked validator's **network address isn't on-chain** (`ValidatorInfo`
  has no multiaddr); non-genesis members get no `bootstrap_server`, so peers reach
  them via **DHT (`BLS→NodeRecord`) only** — unverified end-to-end.
- **Quorum vs reachability**: a counted-but-unreachable member can stall quorum.
- Leave/unstake path, and the live `Observer→CVV` promotion, both need verifying.
