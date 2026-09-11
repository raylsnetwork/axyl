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
#    Knobs shown at their single-host defaults (loopback). To let a node join from ANOTHER machine,
#    set DNSMASQ_BIND=0.0.0.0 (serve DNS off-host) and RELAY_PUBLIC_HOST=<this-host-IP> (advertise
#    the relays at a reachable IP in the public :5354 records). MULTI_LISTEN_BIND stays loopback.
DNSMASQ_BIND=127.0.0.1 RELAY_PUBLIC_HOST=127.0.0.1 MULTI_LISTEN=1 MULTI_LISTEN_BIND=127.0.0.1 \
  ./etc/test-network/local-testnet.sh --start --dev-funds "$DEV_FUNDS" --relay-dns

# 1b. (cross-host only) bundle the genesis a joiner needs; scp the .tgz to the other machine and
#     run the extract command it prints there.
./etc/test-network/local-testnet.sh --export-join-bundle

# 2. add node 6 as a relayed OUTSIDER (resolves the committee via the public/relay DNS view).
#    Hosts shown at single-host defaults. From ANOTHER machine set BOTH: DNSMASQ_HOST=<committee-host-IP>
#    (resolver to reach the committee) and RELAY_HOST=<this-host-IP> (advertise THIS node's relay at a
#    reachable IP so the committee can dial it back). RELAY_HOST must be set on the FIRST add — the
#    relay address is baked at keygen and won't change on restart.
DNSMASQ_HOST=127.0.0.1 RELAY_HOST=127.0.0.1 DNSMASQ_PORT=5354 ./etc/test-network/add-relay-node.sh 6

# 3. stake it into the committee (waits for the chain to be ready, then mint→allowlist→approve→stake→activate)
#    RPC_URL must point at a synced node's RPC. Use node-6's own port so it works on either host:
#    INSTANCE=100+N, so RPC = 8545-(INSTANCE-1) = 8440 for N=6. (The script's default 8545 is only a
#    base committee member — absent on a machine that runs only the joined node.)
RPC_URL=http://localhost:8440 ADMIN_PRIVATE_KEY="$DEV_FUNDS_KEY" ./etc/test-network/stake-relay-node.sh 6

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
  - both resolvers bind **`DNSMASQ_BIND` (default `127.0.0.1`, loopback only)**; set
    `DNSMASQ_BIND=0.0.0.0` to serve the `/dnsaddr` records to another machine that points its
    `RAYLS_DNS_SERVER` here.
  - **Why `/dnsaddr`, not `/dns4`.** These records must carry *whole* multiaddrs, not just a
    resolved IP. `/dns4/host/udp/PORT/quic-v1/p2p/X` only swaps `host`→IP inside a **fixed address
    shape**, so it cannot express a **relay circuit**
    (`…/p2p/<relay>/p2p-circuit/p2p/<validator>`) or return **several** relays for failover.
    `/dnsaddr` resolves the `_dnsaddr.<name>` **TXT** records to one or more *complete* multiaddrs
    (a direct `/ip4/…` **or** a full circuit), which is exactly what the split-horizon views
    (direct vs circuit) and multi-relay failover need. It's also required for correctness: a
    `/p2p-circuit` address makes the relay client dial **through the relay** (classified as
    relayed), whereas a plain `dns4` IP would be dialed **directly on QUIC**, bypassing the relay.
- `MULTI_LISTEN=1` makes each validator additionally open a **direct listener**
  (primary `40000+i`, worker `41000+i`) alongside its relay reservation. It binds
  **`MULTI_LISTEN_BIND` (default `127.0.0.1`, loopback only)** — matching the direct
  `127.0.0.1` dnsaddr records, so co-located nodes mesh directly while the listener is
  never exposed on an external interface (cross-host reach must go via a relay). Set
  `MULTI_LISTEN_BIND=0.0.0.0` to bind all interfaces instead.
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

## Onboarding an observer (no staking)

A pure follower — **never in the committee, never counted toward quorum**, so safe to inject into a
live chain. No relay in front: it dials the committee directly, or — if the committee is advertised
via `/dnsaddr` — through their relays, making no reservation of its own. First fetch the network
files (`genesis.yaml`, `committee.yaml` → `<datadir>/genesis/`, `parameters.yaml` → `<datadir>/`)
from a committee member — the script bails with the exact paths if they're missing (cross-host: use
the `--export-join-bundle` tarball).

```bash
# add/start observer 7. The DNSMASQ_* env is used only if committee.yaml uses /dnsaddr (then it
# points at the public relay view; from another machine set DNSMASQ_HOST=<committee-host-IP>).
# Re-run the same command after a kill to restart.
DNSMASQ_HOST=127.0.0.1 DNSMASQ_PORT=5354 ./etc/test-network/add-observer.sh 7
# stop it: kill $(cat etc/test-network/local-validators/observer-7.pid)
```

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
# base validator (pass the net's mode flags). Always resolves the committee via the private/direct
# view (5353) -- build_relay_env pins it, so DNSMASQ_PORT is NOT honored here; base always meshes direct.
RELAY_DNS=1 MULTI_LISTEN=1 ./fork_test_configs/bounce-node.sh 1
# dynamically-added node — always set DNSMASQ_PORT explicitly, it decides the transport on restart:
#   DNSMASQ_PORT=5353 -> the bounced node resolves the committee to DIRECT addresses and connects
#                        directly (the goal when testing the direct path);
#   DNSMASQ_PORT=5354 -> it resolves to relay circuits and MUST reach the committee THROUGH their
#                        relays (the goal when testing the relay path).
# Use the SAME view you added it with, or the node silently switches transport across the bounce.
ADDED=1 DNSMASQ_PORT=5354 ./fork_test_configs/bounce-node.sh 6
```
If it parks on `still shutting down after Ns…`, that's a real hung shutdown — look
at the node's log; it won't force-kill.

**Bounce a single *relay* (not the node)** with `relay-ctl.sh` — the quickest way to test relay
failure/recovery. It keeps the relay's peer id stable, so the fronted validator re-reserves on its
own (`retry_relay_reservations`, ~15s) with no node restart:
```bash
./etc/test-network/relay-ctl.sh stop 6        # kill relay-node-6's relay (or validator-6's primary)
./etc/test-network/relay-ctl.sh start 6       # respawn it — watch relay-6.log for "reservation accepted … renewed"
./etc/test-network/relay-ctl.sh restart 6     # stop + start
./etc/test-network/relay-ctl.sh restart 1 --backup   # a base validator's *backup* relay (port 51000+i)
```
`N` = the validator/added-node index. Primary relay: port `50000+(N-1)`, seed byte `N`; `--backup`:
port `51000+(N-1)`, seed byte `0xb0+(N-1)`. While a relay is down, a base validator fails over to
its backup; an added node (single relay) stays reachable only via its own outbound links until the
relay returns.

## Split topology: relays on a separate host (migration example)

Validators on **hostA**, relays on **hostB**. The script always spawns relays on the host it runs
on and `RELAY_HOST` only sets the *advertised* IP, so a split setup needs `RELAY_SPAWN=0` on hostA
(skip the local relay spawn, still wire the addresses) plus relays started by hand on hostB. This
example first brings the network up co-located on hostA, then flips the advertised relay IP to
hostB and relaunches against the remote relays.

```bash
# === ON HOSTB (10.10.0.10): run the relays there (matching seeds -> matching peer ids) ===
# relay-ctl.sh needs the local-validators dir to exist for its pidfiles/logs.
rm -rf etc/test-network/local-validators
mkdir -p etc/test-network/local-validators/
BUILD_CONFIG=debug ./etc/test-network/relay-ctl.sh start 1   # relay-1 @ 10.10.0.10:50000
BUILD_CONFIG=debug ./etc/test-network/relay-ctl.sh start 2   # relay-2 @ 10.10.0.10:50001
BUILD_CONFIG=debug ./etc/test-network/relay-ctl.sh start 3   # relay-3 @ 10.10.0.10:50002
BUILD_CONFIG=debug ./etc/test-network/relay-ctl.sh start 4   # relay-4 @ 10.10.0.10:50003
# (drop BUILD_CONFIG=debug for a release build)

# === ON HOSTA (172.16.19.19): bring the network up, then migrate it to the hostB relays ===

# 1. initial bring-up, co-located (relays spawned locally on hostA, advertised at hostA's IP)
RELAY_HOST=172.16.19.19 ./etc/test-network/local-testnet.sh --start \
    --dev-funds 0x57b9D26eF4a6d4738E17932AC4d0191EfE6dBc88 --relay

# 2. stop the validators (leave them down while you flip addresses)
killall rayls-network      # validators first; kill hostA's local relays too if any: killall rayls-relay

# 3. flip the advertised relay IP hostA->hostB in EVERY node's committee.yaml + node-info.yaml.
#    Each node reads its OWN <datadir>/genesis/committee.yaml (re-loaded each epoch), so the shared
#    staging copy is NOT enough -- edit them all.
find etc/test-network/local-validators/ -name "committee.yaml" | xargs sed -i 's/172.16.19.19/10.10.0.10/g'
find etc/test-network/local-validators/ -name "node-info.yaml"  | xargs sed -i 's/172.16.19.19/10.10.0.10/g'

# 4. relaunch WITHOUT spawning local relays (RELAY_SPAWN=0) -- validators now reserve on / dial the
#    hostB relays. --start reuses the existing datadir (it skips config when local-validators exists),
#    so keys/genesis/DB are preserved; it just relaunches.
RELAY_SPAWN=0 RELAY_HOST=10.10.0.10 ./etc/test-network/local-testnet.sh --start \
    --dev-funds 0x57b9D26eF4a6d4738E17932AC4d0191EfE6dBc88 --relay

# 5. wait for peers to shift to the new addresses (reservations re-established on hostB, mesh reforms).

# --- diagnostics: which relay addresses each validator actually dials (should be 10.10.0.10) ---
for i in 0 1 2 3; do curl -s localhost:910$i/metrics | grep peer_addr | grep -v '#' | grep primary; done
```

Gotchas (learned the hard way):
- **One relay identity per host.** Relays share peer ids across hosts (same seeds); libp2p merges all
  addresses it learns for a peer id (config + `identify`), so a stray hostA relay makes validators
  also dial its `172` address -> reservations/circuits land on the wrong copy (`NoReservation`). Use
  `RELAY_SPAWN=0` and `killall rayls-relay` on hostA -- don't just "ignore" the local relays.
- **`--start` skips config if `local-validators/` exists** ("directory already exists"). It never
  regenerates on a re-run; it relaunches the on-disk files. `rm -rf local-validators` for a truly
  fresh network.
- **Edit every per-node `committee.yaml`**, not the shared staging copy -- that's the one loaded.
- `RELAY_B_PEER_IDS[i]` (in `local-testnet.sh`) lets `RELAY_SPAWN=0` point backups at real remote
  backup relays; left empty, the backup reservation just reuses the primary relay.

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
