# Testbed

`testbed.sh` starts a local Rayls network (4 validators as native processes) and runs test plans
against it. It works the same on a laptop and on the Proxmox VM. Chain id, accounts and ports are
fixed.

**Requirements**

- A Rust toolchain, or `RAYLS_BIN` pointing to a prebuilt binary.
- Foundry `cast` and `curl`.
- `jq`, for the ERC-20 test.
- [tps-checker](https://github.com/raylsnetwork/tps-checker) next to this repo (`../tps-checker`),
  for load, soak and the TPS test. If it isn't there, the testbed clones and builds it.

## Usage

```sh
./etc/testbed/testbed.sh up                  # build, genesis on first run, start 4 validators
./etc/testbed/testbed.sh test                # basic plan, ~30s
./etc/testbed/testbed.sh test chaos          # stop / pause / restart scenarios, ~5 min
./etc/testbed/testbed.sh test all            # basic, chaos, extended, then basic and chaos again, ~15 min
./etc/testbed/testbed.sh test chaos/02-lose-quorum   # a single test
./etc/testbed/testbed.sh down                # stop with SIGTERM; `up` resumes the same chain
./etc/testbed/testbed.sh clean               # stop and delete all data

LOAD=1 ./etc/testbed/testbed.sh test chaos   # any plan with steady tps-checker load underneath
SOAK_MINUTES=720 ./etc/testbed/testbed.sh test soak 2>&1 | tee soak.log   # overnight (run in tmux)
HTTP_ADDR=0.0.0.0 ./etc/testbed/testbed.sh up   # on the VM: RPCs reachable at <vm-ip>:8545..
```

## Commands

| Command | What it does |
|---|---|
| `up` | Build, run genesis if needed, start all nodes, wait for the first block |
| `down` / `clean` | Stop all nodes (SIGTERM) / stop them and delete the data |
| `status` | PID, RPC, block, epoch and role of each node |
| `start\|stop\|pause\|resume <node>` | One node: `1`..`5` or `rpc`. `stop` = SIGTERM, `pause`/`resume` = SIGSTOP/SIGCONT |
| `add-validator` | Add validator-5 and stake it into the committee (fund, mint RLS, allowlist, stake, activate) |
| `add-rpc` | Add an RPC (observer) node |
| `logs <node>` | Follow a node's log (`data/<node>.log`) |
| `load` | Steady tps-checker load until Ctrl-C (`data/load.log`) |
| `test [plan\|plan/test ...]` | Run plans or single tests (default `basic`) |

## Test cases

Each test is one small file in `tests/<plan>/` and runs in file order.

| Plan | Test | Checks |
|---|---|---|
| basic | `01-liveness` | Every node is up and its chain grows |
| | `02-consistency` | All nodes have the same block hashes |
| | `03-node-status` | Validators `active_cvv`, rpc `observer`, all caught up, same epoch |
| | `04-transfer` | A transfer sent through each node lands, and all nodes see it |
| | `05-restart` | Restart one validator; the chain continues and the node catches up |
| | `06-epoch` | An epoch closes and its certified record is served |
| chaos | `01-pause` | Freeze one validator (stands in for a partition), then resume it |
| | `02-lose-quorum` | Stop enough validators to break quorum; the chain halts, then resumes without a fork |
| | `03-flicker` | 5 quick freeze/resume cycles |
| | `04-rolling-restart` | Restart every validator in turn; the chain never stops |
| | `05-full-restart` | Stop and start all nodes; the chain resumes from the same height |
| | `06-rapid-restart` | Stop and start one validator 3 times; the stop lands while it is still starting |
| | `07-stop-during-sync` | Stop a validator while it is catching up |
| | `08-long-downtime` | Down for 2 epochs; it syncs the missed epochs and rejoins the committee |
| | `09-stop-at-epoch-boundary` | Stop a validator right after an epoch change |
| | `10-restart-loop` | Stop/start once caught up, `CYCLES` times |
| extended | `01-add-validator` | validator-5 is staked, becomes `active_cvv`, and the committee has 5 members |
| | `02-rpc-node` | RPC node syncs from genesis, relays a transfer, survives a restart |
| | `03-faults-with-new-nodes` | `chaos/01` and `chaos/02` with 5 validators and the RPC node |
| | `04-erc20-precompile` | `etc/test-network/erc20_precompile_test.sh` against validator-1 |
| | `05-tps` | tps-checker burst; every transfer is accepted and lands on-chain (checked by nonces) |
| soak | `01-faults-under-load` | Steady load, and each round stops or freezes a random node (not validator-1). Stops at the first fork, stall or load gap |
| rust | `e2e`, `chaos` | The `e2e-tests` and `chaos-framework` crates. `chaos` needs Linux and root |

After `extended` adds validator-5 and the RPC node, every plan includes them.

## Settings

| Variable | Default | Purpose |
|---|---|---|
| `RAYLS_BIN` | builds `target/release/rayls-network` | Use this binary instead of building |
| `TESTBED_DATA` | `etc/testbed/data` | Data and log directory |
| `EPOCH_SECS` | `30` | Epoch length; applied at genesis |
| `HTTP_ADDR` | `127.0.0.1` | RPC bind address (`0.0.0.0` on the VM) |
| `LOG_LEVEL` / `NODE_ARGS` / `GENESIS_ARGS` | `vvv` / - / - | Node verbosity and extra flags |
| `STOP_TIMEOUT` | `120` | Seconds a node gets to exit after SIGTERM |
| `LOAD_TPS` / `LOAD_WALLETS` | `200` / `4` | Background load rate and wallet count |
| `COLLIDE_SECS` | `10` | Average seconds between deliberate nonce collisions; `0` = off |
| `LOAD_CYCLE_SECS` | `0` | Restart tps-checker every N seconds; `0` = only when it exits |
| `SOAK_MINUTES` / `SOAK_INTERVAL` | `480` / `60` | Soak length and seconds between rounds |
| `TPS_TXS` / `CYCLES` | `3000` / `3` | Burst size in `extended/05`; loops in `chaos/10` |
| `TPS_CHECKER_DIR` / `TPS_CHECKER_BIN` | `../tps-checker` | Where tps-checker lives |

## Notes

- **No SIGKILL, ever.** Nodes are stopped with SIGTERM and must shut down cleanly. If a node is
  still running after `STOP_TIMEOUT`, the stop fails. The node is left running for inspection, and
  `clean` refuses to delete its data.
- **Load.** tps-checker runs in continuous mode through validator-1. Its wallets come from the test
  mnemonic starting at index 10, are funded up front, and pay twice the base fee. If tps-checker
  exits, it is restarted.
- **Nonce collisions.** Every ~`COLLIDE_SECS`, `cast` sends a transfer from the same wallet
  tps-checker is using. Rejections in `load.log` are expected; the chain must not care.
- **Network.** RPC ports: validator-N is on `8546-N` and rpc on `8540`. Consensus metrics:
  validator-N is on `9099+N`. The dev/admin account is anvil #0 (`0xf39F…2266`). Validator
  operators are anvil #5–#9.
- **Rust plan.** `rust/e2e` stops the testbed while it runs, because it uses ports 8545–8542. Run
  `rust/chaos` with the testbed stopped, via `sudo -E env "PATH=$PATH" ./etc/testbed/testbed.sh test rust/chaos`.
  Afterwards it hands `target/` back to your user.
- **One run at a time.** `data/test.lock` stops a second `test`, `down` or `clean` while a run is
  active. Killing the runner also stops its test and load.
- **Adding a test.** Drop a file into `tests/<plan>/`. Its first line is a `# description`. Each
  test runs with `set -e` and `lib.sh` loaded, using helpers such as `wait_blocks`, `wait_synced`,
  `check_consistency`, `stop_node`, `fail` and `skip`. After a failure, the runner restores the
  nodes before the next test.
