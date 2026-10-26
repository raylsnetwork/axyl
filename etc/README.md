# etc/

Developer tooling that does not ship in the published binary: Docker
images / compose files, shell scripts for local networks, operator
runbooks, and ancillary test helpers.

## Subdirectories

| Path | Purpose |
|---|---|
| [`docker-network/compose.yaml`](docker-network/compose.yaml) | Docker Compose file for the local 4-validator network (driven by `make up` / `make down`). |
| [`debug-network/`](debug-network/) | Scripts for a 4-validator local network where validator #1 is launched manually in a debugger via `.vscode/launch.json`. |
| [`docker-network/`](docker-network/) | Dockerfile + per-validator setup scripts referenced by `etc/docker-network/compose.yaml`. See [`docker-network/README.md`](docker-network/README.md). |
| [`manage/`](manage/) | `manage.sh` — orchestrates the full bring-up of a new network from scratch over SSH to a fleet of pre-provisioned machines (Docker-based). See [`manage/README.md`](manage/README.md). |
| [`observer/`](observer/) | `create-observer.sh` and supporting files for provisioning a partner-run observer. See [`observer/README.md`](observer/README.md). |
| [`state-sum/`](state-sum/) | `state-sum` Rust binary — runs the on-chain USDr supply audit (state-trie sum vs `totalSupply` slot vs event replay). See [`state-sum/README.md`](state-sum/README.md). |
| [`test/`](test/) | Cross-cutting test scripts (`edge-cases/`, `test-and-attest.sh`). |
| [`test-network/`](test-network/) | `local-testnet.sh` + helpers for a 4-validator local network. See [`test-network/README.md`](test-network/README.md). |
| [`testbed/`](testbed/) | `testbed.sh` — local 4-validator network (+ optional 5th validator and RPC node) with basic, chaos and extended test plans; laptop or Proxmox VM. See [`testbed/README.md`](testbed/README.md). |
| [`tokenomics-testing/`](tokenomics-testing/) | Helpers for exercising staking, delegation, and fee-distribution flows on a running network. |
| [`tps/`](tps/) | TPS benchmark script for the JSON-RPC layer. See [`tps/README.md`](tps/README.md). |
| [`validator/`](validator/) | Scripts for provisioning a validator and joining an existing network (`create-validator.sh`, `activate-validator.sh`, `exit-validator.sh`). |
