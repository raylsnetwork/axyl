# Provisioning a Rayls validator node

This directory ships the operator workflow for adding a validator to an
existing Rayls network. It is the validator counterpart to
[`etc/observer/`](../observer/README.md) and uses the same general layout
(scripts + `.env` + a local datadir).

There are **two ways** to use `create-validator.sh`:

| Mode | Command | What it does |
|---|---|---|
| **Config-only** | `./create-validator.sh --config-only` | Generate the validator's keys and assemble a self-contained datadir, then **stop**. No on-chain transactions. Use this when the node will run on a **separate host** and/or when **staking will happen later**. |
| **Full (staking)** | `./create-validator.sh` | Same key/datadir generation, then also **funds**, **allowlists**, and **stakes** the operator address on-chain in one shot. |

The rest of the lifecycle is driven by two more scripts:

| Script | What it does |
|---|---|
| [`activate-validator.sh`](activate-validator.sh) | Submit `ConsensusRegistry.activate()` (moves the validator into `PendingActivation`); with `--start`, also launch the node so it is ready to vote when the next epoch promotes it to `Active`. |
| [`exit-validator.sh`](exit-validator.sh) | Submit `ConsensusRegistry.beginExit()` to put the validator in the exit queue at the next epoch boundary. |

The on-chain side of the lifecycle (stake → allowlist → activate → exit →
unstake) is documented in [`rayls-contracts/README.md`](../../rayls-contracts/README.md).

## What you need from the network operator

Before you start you need, from the network operator:

- A **`genesis/`** directory containing `genesis.yaml` and `committee.yaml`,
  with a sibling `parameters.yaml` one level above it. This matches the layout
  `create-validator.sh` expects:

  ```
  <some-path>/
  ├── parameters.yaml
  └── genesis/                <-- this is GENESISDIR
      ├── genesis.yaml
      └── committee.yaml
  ```

- A reachable **RPC URL** of an existing node on the network you are joining.
  *(Only needed for the on-chain steps — not for `--config-only`.)*
- An **admin key** with `MAINTAINER` / `DEFAULT_ADMIN_ROLE` permission on
  `ConsensusRegistry` (`ADMIN_PRIVATE_KEY` below). This key funds the new
  validator address and allowlists it on-chain. In typical operator setups
  this is held by the team running the network, not by the validator operator.
  *(Only needed for the full staking flow — not for `--config-only`.)*

You also need the Rust toolchain matching the workspace `rust-toolchain` /
`Cargo.toml` and, for the on-chain steps, Foundry's `cast`.

## Configuration — `.env`

```sh
cp .env.example .env
```

Populate `.env`. Which variables you need depends on the mode:

| Variable | Needed for | Description |
|---|---|---|
| `ADDRESS` | both | The validator's operator address (`0x...`) — the address that will hold the stake. Baked into `node-info.yaml`. |
| `GENESISDIR` | both | Absolute path to the directory containing `genesis.yaml` and `committee.yaml`. `parameters.yaml` is read from `${GENESISDIR}/..`. |
| `RL_BLS_PASSPHRASE` | both | Passphrase used to encrypt the BLS keystore. Leave empty to default to `"local"` (throwaway nodes only). **Set a strong value for any real node** — the *same* passphrase must be supplied whenever the node is started. See [Custom BLS passphrase](#custom-bls-passphrase). |
| `RL_EXTERNAL_PRIMARY_ADDR` | optional | Externally-reachable libp2p multiaddr for the primary worker, e.g. `/ip4/<public-ip>/udp/49001/quic-v1`. Set to your public IP so peers can dial you; leave at the `0.0.0.0` default for an outbound-only / NAT'd node. |
| `RL_EXTERNAL_WORKER_ADDRS` | optional | Externally-reachable libp2p multiaddr(s) for additional workers, e.g. `/ip4/<public-ip>/udp/49101/quic-v1`. Comma-separated for more than one. |
| `PRIVATE_KEY` | full only | Private key of the operator address (must match `ADDRESS`). Signs the stake transaction. Hex, with or without `0x`. |
| `ADMIN_PRIVATE_KEY` | full only | Key authorised to allowlist validators on `ConsensusRegistry` and to fund the new validator address. Hex, with or without `0x`. |
| `RPC_URL` | full only | RPC endpoint of an existing network node, used during the on-chain steps. If unset, the script prompts for it. |
| `STAKE_AMOUNT` | full only | Stake to lock, in wei (e.g. `1000000000000000000000000` for 1M RLS at 18 decimals). |
| `REGISTRY_CONTRACT_ADDRESS` | full only | *(Optional)* Defaults to the canonical address `0x07E17e17E17e17E17e17E17E17E17e17e17E17e1`. Override only if the operator deployed `ConsensusRegistry` elsewhere. |
| `RPC_PORT` | optional | Only used in a log line printed by `activate-validator.sh --start`. |
| `VALIDATOR` | optional | Human-readable label used in log lines for this validator (e.g. `val1`). |

The same `.env` is read by all three scripts.

> **About the BLS key:** validators **do** sign consensus messages and blocks
> with their BLS key, so the key is central to operation (not optional as it
> effectively is for an observer). The `ADDRESS` + BLS key pair is also what
> gets staked on-chain via the proof-of-possession. Keep the keystore under
> `local-validator/node-keys/` and the passphrase safe.

---

## Config-only workflow (prepare configs, run the node elsewhere)

Use this when you want to generate the validator's identity and data directory
on one machine, ship it to the host that will actually run the node, and do the
staking later.

### Step 1 — set the essentials in `.env`

For config-only you only need:

```sh
ADDRESS="0x<your operator wallet>"
GENESISDIR="/absolute/path/to/genesis"
RL_BLS_PASSPHRASE="<a strong passphrase>"
# optional, if the node should be dialable by peers:
RL_EXTERNAL_PRIMARY_ADDR="/ip4/<public-ip>/udp/49001/quic-v1"
RL_EXTERNAL_WORKER_ADDRS="/ip4/<public-ip>/udp/49101/quic-v1"
```

`PRIVATE_KEY`, `ADMIN_PRIVATE_KEY`, `RPC_URL`, and `STAKE_AMOUNT` can stay empty
— config-only never touches the chain and never prompts for them.

### Step 2 — generate the configs

```sh
./create-validator.sh --config-only
```

This will:

1. Build `rayls-network` in release mode (`cargo build --bin rayls-network --release`).
2. Create `local-validator/` next to the script (the node's `DATADIR`).
3. Run `rayls-network keytool generate validator --datadir local-validator --address ${ADDRESS}`
   to create the validator's BLS and network keys under
   `local-validator/node-keys/` and write `node-info.yaml`, encrypting the BLS
   keystore with `RL_BLS_PASSPHRASE`. If `RL_EXTERNAL_PRIMARY_ADDR` /
   `RL_EXTERNAL_WORKER_ADDRS` are set they are baked into the advertised p2p
   addresses in `node-info.yaml`.
4. Copy `${GENESISDIR}/{genesis,committee}.yaml` into `local-validator/genesis/`
   and `${GENESISDIR}/../parameters.yaml` into `local-validator/`.
5. Print a summary and **exit** — no funding, allowlisting, or staking.

If `local-validator/` already exists the script prints a skip message and
exits without regenerating. Remove the directory and re-run for a fresh
provisioning.

The resulting datadir is self-contained:

```
local-validator/
├── node-keys/               # BLS + network keys (encrypted with RL_BLS_PASSPHRASE)
├── node-info.yaml           # this node's identity + advertised p2p addresses
├── parameters.yaml
└── genesis/
    ├── genesis.yaml
    └── committee.yaml
```

### Step 3 — upload the datadir to the validator host

Copy the entire `local-validator/` directory to the target host — it contains
everything the node needs. For example:

```sh
rsync -a local-validator/ user@validator-host:/opt/rayls/local-validator/
```

Do **not** commit the datadir or share it — `node-keys/` holds the encrypted
BLS keystore.

### Step 4 — start the node on the host

Run the binary directly, pointing `--datadir` at the uploaded directory and
supplying the **same** `RL_BLS_PASSPHRASE` you used to generate the keys.
Mirror the flags that `activate-validator.sh --start` uses:

```sh
RL_BLS_PASSPHRASE="<same passphrase>" \
  ./target/release/rayls-network node \
  --datadir /opt/rayls/local-validator \
  --full \
  --storage.v2 \
  --instance 99 \
  --metrics 127.0.0.1:9109 \
  --log.stdout.format log-fmt \
  --txpool.pending-max-count 1000000 \
  --txpool.pending-max-size 1242880000 \
  --txpool.basefee-max-count 1000000 \
  --txpool.basefee-max-size 20971120000 \
  --txpool.queued-max-count 1000000 \
  --txpool.queued-max-size 20971120000 \
  --txpool.max-pending-txns 1000000 \
  --txpool.max-new-txns 1000000 \
  --txpool.minimal-protocol-fee 0 \
  --txpool.max-tx-input-bytes 999999999999 \
  -vvv \
  --http
```

Notes:

- Do **not** pass `--observer` — this is a validator, so it must be able to
  join the committee once activated.
- Always pass `--full --storage.v2`. The network runs StorageV2, so a validator
  must too; `--full` is the correct mode for a validator. `activate-validator.sh
  --start` passes both for you.
- On Docker, mount `local-validator/` as `--datadir` and pass
  `RL_BLS_PASSPHRASE` via the container environment (or
  `--bls-passphrase-source stdin` and pipe it in). Detailed Docker deployment
  instructions are provided separately by the network operator.

Until it is staked and activated (Step 5), a validator node runs in
`CvvInactive` mode and follows the chain via state-sync without voting — see
[Cold-start sequencing](#cold-start-sequencing).

### Step 5 — stake and activate (later)

When you are ready to stake, you have two options:

- **From the same machine that has the keys and `.env`** — fill in
  `PRIVATE_KEY`, `ADMIN_PRIVATE_KEY`, `RPC_URL`, `STAKE_AMOUNT`, remove
  `local-validator/` **only if you need to regenerate** (you usually don't —
  keep the existing keys), and run the on-chain steps. Because the datadir
  already exists, re-running `create-validator.sh` will skip regeneration; to
  drive staking against the *existing* keys, run the funding / allowlist /
  stake steps manually with `cast` (see [Full staking flow](#full-staking-flow-single-machine)
  for the exact calls) or regenerate on a machine that also owns the keys.
- Then run [`./activate-validator.sh`](activate-validator.sh) to submit
  `activate()`.

> Staking uses `rayls-network keytool stake-calldata` to build the
> proof-of-possession calldata from `node-info.yaml`, so it must run against
> the **same datadir** whose keys you staked. Keep `local-validator/` around.

---

## Full staking flow (single machine)

If the provisioning machine will also run the node and you want to stake
immediately, use the original one-shot flow.

### Step 1 — `./create-validator.sh`

```sh
./create-validator.sh
```

What it does (in addition to the key/datadir generation described above):

1. **Funding** — `cast send` from `ADMIN_PRIVATE_KEY` transfers
   `${STAKE_AMOUNT}` wei to `${ADDRESS}` so the operator has enough RLS to stake.
2. **Allowlisting** — `cast send` calls
   `ConsensusRegistry.allowlistValidator(address)` from `ADMIN_PRIVATE_KEY`.
3. **Stake** — runs `rayls-network keytool stake-calldata` to produce the
   ABI-encoded `stake(...)` calldata (with the proof-of-possession), then
   `cast send` submits the stake transaction signed by `PRIVATE_KEY`.

After this step the validator is **staked but not yet active** — its status on
`ConsensusRegistry` is the post-stake state, prior to `activate()`.

If `local-validator/` already exists, the script prints a skip message and
exits 0 without re-running any steps.

> Note: `create-validator.sh` accepts a `--start` flag for backwards
> compatibility but ignores it (legacy no-op). To launch the node, use
> `activate-validator.sh --start` in Step 2.

### Step 2 — `./activate-validator.sh`

```sh
./activate-validator.sh             # send activate(), don't launch the node
./activate-validator.sh --start     # send activate() AND launch the node
```

`activate-validator.sh` is **always** the step that submits
`ConsensusRegistry.activate()` — the create script does not do this. After the
transaction confirms, the validator is in `PendingActivation` and will be
promoted to `Active` at the next `concludeEpoch()` system call.

With `--start` the script also launches the node (using `RL_BLS_PASSPHRASE`
from `.env`) so it is up and following consensus when activation completes.
Use `--start` only when the provisioning machine also runs the node; for
Docker / remote-host deployments, omit it and start the node on the host as in
[Step 4](#step-4--start-the-node-on-the-host) above.

#### Cold-start sequencing

A newly-activated validator must catch up to the network's current epoch
before it can vote. While catching up it sits in `CvvInactive` mode and runs
the state-sync subscriber instead of participating directly in consensus. Once
it has caught the chain up it transitions to `CvvActive` automatically. See
[`doc/node-lifecycle.md`](../../doc/node-lifecycle.md) for the full transition
state machine.

### Step 3 (when retiring) — `./exit-validator.sh`

```sh
./exit-validator.sh
```

Sends `ConsensusRegistry.beginExit()` signed by `PRIVATE_KEY`. The validator
stays selectable in voter committees until it has been excluded from the
committee for two consecutive epochs (handled by `concludeEpoch()`); only then
is the validator moved to `Exited`. After one further epoch in `Exited`,
`unstake()` can be called to recover the stake and any accrued rewards.

The `exit-validator.sh` script does **not** stop the running node — bring it
down separately (`kill <pid>` or your service manager) once the on-chain exit
has been finalised.

---

## Custom BLS passphrase

All scripts read `RL_BLS_PASSPHRASE` from `.env`. If it is empty they fall back
to `"local"`, which is only appropriate for throwaway local nodes.

- Set a strong `RL_BLS_PASSPHRASE` in `.env` before running
  `create-validator.sh` — it encrypts the BLS keystore written under
  `local-validator/node-keys/`.
- The **same** passphrase must be supplied whenever the node starts. On the
  validator host, export `RL_BLS_PASSPHRASE` (the default
  `--bls-passphrase-source env`), or use `--bls-passphrase-source stdin` and
  pipe it in, or `--bls-passphrase-source ask` to be prompted on a TTY.
- The passphrase is read once at startup and then scrubbed from the process
  environment. It is never written to disk in plaintext.
- To change the passphrase later **without changing node identity**, use
  `rayls-network keytool rotate-passphrase --datadir local-validator`. The
  *current* passphrase comes from `RL_BLS_PASSPHRASE` (or
  `--bls-passphrase-source`); the *new* one is read from `RL_BLS_NEW_PASSPHRASE`
  if set, otherwise prompted for with confirmation. Add `--dry-run` to only
  verify the current passphrase decrypts the keystore.

## Monitoring a running validator

- Prometheus metrics are on `127.0.0.1:9109` by default (override with
  `--metrics`). The execution layer adds its own metrics; the consensus layer
  adds the `tx_*_total` counters documented in
  [`doc/crates/consensus/primary-metrics.md`](../../doc/crates/consensus/primary-metrics.md).
- A `--healthcheck <PORT>` flag exposes a TCP liveness probe; not enabled by
  the `--start` path, but recommended for Kubernetes / systemd setups.
- The standard `eth_*` JSON-RPC and the `rayls_*` namespace (see
  [`doc/crates/execution/rpc.md`](../../doc/crates/execution/rpc.md)) are served
  from `--http.addr / --http.port`.

## Troubleshooting

- **`Error: .env file not found`** — every script reads `etc/validator/.env`;
  copy from `.env.example` first.
- **`Error: ADDRESS is required`** — set `ADDRESS` in `.env` (needed in both
  modes; it is baked into `node-info.yaml`).
- **Node refuses to start / BLS decrypt error** — the passphrase supplied at
  start does not match the one used to generate the keys. Supply the same
  `RL_BLS_PASSPHRASE` (see [Custom BLS passphrase](#custom-bls-passphrase)).
- **`AllowlistValidator: caller is not allowed`** — `ADMIN_PRIVATE_KEY` does
  not hold `MAINTAINER` on `ConsensusRegistry`. Ask the network operator for
  the right key.
- **Activation transaction reverts with `not staked`** — `activate-validator.sh`
  was invoked before the stake transaction landed. Complete the staking flow
  and re-attempt activation.
