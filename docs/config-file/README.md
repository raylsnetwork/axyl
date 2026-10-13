# Starting a private chain with `--config-file`

How to run a **private chain** — a network that is not one of the public Rayls
networks — by giving the node a small YAML file with its hardfork schedule
instead of the schedule baked into the binary. Your network is not one of the
public networks, so the binary has no schedule for it; the file supplies it.

Two companion files:

- [`config-file.example.yaml`](./config-file.example.yaml) — an actual, loadable
  config file. Copy it, change `chain_id`, start the node. This is the template
  this doc points at.
- [`hardforks/README.md`](../hardforks/README.md) — what the hardforks are, what
  each one does, and why the pinned values are pinned. This doc only explains how
  the file is used; the fork model lives there.

---

## 1. What the file is

Every Axyl node needs two things at start:

1. **The datadir** — genesis, `parameters.yaml`, committee, node keys. Produced by
   the `genesis` ceremony and `keytool`. This never changes with the config file.
2. **A hardfork schedule** — at which block each protocol change turns on. Without
   the file this comes baked into the binary, one schedule per public network.

The config file externalizes only the second thing. One file can hold any number
of named networks (called *subnets* in the flags). Each subnet has a `chain_id`
and a `hardforks` map — one entry per fork, a block number or `never`. See the
template for the exact shape: [`config-file.example.yaml`](./config-file.example.yaml).

Rules the node enforces when it reads the file:

| Rule | What happens if broken |
|---|---|
| `chain_id` is required | file fails to parse, node does not start |
| `chain_id` must not be mainnet (72957) or testnet (7295799) | node refuses to start; those networks always run their baked-in schedule via `--network` (section 4) |
| `hardforks` must not be empty | node refuses to start with a clear message |
| Every fork name must be a real Rayls hardfork | node refuses to start and lists the known names |
| Every fork the binary knows must be defined | node refuses to start and names the missing fork |
| Fork names are case-insensitive | `eip1559` and `Eip1559` both work |
| A value is a block number or `never` | anything else is a parse error |

Because every known fork must be listed, a binary upgrade that adds a fork makes an
old file stale: the node will not start until the new fork has a line. Section 2
shows how to get the full, current fork list out of the binary you are deploying.

---

## 2. How to use it

Start the node with both flags. `--subnet` picks one entry from the file.

```sh
rayls-network node \
    --datadir /var/lib/rayls \
    --config-file /etc/rayls/networks.yaml \
    --subnet my-chain \
    ...other flags as usual...
```

Observers use exactly the same flags (add `--observer`). Every node in the network,
validator or observer, must run the **same schedule**. Different schedules on
different nodes split the chain.

Flag rules:

- `--config-file` requires `--subnet`, and the other way round.
- `--config-file` cannot be combined with `--network` (or the `RAYLS_NETWORK` env
  var). Pick one schedule source.
- While the file is active, the `network:` field in the datadir's `parameters.yaml`
  is ignored.

### Getting a starting file from the binary: `schedule export`

Every `rayls-network` binary can print the hardfork schedule it has built in for a
network, already shaped as a config file:

```sh
rayls-network schedule export --network <mainnet|testnet|devnet|local> [--subnet-name <name>]
```

It writes one subnet to stdout, with that network's `chain_id` and **every** fork
this binary knows, under its canonical name. For a private chain, exporting the
`local` schedule is a ready-to-load starting point (then change `chain_id`):

```sh
rayls-network schedule export --network local --subnet-name my-chain > networks.yaml
# then set chain_id to your genesis chain-id
```

Why export instead of copying the template:

- **The fork list always matches the binary.** The template in this repo can fall
  behind the code. The export comes from the binary itself, so it cannot miss a
  fork or use an unknown name. Run it with the exact binary (or image) you are
  deploying.
- **Upgrading the binary.** Export again with the new binary and diff it against
  your file. Any new fork shows up as a new line. Add it to your file with the
  value you want (usually `never`, or a future block agreed with the other
  operators) before you restart the nodes.

Note: the `mainnet`/`testnet` exports are **template only** — the node refuses a
subnet with those chain-ids (section 4) and the export's header says so. To start a
private chain use the `local` (or `devnet`) export, or the template.

---

## 3. When to use it

**Use the config file when:**

- **You run a private chain.** The `genesis` command accepts any `--chain-id`. The
  resulting datadir has `network: null` in `parameters.yaml` (no built-in
  schedule), so the node will not start until you give it a schedule. The config
  file is the intended way to do that. Start from the template and change
  `chain_id` to the value you passed to `genesis`. The template's fork map is the
  canonical new-chain schedule and passes the boot shape gate as-is, so the other
  values need no change unless you have a reason to change a behavioral fork. Which
  values are pinned (must not change) and which are free is in
  [`hardforks/README.md`](../hardforks/README.md).
- **You need to activate a hardfork on a running private chain.** Edit the file,
  set the fork to a future block number, distribute the file to every node
  operator, and have everyone restart before that block. No new binary and no
  re-genesis needed, as long as the binary already knows the fork.
- **You replay history.** A replay must use the schedule the network *actually*
  ran, which is not always the intended one. See `bin/rayls-replay/README.md`.

**You do not need it when:** you run a node on a public network (mainnet, testnet,
devnet or local) with a datadir provisioned for that network. The baked-in schedule
is selected by `--network` / `RAYLS_NETWORK` or `network:` in `parameters.yaml`.

---

## 4. Chain-id checks

Two different things are called "network" around the node. Keep them apart.

### The subnet name is yours

The key under `networks:` is just a label. `my-chain`, `prod`, `client-a-eu` are
all fine. The node only uses it to pick the entry that `--subnet` names. It does
**not** have to match one of the public network names, and calling a subnet
`mainnet` does not make it mainnet.

### Public chain-ids

| Network | Chain-id | Usable as a private-chain subnet id? |
|---|---|---|
| `mainnet` | 72957 | **No** — refused at file load |
| `testnet` | 7295799 | **No** — refused at file load |
| `devnet` | 503 | Yes (not checked) |
| `local` | 487 | Yes (not checked) |

A config-file subnet may **not** declare the `mainnet` or `testnet` chain-id. The
node refuses to start at file load: those two networks always run on their baked-in
schedule via `--network`, and letting a per-client file redefine a shared network's
schedule would let one client's file change a network every node must agree on. The
`devnet` and `local` chain-ids are not subject to that check (the local test network
runs from a config file with `chain_id: 487`).

For a new private chain, pick a chain-id that collides with **none** of the four
public ids above, or with public EVM chains your users' wallets already know.

### The datadir chain-id is checked, every boot

At every start the node compares two numbers and refuses to run if they differ:

- the `chain_id` in the datadir's genesis file, and
- the `chain_id` of the schedule source you selected (the subnet's, for
  `--config-file`).

On a mismatch the node exits with an error naming both ids. This check exists
because running the wrong schedule against a datadir silently produces wrong state.
Common causes:

- **Wrong `chain_id` in the file.** Fix the file to match the genesis. Never change
  the genesis to match the file on a chain that already has blocks.
- **Wrong `--subnet`.** You selected another client's or another environment's entry.
- **Wrong datadir.** You pointed the node at a datadir provisioned for a different
  chain.

---

## 5. The fork schedule: what is pinned, what is free

The template ships the canonical new-chain schedule, so if you use it as-is (change
only `chain_id`) you are done. Of the 16 forks:

- **Seven are pinned** by the boot shape gate (`verify_schedule_shape`): the node
  refuses to start if any of them deviates. They are the migration forks whose
  values the genesis requires.
- **Nine are behavioral** and free to choose: `0` (on from genesis), `never` (off),
  or a future block (activate later — section 3). The canonical schedule turns all
  nine on from genesis.

What each fork does, and why each pinned value is pinned, is in
[`hardforks/README.md`](../hardforks/README.md). The quick list of the seven pinned
values:

| Fork | Pinned value |
|---|---|
| `AdminTransfer`, `RlsStorage`, `Tokenomics`, `Uups` | `0` |
| `Erc20PrecompileBytecode` | `1` |
| `UsdrSupplyCorrection` | `never` |
| `HybridRewards` | `1` (except the devnet chain-id `503`, which keeps `never`) |

`HybridRewards` needs no post-activation action — the migration and the ABI
switch happen automatically at the activation block. The optional governance
steps it unlocks are in
[`hardforks/README.md`](../hardforks/README.md) (Set C).

### After the chain has blocks

Once the chain has produced blocks, treat the file as append-only: change `never` to
a future block to activate a fork, but never move an activation into the past or
change a fork that is already active. The datadir keeps a `schedule-record.yaml`
pinning the schedule its executed blocks ran under. At boot the selected schedule is
verified against it: a fork that has already activated (boundary at or below the
current head) cannot be moved, removed, or back-dated — the node refuses to start,
because that schedule would re-interpret the chain's history. The refusal names the
record and its remedy: add or fix the fork's entry in the record, or delete the
record to re-record the selected schedule (the check then starts from the current
head). A boundary still in the future may be changed — that is how agreed schedule
updates ship — and the node warns about the change and re-records the new schedule.

---

## 6. Checklist for a private deployment

1. Run the `genesis` ceremony with your `--chain-id`.
2. Create the file. Either copy `docs/config-file/config-file.example.yaml`, or run
   `rayls-network schedule export --network local --subnet-name <name> > <file>`
   with the binary you will deploy (section 2). Set `chain_id` to the same value.
3. Ship the datadir and the config file to every validator and observer.
4. Start every node with `--config-file <file> --subnet <name>`. Do not set
   `RAYLS_NETWORK`.
5. To activate a fork later: agree on a block number, update the file everywhere,
   restart all nodes before that block.
6. Before a binary upgrade: run `schedule export` with the new binary and diff it
   against your file. Add any new fork to the file first, or the upgraded nodes will
   not start.
