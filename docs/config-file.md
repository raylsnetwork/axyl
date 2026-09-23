# Hardfork Config File (`--config-file`)

How to tell an Axyl node which hardfork schedule to run, using a small YAML file instead of
the schedule built into the binary. This is the normal way to run a **private chain**: your
network is not one of the four public Rayls networks, so the binary has no schedule for it.

The template lives next to this doc: [`config-file.example.yaml`](./config-file.example.yaml).

---

## 1. What the file is

Every Axyl node needs two things at start:

1. **The datadir** — genesis, `parameters.yaml`, committee, node keys. Produced by the
   `genesis` ceremony and `keytool`. This never changes with the config file.
2. **A hardfork schedule** — at which block each protocol change turns on. Without the file
   this comes baked into the binary, one schedule per public network (`mainnet`, `testnet`,
   `devnet`, `local`).

The config file externalizes only the second thing. One file can hold any number of named
networks (called *subnets* in the flags). Each subnet has a `chain_id` and a `hardforks` map.

```yaml
networks:
  my-chain:                     # subnet name, anything you like
    chain_id: 9001              # must equal the chain-id in the datadir's genesis
    hardforks:
      Eip1559: 0                # active from genesis
      BatchDigestV2: 0
      PrecompileGasFix: 0
      EmptyOutputBlock: 0
      DynamicCommitteeSizing: 0
      HybridRewards: 1          # activates at block 1 (see section 5 for why not 0)
      AdminTransfer: never      # never activates
      # ... one line per fork, see the template for the full list
```

Rules the node enforces when it reads the file:

| Rule | What happens if broken |
|---|---|
| `chain_id` is required | file fails to parse, node does not start |
| `hardforks` must not be empty | node refuses to start with a clear message |
| Every fork name must be a real Rayls hardfork | node refuses to start and lists the known names |
| Fork names are case-insensitive | `eip1559` and `Eip1559` both work |
| A value is a block number or `never` | anything else is a parse error |
| A fork missing from the map | treated as `never` |

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
validator or observer, must run the **same schedule**. Different schedules on different nodes
split the chain.

Flag rules:

- `--config-file` requires `--subnet`, and the other way round.
- `--config-file` cannot be combined with `--network` (or the `RAYLS_NETWORK` env var). Pick
  one schedule source.
- While the file is active, the `network:` field in the datadir's `parameters.yaml` is ignored.

A working example is the local test network: `etc/test-network/config.yaml` holds all four
public schedules, and `etc/test-network/start-local-validator-config.sh` starts a validator
with `--config-file ./config.yaml --subnet local`.

---

## 3. When to use it

**Use the config file when:**

- **You run a private chain.** The `genesis` command accepts any `--chain-id`. The resulting
  datadir has `network: null` in `parameters.yaml` ("external", no built-in schedule), so the
  node will not start until you give it a schedule. The config file is the intended way to do
  that. Start from the template, set `chain_id` to the value you passed to `genesis`, and keep
  the fork values the template ships with unless you have a reason to change them. The
  template behaves like the baked-in `local` schedule: every current protocol feature is on
  from the start, and the one-off fixes written for the public networks are off.
- **You need to activate a hardfork on a running private chain.** Edit the file, set the fork
  to a future block number, distribute the file to every node operator, and have everyone
  restart before that block. No new binary and no re-genesis needed, as long as the binary
  already knows the fork.
- **You replay history.** A replay must use the schedule the network *actually* ran, which is
  not always the intended one. See `bin/rayls-replay/README.md`.

**You do not need it when:**

- You run a node on **mainnet, testnet or devnet** with a datadir provisioned for that network.
  The baked-in schedule is selected by `network:` in `parameters.yaml`, or by `--network` /
  `RAYLS_NETWORK`. Nothing changes for these nodes.

---

## 4. Chain name and chain-id checks

Two different things are called "network" around the node. Keep them apart.

### The subnet name is yours

The key under `networks:` is just a label. `my-chain`, `prod`, `client-a-eu` are all fine.
The node only uses it to pick the entry that `--subnet` names. It does **not** have to match
one of the public network names, and calling a subnet `mainnet` does not make it mainnet.

### The chain-id is checked, every boot

At every start the node compares two numbers and refuses to run if they differ:

- the `chain_id` in the datadir's genesis file, and
- the `chain_id` of the schedule source you selected.

The schedule source is resolved in this order:

1. `--config-file` + `--subnet`: the subnet's `chain_id`.
2. Otherwise `--network` / `RAYLS_NETWORK`, or `network:` in `parameters.yaml`: that public
   network's fixed chain-id.
3. Otherwise (datadir has `network: null` and no flags): the node stops and asks for one of
   the above.

Fixed chain-ids of the public networks:

| Network | Chain-id |
|---|---|
| `mainnet` | 72957 |
| `testnet` | 7295799 |
| `devnet` | 503 |
| `local` | 487 |

On a mismatch the node exits with:

```
datadir chain-id 9001 does not match the expected chain-id 487 from network 'local'.
The datadir appears to belong to a different network or client. Use a datadir whose
genesis chain-id is 487, or select a schedule source whose chain-id is 9001.
```

This check exists because running the wrong schedule against a datadir silently produces
wrong state. Common causes:

- **Wrong `chain_id` in the file.** Fix the file to match the genesis. Never change the
  genesis to match the file on a chain that already has blocks.
- **Wrong `--subnet`.** You selected another client's or another environment's entry.
- **Wrong datadir.** You pointed the node at a datadir provisioned for a different chain.
- **Using `--network` for a private chain.** `--network local` only works when your genesis
  chain-id is 487. For any other id you must use the config file.

Pick your private chain-id so it does not collide with the four above, or with public EVM
chains your users' wallets already know.

---

## 5. Choosing fork values for a new private chain

You only need this section if you want to deviate from the template.

- `0` means "active from genesis". Use it for the protocol features you want from day one.
- `never` means the fork does not exist on your chain. Use it for one-off fixes that were
  written for a specific public network's history (for example `RlsStorage`,
  `UsdrSupplyCorrection`, `AdminTransfer`).
- A positive block number schedules an activation in the future.

One detail matters for the *migration* forks (`AdminTransfer`, `RlsStorage`, `Tokenomics`,
`Uups`, `Erc20PrecompileBytecode`, `UsdrSupplyCorrection`, `HybridRewards`). These run a
one-time state change when the chain crosses their activation block. A migration set to `0`
is considered already active at genesis, so its state change **never runs**. If you want a
migration to actually execute on a fresh chain, set it to `1` or later. This is why the
template sets `HybridRewards: 1` and not `0`: genesis deploys the old reward contract, and
the swap to the hybrid-reward contract only happens if the migration runs.

The remaining forks (`Eip1559`, `BatchDigestV2`, `PrecompileGasFix`,
`TransactionLoadBalancing`, `EmptyOutputBlock`, `DynamicCommitteeSizing`,
`OutputSeqNormalization`, `SenderAffinityLoadBalancing`) are behavior switches. `0` is safe
for them.

### Template vs. the baked-in `local` schedule

The template is not a byte-for-byte copy of `local`, but on a fresh chain it behaves the
same. The values that differ have no effect on a new chain:

| Fork | `local` | template | Why the template value is fine |
|---|---|---|---|
| `AdminTransfer` | 0 | never | migration at block 0 never runs anyway |
| `TransactionLoadBalancing` | 0 | never | ignored while `SenderAffinityLoadBalancing` is active |
| `UsdrSupplyCorrection` | 100 | never | fixes historical supply drift; nothing to fix on a new chain |
| `Erc20PrecompileBytecode` | 1 | never | the node re-seeds the precompile bytecode itself on every call into it |

Everything else matches, including the three forks that do change behavior:
`EmptyOutputBlock: 0`, `DynamicCommitteeSizing: 0` and `HybridRewards: 1`.

Once the chain has produced blocks, treat the file as append-only: change `never` to a future
block to activate a fork, but never move an activation into the past or change a fork that is
already active.

---

## 6. Checklist for a private deployment

1. Run the `genesis` ceremony with your `--chain-id`.
2. Copy `docs/config-file.example.yaml` to a real file, rename the subnet, set `chain_id` to
   the same value.
3. Ship the datadir and the config file to every validator and observer.
4. Start every node with `--config-file <file> --subnet <name>`. Do not set `RAYLS_NETWORK`.
5. To activate a fork later: agree on a block number, update the file everywhere, restart all
   nodes before that block.
