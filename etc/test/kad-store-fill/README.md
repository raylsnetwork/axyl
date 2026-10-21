# Live reproduction: a filled peer-discovery store stops a newly joined validator

This directory reproduces, on a real local network, the issue where an attacker fills a validator's
kad record store so that a validator joining after genesis is never recognized by that node, and the
upper bound where several validators joining at once stall the chain.

## The attacker

The attack needs only a peer that can connect to the victim: no committee seat, no stake, no gas. It
mints throwaway BLS identities, signs a peer-discovery record for each exactly as a real node signs
its own, and sends them to the victim until its 1024-record store is full. Each identity is a fresh
libp2p connection, which gets past the victim's per-connection put budget.

The attacker is a standalone process (`kadfill-attacker`), compiled only with the `kadfill-test-hooks`
cargo feature, so a normal build never contains it. It lives in the network crate at
`crates/consensus/network/src/bin/kadfill_attacker.rs`. The scripts below build it with the feature.

## Run it

From a checkout with a Rust toolchain, `cast` (Foundry) and `curl` on PATH:

```
./etc/test/kad-store-fill/run.sh      # one validator isolated (the main reproduction)
./etc/test/kad-store-fill/stall.sh    # the upper bound: several joiners stall the chain
```

Each script is self-contained. It builds `rayls-network` and the attacker, generates keys and genesis
for a fresh local network under `target/kad-store-fill*/<timestamp>/`, runs the scenario, and stops
its processes on exit.

Platforms: the scripts run on Linux and macOS. On Linux with user systemd, each node runs in its own
`systemd-run --user` scope capped at 1 GiB (override with `MEM_MAX`), so an out-of-memory condition
kills only that node. On macOS, or any host without systemd user scopes, the nodes run uncapped
automatically (set `FORCE_UNCAPPED=1` on Linux to do the same); plan for about 1 GiB of RAM per node,
which is four nodes for `run.sh` and seven for `stall.sh`. The scripts avoid bash 4 features, so
macOS's system bash works, and use only tools present on both platforms (`cast`, `curl`, `awk`,
`sed`, `pkill`).

`run.sh` does:

1. Starts four validators and lets them produce blocks.
2. Keeps validator 1's store full with the attacker.
3. Stakes a fifth validator through the ConsensusRegistry and lets it be seated at the next epoch
   boundary.
4. Checks that the attacked validator never binds the new validator (its bound-peer count stays at
   four), rejects its requests ("requesting peer unknown") and drops its gossip, while the other
   three validators bind it and the chain keeps producing.

`stall.sh` does the same against all four validators at once and seats three new validators in the
same epoch. The committee becomes seven (quorum five), only the four original validators can take
part in consensus, and block production stops while the attack is sustained.

## Exit status

- `run.sh`: `0` the attacked validator bound the new validator anyway (bug absent or fixed); `1`
  reproduced (the seated validator is never bound and its traffic is rejected); `2` inconclusive.
- `stall.sh`: `0` the chain kept producing; `1` the chain stalled (upper bound confirmed); `2`
  inconclusive, including a genesis validator dying, which would confound the result.

On current code `run.sh` exits 1 and `stall.sh` exits 1.

## Options

Environment overrides: `EPOCH_SECS`, `WARMUP_BLOCKS`, `ATTACK_DURATION`, `MEM_MAX`, `FORCE_UNCAPPED`,
`WORKDIR`.

## Unit reproduction

The same defects are covered by unit tests that run in CI with plain `cargo test`, without a live
network, in the network crate:

```
cargo test -p rayls-consensus-network --lib -- \
  test_minted_identities_fill_kad_store_and_block_new_member_binding \
  test_minted_short_ttl_records_evict_next_epoch_member_binding \
  test_next_epoch_member_binding_evicted_by_newer_dial_entries
```

On current code these three tests fail, each for the reason stated in its doc comment.
