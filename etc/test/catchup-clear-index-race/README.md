# Live reproduction: catch-up deletes the by-hash index entry of a block it has just saved

When a header does not link to the row below it, the forward streamer
(`catch_up_consensus_from_to` in `crates/consensus/state-sync/src/lib.rs`) checks that the row
below is not a saved block, then deletes that row from `ConsensusBlocksCache` and its digest from
`ConsensusBlockNumbersByDigest`, in a separate write. The row below is the one the streamer sent
to the subscriber a moment earlier, and the subscriber saves it with `save_consensus`, which
writes the same index entry. A save that lands between the check and the delete leaves a
canonical block that the node can no longer find by hash.

`run.sh` shows this on a local network of 4 validators and 1 observer:

1. A following node (the victim: the observer, or validator 4) follows the chain, is stopped, and
   falls `-g` blocks behind.
2. Validator 1 restarts armed: it answers every by-number consensus-header request with the real
   header for that number but a different `parent_hash`, so the reply does not link to the real
   header below.
3. The victim restarts and catches up. Each unlinked reply sends its streamer into the delete
   branch, right after it sent the row below to its subscriber.
4. The victim is stopped and `rayls-db-inspect header-check` walks its stored chain. A header
   whose parent is stored but not indexed by hash (`INDEX MISMATCH (not indexed)`) is the bug.

The restart modes then restart the victim on an honest network, to show what boot does when the
block that lost its entry is the last block it executed.

## Test-only hooks

All are compiled only with the cargo features below and armed by environment, so a default or
release build has none of them.

| Feature | Variable | Process | Effect |
|---|---|---|---|
| `byzantine-test-hooks` | `RL_BYZ_UNLINK_TARGET=<N or A-B>` | attacker | by-number replies for these numbers do not link to the real header below |
| `fault-injection-test-hooks` | `RL_FAULT_CLEAR_RACE_DELAY_MS=<ms>` | victim | sleep between the check and the delete |
| `fault-injection-test-hooks` | `RL_FAULT_SUBSCRIBER_SAVE_DELAY_MS=<ms>` | victim | sleep before each catch-up save, so the save can land after the check |
| `fault-injection-test-hooks` | `RL_FAULT_GAP_FETCH=odd` | victim | the first time the streamer reaches an odd number, it fetches it by number instead of reading the stored row (the streamer outran the backwards walk) |
| `fault-injection-test-hooks` | `RL_FAULT_GAP_FETCH=after:<K>` | victim | each pass streams K stored rows, then fetches every later number by number |
| `fault-injection-test-hooks` | `RL_FAULT_GAP_FETCH_PEER=<BLS key>` | victim | by-number fetches go to this committee peer only |

The delays only widen the race window; the check and the delete are unchanged. With
`fault-injection-test-hooks` the victim also logs each run of the delete branch
(`FAULT HOOK: mismatch branch checked row N ...`) and, after the delete, `RACE HIT: canonical
block N ... lost its by-hash index entry` when the row it deleted had become canonical.

## Modes

| Mode | Victim | Victim switches | Expect |
|---|---|---|---|
| `race` (default) | observer | 200 ms delete delay, `odd` | exit 1, REPRODUCED |
| `nodelay` | observer | no delay, `odd` | how often the natural window is hit |
| `honest` | observer | as `race`, attacker not armed | exit 0, the delete branch never runs |
| `restart` | observer | 2000 ms delete delay, 300 ms save delay, `after:8`, peer pinned to the attacker | exit 1, then the outcome of an honest restart |
| `vrestart` | validator 4 | as `restart` | exit 1, then the outcome of an honest restart |

In the restart modes the attacker is the only peer the victim fetches by number from, so after the
race the victim fails every pass at the same number, and the block that lost its entry stays its
last executed block. The harness stops the victim, restarts the attacker honest, restarts the
victim with no switches, and reports CRASHED, FORKED, STALLED or RECOVERED. A stalled victim is
restarted once more.

Options for the restart modes:

- `-e 120 -E`: 120 s epochs, and the victim follows past the first epoch boundary before it is
  stopped, so the race and the restart happen in a later epoch instead of epoch 0.
- `-C`: control. No delays, so the race does not land; the victim is held at the attacker the
  same way for 40 s, then restarted. Exit 0 when it recovers.

## Usage

```
cargo build -p rayls-network --features byzantine-test-hooks,fault-injection-test-hooks
cargo build -p rayls-db-inspect
./etc/test/catchup-clear-index-race/run.sh -m race \
    -b target/debug/rayls-network -t target/debug/rayls-db-inspect
./etc/test/catchup-clear-index-race/run.sh -m restart -e 120 -E \
    -b target/debug/rayls-network -t target/debug/rayls-db-inspect
```

Without `-b`/`-t` the script builds both. Each run writes to
`target/catchup-clear-index-race/<timestamp>-<mode>/`: node logs (`victim-catchup.log`,
`victim-restart*.log`), `summary.txt`, copies of the victim's consensus DB taken while it was
stopped (`backup-*`), and the `rayls-db-inspect` output (`inspect-*`).

Exit status: 0 healthy, 1 reproduced, 2 inconclusive.
