# Boot gate: refuse a hardfork schedule inconsistent with the executed chain

## Problem

The node already verifies the datadir's genesis chain-id against the schedule
source selected at boot (built-in `--network` profile or `--config-file`
subnet), and refuses to start on a mismatch. It did **not** verify the
hardfork schedule itself. An operator who edits fork activation blocks in the
schedule source can move an already-activated fork, remove one, or back-date a
new fork into the executed history — and the node would happily start,
re-interpreting the blocks the chain already ran under the old schedule.

## Change

Trust-on-first-use schedule record, checked pre-launch:

- New `<datadir>/schedule-record.yaml` — `{chain_id, as_of_block, hardforks}`
  (reuses the existing `ForkActivation` serde shape) — pinning the schedule
  the datadir's blocks were produced under, plus the chain head at the last
  boot.
- At boot, before the node launches, the selected schedule is verified
  against the record (`verify_schedule`):
  - a fork whose boundary disagrees at or below the **live** chain head
    (executed, removed, or back-dated) → **refuse to start**;
  - a disagreement still in the future (agreed schedule updates) → **allow,
    warn**, and re-record the new schedule;
  - record chain-id mismatch, unknown recorded fork, or unparseable record →
    refuse;
  - no record (fresh datadir, or one created before this feature) → trust the
    selected schedule and start recording from this boot.
- The head is read the same way reth tracks the executed head: the `Finish`
  stage checkpoint in the execution DB (`RethEnv::best_block_number`) — a
  single keyed read, no provider, no sync, cheap enough for boot-time
  validation.

The gate lives in `rayls-network node`'s pre-launch path (network-cli), so it
covers real node boots and maintenance runs, while offline tools
(`rayls-replay`, cold-migrate) and in-process test/replay environments are
untouched.

### Files

| File | Change |
| --- | --- |
| `crates/execution/evm/src/network_profile.rs` | `ScheduleRecord`, `FutureForkMove`, `verify_schedule` + unit tests |
| `crates/execution/evm/src/reth_env/init.rs` | `RethEnv::best_block_number` (reads the `Finish` stage checkpoint) |
| `crates/execution/evm/src/lib.rs` | re-exports |
| `crates/infrastructure/config/src/traits.rs` | `RaylsDirs::schedule_record_path()` |
| `crates/execution/evm/src/dirs.rs`, `crates/testing/test-utils/src/temp_dirs.rs` | trait impls |
| `crates/infrastructure/network-cli/src/node.rs` | `verify_schedule_record` boot gate + ITs |
| `crates/testing/e2e-tests/tests/it/schedule_record.rs` | full-process e2e (dev build) |
| `docs/config-file.example.yaml` | operator docs for the record |

## Correctness sketch

Induction over boots: at every boot the schedule is either equal to the
recorded one, or it differs only at boundaries strictly above the head.
Re-recording then pins exactly the schedule every executed block ran under.
The boundary `== head` counts as executed (a fork activating at the current
head already affects the chain), so it is refused.

## Backward compatibility

- Existing datadirs have no record: first boot trusts the current schedule
  (warns when the chain already has history) and records from then on. No
  migration, no data loss.
- Future schedule changes still ship by editing the schedule source; they now
  log a warning at boot.
- No consensus, RPC, or storage changes; one small file per datadir.

## Tests

- 14 unit tests for `verify_schedule`/`ScheduleRecord` (evm crate): identical
  schedule, executed move, boundary-at-head, boundary-head+1, back-date,
  removed fork, chain-id mismatch, unknown fork, never-entry, yaml roundtrip,
  `ForkActivation::block` accessor, `ScheduleRecord::activation` lookup.
- 7 integration tests (network-cli): fresh datadir records, re-record at the
  chain head, executed move refused, future move allowed + re-recorded,
  deleted record re-established, chain-id mismatch, unparseable record.
- Full-process e2e (dev build, `--ignored`, boots real nodes): record written
  on dev boot; tampered config file moving an executed fork is refused
  pre-launch with the fork named; future activation allowed and re-recorded;
  deleted record re-established.
- Regression: unit suites green; default e2e suite green (4 passed).

Run:

```
cargo test -p rayls-execution-evm
cargo test -p rayls-network-cli
cargo test -p e2e-tests --features dev-single-node-setup --test it schedule_record -- --ignored
```
