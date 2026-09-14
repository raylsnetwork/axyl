# Task: Boot gate — refuse a hardfork schedule inconsistent with the executed chain

## Background

At boot the node verifies the datadir's genesis chain-id against the schedule
source selected for the run (built-in `--network` profile or `--config-file`
subnet) and refuses to start on a mismatch. It does **not** verify the
hardfork schedule itself.

An operator who edits fork activation blocks in the schedule source can:

- move an already-activated fork to a different block,
- remove a fork the chain already ran under,
- back-date a new fork into blocks the chain already executed.

Today the node starts anyway, silently re-interpreting the chain's executed
history under a different schedule — different state transitions, rewards,
and precompiles for blocks that already exist.

## Goal

Make the hardfork schedule a pinned, boot-time-verified property of the
datadir, with trust-on-first-use semantics:

1. Each datadir carries `<datadir>/schedule-record.yaml` recording
   `{chain_id, as_of_block, hardforks}` — the schedule the chain's executed
   blocks were produced under, plus the head at the last recorded boot
   (reuses the existing `ForkActivation` serde shape; absent fork = `Never`).
2. At boot, **pre-launch**, the selected schedule is verified against the
   record using the chain's **live** head:
   - for any fork whose boundary differs between record and selection, the
     lower of the two boundaries is the first block of disagreement;
     `disagreement <= head` → the executed history is affected → **refuse to
     start** with an error naming the fork and both boundaries;
     `disagreement > head` → the move is in the future → **allow, log a
     warning**, and re-record the new schedule (this is how agreed schedule
     updates ship);
   - record chain-id mismatch, unknown recorded fork, or unparseable record →
     refuse;
   - no record (fresh datadir, or one predating this feature) → trust the
     selected schedule, warn if the chain already has history, and start
     recording from this boot.
3. The chain head is read the same way reth tracks the executed head — the
   `Finish` stage checkpoint in the execution DB (single keyed read, no
   provider, no sync; **not** `CanonicalHeaders`, which is empty in v2
   storage once blocks are persisted).

## Acceptance criteria

- [ ] `rayls-network node` refuses to start (non-zero exit, pre-launch, no
      RPC) when the selected schedule moves/removes/back-dates a fork whose
      boundary is at or below the current head; the error names the fork and
      both boundaries.
- [ ] A schedule that only moves fork boundaries strictly above the head is
      accepted, a warning is logged, and the record is re-written with the new
      schedule.
- [ ] A fresh datadir boots, and its datadir gains a `schedule-record.yaml`
      pinning the selected schedule at head 0.
- [ ] A datadir with executed blocks but no record boots (trust-on-first-use,
      warned) and re-establishes the record at the live head.
- [ ] Chain-id mismatch between record and selected schedule, an unknown fork
      name in the record, or an unparseable record file all refuse the boot.
- [ ] Offline tools (`rayls-replay`, cold-migrate) and in-process test/replay
      environments are unaffected.
- [ ] Existing datadirs work unchanged on first boot (no migration, no data
      loss).
- [ ] Operator docs updated (`docs/config-file.example.yaml`) describing the
      record and its semantics.

## Test plan

- Unit tests (evm crate, `network_profile`): record construction/serde
  roundtrip, `ForkActivation::block` accessor, `ScheduleRecord::activation`
  lookup (case-insensitive, absent ≡ `never`), identical schedule passes,
  executed move refused, boundary-at-head refused, boundary-head+1 allowed,
  back-date refused, removed fork refused, chain-id mismatch refused, unknown
  fork refused, `Never` record entry equivalent to absent fork.
- Integration tests (network-cli, real `init_db` on temp dirs, faked head via
  a `Finish` checkpoint row): fresh datadir records, second boot re-records at
  the head, executed move refused, future move allowed + re-recorded, deleted
  record re-established, chain-id mismatch, unparseable record.
- Full-process e2e (dev build, `#[ignore]`, boots real nodes): dev chain
  executes → record present; tampered `--config-file` moving an executed fork
  is refused pre-launch with the fork named; future activation allowed and
  re-recorded; deleted record re-established with `as_of_block > 0`.
- Regression: evm + network-cli suites and the default e2e suite stay green.

```
cargo test -p rayls-execution-evm
cargo test -p rayls-network-cli
cargo test -p e2e-tests --features dev-single-node-setup --test it schedule_record -- --ignored
cargo test -p e2e-tests
```

## Proposed design notes

- Record file: `<datadir>/schedule-record.yaml`; add
  `RaylsDirs::schedule_record_path()` (trait + all impls).
- Verification core in the evm crate: `ScheduleRecord`, `FutureForkMove`,
  `verify_schedule(record, selected, chain_id, head) -> Result<Vec<FutureForkMove>>`
  (pure function, unit-testable; returns the future moves for the caller to
  warn about).
- Head read: `RethEnv::best_block_number` — opens the execution DB env and
  reads the `Finish` stage checkpoint. Note: the boundary `== head` counts as
  executed (a fork activating at the current head already affects the chain).
- Gate placement: network-cli `node` pre-launch path, after the chain-id
  verification and before the node opens its DB — covers real node boots and
  maintenance runs; `dev` boots funnel through the same path.
- Correctness sketch: induction over boots — at every boot the schedule is
  either equal to the recorded one or differs only at boundaries strictly
  above the head; re-recording then pins exactly the schedule every executed
  block ran under.
- `ForkCondition` variants beyond `Block`/`Never` (e.g. TTD/timestamp) are
  defensive `None` in the record mapping; Rayls schedules only use block-based
  activations.

## Out of scope

- Continuous/in-block record updates (the record pins the head at boot time;
  the gate always compares against the live head).
- Consensus, RPC, or storage-format changes.
- Multi-validator coordination of schedule updates (operational process, not
  enforced by the node).
