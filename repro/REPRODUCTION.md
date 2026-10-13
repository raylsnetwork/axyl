# Power-loss durability reproduction

Self-contained reproduction branch (`repro/power-loss-durability`). Everything here is throwaway and
never merged.

## The bug

The consensus DB runs MDBX in `SafeNoSync` (`mdbx/database.rs:503`, 5s sync period). A vote, header
or certificate is made durable *before* it is sent: the record is committed and readable, so it
survives a process crash. But the commit is not fsynced, so a **power loss** (or kernel panic) in the
sync window drops it even though peers already saw it. After restart the validator no longer knows
what it sent, can build a second header / cast a second vote for a round it already acted on, and two
certificates for one round are produced → `CertificateEquivocation` → consensus halt.

## Deterministic regression tests (storage layer)

`crates/infrastructure/storage/src/mdbx/database.rs` (test module), 100% deterministic:

- `committed_write_without_sync_is_lost_on_power_loss`: a committed, readable (process-crash-survivable)
  record is lost when the datafile reverts to its last fsync. **This is the bug.**
- `force_synced_write_survives_power_loss`: `sync_to_disk()` (the fix primitive, `mdbx_env_sync_ex`
  force=true) closes it.

A power loss is modelled by restoring the datafile to its last-synced image (snapshot before the
write, restore over the live file, drop the lock), which is exactly what MDBX recovers after power loss.

Run: `cargo test -p rayls-infrastructure-storage --lib power_loss`

## Live end-to-end (N validators, one machine, no attacker)

The debug binary carries an env-gated hook (`crates/infrastructure/storage/src/repro.rs`): when a node
is armed, before its next header/vote write it takes a consistent `mdbx_env_copy` snapshot of its
consensus datafile (excluding that round's record). The driver kills the node and restores the
snapshot, the power loss that drops the committed-but-unsynced tail.

- `repro/net.sh`: bring up / power-loss / inspect an N-validator debug network.
- `repro/power_loss.sh <N> [powerloss|crash]`: one scenario run. `powerloss` restores the snapshot
  after the kill; `crash` does not (the control).
- `repro/reproduce.sh [N] [max_attempts]`: **runs the scenario until the halt is observed.**

The first run builds `target/debug/rayls-network` automatically (a few minutes); everything else (keys,
genesis, datadirs, logs) is generated under the gitignored `repro/runs/`, so a fresh
`git fetch` of this branch plus `./repro/reproduce.sh 4` is self-contained.

### Why a retry wrapper

The *durability gap* is deterministic (the unit tests). Whether it *escalates* to a halt depends on a
consensus-timing race: `CertificateEquivocation` is only raised by `try_insert_in_dag`
(`consensus/state.rs:187`) during active consensus, not on the catch-up sync path. So the witnessing
node must stay in the live DAG (holder up), but then the restarted author sometimes re-fetches its own
certificate 1 from that live holder and skips the rebuild. Per run the halt lands ~1 in 3;
`reproduce.sh` repeats until it does, which it always does within a few attempts.

### Observed (N=4, `reproduce.sh`)

- **PASS**: two certificates for one author and round (e.g. C440 `BLDAQJjsJM471eh2` vs
  `DFgVx51BHTyuigd5`; C449 `Dyez5Ha9waggXtgq` vs `FCRWJo…`) → `CertificateEquivocation` → consensus
  critical task panics (`task_manager.rs:145`, "node exiting") on every validator → **tips frozen**
  (chain halted). Debug build unwinds so the process limps; a release `panic=abort` build exits.
- **crash control** (`power_loss.sh 4 crash`): the author recovers its persisted certificate (one
  digest only, no rebuild), the chain stays live, zero equivocation, durable-before-send holds for a
  process crash, and only a power loss breaks it.

Per-attempt logs are written under `repro/runs/evidence/` (gitignored).

## Fix

Force-sync the durable barrier before send: call `sync_to_disk()` (added here on `Database` /
`MdbxDatabase` / `LayeredDatabase`) after the `persist()` in `handler.rs` (vote), `header_builder.rs`
(header) and `cert_validator.rs` (own cert). Benchmark first; votes are frequent. Alternatively bound
the window with a much shorter sync period. `init.rs`'s unwind-to-0 guard is bypassed only on repro
nodes (gated on `RL_REPRO_PL_DBFILE`), so production keeps the panic.
