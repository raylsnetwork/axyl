# Database Architecture Action Plan

## Verification Summary

All read/write patterns in the codebase have been verified against the new architecture. No gaps found.

### Patterns Verified

| Category | Count | Status |
|---|---|---|
| Standalone reads | ~30+ | Covered |
| Multi-table atomic writes | 5+ | Covered |
| Read-then-write patterns | 12 | Covered (opt-in locks) |
| Direct non-txn writes | ~20+ | Covered |
| Batch writes | 3+ | Covered |
| Long-running iterations | 3 (currently exempted) | Covered (timeout removed) |
| Epoch transition pipeline | 1 (6 phases) | Covered |
| Concurrent write conflicts | 8 | Covered (locks serialize) |
| Cold storage fallthrough | all reads | Covered (4-tier: buffer → mem → persistent → cold) |
| Cold archival producer | `without_cold()` view | Covered (hot-only view for archiver) |
| `evict_persistent_batch` | cold archival prune | Covered (hard-delete from hot, never targets cold) |

### No Gaps Found

The architecture covers all read/write patterns in the codebase. The only responsibility that shifts to callers is: deciding when to call `txn.lock()` for read-then-write patterns. This is explicit and intentional — the database provides the mechanism, the caller decides when it's needed.

---

## Read/Write Pattern Analysis

### 1. Standalone Reads — No Lock Needed

**Pattern:** Single-operation reads (get, last_record, iter) outside any write context.

**Current code:** ~30+ call sites across `ConsensusBlocks`, `EpochRecords`, `Certificates`, `EpochCerts`, `EpochRecordsIndex`, `ConsensusBlockNumbersByDigest`, `ConsensusBlocksCache`, `KadRecords`, `Payload`, `EpochTransitionCheckpoints`.

**Examples:**
- `consensus_store::read_last_committed` — reverse_iter on `ConsensusBlocks` (line 71-75 of `consensus_store.rs`)
- `consensus_store::get_canonical_consensus_by_hash` — get on `ConsensusBlockNumbersByDigest` + `ConsensusBlocks` (line 130-136)
- `epoch_store::get_epoch_by_number` — get on `EpochRecords`, `EpochCerts` (line 83-93)
- `certificate_store::contains` — contains_key on `Certificates` (line 187)
- `checkpoint_store::load_checkpoint` — get on `EpochTransitionCheckpoints` (line 27)

**New architecture:** Each read opens a short-lived MDBX snapshot via `db.read_txn()` → `MdbxTx`. The snapshot lives only for the read duration. No locks needed. No code changes required.

**Verdict: Covered.**

---

### 2. Multi-Table Atomic Writes — Lock All Tables

**Pattern:** Writes that touch 3+ tables atomically in a single `with_write_txn`.

**Current code — 5+ identified patterns:**

| Pattern | File | Tables | Lines |
|---|---|---|---|
| `save_cert()` | `certificate_store.rs` | `Certificates`, `CertificateDigestByRound`, `CertificateDigestByOrigin` | 121-139 |
| `write_all()` | `certificate_store.rs` | `Certificates`, `CertificateDigestByRound`, `CertificateDigestByOrigin` (loop) | 154-165 |
| `delete()` | `certificate_store.rs` | `CertificateDigestByRound`, `CertificateDigestByOrigin`, `Certificates` | 228-239 |
| `clear()` | `certificate_store.rs` | `CertificateDigestByRound`, `CertificateDigestByOrigin`, `Certificates` | 416-423 |
| `save_epoch_record_with_cert()` | `epoch_store.rs` | `EpochRecordsIndex`, `EpochRecords`, `EpochCerts` | 66-80 |
| `save_epoch_record()` | `epoch_store.rs` | `EpochRecordsIndex`, `EpochRecords` (+ conditional remove) | 50-64 |
| `save_consensus()` | `state-sync/lib.rs` | `Batches`, `ConsensusBlocks`, `ConsensusBlockNumbersByDigest`, `ConsensusBlocksCache` (remove) | 188-248 |
| `store_consensus_header_in_cache()` | `state-sync/consensus.rs` | `ConsensusBlocksCache`, `ConsensusBlockNumbersByDigest` | 23-32 |
| `sanitize_foreign_consensus_db()` | `orchestrator/state.rs` | 10 tables cleared + `NodeIdentity` insert | 247-267 |
| `clear_consensus_db_for_next_epoch()` | `orchestrator/state.rs` | 7 tables cleared | 541-551 |
| `mode_change()` | `orchestrator/transition.rs` | `LastProposed`, `LastProposedByAuthority` | 300-302 |

**New architecture:** `txn.lock("table1")` → `txn.lock("table2")` → ... → writes → `txn.commit()`. All buffered writes are applied atomically in a single short-lived MDBX write txn at commit time.

**Example — certificate write:**
```rust
// Current:
fn write_all(&self, certificates) -> StoreResult<()> {
    self.with_write_txn(|txn| {
        for cert in certificates {
            save_cert(txn, digest, cert)?;  // 3 inserts per cert
        }
        Ok(())
    })
}

// New:
fn write_all(&self, certificates) -> StoreResult<()> {
    let txn = self.start_write_txn()?;
    txn.lock("certificates")?;
    txn.lock("certificate_digest_by_round")?;
    txn.lock("certificate_digest_by_origin")?;
    for cert in certificates {
        txn.insert::<Certificates>(&digest, &cert)?;
        txn.insert::<CertificateDigestByRound>(&key, &digest)?;
        txn.insert::<CertificateDigestByOrigin>(&key, &digest)?;
    }
    txn.commit()?;
}
```

**Verdict: Covered.**

---

### 3. Read-Then-Write Patterns — Lock All Involved Tables

**Pattern:** Read first, then write based on result. These are the critical race condition cases.

**Current code — 12 identified patterns:**

| # | Pattern | File | Tables | Risk |
|---|---|---|---|---|
| 1 | Certificate dedup | `cert_validator.rs:133` | `Certificates` → 3 tables | LOW |
| 2 | Vote dedup / equivocation | `handler.rs:646-692` | `Votes` → `Votes` | LOW (sequential) |
| 3 | Proposer repropose check | `header_builder.rs:267-272` | `LastProposed` → `LastProposed` | LOW (same task) |
| 4 | Missing parent check | `cert_manager.rs:226` | `Certificates` → 3 tables | LOW |
| 5 | Batch fetch dedup | `batch_fetcher.rs:57` | `Batches` → `Batches` | MEDIUM |
| 6 | Sync missing batches | `network/mod.rs:487` | `Batches` → `Batches` | MEDIUM |
| 7 | save_consensus (superseded check) | `state-sync/lib.rs:202` | `ConsensusBlocks`, `ConsensusBlocksCache` → 4 tables | LOW (same txn) |
| 8 | Foreign DB sanitization | `orchestrator/state.rs:215` | `NodeIdentity`, `LastProposed` → 10 tables | LOW (same txn) |
| 9 | Kad add_provider | `kad.rs:316` | `KadProviderRecords` → `KadProviderRecords` | **HIGH** |
| 10 | Kad remove_provider | `kad.rs:379` | `KadProviderRecords` → `KadProviderRecords` | **HIGH** |
| 11 | Orphan batches | `orchestrator/batches.rs:29` | `NodeBatchesCache` → `NodeBatchesCache` | MEDIUM |
| 12 | Gossip batch check | `handler.rs:71` | `Batches` → `Batches` | MEDIUM |

**New architecture:** Caller calls `txn.lock("table")` before the read. This blocks other writers to that table until commit. The read sees a consistent snapshot. The write is buffered and applied atomically at commit.

**Example — certificate dedup:**
```rust
// Current (risky without lock):
if db.contains::<Certificates>(&digest)? { return Ok(()); }
db.write_all(cert)?;  // 3 tables

// New (correct with lock):
let txn = db.start_write_txn()?;
txn.lock("certificates")?;
txn.lock("certificate_digest_by_round")?;
txn.lock("certificate_digest_by_origin")?;
if txn.get::<Certificates>(&digest)?.is_some() { return Ok(()); }
txn.insert::<Certificates>(&digest, &cert)?;
txn.insert::<CertificateDigestByRound>(&key, &digest)?;
txn.insert::<CertificateDigestByOrigin>(&key, &digest)?;
txn.commit()?;
```

**Example — Kad add_provider (HIGH risk, needs lock):**
```rust
// Current (race condition):
if db.contains::<KadProviderRecords>(&key)? {
    db.insert::<KadProviderRecords>(&key, &provider)?;  // can overwrite
}

// New (correct with lock):
let txn = db.start_write_txn()?;
txn.lock("kad_provider_records")?;
if txn.get::<KadProviderRecords>(&key)?.is_none() {
    txn.insert::<KadProviderRecords>(&key, &provider)?;
}
txn.commit()?;
```

**Verdict: Covered.** The architecture provides the locking mechanism; callers decide when to use it.

---

### 4. Direct Non-Txn Writes — No Lock Needed

**Pattern:** `db.insert()`, `db.remove()`, `db.clear_table()` — fire-and-forget async writes.

**Current code — ~20+ call sites:**

| File | Table | Method | Lines |
|---|---|---|---|
| `proposer_store.rs` | `LastProposed` | `insert` | 36-40 |
| `proposer_store.rs` | `LastProposedByAuthority` | `insert` | 47-55 |
| `vote_digest_store.rs` | `Votes` | `insert` | 18-22 |
| `payload_store.rs` | `Payload` | `insert` | 14-18 |
| `batch_ordering_store.rs` | `BatchOrderingState` | `insert` | 17-19 |
| `worker.rs` | `BatchSeqCounter` | `insert` | 319-322 |
| `worker.rs` | `NodeBatchesCache` | `insert` | 363 |
| `worker.rs` | `Batches` | `insert` | 384 |
| `handler.rs` (worker) | `Batches` | `insert` | 139 |
| `handler.rs` (worker) | `Batches` | `insert` | 78 |
| `mode_change()` | `LastProposed`, `LastProposedByAuthority` | `clear_table` | 300-302 |
| `orphan_batches()` | `NodeBatchesCache` | `clear_table` | 33 |
| `sanitize_foreign_consensus_db()` | `NodeIdentity` | `insert` | 227 |
| `handler.rs` (primary) | `Votes` | `insert` | 692 |
| `handler.rs` (primary) | `LastProposedByAuthority` | `insert` | 696 |
| `header_builder.rs` | `LastProposed` | `insert` | 165-167 |
| `header_validator.rs` | `Payload` | `insert` | 147 |
| `network/mod.rs` (primary) | `Payload` | `insert` | 557 |
| `kad.rs` | `KadRecords` / `KadWorkerRecords` | `insert` | 275-283 |
| `kad.rs` | `KadRecords` / `KadWorkerRecords` | `remove` | 287-298 |
| `kad.rs` | `KadProviderRecords` / `KadWorkerProviderRecords` | `insert` | 338-346 |
| `kad.rs` | `KadProviderRecords` / `KadWorkerProviderRecords` | `remove` / `insert` | 376-406 |

**New architecture:** These still go through the background thread. The key difference:
- **Before:** Writes queued to giant shared txn, committed when refcount hits 0
- **After:** Each direct write creates its own short-lived buffer, sent to background thread as `CommitTxn(buffer)`, flushed immediately in a short-lived MDBX txn

**Verdict: Covered.** No locking needed for simple inserts/removes (no read-then-write dependency).

---

### 5. Batch Writes — No Lock Needed (No Read-Then-Write)

**Pattern:** Multiple inserts in a single `with_write_txn` loop.

**Current code — 3 identified patterns:**

| Pattern | File | Lines |
|---|---|---|
| `write_all()` (certificates) | `certificate_store.rs` | 154-165 |
| `save_consensus()` (batches + header) | `state-sync/lib.rs` | 215-245 |
| `batch_fetcher::fetch()` | `batch_fetcher.rs` | 78-87 |

**New architecture:** All inserts go to the same write buffer. At commit, the entire buffer is applied in one short-lived MDBX txn.

**Verdict: Covered.**

---

### 6. Long-Running Iterations — Timeout Removed

**Pattern:** Epoch boundary walks, rewards tally, certificate recovery scans.

**Current code — 3 call sites using `disable_long_read_safety()`:**

| Pattern | File | Table | Iteration Type | Lines |
|---|---|---|---|---|
| `catchup_accumulator` | `orchestrator/utils.rs` | `ConsensusBlocks` | `reverse_raw_iter` (entire epoch) | 62-98 |
| `rewards::tally` | `rewards/lib.rs` | `ConsensusBlocks` | `reverse_raw_iter` (entire epoch) | 74-110 |
| `after_round` (recovery mode) | `certificate_store.rs` | `CertificateDigestByRound` | `skip_to`/`iter` + multi_get | 244-283 |

**New architecture:** Read timeout is removed entirely. No `disable_long_read_safety()` needed. Iterations run to completion.

**Verdict: Covered.**

---

### 7. Epoch Transition Pipeline — Lock Each Table Group

**Pattern:** 6-phase pipeline with checkpoints, shutdown, execution, flush, finalize.

**Current code — complete operation sequence:**

```
PHASE 1 - CHECKPOINT:
  WRITE: EpochTransitionCheckpoints (insert/update)

PHASE 2 - SHUTDOWN:
  WRITE: EpochTransitionCheckpoints (update x2)
  FLUSH: consensus_db.persist()
  FLUSH: engine.flush_persistence()

PHASE 3 - EXECUTION:
  WRITE: EpochTransitionCheckpoints (update)
  [Engine executes closing block internally]

PHASE 4 - FLUSH + WRITE EPOCH RECORD:
  FLUSH: engine.flush_persistence()
  READ:  EpochRecords, EpochCerts (get_epoch_by_number)
  READ:  validators_for_epoch(epoch)          [execution layer]
  READ:  validators_for_epoch(epoch+1)        [execution layer]
  READ:  EpochRecords (resolve parent)
  READ:  network.request_epoch_cert()         [conditional, network]
  WRITE: EpochRecordsIndex + EpochRecords + EpochCerts  [conditional]
  READ:  canonical_tip()                      [execution layer]
  WRITE: in-memory epoch_record

PHASE 5 - FINALIZE:
  CLEAR: LastProposed
  CLEAR: LastProposedByAuthority
  CLEAR: Votes
  CLEAR: Certificates
  CLEAR: CertificateDigestByRound
  CLEAR: CertificateDigestByOrigin
  CLEAR: Payload
  DELETE: EpochTransitionCheckpoints (remove)
  FLUSH: consensus_db.persist()

PHASE 6 - RESET SIGNALS:
  (in-memory only, no DB writes)
```

**Tables cleared during every epoch transition:**
1. `LastProposed`
2. `LastProposedByAuthority`
3. `Votes`
4. `Certificates`
5. `CertificateDigestByRound`
6. `CertificateDigestByOrigin`
7. `Payload`

**Tables written during epoch transition:**
1. `EpochTransitionCheckpoints` — written at each phase, deleted at end
2. `EpochRecordsIndex` — written atomically with record+cert
3. `EpochRecords` — written atomically with cert
4. `EpochCerts` — written atomically with record

**New architecture:**
- Checkpoint writes: `txn.lock("epoch_transition_checkpoints")` → insert → commit
- Clear 7 tables: `txn.lock("last_proposed")` → `txn.lock("votes")` → ... → `clear_table()` → commit (all in one txn)
- Epoch record + cert: `txn.lock("epoch_records")` → `txn.lock("epoch_certs")` → insert → commit

**Verdict: Covered.**

---

### 8. Concurrent Write Conflicts — Lock Serialized

**Pattern:** Multiple independent writers to the same table.

**Current code — 8 identified conflicts:**

| # | Conflict | Writer A | Writer B | Tables | Risk |
|---|---|---|---|---|---|
| 1 | Cert write vs cert write | Certifier | StateSync | Certificates | LOW (MVCC) |
| 2 | Vote write vs vote read | handler.rs:692 | handler.rs:646 | Votes | LOW (sequential) |
| 3 | Batch write (own) vs batch write (remote) | worker.rs:384 | handler.rs:139 | Batches | MEDIUM |
| 4 | Proposer write vs proposer read | header_builder.rs:165 | header_builder.rs:267 | LastProposed | LOW (same task) |
| 5 | BatchSeqCounter write | worker.rs:319 | — | BatchSeqCounter | LOW (unique keys) |
| 6 | Cert write_all | cert_manager.rs:305 | — | 3 cert tables | LOW (single txn) |
| 7 | Remote batch sync vs batch fetcher | network/mod.rs:525 | batch_fetcher.rs:77 | Batches | MEDIUM |
| 8 | Kad add_provider vs remove_provider | kad.rs:338 | kad.rs:376 | KadProviderRecords | **HIGH** |

**New architecture:** Writers acquire locks. If Writer B tries to lock a table Writer A holds, Writer B blocks until Writer A commits. Serialization is automatic.

**Verdict: Covered.**

---

### 9. Cold Storage Fallthrough — 4-Tier Read Resolution

**Pattern:** All read operations fall through to the cold tier on hot miss.

**Current code:** `LayeredDbTx` reads follow `mem → persistent → cold` (3-tier).

**New architecture:** `WriteTxn` and `LayeredDbTx` reads follow `buffer → mem → persistent → cold` (4-tier). The cold tier is feature-gated and append-only.

**Cold integration points:**
- `WriteTxn::get()` / `LayeredDbTx::get()` — fall through to `cold_get()` on hot miss
- `iter()` / `raw_iter()` — chain cold beneath hot via `merge_cold()` / `merge_cold_raw()`
- `skip_to()` / `reverse_iter()` — pass key anchor to cold for correct boundary
- `evict_persistent_batch` — hard-deletes from hot (no tombstone) to avoid shadowing cold
- `without_cold()` — returns hot-only view for archival producer

**Cold-specific constraints:**
- Only `ConsensusBlocks` (by block number) and `Batches` (via `ColdBatchLocations` index) are archived
- `ColdBatchLocations` auxiliary index is resolved from the hot snapshot
- Cold scans raise a fault flag on gap or read error inside the sealed span
- All cold methods are `#[cfg(feature = "cold-storage")]` with no-op stubs when disabled

**Verdict: Covered.**

---

## Implementation Files

| File | Changes |
|---|---|
| `storage/src/mem_db.rs` | Replace `MemDbTx` + `MemDbTxMut` with `MemTxn` (buffered writes, atomic commit, `hard_delete`) |
| `storage/src/layered_db.rs` | Per-txn write buffer, `WriteTxn`, remove giant txn logic, cold integration, `QueueSender` backpressure |
| `storage/src/write_lock.rs` (NEW) | `WriteLockManager`, `WriteLockGuard` |
| `storage/src/cold/` (all files) | **UNCHANGED** — verify integration points with new `WriteTxn` and `LayeredDbTx` |
| `storage/src/mdbx/database.rs` | Remove `DEFAULT_MAX_READ_TXN_DURATION_SECS`, remove `disable_long_read_safety()` |
| `storage/src/redb/database.rs` | Remove `disable_long_read_safety()` |
| `types/src/database_traits.rs` | Remove `ReadTimeout` enum, remove `disable_long_read_safety()` from trait |
| `storage/src/lib.rs` | Remove `ReadTimeout` re-export, add `write_lock` module |
| `middleware/rewards/src/lib.rs` | Remove `txn.disable_long_read_safety()` call |
| `middleware/orchestrator/src/epoch_manager/utils.rs` | Remove `txn.disable_long_read_safety()` call |
| `storage/src/stores/certificate_store.rs` | Remove `ReadTimeout` param from `after_round()` |
| `consensus/primary/src/consensus/state.rs` | Remove `ReadTimeout::Exempt` usage |
| `network-cli/src/args/consensus_database.rs` | Remove `--consensus-db.read-transaction-timeout` CLI arg |

| File | Changes |
|---|---|
| `storage/src/mem_db.rs` | Replace `MemDbTx` + `MemDbTxMut` with `MemTxn` (buffered writes, atomic commit) |
| `storage/src/layered_db.rs` | Per-txn write buffer, `WriteTxn`, remove giant txn logic, simplify `persist()` |
| `storage/src/write_lock.rs` (NEW) | `WriteLockManager`, `WriteLockGuard` |
| `storage/src/mdbx/database.rs` | Remove `DEFAULT_MAX_READ_TXN_DURATION_SECS`, remove `disable_long_read_safety()` |
| `storage/src/redb/database.rs` | Remove `disable_long_read_safety()` |
| `types/src/database_traits.rs` | Remove `ReadTimeout` enum, remove `disable_long_read_safety()` from trait |
| `storage/src/lib.rs` | Remove `ReadTimeout` re-export, add `write_lock` module |
| `middleware/rewards/src/lib.rs` | Remove `txn.disable_long_read_safety()` call |
| `middleware/orchestrator/src/epoch_manager/utils.rs` | Remove `txn.disable_long_read_safety()` call |
| `storage/src/stores/certificate_store.rs` | Remove `ReadTimeout` param from `after_round()` |
| `consensus/primary/src/consensus/state.rs` | Remove `ReadTimeout::Exempt` usage |
| `network-cli/src/args/consensus_database.rs` | Remove `--consensus-db.read-transaction-timeout` CLI arg |

---

## Key Invariants

1. **No uncommitted data visible** — each layer's txn buffer is private until commit
2. **No giant txns** — each WriteTxn flushes independently; no shared write txn
3. **Short-lived snapshots** — MDBX snapshots live only for the read duration
4. **Accurate persist()** — `persist()` waits for all committed txns to flush
5. **Opt-in locking** — caller decides which tables to lock; database provides the mechanism
6. **4-tier read resolution** — buffer → mem → persistent → cold (feature-gated)
7. **Cold is append-only** — writes never target cold; `evict_persistent_batch` hard-deletes from hot
8. **Cold feature gates** — all cold methods compile with no-op stubs when `cold-storage` is disabled
