# rayls-db-inspect

Read-only inspection of a node's `consensus-db`, comparing several nodes side by side. A node is
a database directory (`--db`).

When an epoch boundary fails to certify, or nodes stop agreeing on consensus headers, the
question is "what does each node actually have on disk?". `rayls-db-inspect` answers it
without ad-hoc code: it opens each node's MDBX consensus database read-only, reports the
epoch record / certificate / header state per node.

## When to use it

- A node cannot restart or sync after an epoch transition: `epoch <N>` shows whether the
  record and its certificate exist on each node, and whether they agree.
- You suspect a fork: `cert <N>` compares the leader certificate at a consensus header,
  signer sets included (two nodes can hold the same certificate digest with different
  signatures).
- Triage after an incident: `summary` and `epoch-check` give a quick health picture of every
  node's consensus database.
- A block's transactions are in question: `get-tx <TX_HASH>` follows a transaction through the
  stored rows: the batch that carries it, the sub-dag certificate whose payload lists that batch,
  and the consensus header that committed it; `get-batch <DIGEST>` shows the batch itself, hot or
  archived, with the same path.

## Safety: stopped nodes only

**The tool never reads a running node.** Every database is opened with `MDBX_RDONLY` and
`MDBX_EXCLUSIVE`: nothing is written, no table is created, and the open fails if any other
process holds the environment, with a message saying so. That is deliberate. A running node
writes through an in-memory cache and syncs lazily, so its files lag its state by seconds, and
every command reads in several short transactions, so a report taken from a moving database
would mix moments and its verdict would mean nothing. Refusing is the only reading that cannot
mislead.

To inspect a node, stop it first, or work on a copy:

```sh
# stopped node: point at its datadir (or its consensus-db directory)
rayls-db-inspect epoch 42 --db n1=/data/node1

# evidence, or inspection elsewhere: copy the stopped node's files, then inspect the copy
cp -a --sparse=always /data/node1/consensus-db /var/tmp/node1-copy
rayls-db-inspect epoch 42 --db n1=/var/tmp/node1-copy
```

A stopped node's files are already one consistent state, so a plain copy is all a copy needs.
`mdbx.dat` is sparse (its length sits at the geometry's floor, 1 GiB by default, while it
occupies only the used pages), so copy it with `cp --sparse=always` or `rsync -S`. Leave the
node's `lock` file behind. `summary` dates any directory by its tip header's commit time, so a
copy needs no marker to say what it holds and as of when.

A node that was killed or crashed may have a last commit that was never synced. MDBX refuses to
read such a database until it is recovered, and recovery writes, so recover a copy, not the
node's own directory: `--recover` performs one read-write, exclusive open that settles the head,
the same recovery the node itself performs when it restarts, and marks the node `recovered` in
`summary`. It rewrites the copy's meta pages (hash the copy first if it is evidence), and **on
any host other than the one that took the copy, or after that host reboots, MDBX rolls the copy
back to its last steady commit, silently dropping up to a few seconds of writes**. It never
repairs damage.

A raw file copy of a **running** node's `mdbx.dat` is not a consistent state: MDBX reuses the
pages that earlier commits freed while the copy is still reading, so the copy can pair one
commit's meta page with later data pages, and neither MDBX's meta pages nor its data pages carry
checksums that would reveal it. `--recover` will make such a copy openable, but not correct.
Stop the node before copying.

A copy whose files are not writable (a read-only mount) opens too: the exclusive open never
registers in `mdbx.lck`. The scans (`get-tx`, and the header walks of `get-batch`) read a whole
hot table in one transaction; on a stopped database that is harmless.

## Build

```sh
cargo build --release -p rayls-db-inspect        # binary at target/release/rayls-db-inspect
cargo install --path bin/rayls-db-inspect        # or put rayls-db-inspect on PATH
```

The examples below assume the binary is on `PATH`.

## Usage

Every command takes one or more nodes, before or after the command name. `--db` names a node
datadir (the directory holding `consensus-db/`) or the `consensus-db` directory itself, and may
carry a label (`v1=/data/node1`). One value per flag: repeat it or separate values with commas.
The flag never swallows the arguments that follow it, so `--db ... epochs 0 5`,
`epochs --db ... 0 5` and `epochs 0 5 --db ...` all work.

Certificate signatures are always re-checked: `cert`, `header` and `header-check` verify the
quorum and aggregate BLS signature of every certificate they show against the committee the
node's own epoch records hold for that epoch (the record of the epoch, or the `next_committee`
of the one before), instead of trusting the verification state stored with the certificate. The
`verify` column reads `ok`, `genesis`, `FAILED` or `no keys`; a failure makes the verdict
`BROKEN sig_failed=N`; a node without the epoch record (nor the one before) cannot prove the
certificate, which makes an `OK` verdict `PARTIAL unverifiable=N`, and `header-check` names the
records that node lacks. Re-checking is BLS work in an optimized library, so the build profile
hardly matters: a header whose sub-dag holds six to eight certificates costs 5 ms on an idle
core and two to three times that on a busy machine. `header` and `cert` are unaffected.
`header-check` checks the nodes in parallel and verifies each batch of 256 hops on every core, so
a long check costs roughly one certificate check per core per hop: 2000 archived hops took 7 s
for one node and 27 s for five nodes on a 22-core machine already running those five nodes.
For checks of 1000 hops or more it prints a progress line per node on stderr every 1000 hops.

```sh
# Is epoch 42's record on disk, and is its certificate present and valid, on each node?
rayls-db-inspect --db v1=/data/node1 --db v2=/data/node2 --db v3=/data/node3 epoch 42

# Matrix of record / record+cert / missing for a range (or --all)
rayls-db-inspect epochs 40 45 --db /data/node1,/data/node2
rayls-db-inspect epochs --all --db /data/node1 --db /data/node2

# Check the whole epoch-record chain: linkage, certificate validity, gaps, digest index
rayls-db-inspect epoch-check --db /data/node1 --db /data/node2

# Consensus header and leader certificate at consensus number 1234
rayls-db-inspect header 1234 --db /data/node1 --db /data/node2
rayls-db-inspect cert 1234 -v --db /data/node1 --db /data/node2

# Check the header chain back 20 headers from 1234: links, digest index, certificates
rayls-db-inspect header-check 1234 --back 20 --db /data/node1 --db /data/node2

# The record chain around one epoch, record by record
rayls-db-inspect epoch-check --from 40 --to 45 -v --db /data/node1 --db /data/node2

# One batch: where it is (hot or cold), its transactions, the certificate that carried it and
# the consensus header that committed it (`header -v` lists the digests a header commits)
rayls-db-inspect get-batch 0x3f9c... --db /data/node1 --db /data/node2

# A transaction's stored path: batch, carrying certificate, committing header
rayls-db-inspect get-tx 0x8a1e... --db /data/node1 --db /data/node2
rayls-db-inspect get-tx 0x8a1e... --epoch 42 --db /data/node1     # scan only epoch 42's batches

# Per-node overview
rayls-db-inspect summary --db /data/node1

# Machine-readable output for scripts / jq
rayls-db-inspect --json epoch 42 --db /data/node1 | jq .verdict
```

`rayls-db-inspect --help` and `rayls-db-inspect <command> --help` document every flag.

## Subcommands

| Command | Per node | Verdict fields |
|---|---|---|
| `epoch <EPOCH>` | record present, digest index consistent, certificate present; `epoch_hash` matches the record; signer count vs. super-quorum; BLS aggregate verifies; `parent_hash` links to record N-1; committee hand-off matches; boundary header resolves (and in which tier); leftover transition checkpoint | `nodes variants certified genesis record_only missing not_reached` |
| `epochs <FROM_EPOCH> <TO_EPOCH>` / `--all` | per epoch `RC` record+cert, `R-` record only, `--` missing, `..` not reached yet, `??` table absent (presence only, no BLS check); row status `ok` / `partial` / `missing` / `not-reached` / `divergent` | `epochs ok partial missing divergent not_reached first` |
| `epoch-check [--from EPOCH] [--to EPOCH]` | gaps, broken `parent_hash` links, uncertified epochs, invalid certificates, committee hand-off mismatches, digest-index mismatches, nodes with no records although epochs have closed; `-v` lists every record in range (digest, parent, certificate state, index, link, hand-off, committee size); a range past the latest record is clamped and noted; issue counts are summed over nodes, `checked` is the fullest node's count | `nodes checked divergent gaps broken uncertified invalid handoff index no_records first` |
| `header <HEADER_NUMBER>` | digest, parent, tier (hot / cold / cache), leader, certificate and batch counts, commit timestamp; `-v` adds where each committed batch is stored and, in text, the sub-dag certificates and reputation scores; in JSON those come inside `raw`, the stored header itself; every certificate of the sub-dag is re-checked against the epoch's committee (`verify` column) | `nodes found missing not_reached what variants sig_failed unverifiable` |
| `cert <HEADER_NUMBER>` | leader certificate of header N: the consensus header's digest (what a `what=header` divergence refers to), certificate digest, author, round, epoch, signer indices, aggregate signature, verification state; `-v` adds parents and payload in text; in JSON they come inside `raw`, the stored certificate itself; the leader certificate's quorum and BLS signature are re-checked (`verify` column: `ok`, `genesis`, `FAILED`, `no keys`) | `nodes found missing not_reached what variants sig_failed unverifiable` |
| `get-batch <DIGEST>` | tier (hot / cold), epoch, worker, sequence number, transaction count and bytes, the sealing authority (by the execution address stored in the batch), base fee, whether the stored bytes hash to the digest, the DAG round of the certificate that carried it; each transaction (hash, type, nonce, sender, recipient, value, gas); the committing header (`hot`/`cold`, or `cache, not processed`) or `not committed`; when committed, the stored path: the sub-dag certificate whose payload lists the batch (digest, author, round, epoch, worker, header digest, created_at, signer indices, stored verification state) and the header's own fields (digest, parent, leader, certificate and batch counts, commit timestamp); a node without it is judged against the header that committed the batch (found on the nodes that hold it): `not reached` while its tip is below that header, `missing` once its tip is at or past it, `not reached (not committed on any node)` when no header commits it yet, and `not found` when no node holds the batch at all; a cold index entry whose jar row is gone is `dangling` (and counted in `missing` too) | `nodes found missing not_reached not_found bad_digest dangling` |
| `get-tx <TX_HASH> [--epoch EPOCH]` | every batch holding the transaction on the node (digest, tier, position, epoch, worker, sequence number, sealing authority, DAG round of the carrying certificate; a transaction can be sealed more than once: `copies`), each with its committing header or `not committed` and, when committed, the same stored path as `get-batch` (carrying certificate, then header); the decoded transaction; and how many batches were read (`read/total hot, cold (epochs)`); absence is `not reached` / `missing` / `not found` as for `get-batch`; `DIVERGENT what=batch` only when nodes name different batches for the same committing header | `nodes found missing not_reached not_found what variants bad_digest copies uncommitted` |
| `header-check <HEADER_NUMBER> [--back COUNT]` | one row per hop: number, digest, parent, tier, link status (`ok`, `genesis`, `end of range` for the last row, parent missing, digest mismatch, index mismatch), and the hop's certificates re-checked (`verify`); links decide where the check stops, a start header absent below the tip is `missing`, a failed signature makes the verdict `BROKEN` | `nodes hops divergent broken missing not_reached first sig_failed unverifiable` |
| `summary` | datafile size, epoch range and counts, consensus tip and its commit time, cache tip, cold tier high-water mark, node identity, leftover checkpoints, entry count of every table | none |

## Verdict

The last line of every report (except `summary`) is

```
verdict: <CODE> [<key>=<value> ...]
```

`CODE` has the same meaning in every command:

| Code | Meaning | Exit |
|---|---|---|
| `OK` | every node has it and they agree (certified where that applies) | 0 |
| `NOT_REACHED` | no node has reached it yet | 1 |
| `PARTIAL` | some nodes lack it, are behind, or are uncertified (`header-check`: some nodes have not reached the start); or a node holds no committee to re-check a certificate (`unverifiable`) | 1 |
| `MISSING` | no node has it although all should | 1 |
| `DIVERGENT` | nodes disagree on content (`what=header`; `leader` or `signers` for `cert`; `batch` for `get-tx`) | 1 |
| `BROKEN` | a chain link check failed, or stored data is inconsistent (`bad_digest`, `dangling`), or a re-checked signature failed (`sig_failed`) | 1 |
| `EMPTY` | nothing to check; for `get-batch` / `get-tx`, no node holds it and nothing says one should | 1 |

Fields come from the fixed vocabulary in the table above; zero counts are omitted. Values are
an integer, a range `a..=b`, or a word. Examples:

```
verdict: OK nodes=5 certified=5
verdict: PARTIAL nodes=3 certified=2 not_reached=1
verdict: OK epochs=3 ok=1 not_reached=1..=2
verdict: BROKEN checked=3 broken=1 first=2
verdict: DIVERGENT nodes=5 what=signers variants=2
```

Parse with `^verdict: (\w+)((?: \w+=\S+)*)$`. In JSON the same data is
`"verdict": {"code", "healthy", "fields": {...}}`, with ranges as `[a, b]`.

Notes on what the data means:

- "Missing" always means the node should have the row and does not. An epoch the node has not
  closed yet, or a header past its consensus tip, is reported as "not reached" together with
  the node's position (current epoch, latest record, tip), so an input beyond the tip is not
  mistaken for a gap.
- Epoch 0 is written as an unsigned dummy record and certified at the first epoch transition;
  until then `record-only` is its healthy state.
- The consensus chain starts at header 1, whose `parent_hash` is the digest of the default
  header (the genesis anchor); no node stores a header 0. `header-check` treats that anchor as
  genesis and `header 0` reports `not found`.
- A node that lost an epoch's records shows it in three places: `epochs` and `epoch-check`
  report the gap on that node alone (`prev-missing` at the next record); `header`, `cert` and
  `header-check` report the following epoch's certificates as `no keys`, with a `PARTIAL`
  verdict, `unverifiable=N`, and the records the node lacks. The other nodes verify the same
  headers, so the fault is that node's record collection, not the chain.
- The certificate table (`epoch_cert_by_number`) is keyed by the record's digest, not the
  epoch number. The tool follows the record to find its certificate, like the node does.
- `DIVERGENT what=signers` from `cert` means nodes hold the same leader certificate by digest
  but with different signer sets. Consensus does not hash signatures, yet the
  epoch-closing block hashes the leader signature, so this is how a fork shows up.
- DAG certificate tables are cleared at every epoch boundary. Parent digests of a past-epoch
  certificate therefore do not resolve there; the only surviving copies are inside
  `consensus_block` rows.
- Consensus headers and batches of archived epochs move to the cold tier under `cold/`. The
  tool reads that tier when the directory exists and reports the tier for every hit.
- Damaged databases are read as far as MDBX and the jars allow, and never crash the tool
  (a test suite damages copies in twenty ways: truncated, zeroed and bit-flipped pages, missing
  or garbled jar files, garbled lock files). What damage looks like: a flipped byte in a header
  is a `BROKEN` link or a failed signature; a damaged row or jar row is an error naming the node,
  the table or jar and the row; a cold tier whose index cannot be read is skipped with a note,
  the hot tables still answer; MDBX keeps three copies of its meta page at the start of the
  datafile and opens with the newest intact one it can locate (with the default 4 KiB pages a
  damaged first page or two still opens; with larger pages MDBX cannot find the others once the
  first is gone), while a file that lost all three cannot be opened at all. `header-check` reports an
  unreadable node as such and checks the others (`BROKEN unreadable=N`); the other commands stop
  at the first unreadable node, whose label the error names, so drop that node and rerun.
- Node roles and archive modes do not change what the tool reads. Validators (active or
  inactive) and observers keep the same consensus database with the same tables and the same
  lifecycle: the cold archiver runs on every role, and nothing deletes cold data. The
  execution-layer `--full` / archive distinction (reth pruning of account and storage history)
  only affects the execution database, which the tool never opens. A node built without the
  `cold-storage` feature keeps every header and batch hot; the tool then simply sees no `cold/`
  directory. Tables a role never writes (an observer's proposals and votes) are present and
  empty, and are reported as such.
- The consensus database has no transaction index. `get-tx` hashes every transaction of every
  batch (hot table, then each sealed cold epoch) and reports every match; `--epoch` skips hot
  batches of other epochs and reads only that epoch's cold jar. Not holding a batch is normal
  until the node executes the header that committed it (a worker stops distributing a batch once
  a quorum has it), so absence is judged against that header's number and the node's tip. When
  no node holds the batch (or transaction) at all, nothing says any of them should: it is
  `not found` and the verdict is `EMPTY`, not `MISSING`.
- In JSON, the `raw` field of `header -v` and `cert -v` is the wire type in its own JSON form,
  the same object the `rayls_latestHeader` RPC returns: `B256` hashes as `0x` hex; digests,
  signatures and authority identifiers as base58 strings; `signed_authorities` as the base58 of
  the serialized bitmap; `payload` as `[digest, worker]` pairs. Everything else in the report
  stays `0x` hex.

## Exit status

| Code | Meaning |
|---|---|
| 0 | every node agrees and nothing is missing (or `summary`) |
| 1 | any verdict other than `OK` |
| 2 | a database could not be opened, or the arguments were invalid |

## Output

Text by default: one aligned table per report, details per node below it, then the verdict
line. `--json` prints a single object `{ "command", ..., "nodes": [...], "verdict": {...} }`;
`command` is the subcommand name. All digests,
keys and signatures in the tool's own fields are full `0x` hex in both modes; only the `raw`
objects use the wire types' encoding described above.

## Module layout

| Path | Role |
|---|---|
| `src/main.rs` | entry point: parses arguments, prints the report as text or JSON, sets the exit code |
| `src/lib.rs` | `run`: opens the nodes' databases and dispatches to a report |
| `src/cli.rs` | clap definitions and `--help` text |
| `src/report/mod.rs` | verdict type and codes; lookup and chain-link helpers shared by the reports |
| `src/node_db.rs` | exclusive read-only open, recovery of copies, tier-aware reads |
| `src/report/epoch.rs` | `epoch`, `epochs`, `epoch-check` |
| `src/report/header.rs` | `header`, `cert`, `header-check` |
| `src/report/batch.rs` | `get-batch`, `get-tx` |
| `src/report/summary.rs` | `summary` |
| `src/view.rs` | serializable hex views of consensus types |
| `src/render.rs` | text tables |
| `tests/` | integration tests against seeded MDBX fixtures |

The read-only opener itself lives in the storage crate (`MdbxDatabase::open_read_only`,
`has_table`, `table_entry_counts`); the `get-tx` cold scan walks each jar's `(row, digest)` pairs
(`ColdStore::for_each_batch_digest_in_epoch`) and reads every payload through the same checked
lookup the node uses (`read_batch_checked`).

## Ideas for later

- `browse`: an interactive drill-down (header → certificates → batches → transactions) on top
  of the same report structs, e.g. with `ratatui`.
- A transaction-hash index for `get-tx`, so a lookup stops being a scan on large histories.
