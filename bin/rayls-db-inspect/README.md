# rayls-db-inspect

Read-only inspection of a node's `consensus-db`, comparing several nodes side by side. A node is
a database directory (`--db`) or, for the epoch and header commands, a running node's RPC
endpoint (`--rpc`); the two mix freely in one run.

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
- A block's transactions are in question: `get-tx <TX_HASH>` finds the batch that carries a
  transaction and the consensus header that committed it; `get-batch <DIGEST>` shows the batch
  itself, hot or archived.

## Safety: live nodes, stopped nodes, copies

The tool never modifies a database's contents and never takes the node's `lock` file. It opens
MDBX with `MDBX_RDONLY`, never creates tables, and holds one short read transaction per query.
MDBX allows one writer and many readers across processes, so **it is safe to run against a
running node**. What it does write, so the promise is exact:

- MDBX registers every reader in `mdbx.lck` (creating the file if it is missing) and takes
  advisory `fcntl` locks on `mdbx.lck` and `mdbx.dat` for the run. Run the tool as the node's
  user, so a file it creates is one the node can use.
- `--recover` performs one read-write open (below). Nothing else writes.

Caveats when the node is running:

- Results reflect what the node has flushed to MDBX. The node batches writes through an
  in-memory cache and syncs lazily, so very recent rows (seconds) may not be visible yet.
  Every report shows each node's `live` state (a column, or the first row of the `epochs`
  matrix): `yes` when a process holds an OS lock on `mdbx.lck` (read from `/proc/locks` by
  inode), `no` when none does, `?` when that cannot be determined (not Linux), `rpc` for an RPC
  node, `recovered` for a copy `--recover` just opened.
- A read transaction pins the pages it sees until it ends, and the node cannot evict a reader
  in another process. The tool keeps transactions to one short query each; do not wrap it in
  something that holds it open for long against a busy node. The exceptions are the scans and
  the snapshot: `get-tx` reads the whole hot batch table in one transaction (`--epoch` filters
  what it looks at but the hot table is still read end to end; it does bound the cold tier to
  one epoch's jar); `get-tx`, and `get-batch` whenever some node lacks the batch or under `-v`,
  read the hot header tables in one transaction each; `snapshot` copies the whole database
  inside one read transaction (well under a second per hundred megabytes of used pages). On a
  busy node, run the scans against a snapshot.
- A run consumes one MDBX reader slot per node per concurrent read (the node has 256); a check
  never uses more than one per node.

For a **stopped node** nothing special is needed. `--exclusive` opens with `MDBX_EXCLUSIVE`: it
fails if any other process has the database open, a useful guard against pointing at the live
node by mistake. A copy whose `mdbx.lck` is not writable (for example on a read-only mount) is
opened exclusively on its own. `--require-stopped` refuses to inspect a database that a running
process holds open, and one whose liveness it cannot determine. MDBX's own file locks are what
keep a wrong liveness answer harmless: a read-write open against a live environment fails with
`MDBX_BUSY`. The tool's probe exists to refuse early with a clear message.

Copies. To inspect offline, or to keep evidence, take a snapshot rather than copying files:

```sh
rayls-db-inspect snapshot --to /var/tmp/node1-snap --db n1=/data/node1   # works on a running node
rayls-db-inspect epoch 42 --db n1=/var/tmp/node1-snap
```

`snapshot` lets MDBX copy the environment inside one read transaction (`MDBX_CP_COMPACT`: every
page is walked and validated, only used pages are written, and the copy's head meta is steady),
then copies the sealed cold jars, verifies them and reads the copy back. The result is one
committed hot state plus a cold tier at or after it (a row can appear in both tiers, never in
neither) and opens read-only with no recovery step. `summary` dates any directory by its tip
header's commit time, so a copy needs no marker to say what it holds and as of when. When the
source is a stopped node whose last commit was never synced (killed or crashed), MDBX refuses to
read it until it is recovered and recovery writes, so `snapshot` copies its files as they are
(nothing else writes to a stopped node) and recovers the copy, leaving the original untouched
for the node's own restart. The `mdbx.dat` it writes is sparse: its length stays at the geometry's
floor (1 GiB by default) while it occupies only the used pages, so copy it onward with
`cp --sparse=always` or `rsync -S`. MDBX writes the copy with `O_DIRECT`, which tmpfs (a
`/tmp` on many hosts) refuses; use a disk-backed destination. The destination must not exist,
or be an empty directory, outside the source; a second run racing for the same destination
fails instead of touching the first. MDBX writes the copy's meta pages last, so an interrupted
copy has no valid head and is reported as unreadable rather than mistaken for a whole one.

A raw file copy (`cp`, `rsync`) of a **running** node's `mdbx.dat` is not a snapshot: MDBX
reuses the pages that earlier commits freed while the copy is still reading, so the copy can
pair one commit's meta page with later data pages, and neither MDBX's meta pages nor its data
pages carry checksums that would reveal it. Such a copy also has an unsynced ("weak") head that
a read-only open refuses. `--recover` makes it openable: one read-write, exclusive open that
settles the head, the same recovery the node itself performs when it restarts; it refuses a
directory a running process holds and marks the node `recovered` in the report. It rewrites the copy's meta pages (hash the copy first if it is evidence), and
**on any host other
than the one that took the copy, or after that host reboots, MDBX rolls the copy back to its
last steady commit, silently dropping up to a few seconds of writes**. It never makes a torn copy
consistent. Reserve raw copies for stopped nodes, and even then prefer `snapshot`.

## Build

```sh
cargo build --release -p rayls-db-inspect        # binary at target/release/rayls-db-inspect
cargo install --path bin/rayls-db-inspect        # or put rayls-db-inspect on PATH
```

The examples below assume the binary is on `PATH`.

## Usage

Every command takes one or more nodes, before or after the command name. `--db` names a node
datadir (the directory holding `consensus-db/`) or the `consensus-db` directory itself; `--rpc`
names a node's JSON-RPC URL. Either may carry a label (`v1=/data/node1`, `v2=http://10.0.0.2:8545`).
One value per flag: repeat it or separate values with commas. The flags never swallow the
arguments that follow them, so `--db ... epochs 0 5`, `epochs --db ... 0 5` and
`epochs 0 5 --db ...` all work.

An RPC node answers what the `rayls` namespace serves: certified epoch records
(`rayls_epochRecord`, `rayls_epochRecordByHash`) and consensus headers (`rayls_latestHeader`,
plus `rayls_consensusHeaderByNumber` and `rayls_consensusHeaderByHash`, added with this tool so
that a given header can be fetched over RPC at all). So `epoch`, `epochs`,
`epoch-check`, `header`, `cert` and `header-check` accept RPC nodes; `get-batch`,
`get-tx` and `summary` need databases. Two limits follow from what the RPC serves: a record
stored without its certificate (epoch 0 until the first transition, or the incident case) is
reported absent by an RPC node, and `header -v` cannot list batch presence for one. Nodes older
than this change serve only `rayls_latestHeader`: their tip is what it reports, and the tool says
so when it needs another header.
A header served over RPC may come from the node's verified-but-unprocessed cache; since every
certificate is re-checked, a cached header that was never verified fails the check instead of
passing as canonical. An RPC node's tip is found by probing `rayls_consensusHeaderByNumber`
upward from the number `rayls_latestHeader` reports, about two calls per doubling plus a
bisection; this also finds the headers a node holds above its canonical tip. The RPC has no
record listing, so `epochs --all` and `epoch-check` probe one epoch per call from 0 to the node's
current epoch; `header-check` makes two calls per hop (the parent by number, the digest index
by hash) plus one per epoch for the committee; the other commands make a handful of calls per
node. Nodes are queried in parallel, each one sequentially, and `--rpc-rate` (default 10
requests per second per node, `0` to lift it) spaces the requests so a check cannot flood a
node; a long `header-check` over RPC is therefore slow by design, and databases are the right
source for whole-chain checks.

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
The `live` column reads `rpc` for RPC nodes.

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

# One batch: where it is (hot or cold), what it holds; -v lists the transactions and the
# consensus header that committed it (`header -v` lists the digests a header commits)
rayls-db-inspect get-batch 0x3f9c... -v --db /data/node1 --db /data/node2

# Which batch carries a transaction, and which header committed that batch
rayls-db-inspect get-tx 0x8a1e... --db /data/node1 --db /data/node2
rayls-db-inspect get-tx 0x8a1e... --epoch 42 --db /data/node1     # scan only epoch 42's batches

# Per-node overview
rayls-db-inspect summary --db /data/node1

# Compare a local database with two other operators' running nodes
rayls-db-inspect cert 1234 --db v1=/data/node1 \
    --rpc org2=http://org2.example:8545 --rpc org3=http://org3.example:8545

# Machine-readable output for scripts / jq
rayls-db-inspect --json epoch 42 --db /data/node1 | jq .verdict
```

`rayls-db-inspect --help` and `rayls-db-inspect <command> --help` document every flag.

## Subcommands

| Command | Per node | Verdict fields |
|---|---|---|
| `epoch <EPOCH>` | record present, digest index consistent, certificate present; `epoch_hash` matches the record; signer count vs. super-quorum; BLS aggregate verifies; `parent_hash` links to record N-1; committee hand-off matches; boundary header resolves (and in which tier); leftover transition checkpoint | `nodes variants certified genesis record_only missing not_reached` |
| `epochs <FROM_EPOCH> <TO_EPOCH>` / `--all` | first row: each node's `live` state; then per epoch `RC` record+cert, `R-` record only, `--` missing, `..` not reached yet, `??` table absent (presence only, no BLS check); row status `ok` / `partial` / `missing` / `not-reached` / `divergent` | `epochs ok partial missing divergent not_reached first` |
| `epoch-check [--from EPOCH] [--to EPOCH]` | gaps, broken `parent_hash` links, uncertified epochs, invalid certificates, committee hand-off mismatches, digest-index mismatches, nodes with no records although epochs have closed; `-v` lists every record in range (digest, parent, certificate state, index, link, hand-off, committee size); a range past the latest record is clamped and noted; issue counts are summed over nodes, `checked` is the fullest node's count | `nodes checked divergent gaps broken uncertified invalid handoff index no_records first` |
| `header <HEADER_NUMBER>` | digest, parent, tier (hot / cold / cache), leader, certificate and batch counts, commit timestamp; `-v` adds where each committed batch is stored and, in text, the sub-dag certificates and reputation scores; in JSON those come inside `raw`, the stored header itself; every certificate of the sub-dag is re-checked against the epoch's committee (`verify` column) | `nodes found missing not_reached what variants sig_failed unverifiable` |
| `cert <HEADER_NUMBER>` | leader certificate of header N: the consensus header's digest (what a `what=header` divergence refers to), certificate digest, author, round, epoch, signer indices, aggregate signature, verification state; `-v` adds parents and payload in text; in JSON they come inside `raw`, the stored certificate itself; the leader certificate's quorum and BLS signature are re-checked (`verify` column: `ok`, `genesis`, `FAILED`, `no keys`) | `nodes found missing not_reached what variants sig_failed unverifiable` |
| `get-batch <DIGEST>` | tier (hot / cold), epoch, worker, sequence number, transaction count and bytes, beneficiary, base fee, whether the stored bytes hash to the digest; a node without it is judged against the header that committed the batch (found on the nodes that hold it): `not reached` while its tip is below that header, `missing` once its tip is at or past it, `not reached (not committed on any node)` when no header commits it yet, and `not found` when no node holds the batch at all; a cold index entry whose jar row is gone is `dangling` (and counted in `missing` too); `-v` lists each transaction (hash, type, nonce, sender, recipient, value, gas) and the committing header (`hot`/`cold`, or `cache, not processed`) | `nodes found missing not_reached not_found bad_digest dangling` |
| `get-tx <TX_HASH> [--epoch EPOCH]` | the batch holding the transaction (digest, tier, position, epoch, worker, sequence number), the consensus header that committed that batch or `not committed`, every batch holding the transaction on the node (a transaction can be sealed more than once: `copies`), each with its committing header or `not committed`, the decoded transaction, and how many batches were read (`read/total hot, cold (epochs)`); absence is `not reached` / `missing` / `not found` as for `get-batch`; `DIVERGENT what=batch` only when nodes name different batches for the same committing header | `nodes found missing not_reached not_found what variants bad_digest copies uncommitted` |
| `header-check <HEADER_NUMBER> [--back COUNT]` | one row per hop: number, digest, parent, tier, link status (`ok`, `genesis`, `end of range` for the last row, parent missing, digest mismatch, index mismatch), and the hop's certificates re-checked (`verify`); links decide where the check stops, a start header absent below the tip is `missing`, a failed signature makes the verdict `BROKEN` | `nodes hops divergent broken missing not_reached first sig_failed unverifiable` |
| `summary` | live status, datafile size, epoch range and counts, consensus tip and its commit time, cache tip, cold tier high-water mark, node identity, leftover checkpoints, entry count of every table | none |
| `snapshot --to DIR` | copies one node's consensus database into `DIR` as one committed state (MDBX copies inside a read transaction, compacted) with its sealed cold jars; the copy opens without `--recover`; a stopped node with an unsynced last commit is copied file by file and the copy recovered; no verdict |

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
  tool reads that tier when the directory exists and reports the tier for every hit. A live
  node keeps sealing epochs while the tool runs, so the tool attaches a cold tier created after
  the open and, when a header or batch is in no tier, re-reads the jar index from disk once
  before reporting it absent; a check that spans an epoch transition does not misreport the
  headers that moved.
- Damaged databases are read as far as MDBX and the jars allow, and never crash the tool
  (a test suite damages copies in twenty ways: truncated, zeroed and bit-flipped pages, missing
  or garbled jar files, garbled lock files). What damage looks like: a flipped byte in a header
  is a `BROKEN` link or a failed signature; a damaged row or jar row is an error naming the node,
  the table or jar and the row; a cold tier whose index cannot be read is skipped with a note,
  the hot tables still answer; MDBX keeps three copies of its meta page at the start of the
  datafile and opens with the newest intact one, so a damaged first page or two still opens,
  while a file that lost all three cannot be opened at all. `header-check` reports an
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
| `src/lib.rs` | `run`: opens the nodes (databases or RPC endpoints) and dispatches to a report |
| `src/cli.rs` | clap definitions and `--help` text |
| `src/report/mod.rs` | verdict type and codes; lookup and chain-link helpers shared by the reports |
| `src/node_db.rs` | read-only open, live-process probe, tier-aware reads |
| `src/source.rs` | a node to inspect: a database, or an RPC endpoint with memoized `rayls_*` calls |
| `src/report/epoch.rs` | `epoch`, `epochs`, `epoch-check` |
| `src/report/header.rs` | `header`, `cert`, `header-check` |
| `src/report/batch.rs` | `get-batch`, `get-tx` |
| `src/report/summary.rs` | `summary` |
| `src/report/snapshot.rs` | `snapshot`: consistent copy of one node (MDBX copy in a read transaction, sealed cold jars, marker) |
| `src/view.rs` | serializable hex views of consensus types |
| `src/render.rs` | text tables |
| `tests/` | integration tests against seeded MDBX fixtures and an in-process mock RPC node |

The read-only opener itself lives in the storage crate (`MdbxDatabase::open_read_only`,
`has_table`, `table_entry_counts`), as does the sequential cold-batch iterator the `get-tx` scan uses
(`ColdStore::for_each_batch_in_epoch`).

## Ideas for later

- `browse`: an interactive drill-down (header → certificates → batches → transactions) on top
  of the same report structs, e.g. with `ratatui`.
- A transaction-hash index for `get-tx`, so a lookup stops being a scan on large histories.
