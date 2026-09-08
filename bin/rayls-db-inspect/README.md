# rayls-db-inspect

Read-only inspection of a node's `consensus-db`, comparing several nodes side by side.

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
- Triage after an incident: `summary` and `chain-check` give a quick health picture of every
  node's consensus database.

## Safety: live nodes, stopped nodes, copies

The tool never writes and never takes the node's lock file. It opens MDBX with `MDBX_RDONLY`,
never creates tables, and only ever holds short read transactions. MDBX allows one writer and
many readers across processes, so **it is safe to run against a running node**. Two caveats:

- Results reflect what the node has flushed to MDBX. The node batches writes through an
  in-memory cache and syncs lazily, so very recent rows (seconds) may not be visible yet.
  Every table has a `live` column: `yes` when a process holds an OS lock on the directory's
  `mdbx.lck` (read from `/proc/locks`), `no` otherwise. A copied datadir reads `no`.
- A read transaction pins the pages it sees until it ends, and the node cannot evict a reader
  in another process. The tool keeps transactions to one short query each; do not wrap it in
  something that holds it open for long against a busy node.

For a **stopped node** or a **copied datadir** nothing special is needed. Pass `--exclusive`
on copies to open with `MDBX_EXCLUSIVE`: it fails if any other process has the database open,
a useful guard against pointing at the live node by mistake. A copy whose `mdbx.lck` is not
writable (for example on a read-only mount) is opened exclusively on its own. `--require-stopped`
refuses to inspect a database that a running process holds open.

Copy workflow, when you would rather not touch the live directory at all. A copy taken while the
node was writing needs one MDBX recovery pass before a read-only open works; `--recover` does
it (read-write, exclusive, no table access) and fails if the database is in use:

```sh
rsync -a --exclude lock /data/node1/consensus-db/ /tmp/node1-consensus-db/
rayls-db-inspect --recover summary --db n1=/tmp/node1-consensus-db   # once per copy
rayls-db-inspect --exclusive epoch 42 --db n1=/tmp/node1-consensus-db
```

## Build

```sh
cargo build --release -p rayls-db-inspect        # binary at target/release/rayls-db-inspect
cargo install --path bin/rayls-db-inspect        # or put rayls-db-inspect on PATH
```

The examples below assume the binary is on `PATH`.

## Usage

Every subcommand takes one or more `--db` arguments. Each is a node datadir (the directory
holding `consensus-db/`) or the `consensus-db` directory itself, optionally prefixed with a
label. One path per value: repeat the flag (`--db v1=/data/node1 --db v2=/data/node2`) or
separate paths with commas (`--db v1=/data/node1,v2=/data/node2`). The flag therefore never
swallows the positional arguments, so `epochs --db ... 0 5` and `epochs 0 5 --db ...` both
work.

```sh
# Is epoch 42's record on disk, and is its certificate present and valid, on each node?
rayls-db-inspect epoch 42 --db v1=/data/node1 --db v2=/data/node2 --db v3=/data/node3

# Matrix of record / record+cert / missing for a range (or --all)
rayls-db-inspect epochs 40 45 --db /data/node1,/data/node2
rayls-db-inspect epochs --all --db /data/node1 --db /data/node2

# Walk the whole epoch-record chain: linkage, certificate validity, gaps
rayls-db-inspect chain-check --db /data/node1 --db /data/node2

# Consensus header and leader certificate at consensus number 1234
rayls-db-inspect header 1234 --db /data/node1 --db /data/node2
rayls-db-inspect cert 1234 -v --db /data/node1 --db /data/node2

# Follow parent_hash back 20 headers and report the first broken link
rayls-db-inspect walk header 1234 --back 20 --db /data/node1 --db /data/node2

# Per-node overview
rayls-db-inspect summary --db /data/node1

# Machine-readable output for scripts / jq
rayls-db-inspect --json epoch 42 --db /data/node1 | jq .verdict
```

`rayls-db-inspect --help` and `rayls-db-inspect <command> --help` document every flag.

## Subcommands

| Command | Per node | Verdict fields |
|---|---|---|
| `epoch <EPOCH>` | record present, digest index consistent, certificate present; `epoch_hash` matches the record; signer count vs. super-quorum; BLS aggregate verifies; `parent_hash` links to record N-1; committee hand-off matches; boundary header resolves (and in which tier); leftover transition checkpoint | `nodes certified genesis record_only missing not_reached variants` |
| `epochs <FROM_EPOCH> <TO_EPOCH>` / `--all` | `RC` record+cert, `R-` record only, `--` missing, `..` not reached yet, `??` table absent (presence only, no BLS check); row status `ok` / `partial` / `missing` / `not-reached` / `divergent` | `epochs ok partial missing divergent not_reached first` |
| `chain-check [--from EPOCH] [--to EPOCH]` | gaps, broken `parent_hash` links, uncertified epochs, invalid certificates, committee hand-off mismatches; a range past the latest record is clamped and noted | `checked gaps broken uncertified invalid handoff divergent first` |
| `header <HEADER_NUMBER>` | digest, parent, tier (hot / cache / cold), leader, certificate and batch counts, commit timestamp; `-v` adds sub-dag certificates, batch presence, reputation | `nodes found missing not_reached what variants` |
| `cert <HEADER_NUMBER>` | leader certificate of header N: digest, author, round, epoch, signer indices, aggregate signature, verification state; `-v` adds parents and payload | `nodes found missing not_reached what variants` |
| `walk header <HEADER_NUMBER> [--back COUNT]` | one row per hop: number, digest, parent, tier, link status (`ok`, `genesis`, parent missing, digest mismatch, index mismatch) | `nodes hops broken not_reached divergent first` |
| `summary` | live status, datafile size, epoch range and counts, consensus tip and cache tip, cold tier high-water mark, node identity, leftover checkpoints, entry count of every table | none |

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
| `PARTIAL` | some nodes lack it, are behind, or are uncertified | 1 |
| `MISSING` | no node has it although all should | 1 |
| `DIVERGENT` | nodes disagree on content (`what=header`, `leader` or `signers` for `cert`) | 1 |
| `BROKEN` | a chain link check failed | 1 |
| `EMPTY` | nothing to check | 1 |

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

## Exit status

| Code | Meaning |
|---|---|
| 0 | every node agrees and nothing is missing (or `summary`) |
| 1 | any verdict other than `OK` |
| 2 | a database could not be opened, or the arguments were invalid |

## Output

Text by default: one aligned table per report, details per node below it, then the verdict
line. `--json` prints a single object `{ "command", ..., "nodes": [...], "verdict": {...} }`.
All digests, keys and signatures are full `0x` hex in both modes.

## Module layout

| Path | Role |
|---|---|
| `src/main.rs` | argument parsing, text/JSON rendering, exit code |
| `src/cli.rs` | clap definitions and `--help` text |
| `src/node_db.rs` | read-only open, live-process probe, tier-aware reads |
| `src/report/epoch.rs` | `epoch`, `epochs`, `chain-check` |
| `src/report/header.rs` | `header`, `cert`, `walk header` |
| `src/report/summary.rs` | `summary` |
| `src/view.rs` | serializable hex views of consensus types |
| `src/render.rs` | text tables |
| `tests/` | one file per subcommand against seeded MDBX fixtures |

The read-only opener itself lives in the storage crate
(`MdbxDatabase::open_read_only`, `has_table`, `table_entry_counts`).

## Ideas for later

- `batch <digest>`: locate a batch in the hot table or a cold jar and dump it.
- `walk epoch <N> --back K`: the epoch-record analogue of `walk header`.
- `browse`: an interactive drill-down (header → certificates → batches) on top of the same
  report structs, e.g. with `ratatui`.
- Make `ConsensusHeader` serialize to JSON so the RPC can serve what this tool reads from disk.
