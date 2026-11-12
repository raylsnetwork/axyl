# Catch-up benchmark

Measures how fast observers catch up after falling behind, on a local network where every message between nodes pays a real round trip. Use it to compare catch-up changes against `main` (see #276, #369 and PR #371).

A run starts 4 validators and 1 or more observers in one container. The container's loopback adds a fixed delay with netem. Optional senders keep transactions flowing. For each round and each binary under test, in turn, the observers stop, fall behind by the given number of blocks, and restart together on that binary.

## Requirements

- Docker, allowed to run containers with `NET_ADMIN` (for netem). `run.sh` builds the small `axyl-catchup-bench:1` image from the `Dockerfile` here on first use.
- Foundry's `cast` on `PATH`, for the transaction load (`-t`).
- Release binaries built with:

  ```sh
  cargo build --release -p rayls-network
  ```

  Build with `-p`, not `--bin rayls-network`. The benchmark creates its validator keys with the binary itself, and a binary built with `--bin` cannot decrypt them ("Could not decrypt BLS key").

To compare two versions, build each one and copy `target/release/rayls-network` somewhere stable between builds.

## Usage

```sh
etc/catchup-bench/run.sh DELAY_MS WORKDIR -v VALIDATOR_BIN -b LABEL=BIN [-b LABEL=BIN ...] [options]
```

- `DELAY_MS`: delay added each way, so the round trip is twice this.
- `WORKDIR`: an empty or new directory for datadirs, logs and `summary.txt`.
- `-v`: the binary the validators run.
- `-b LABEL=BIN`: a binary under test. Repeat it to compare. The observers warm up on the first one.

| Option | Default | Meaning |
|---|---|---|
| `-g GAP` | 300 | Blocks the validators advance while the observers are stopped |
| `-r ROUNDS` | 1 | Rounds; each round runs every `-b` binary once, in order |
| `-m OBSERVERS` | 1 | Observers that catch up together |
| `-t TX_PER_SEC` | 0 | Transaction load, spread over funded accounts at 4 tx/s each |
| `-e EPOCH_SECS` | 36000 | Epoch length. 10 hours keeps epoch ends out of the gap; 60 puts several in it |

Example: `main` against a branch, 100 ms round trip, 50 tx/s, 1500 blocks behind, 3 observers:

```sh
etc/catchup-bench/run.sh 50 /tmp/bench-1500 -v "$HOME/bins/main" \
    -b main="$HOME/bins/main" -b pr="$HOME/bins/pr" -t 50 -g 1500 -m 3
```

`matrix.sh OUTDIR LABEL=BIN [LABEL=BIN ...]` runs the cases behind the numbers in PR #371 (about 3 hours).

## Output

Each round prints one line and appends it to `WORKDIR/summary.txt`:

```text
round 1 pr: 3 obs, gap 1516 blocks (202s offline, ~6 tx/block), first new block 25.4/24.1/24.1s, all caught up in 85.7s (boot included), served header requests 893 by number/196 by hash/12 latest, same hash
```

- `gap`: blocks behind, counted in execution blocks. One consensus header usually carries several.
- `first new block`: time from restart until each observer has a block past where it stopped.
- `all caught up`: time from restart until every observer reaches the validators' height at restart.
- `served header requests`: consensus-header requests the validators answered during the round, by number, by hash, and for the latest block. This is the load catch-up puts on validators.
- `same hash`: every observer has the validators' block hash at that height. Anything else is printed as `HASH DIFFERS`.

`WORKDIR/progress-<round>-<label>.tsv` traces the first observer's height every 2 s. Node logs stay in `WORKDIR`.

## Caveats

- Other load on the machine changes the timings. Check `uptime` before trusting a run, and repeat a run that looks off.
- The request counts come from the validators' debug `request received` log line (`crates/consensus/network/src/consensus/reqres.rs`). If that line changes, the counts read 0.
- All nodes share one machine and one delay. This adds real round trips, but not bandwidth limits, packet loss, or different delays per peer.
- The keys and funded accounts are local test keys, for this benchmark only.
