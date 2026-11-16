# #399: time to the first new block during catch-up

Build two binaries with `cargo build --release -p rayls-network` (for example `main` and this branch, or this branch and your change on top of it), then:

```sh
etc/test/first-block-latency/run.sh /tmp/first-block main="$MAIN_BIN" pr="$PR_BIN"
```

Needs Docker allowed to run containers with `NET_ADMIN` (the container adds a fixed network delay) and Foundry's `cast` on `PATH` (for the transaction load). Build with `-p rayls-network`, not `--bin rayls-network`: a `--bin` build cannot decrypt the keys the benchmark creates. Use separate checkouts with separate target directories for the two binaries, so cargo does not mix their crates. It takes about 75 minutes; `bench/run.sh` builds its small Docker image on first use.

It runs three cases: a gap with one epoch end and 1 observer, the same with 3 observers catching up together, and a gap with no epoch end. Each run prints one line:

- `first new block`: time from the observer's restart until it has a block past where it stopped (one value per observer).
- `all caught up`: time from the restart until every observer reaches the validators' height at restart (boot included).
- `served header requests` (with the peak per second) and `validator CPU`: the load catch-up put on the validators. The CPU includes producing blocks under the transaction load, so compare it between the two binaries of the same case.
- `same hash`: every observer ended on the validators' block hash.

A fix for #399 should lower the first new block without much more validator load, above all with 3 observers.
