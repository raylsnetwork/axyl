# #397: cost of a failed walk to an epoch end

```sh
cargo build --release -p rayls-network --features epoch-end-fault
etc/test/epoch-end-retry/run.sh /tmp/epoch-end-retry target/release/rayls-network
```

Needs Docker allowed to run containers with `NET_ADMIN` (the container adds a fixed network delay) and Foundry's `cast` on `PATH` (for the transaction load). Build with `-p rayls-network`, not `--bin rayls-network`: a `--bin` build cannot decrypt the keys the benchmark creates. Three rounds take about 45 minutes. `bench/run.sh` builds its small Docker image on first use.

The `epoch-end-fault` feature compiles in a test hook (`epoch_end_fault` in `crates/consensus/state-sync/src/consensus.rs`). It does nothing unless `RL_EPOCH_END_FAULT` is set; then the first walk to an epoch end fails its fetch of that header once, as if every peer refused it.

Each run prints one line:

```text
round 1 refuse: 1 obs, gap 1513 blocks (226s offline, ~7 tx/block), first new block 13.6s, all caught up in 56.2s (boot included), served header requests 313 by number/56 by hash/20 latest (peak 40/s), validator CPU 94.3s (max 24s on one), same hash
```

- `first new block`: time from the observer's restart until it has a block past where it stopped.
- `all caught up`: time from the restart until it reaches the validators' height at restart (boot included).
- `served header requests` and `validator CPU`: the load catch-up put on the validators.
- `same hash`: the observer ended on the validators' block hash.

To measure a fix for #397, put it on top of this branch, build and run the same commands: the `refuse` runs should come close to the `none` runs.
