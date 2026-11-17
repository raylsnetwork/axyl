# Rust e2e suite (genesis, restarts, epochs). It runs its own nodes on ports 8545-8542, so the testbed is paused.
stop_all
trap start_all EXIT
cd "$ROOT"
cargo test -p e2e-tests --test it -- --include-ignored --test-threads 1
