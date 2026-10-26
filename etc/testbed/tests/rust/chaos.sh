# Rust chaos-framework (25 scenarios). Its tc/iptables faults need Linux + root and silently pass otherwise.
[[ $(uname) == Linux && $EUID -eq 0 ]] || skip "needs Linux and root"
cd "$ROOT"
cargo test -p chaos-framework --test chaos_scenarios -- --ignored --test-threads 1
