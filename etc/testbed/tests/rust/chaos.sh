# Rust chaos-framework (25 scenarios). Its tc/iptables faults need Linux + root and silently pass otherwise.
[[ $(uname) == Linux && $EUID -eq 0 ]] || skip "needs Linux and root"
# when run through sudo, hand target/ back so later non-root builds still work
[[ -z $SUDO_UID ]] || trap 'chown -R "$SUDO_UID:$SUDO_GID" "$ROOT/target"' EXIT
cd "$ROOT"
cargo test -p chaos-framework --test chaos_scenarios -- --ignored --test-threads 1
