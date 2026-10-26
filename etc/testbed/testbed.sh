#!/usr/bin/env bash
# Local Rayls testbed: a 4-validator network as native processes, plus test plans. See README.md.
set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
    cat << EOF
usage: $0 <command>

  up                      build, run genesis (first time only) and start all nodes
  down                    stop all nodes (keeps data)
  clean                   stop all nodes and delete $DATA
  status                  show every node's pid, rpc, block, epoch and role
  start|stop|kill|pause|resume <node>
                          control one node: 1..5 (validator-N) or rpc
  add-validator           add validator-5 and stake it into the committee
  add-rpc                 add an rpc (observer) node
  logs <node>             follow a node's log
  load                    send a steady stream of etc/tps transfers until Ctrl-C
  test [plan|plan/test]   run tests (default: basic). Plans: $(ls "$TESTBED/tests" | tr '\n' ' ')
                          'all' = basic chaos extended basic chaos; LOAD=1 runs them under load

settings (env): RAYLS_BIN, TESTBED_DATA, EPOCH_SECS ($EPOCH_SECS), HTTP_ADDR ($HTTP_ADDR),
                LOG_LEVEL ($LOG_LEVEL), NODE_ARGS, GENESIS_ARGS, LOAD_TXS, SOAK_MINUTES, SOAK_INTERVAL
EOF
    exit 1
}

die() { echo "error: $*" >&2; exit 1; }

node_name() { if [[ $1 =~ ^[0-9]+$ ]]; then echo "validator-$1"; else echo "$1"; fi; }

# one test run at a time: test.lock holds the runner's pid
not_busy() {
    local p
    if p=$(cat "$DATA/test.lock" 2> /dev/null) && kill -0 "$p" 2> /dev/null; then
        die "test run $p is using this testbed; stop it first"
    fi
}

# create the lock atomically (noclobber), replacing it only if its runner is gone
lock() {
    until (set -C; echo $$ > "$DATA/test.lock") 2> /dev/null; do
        not_busy
        rm -f "$DATA/test.lock"
    done
}

up() {
    command -v cast > /dev/null || die "Foundry 'cast' is required (https://getfoundry.sh)"
    if [[ -z $RAYLS_BIN ]]; then
        (cd "$ROOT" && cargo build --release -p rayls-network --bin rayls-network)
    fi
    [[ -x $BIN ]] || die "binary not found: $BIN"
    [[ -f $DATA/genesis/genesis.yaml ]] || genesis
    start_all
    wait_for 120 "the first block" at_height validator-1 1
    status
}

status() {
    local n f='%-12s %-7s %-23s %-7s %-6s %s\n'
    printf "$f" NODE PID RPC BLOCK EPOCH ROLE
    for n in $(nodes); do
        if running "$n"; then
            printf "$f" "$n" "$(pid "$n")" "$(url "$n")" "$(height "$n" || echo -)" "$(epoch "$n" || echo -)" "$(role "$n" || echo -)"
        else
            printf "$f" "$n" down "$(url "$n")" - - -
        fi
    done
}

run_tests() {
    local files=() f p n name rc t0 before passed=0 skipped=0 failed=()
    [[ $# -gt 0 ]] || set -- basic
    # basic + chaos on 4 validators, extended adds validator-5 and rpc, then basic + chaos again on those
    [[ $* == all ]] && set -- basic chaos extended basic chaos
    [[ -f $DATA/genesis/genesis.yaml ]] || die "no testbed in $DATA, start one with: $0 up"
    lock
    trap 'stop_load; rm -f "$DATA/test.lock"' EXIT
    [[ $LOAD != 1 ]] || start_load
    for p in "$@"; do
        if [[ -d $TESTBED/tests/$p ]]; then
            files+=("$TESTBED/tests/$p"/*.sh)
        elif [[ -f $TESTBED/tests/$p.sh ]]; then
            files+=("$TESTBED/tests/$p.sh")
        else
            die "unknown plan or test: $p"
        fi
    done
    for f in "${files[@]}"; do
        name=${f#"$TESTBED/tests/"}
        name=${name%.sh}
        echo "=== $name: $(sed -n 's/^# //p;q' "$f")"
        before=$(running_nodes)
        t0=$SECONDS
        rc=0
        bash -c 'set -eo pipefail; source "$1"; source "$2"' _ "$TESTBED/lib.sh" "$f" || rc=$?
        case $rc in
            0) passed=$((passed + 1)); echo "--- PASS $name ($((SECONDS - t0))s)" ;;
            77) skipped=$((skipped + 1)); echo "--- SKIP $name" ;;
            *)
                failed+=("$name")
                echo "--- FAIL $name ($((SECONDS - t0))s)"
                # bring back whatever the test left paused or stopped, and let it catch up before the next test
                for n in $before; do resume_node "$n"; start_node "$n"; done
                for n in $before; do (wait_synced "$n") || echo "  recovery: $n did not catch up" >&2; done
                ;;
        esac
    done
    echo
    echo "passed: $passed  skipped: $skipped  failed: ${#failed[@]} ${failed[*]}"
    [[ ${#failed[@]} -eq 0 ]]
}

case "$1" in
    up) up ;;
    down) not_busy; stop_all ;;
    clean) not_busy; stop_all; rm -rf "$DATA" ;;
    status) status ;;
    start | stop | kill | pause | resume)
        [[ -n $2 ]] || usage
        "$1_node" "$(node_name "$2")"
        ;;
    add-validator)
        add_validator
        echo "validator-5 staked; it joins the committee within a few epochs"
        ;;
    add-rpc)
        add_rpc
        echo "rpc node started at $(url rpc)"
        ;;
    logs) tail -f "$DATA/$(node_name "${2:?node}").log" ;;
    load)
        echo "sending load through validator-1, see $DATA/load.log (Ctrl-C to stop)"
        load_loop
        ;;
    test) shift; run_tests "$@" ;;
    *) usage ;;
esac
