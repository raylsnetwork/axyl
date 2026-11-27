# Committee members report active_cvv, every other node observer; all are caught up and in the same epoch.
# An observer's is_caught_up can blink false while a new tip propagates, so it is polled, not read once.
caught_up() { [[ $(status_field "$1" is_caught_up) == true ]]; }
e=$(epoch validator-1) || fail "validator-1 did not report its epoch"
members=$(committee) && [[ -n $members ]] || fail "could not read the committee from the registry"
for n in $(nodes); do
    # rpc nodes, and a validator that is staked but not in the committee yet, follow it as observers
    want=observer
    grep -qx "$n" <<< "$members" && want=active_cvv
    has_role "$n" "$want" || fail "$n role is $(role "$n"), want $want"
    wait_for 30 "$n to report caught up" caught_up "$n"
    ne=$(epoch "$n") || fail "$n did not report its epoch"
    ((ne >= e - 1 && ne <= e + 1)) || fail "$n is in epoch $ne, validator-1 in $e"
    log "$n: $want, epoch $ne"
done
