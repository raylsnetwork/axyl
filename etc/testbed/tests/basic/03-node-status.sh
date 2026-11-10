# Validators report active_cvv, rpc nodes observer; all are caught up and in the same epoch.
# An observer's is_caught_up can blink false while a new tip propagates, so it is polled, not read once.
caught_up() { [[ $(status_field "$1" is_caught_up) == true ]]; }
e=$(epoch validator-1)
for n in $(nodes); do
    want=active_cvv
    [[ $n == rpc-* ]] && want=observer
    has_role "$n" "$want" || fail "$n role is $(role "$n"), want $want"
    wait_for 30 "$n to report caught up" caught_up "$n"
    ne=$(epoch "$n")
    ((ne >= e - 1 && ne <= e + 1)) || fail "$n is in epoch $ne, validator-1 in $e"
    log "$n: $want, epoch $ne"
done
