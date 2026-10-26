# Validators report active_cvv, the rpc node observer; all are caught up and in the same epoch.
e=$(epoch validator-1)
for n in $(nodes); do
    want=active_cvv
    [[ $n == rpc ]] && want=observer
    has_role "$n" "$want" || fail "$n role is $(role "$n"), want $want"
    [[ $(status_field "$n" is_caught_up) == true ]] || fail "$n is not caught up"
    ne=$(epoch "$n")
    ((ne >= e - 1 && ne <= e + 1)) || fail "$n is in epoch $ne, validator-1 in $e"
    log "$n: $want, epoch $ne"
done
