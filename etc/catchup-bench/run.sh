#!/usr/bin/env bash
#
# Runs bench.sh in a container whose loopback adds DELAY_MS each way (round trip = 2 x DELAY_MS),
# so every message between the local nodes pays a real round trip. See README.md.
#
# usage: run.sh DELAY_MS WORKDIR -v VALIDATOR_BIN -b LABEL=BIN [-b ...] [other bench.sh options]
#
# Binaries, WORKDIR and Foundry's cast are mounted at their host paths. The container needs
# NET_ADMIN for netem; the nodes run as the calling user.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
IMAGE=axyl-catchup-bench:1

usage() { echo "usage: $0 DELAY_MS WORKDIR -v VALIDATOR_BIN -b LABEL=BIN [-b ...] [bench.sh options]"; exit 2; }
[[ $# -ge 2 && $1 =~ ^[0-9]+$ ]] || usage
DELAY=$1 WORKDIR=$2
shift 2
mkdir -p "$WORKDIR"
WORKDIR=$(cd "$WORKDIR" && pwd)

declare -A MOUNTED=()
MOUNTS=()
mount_ro() { [[ -n ${MOUNTED[$1]:-} ]] || { MOUNTS+=(-v "$1:$1:ro"); MOUNTED[$1]=1; }; }
mount_ro "$HERE"
MOUNTS+=(-v "$WORKDIR:$WORKDIR")

ARGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
    -v)
        [[ $# -ge 2 ]] || usage
        bin=$(realpath "$2"); mount_ro "$bin"; ARGS+=(-v "$bin"); shift 2 ;;
    -b)
        [[ $# -ge 2 && $2 == *=* ]] || usage
        bin=$(realpath "${2#*=}"); mount_ro "$bin"; ARGS+=(-b "${2%%=*}=$bin"); shift 2 ;;
    *)
        ARGS+=("$1"); shift ;;
    esac
done
if cast=$(command -v cast); then
    cast=$(realpath "$cast")
    MOUNTS+=(-v "$cast:/usr/local/bin/cast:ro")
fi

docker image inspect "$IMAGE" >/dev/null 2>&1 || docker build -q -t "$IMAGE" "$HERE" >/dev/null

exec docker run --rm --cap-add NET_ADMIN --ulimit nofile=65536:65536 \
    -e DELAY="$DELAY" -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" "${MOUNTS[@]}" "$IMAGE" \
    bash -c 'set -e
        tc qdisc add dev lo root netem delay "${DELAY}ms" limit 1000000
        exec setpriv --reuid="$HOST_UID" --regid="$HOST_GID" --clear-groups env HOME=/tmp \
            bash "$0" "$@"' \
    "$HERE/bench.sh" -d "$WORKDIR" "${ARGS[@]}"
