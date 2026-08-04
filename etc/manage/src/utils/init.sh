#!/bin/bash -i

STYLE_BOLD='\033[1m'
STYLE_RED='\033[1;31m'
STYLE_GREEN='\033[1;32m'
STYLE_ORANGE='\033[1;33m'
STYLE_DEFAULT='\033[0m'

DIST_DIR="$SCRIPT_DIR/dist"

DOCKER_RAYLS_STACK="rayls-stack"
DOCKER_RAYLS_STACK_NETWORK="$DOCKER_RAYLS_STACK-network"
DOCKER_TAG_RAYLS_NODE_HOST="$DOCKER_RAYLS_STACK-node-host"
DOCKER_TAG_RAYLS_NODE_CLIENT="$DOCKER_RAYLS_STACK-node-client"

USE_DOCKER_FOR_HOST_NODES="0"

# Commit the client image is built from. The builder image has no git and the
# source tarball carries no .git directory, so the sha has to be passed in as a
# docker build arg to end up in `rayls version`.
RAYLS_GIT_SHA=""

function init() {
    mkdir -p "$DIST_DIR"

    RAYLS_GIT_SHA=$(git -C "$SCRIPT_DIR/../.." rev-parse HEAD 2>/dev/null)
}
