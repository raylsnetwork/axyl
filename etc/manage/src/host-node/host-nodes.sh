#!/bin/bash -i
function prepareHostEnvironments() {
    echo -ne "Preparing host environments...";

    local opResult=""
    local sourceTar="$DIST_DIR/source.tar.gz"

    if [[ "$USE_DOCKER_FOR_HOST_NODES" == "0" ]]; then
        local sourceDir="$SCRIPT_DIR/../.."
        cd "$sourceDir" && tar -czf "$sourceTar" ./Cargo* ./bin ./crates ./rayls-contracts ./etc/docker-network/Dockerfile
    fi


    local computersSize=$(getComputersSize)
    for i in $(seq 0 $(($computersSize-1)))
    do
        opResult=$(executeOnNodeClient $i "sudo rm -rf /usr/src/rayls-network && sudo mkdir -p /usr/src/rayls-network && sudo chmod -R 777 /usr/src/rayls-network")
        if [[ "$?" != 0 ]]; then
            echo -e "${STYLE_RED}Error:${STYLE_DEFAULT} Unable to create /usr/src/rayls-network on computer[$i]";
            exit $?;
        fi

        if [[ "$USE_DOCKER_FOR_HOST_NODES" == "0" ]]; then
            opResult=$(copyOnNodeClient $i "$sourceTar" "/usr/src/rayls-network")
            if [[ "$?" != 0 ]]; then
                echo -e "${STYLE_RED}Error:${STYLE_DEFAULT} Unable to copy to computer[$i]";
                exit $?;
            fi
        fi
    done

    rm -rf "$sourceTar"

    echo -e "${STYLE_GREEN}OK${STYLE_DEFAULT}";
}
