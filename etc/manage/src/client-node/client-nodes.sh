#!/bin/bash -i

function makeClientNodeByComputerIndex() {
    local computerIndex="$1"
    local dockerBuildArgNoCache=""

    local computerId=$(getComputerId $computerIndex)
    local containerName="$DOCKER_TAG_RAYLS_NODE_CLIENT--$computerId"

    local consensusIp=$(getComputerConsensusIp $computerIndex)
    local consensusPort=$(getComputerConsensusPort $computerIndex)
    if [[ "$USE_DOCKER_FOR_HOST_NODES" == "1" ]]; then
        consensusIp=$(calcConsensusIpByComputerIndex $computerIndex)
    fi

    _=$(executeOnNodeClientStopAndRemoveDocker $computerIndex "$containerName")

    if [[ "$USE_DOCKER_FOR_HOST_NODES" == "0" && "$USE_CACHE" == "0" ]]; then
        dockerBuildArgNoCache="--no-cache"
        _=$(executeOnNodeClient $computerIndex "sudo docker image rm $DOCKER_TAG_RAYLS_NODE_CLIENT 2>&1")
    fi

    local imageId=$(executeOnNodeClient $computerIndex "sudo docker image ls -f 'reference=$DOCKER_TAG_RAYLS_NODE_CLIENT' -q 2>&1")
    if [ "$?" != 0 ]; then
        exit 1
    fi

    if [ -z "$imageId" ]; then
        local sourceTar="/usr/src/rayls-network/source.tar.gz"
        local dockerBuildArgGitSha=""
        if [[ "$RAYLS_GIT_SHA" != "" ]]; then
            dockerBuildArgGitSha="--build-arg VERGEN_GIT_SHA=$RAYLS_GIT_SHA"
        fi

        _=$(executeOnNodeClient $computerIndex "cd /usr/src/rayls-network && sudo tar -xzf $sourceTar && rm -rf $sourceTar")

        local buildResult
        buildResult=$(executeOnNodeClient $computerIndex "cd /usr/src/rayls-network && sudo docker build $dockerBuildArgNoCache $dockerBuildArgGitSha -t "$DOCKER_TAG_RAYLS_NODE_CLIENT" --file "./etc/docker-network/Dockerfile" ./ 2>&1")
        if [ "$?" != 0 ]; then
            echo "$buildResult" > "$DIST_DIR/build-$computerIndex.log"
            exit 2
        fi
    fi

    if [[ "$UPGRADE_ONLY" == "1" ]]; then
        exit 0
    fi

    local dataDirPath=$(getComputerDataDirPath $computerIndex)
    local isValidator=$(isValidatorByComputerId $computerId)
    local blsPassphrase="local"
    if [[ "$isValidator" == "1" ]]; then
        blsPassphrase=$(getValidatorBlsPassphraseByComputerId $computerId)
    fi
    local dockerArgName="--name $containerName"
    local dockerArgUser="-u root"
    local dockerArgVolume="-v '$dataDirPath:/home/nonroot/data'"
    local dockerArgEnv="-e RL_EXTERNAL_PRIMARY_ADDR=/ip4/$consensusIp/udp/$consensusPort/quic-v1 -e RL_EXTERNAL_WORKER_ADDRS=/ip4/$consensusIp/udp/$(( $consensusPort + 100 ))/quic-v1 -e RL_BLS_PASSPHRASE='$blsPassphrase'"
    local dockerArgEntryPoint="--entrypoint /bin/bash"
    _=$(executeOnNodeClient $computerIndex "sudo docker run -d $dockerArgName $dockerArgUser $dockerArgVolume $dockerArgEnv $dockerArgEntryPoint $DOCKER_TAG_RAYLS_NODE_CLIENT -c 'sleep infinity'")
    if [ "$?" != 0 ]; then
        exit 3
    fi

    _=$(executeOnNodeClient $computerIndex "sudo docker exec $containerName /bin/bash -c 'rm -rf /home/nonroot/data/*'")

    if [[ "$isValidator" == "1" ]]; then
        local walletAddress=$(getValidatorWalletAddressIdByIndex $computerIndex)
        opResult=$(executeOnNodeClient $computerIndex "sudo docker exec $containerName rayls keytool generate validator --datadir /home/nonroot/data --address $walletAddress")
        if [ "$?" != 0 ]; then
            echo $opResult
            exit 4
        fi

        _=$(executeOnNodeClient $computerIndex "sudo docker cp $containerName:/home/nonroot/data/node-info.yaml /usr/src/rayls-network/ && sudo chmod -R 777 /usr/src/rayls-network")

        _=$(executeOnNodeClientStopAndRemoveDocker $computerIndex "$containerName")

        _=$(copyFromNodeClient $computerIndex "/usr/src/rayls-network/node-info.yaml" "$DIST_DIR/validator-$computerIndex.yaml")

        _=$(executeOnNodeClient $computerIndex "sudo rm -rf /usr/src/rayls-network/node-info.yaml")
    else
        opResult=$(executeOnNodeClient $computerIndex "sudo docker exec $containerName rayls keytool generate observer --datadir /home/nonroot/data --address 0x0000000000000000000000000000000000000000")
        if [ "$?" != 0 ]; then
            echo $opResult
            exit 4
        fi

        _=$(executeOnNodeClient $computerIndex "sudo docker cp $containerName:/home/nonroot/data/node-info.yaml /usr/src/rayls-network/ && sudo chmod -R 777 /usr/src/rayls-network")

        _=$(executeOnNodeClientStopAndRemoveDocker $computerIndex "$containerName")

        _=$(copyFromNodeClient $computerIndex "/usr/src/rayls-network/node-info.yaml" "$DIST_DIR/observer-$computerIndex.yaml")

        _=$(executeOnNodeClient $computerIndex "sudo rm -rf /usr/src/rayls-network/node-info.yaml")
    fi
}

function makeClientNodes() {
    echo -ne "Making client nodes...";

    local computersSize=$(getComputersSize)
    local pids=()
    for i in $(seq 0 $(($computersSize-1)))
    do
        makeClientNodeByComputerIndex "$i" &
        pids+=($!)
    done

    for i in "${!pids[@]}"; do
        local pid="${pids[$i]}"
        wait "$pid" &> /dev/null
        local exitCode="$?"

        if [[ "$exitCode" != 0 ]]; then
            if [[ "$exitCode" == 1 ]]; then
                echo -e "${STYLE_RED}Error:${STYLE_DEFAULT} Unable to check for available docker image at computer[$i]";
                exit 1;
            elif [[ "$exitCode" == 2 ]]; then
                echo -e "${STYLE_RED}Error:${STYLE_DEFAULT} Unable to build client docker on computer[$i]. Build output: $DIST_DIR/build-$i.log";
                exit 1;
            elif [[ "$exitCode" == 3 ]]; then
                echo -e "${STYLE_RED}Error:${STYLE_DEFAULT} Unable to start client on computer[$i]";
                exit 1;
            elif [[ "$exitCode" == 4 ]]; then
                echo -e "${STYLE_RED}Error:${STYLE_DEFAULT} Unable to generate validator/observer keys on computer[$i]";
                exit 1;
            else
                echo -e "${STYLE_RED}Error:${STYLE_DEFAULT} Error at computer[$i]";
                exit 1;
            fi 
        fi
    done

    echo -e "${STYLE_GREEN}OK${STYLE_DEFAULT}";
}
