#!/usr/bin/env bash

# Usage: ./ready-check.sh [numDroplets]

NUM_DROPLETS=${1:-0}

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
COMMON_FILE="$FILE_DIR/common.sh"
source $COMMON_FILE

function check_ready {
    local IP=${1}

    local result=$(ssh_cmd ${IP} "ls")
    if [[ ${result} == *"run.sh"* ]]; then
        echo "ready"
    fi
}

if [[ $NUM_DROPLETS -gt 0 ]]; then
    wait_for_ips $NUM_DROPLETS ""
fi

ALL_IPS=($(get_all_ips))
NUM_DROPLETS=${#ALL_IPS[@]}

READY_IPS=()
while [[ ${#READY_IPS[@]} -lt ${NUM_DROPLETS} ]]; do
    UNREADY_IPS=($(echo ${ALL_IPS[@]} ${READY_IPS[@]} | tr ' ' '\n' | sort | uniq -u | tr '\n' ' '))
    echo -ne "\rWaiting for ${#UNREADY_IPS[@]} more node(s) to become ready..."

    for ip in ${UNREADY_IPS[@]}; do
        READY_STATUS=$(check_ready ${ip})
        if [[ ${READY_STATUS} == "ready" ]]; then
            READY_IPS+=(${ip})
        fi
    done
    if [[ ${#READY_IPS[@]} -lt ${NUM_DROPLETS} ]]; then
        sleep 5
    fi
done
echo
