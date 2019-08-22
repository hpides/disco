#!/usr/bin/env bash

# Usage: ./complete-check.sh timeoutSeconds

TIMEOUT=${1}

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
COMMON_FILE="$FILE_DIR/common.sh"
source $COMMON_FILE

function complete_check {
    local IP=${1}

    local result=$(ssh_cmd ${IP} "kill -0 \$(cat /tmp/RUN_PID) 2>&1")
    if [[ ${result} == *"No such process"* ]]; then
        echo "complete"
    fi
}

ALL_IPS=($(get_all_ips))
NUM_DROPLETS=${#ALL_IPS[@]}

SLEEP_DURATION=10
MAX_NUM_SLEEPS=$(expr $TIMEOUT / $SLEEP_DURATION)
NUM_SLEEPS=0

COMPLETE_IPS=()
while [[ ${#COMPLETE_IPS[@]} -lt ${NUM_DROPLETS} ]]; do
    INCOMPLETE_IPS=($(echo ${ALL_IPS[@]} ${COMPLETE_IPS[@]} | tr ' ' '\n' | sort | uniq -u | tr '\n' ' '))
    echo -ne "\rWaiting for ${#INCOMPLETE_IPS[@]} more node(s) to complete..."

    for ip in ${INCOMPLETE_IPS[@]}; do
        COMPLETE_STATUS=$(complete_check ${ip})
        if [[ ${COMPLETE_STATUS} == "complete" ]]; then
            COMPLETE_IPS+=(${ip})
        fi
    done

    if [[ ${#COMPLETE_IPS[@]} -lt ${NUM_DROPLETS} ]]; then
        if [[ $NUM_SLEEPS -eq $MAX_NUM_SLEEPS ]]; then
            let "difference = ${NUM_DROPLETS} - ${#COMPLETE_IPS[@]}"
            echo
            echo "$difference application(s) still running after timeout. They will be terminated."
            echo "This most likely indicates an error or a missing stream/child end message."
            return
        fi
        sleep $SLEEP_DURATION
        NUM_SLEEPS=$(expr $NUM_SLEEPS + 1)
    fi
done

echo
echo "All applications terminated before the timeout."
echo
