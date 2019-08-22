#!/usr/bin/env bash

# Usage: ./run-all.sh numNodes numEventsPerSecond runDuration windows aggFunctions [--delete | --no-delete]

NUM_DROPLETS=${1}
NUM_EVENTS_PER_SECOND=${2}
RUN_DURATION_SECONDS=${3}
WINDOW_STRING=${4}
AGG_STRING=${5}

DELETE_AFTER=""
if [[ "$*" == *delete* ]]
then
    DELETE_AFTER="--delete"
fi
if [[ "$*" == *--no-delete* ]]
then
    DELETE_AFTER="--no-delete"
fi

RUN_TYPE="long"
if [[ "$*" == *--short* ]]
then
    RUN_TYPE="short"
fi

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
READY_CHECK="$FILE_DIR/ready-check.sh"
COMPLETE_CHECK="$FILE_DIR/complete-check.sh"
ADD_HOSTS="$FILE_DIR/add-hosts.sh"

COMMON_FILE="$FILE_DIR/common.sh"
source $COMMON_FILE


BASE_DIR=$(cd "$FILE_DIR/.." && pwd)
RUN_FILES_DIR="$BASE_DIR/benchmark-runs/$(date +"%Y-%m-%d-%H%M")_$NUM_DROPLETS-nodes_$NUM_EVENTS_PER_SECOND-events_$RUN_DURATION_SECONDS-seconds"

function run_droplet {
    local IP=${1}
    local NAME=${2}
    ssh_cmd ${IP} "~/run.sh" &> "$RUN_FILES_DIR/$NAME.log"
}

function upload_run_params() {
    local IP=${1}
    local BENCHMARK_ARGS="${@:2}"
    ssh_cmd ${IP} "echo \"export BENCHMARK_ARGS=\\\"${BENCHMARK_ARGS}\\\"\" >> ~/benchmark_env"
}

#########################
# ACTUAL CODE THAT IS RUN
#########################

if [[ -z "$NUM_DROPLETS" ]] || ! [[ ${NUM_DROPLETS} =~ ^[0-9]+$ ]]; then
    echo "Need to specify expected number of nodes. Got: '$NUM_DROPLETS'"
    exit 1
fi

mkdir -p ${RUN_FILES_DIR}
echo "Writing logs to $RUN_FILES_DIR"
echo

echo "Getting IPs..."
wait_for_ips $NUM_DROPLETS ""
ALL_IPS=($(get_all_ips))

if [[ ${#ALL_IPS[@]} -ne $NUM_DROPLETS ]]; then
    echo "Did not get enough IPs while waiting."
    exit 1
fi

echo "All IPs (${#ALL_IPS[@]}): ${ALL_IPS[@]}"
echo

if [[ $RUN_TYPE == "long" ]]; then
  echo "Adding IPs to $KNOWN_HOSTS_FILE"
  echo "This may take a while if the nodes were just created."
  $ADD_HOSTS

  echo "Waiting for node setup to complete..."
  $READY_CHECK
fi

echo "Setup done. Uploading benchmark arguments on all nodes."

CHILD_IPS=($(get_droplet_list "PublicIPv4" "child"))
NUM_CHILDREN=${#CHILD_IPS[@]}

STREAM_IPS=($(get_droplet_list "PublicIPv4" "stream"))
NUM_STREAMS=${#STREAM_IPS[@]}

NUM_STREAMS_PER_CHILD=$(expr $NUM_STREAMS / $NUM_CHILDREN)

ROOT_IP=$(get_droplet_list "PublicIPv4" "root")
upload_run_params $ROOT_IP $NUM_CHILDREN $WINDOW_STRING $AGG_STRING

for i in ${!CHILD_IPS[@]}; do
    upload_run_params ${CHILD_IPS[$i]} ${NUM_STREAMS_PER_CHILD}
done

for i in ${!STREAM_IPS[@]}; do
    upload_run_params ${STREAM_IPS[$i]} ${NUM_EVENTS_PER_SECOND} ${RUN_DURATION_SECONDS}
done
echo

echo "Starting \`run.sh\` on all nodes."

ALL_NAMES=($(get_all_names))
for i in ${!ALL_IPS[@]}; do
    run_droplet ${ALL_IPS[$i]} ${ALL_NAMES[$i]} &
done
echo

echo "To view root logs:"
echo "tail -F $RUN_FILES_DIR/root.log"

echo
MAX_RUN_DURATION=$(expr $RUN_DURATION_SECONDS + 30)
echo "Running on nodes for a maximum of $MAX_RUN_DURATION seconds..."
$COMPLETE_CHECK $MAX_RUN_DURATION

echo
echo "Ending script by killing all PIDs (if they are still running)"
for ip in ${ALL_IPS[@]}; do
    ssh_cmd ${ip} "kill -9 \$(cat /tmp/RUN_PID) > /dev/null"
done

echo "Killed all PIDs."

if [[ ${DELETE_AFTER} == "" ]]; then
    read -p "Delete all droplets? (y/N) " delete_droplets
    if [[ ${delete_droplets} == "y" ]]; then
        DELETE_AFTER="--delete"
    fi
fi

if [[ ${DELETE_AFTER} == "--delete" ]]; then
    echo "Deleting all droplets..."
    doctl compute droplet delete --force $(doctl compute droplet list --format="ID" --no-header)
fi

echo "Done."
