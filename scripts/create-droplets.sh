#!/usr/bin/env bash

# Usage: ./create-droplets.sh numChildren numStreams

NUM_CHILDREN=${1:-0}
NUM_STREAMS=${2:-0}

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
COMMON_FILE="$FILE_DIR/common.sh"
source $COMMON_FILE

CREATE_SCRIPT_FILE="$FILE_DIR/create-droplet.sh"
source $CREATE_SCRIPT_FILE

echo -e "Creating root node\n=================="
ROOT_SETUP_SCRIPT=$(create_init_script DistributedRootMain ${ROOT_CONTROL_PORT} ${ROOT_WINDOW_PORT} "/tmp/disco-res")
creat_droplet "$ROOT_TAG" "$ROOT_SETUP_SCRIPT" "root"
echo

if [[ "$NUM_CHILDREN" -gt "0" ]]; then
    echo -e "Creating child nodes\n===================="

    wait_for_ips 1 "$ROOT_TAG"
    ROOT_IP=$(get_ips ${ROOT_TAG})

    for child_id in $(seq ${NUM_CHILDREN}); do
        create_child "$ROOT_IP" "$child_id" "$child_id"
    done
    echo
fi

if [[ "$NUM_STREAMS" -gt "0" ]]; then
    wait_for_ips $NUM_CHILDREN "$CHILD_TAG"
    CHILD_IPS=($(get_ips "$CHILD_TAG"))

    echo
    echo -e "Creating stream nodes\n====================="
    let "stream_max_idx = ${NUM_STREAMS} - 1"
    for i in $(seq 0 ${stream_max_idx}); do
        let "child_idx = ${i} % ${NUM_CHILDREN}"
        let "stream_id = ${i} + 1"
        create_stream ${CHILD_IPS[$child_idx]} "$stream_id" "$stream_id"
    done
fi

echo
let "total_num = ${NUM_CHILDREN} + ${NUM_STREAMS} + 1"
echo "Created $total_num droplets."
