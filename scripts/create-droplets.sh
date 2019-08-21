#!/usr/bin/env bash

# Usage: ./create-droplets.sh numChildren numStreams

NUM_CHILDREN=${1:-0}
NUM_STREAMS=${2:-0}

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CREATE_SCRIPT_FILE="$FILE_DIR/create-droplet.sh"
source $CREATE_SCRIPT_FILE

function get_ips {
    local TAG_NAME="$1"
    doctl compute droplet list --format="PublicIPv4" --no-header --tag-name="$TAG_NAME"
}

[[ ${NUM_STREAMS} -ge ${NUM_CHILDREN} ]] || (>&2 echo "Need at least as many streams as children!" && exit 1)

echo -e "Creating root node\n=================="
ROOT_SETUP_SCRIPT=$(create_init_script DistributedRootMain ${ROOT_CONTROL_PORT} ${ROOT_WINDOW_PORT} "/tmp/disco-res")
creat_droplet "$ROOT_TAG" "$ROOT_SETUP_SCRIPT" "root"
echo

if [[ "$NUM_CHILDREN" -gt "0" ]]; then
    echo -e "Creating child nodes\n===================="

    wait_for_ips 1 "$ROOT_TAG"
    ROOT_IP=$(get_ips ${ROOT_TAG})

    for i in $(seq ${NUM_CHILDREN}); do
        CHILD_SETUP_SCRIPT=$(create_init_script DistributedChildMain ${ROOT_IP} ${ROOT_CONTROL_PORT} ${ROOT_WINDOW_PORT} \
                                ${CHILD_PORT} "$i" "1")
        creat_droplet "$CHILD_TAG" "$CHILD_SETUP_SCRIPT" "child-$i" ${i}
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
        STREAM_SETUP_SCRIPT=$(create_init_script SustainableThroughputRunner ${stream_id} \
                                "${CHILD_IPS[$child_idx]}:${CHILD_PORT}")
        creat_droplet "$STREAM_TAG" "$STREAM_SETUP_SCRIPT" "stream-$stream_id" ${stream_id}
    done
fi

echo
let "total_num = ${NUM_CHILDREN} + ${NUM_STREAMS} + 1"
echo "Created $total_num droplets."
