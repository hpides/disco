#!/usr/bin/env bash

# Usage: ./add-stream.sh childName streamId

CHILD_NAME=${1}
STREAM_ID=${2}

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CREATE_SCRIPT_FILE="$FILE_DIR/create-droplet.sh"
source $CREATE_SCRIPT_FILE

echo "Creating stream node"
echo "===================="

wait_for_ips 1 "$CHILD_NAME"
CHILD_IP=$(get_ips ${CHILD_NAME})

STREAM_SETUP_SCRIPT=$(create_init_script SustainableThroughputRunner ${STREAM_ID} \
                        "$CHILD_IP:${CHILD_PORT}")
creat_droplet "$STREAM_TAG" "$STREAM_SETUP_SCRIPT" "stream-$STREAM_ID"
