#!/usr/bin/env bash

# Usage: ./add-child-stream.sh rootIp childId

ROOT_IP=${1}
CHILD_ID=${2}

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CREATE_SCRIPT_FILE="$FILE_DIR/create-droplet.sh"
ADD_STREAM_SCRIPT="$FILE_DIR/add-stream.sh"
source $CREATE_SCRIPT_FILE

### Create child and stream
CHILD_NAME="child-$CHILD_ID"

echo "Creating child node"
echo "==================="

CHILD_SETUP_SCRIPT=$(create_init_script DistributedChildMain ${ROOT_IP} ${ROOT_CONTROL_PORT} ${ROOT_WINDOW_PORT} \
                        ${CHILD_PORT} $CHILD_ID "1")
creat_droplet "$CHILD_TAG" "$CHILD_SETUP_SCRIPT" "$CHILD_NAME"
echo

$ADD_STREAM_SCRIPT $CHILD_NAME $CHILD_ID
