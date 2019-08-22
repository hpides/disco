#!/usr/bin/env bash

# Usage: ./add-child-stream.sh childId

CHILD_ID=${1}

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CREATE_SCRIPT_FILE="$FILE_DIR/create-droplet.sh"
ADD_STREAM_SCRIPT="$FILE_DIR/add-stream.sh"
source $CREATE_SCRIPT_FILE

echo "Creating child node"
echo "==================="

ROOT_IP=$(get_ips "$ROOT_TAG")
create_child "$ROOT_IP" "$CHILD_ID"
echo

CHILD_NAME="$CHILD_TAG-$CHILD_ID"
$ADD_STREAM_SCRIPT "$CHILD_NAME" "$CHILD_ID"
