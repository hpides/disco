#!/usr/bin/env bash

# Usage: ./add-child-stream.sh rootIp childId

ROOT_IP=${1}
CHILD_ID=${2}

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CREATE_SCRIPT_FILE="$FILE_DIR/create-droplet.sh"
ADD_STREAM_SCRIPT="$FILE_DIR/add-stream.sh"
source $CREATE_SCRIPT_FILE

echo "Creating child node"
echo "==================="

create_child "$ROOT_IP" "$CHILD_ID"
echo

CHILD_NAME="$CHILD_TAG-$CHILD_ID"
$ADD_STREAM_SCRIPT "$CHILD_NAME" "$CHILD_ID"
