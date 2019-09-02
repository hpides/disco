#!/usr/bin/env bash

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

PYTON_SCRIPT="$FILE_DIR/find-sustainable-throughput.py"
STREAM_ADD_SCRIPT="$FILE_DIR/../../scripts/add-stream.sh"
CHILD_ADD_SCRIPT="$FILE_DIR/../../scripts/add-child-stream.sh"
VENV_DIR="$FILE_DIR/../../venv"
source "$VENV_DIR/bin/activate"

WINDOWS="TUMBLING,1000"
AGG_FNS="MAX"

function single_run() {
    local NUM_CHILDREN=${1}
    local NUM_STREAMS=${2}
    local OTHER_ARGS=${@:3}

    python3 -u "$PYTON_SCRIPT" --num-children "$NUM_CHILDREN" --num-streams "$NUM_STREAMS" \
                               --windows "$WINDOWS" --agg-functions "$AGG_FNS" $OTHER_ARGS
}

function delete_droplet {
    local NAME=${1}

    doctl compute droplet delete --force $(doctl compute droplet list --format="ID" --no-header --tag-name="$NAME")
}

# ===================================
# SINGLE-CHILD RUNS
# ===================================

# Run 1 child, 1 stream
echo "Running 1 child, 1 stream"
single_run 1 1 --no-delete

# Add second stream
echo "Adding second stream"
$STREAM_ADD_SCRIPT "child-1" "2"


# Run 1 child, 2 streams
echo "Running 1 child, 2 streams"
single_run 1 2 --no-delete --no-create

# Add third and fourth stream
echo "Adding third and fourth stream"
$STREAM_ADD_SCRIPT "child-1" "3"
$STREAM_ADD_SCRIPT "child-1" "4"


# Run 1 child, 4 streams
echo "Running 1 child, 4 streams"
single_run 1 4 --no-delete --no-create

# Add four more streams
$STREAM_ADD_SCRIPT "child-1" "5"
$STREAM_ADD_SCRIPT "child-1" "6"
$STREAM_ADD_SCRIPT "child-1" "7"
$STREAM_ADD_SCRIPT "child-1" "8"

# Run 1 child, 8 streams and delete afterwards
echo "Running 1 child, 8 streams"
single_run 1 8 --no-create

delete_droplet ""

# ===================================
# MULTI-CHILD RUNS
# ===================================

# Run 2 children, 2 streams
echo "Running 2 children, 2 streams"
single_run 2 2 --no-delete

echo "Adding third and fourth stream"
$STREAM_ADD_SCRIPT "child-1" "3"
#$STREAM_ADD_SCRIPT "child-2" "4"

# Run 2 children, 4 streams
echo "Running 2 children, 4 streams"
single_run 2 4 --no-delete --no-create

echo "Deleting third and fourth stream"
delete_droplet "stream-3"
delete_droplet "stream-4"
sleep 5

# Add third and fourth child/stream
echo "Adding third and fourth child/stream"
$CHILD_ADD_SCRIPT 3
$CHILD_ADD_SCRIPT 4

# Run 4 children, 4 streams
echo "Running 4 children, 4 streams"
single_run 4 4 --no-delete --no-create

echo "Adding four more streams"
$STREAM_ADD_SCRIPT "child-1" "5"
$STREAM_ADD_SCRIPT "child-2" "6"
$STREAM_ADD_SCRIPT "child-3" "7"
$STREAM_ADD_SCRIPT "child-4" "8"

# Run 4 children, 8 streams
echo "Running 4 children, 8 streams"
single_run 4 8 --no-delete --no-create

echo "Deleting last four streams"
delete_droplet "stream-5"
delete_droplet "stream-6"
delete_droplet "stream-7"
delete_droplet "stream-8"
sleep 5

# Add four more child/streams
$CHILD_ADD_SCRIPT 5
$CHILD_ADD_SCRIPT 6
$CHILD_ADD_SCRIPT 7
$CHILD_ADD_SCRIPT 8

# Run 8 child, 8 streams
echo "Running 8 children, 8 streams"
single_run 8 8 --no-delete --no-create

echo "Adding eigth more streams"
$STREAM_ADD_SCRIPT "child-1" "9"
$STREAM_ADD_SCRIPT "child-2" "10"
$STREAM_ADD_SCRIPT "child-3" "11"
$STREAM_ADD_SCRIPT "child-4" "12"
$STREAM_ADD_SCRIPT "child-5" "13"
$STREAM_ADD_SCRIPT "child-6" "14"
$STREAM_ADD_SCRIPT "child-7" "15"
$STREAM_ADD_SCRIPT "child-8" "16"

# Run 8 child, 16 streams and delete afterwards
echo "Running 8 children, 16 streams"
single_run 8 16 --no-create

delete_droplet ""
