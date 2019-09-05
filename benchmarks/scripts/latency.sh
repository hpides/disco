#!/usr/bin/env bash

RUN_DURATION=120
WINDOW_STRING="TUMBLING,1000"
AGG_STRING="MAX"

echo "BENCHMARK WITH: $RUN_DURATION seconds, window: $WINDOW_STRING, agg: $AGG_STRING"

BM_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BASE_DIR="$( cd "$BM_DIR/../.." && pwd )"

RUN_SCRIPT="$BM_DIR/latency.py"
STREAM_ADD_SCRIPT="$BASE_DIR/scripts/add-stream.sh"
CREATE_DROPLETS_SCRIPT="$BASE_DIR/scripts/create-droplets.sh"
CHILD_ADD_SCRIPT="$BASE_DIR/scripts/add-child-stream.sh"
VENV_DIR="$BASE_DIR/venv"
source "$VENV_DIR/bin/activate"


function run_latency_bm() {
    local NUM_DROPLETS=${1}
    local NUM_EVENTS=${2}
    local OTHER_ARGS=${@:3}

    python3 -u "$RUN_SCRIPT" --num-nodes "$NUM_DROPLETS" \
                             --num-events "$NUM_EVENTS" \
                             --duration "$RUN_DURATION" \
                             --windows "$WINDOW_STRING" \
                             --agg-functions "$AGG_STRING" ${OTHER_ARGS}
}

function delete_droplet {
    local NAME=${1}

    doctl compute droplet delete --force $(doctl compute droplet list --format="ID" --no-header --tag-name="$NAME")
}

# ===================================
# SINGLE-CHILD RUNS
# ===================================

$CREATE_DROPLETS_SCRIPT 1 1

## Run 1 child, 1 stream
echo "Running 1 child, 1 stream"
run_latency_bm 3 105000

# Add second stream
echo "Adding second stream"
$STREAM_ADD_SCRIPT "child-1" "2"

# Run 1 child, 2 streams
echo "Running 1 child, 2 streams"
run_latency_bm 4 664000

# Add third and fourth stream
echo "Adding third and fourth stream"
$STREAM_ADD_SCRIPT "child-1" "3"
$STREAM_ADD_SCRIPT "child-1" "4"

# Run 1 child, 4 streams
echo "Running 1 child, 4 streams"
run_latency_bm 6 343000

# Add four more streams
$STREAM_ADD_SCRIPT "child-1" "5"
$STREAM_ADD_SCRIPT "child-1" "6"
$STREAM_ADD_SCRIPT "child-1" "7"
$STREAM_ADD_SCRIPT "child-1" "8"

# Run 1 child, 8 streams and delete afterwards
echo "Running 1 child, 8 streams"
run_latency_bm 10 164000

delete_droplet ""


# ===================================
# MULTI-CHILD RUNS
# ===================================

$CREATE_DROPLETS_SCRIPT
$CHILD_ADD_SCRIPT 1
$STREAM_ADD_SCRIPT "child-1" "9"
$CHILD_ADD_SCRIPT 2
$STREAM_ADD_SCRIPT "child-2" "10"

# Run 2 children, 4 streams
echo "Running 2 children, 4 streams"
run_latency_bm 7 1085000

# Add third and fourth child/stream
echo "Adding third and fourth child/streams"
$CHILD_ADD_SCRIPT 3
$STREAM_ADD_SCRIPT "child-3" "11"
$CHILD_ADD_SCRIPT 4
$STREAM_ADD_SCRIPT "child-4" "12"

# Run 4 children, 8 streams
echo "Running 4 children, 8 streams"
run_latency_bm 13 695000

# Add four more child/streams
$CHILD_ADD_SCRIPT 5
$STREAM_ADD_SCRIPT "child-5" "13"
$CHILD_ADD_SCRIPT 6
$STREAM_ADD_SCRIPT "child-6" "14"
$CHILD_ADD_SCRIPT 7
$STREAM_ADD_SCRIPT "child-7" "15"
$CHILD_ADD_SCRIPT 8
$STREAM_ADD_SCRIPT "child-8" "16"

# Run 8 child, 16 streams and delete afterwards
echo "Running 8 children, 16 streams"
run_latency_bm 25 726000

delete_droplet ""
