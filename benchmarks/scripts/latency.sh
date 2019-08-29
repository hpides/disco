#!/usr/bin/env bash

# Usage: ./latency.sh numEventsPerSecond runDuration windows aggFunctions

NUM_EVENTS_PER_SECOND=${1}
RUN_DURATION=${2}
WINDOW_STRING=${3}
AGG_STRING=${4}

BM_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BASE_DIR="$( cd "$BM_DIR/../.." && pwd )"

RUN_SCRIPT="$BM_DIR/lib/run.py"
STREAM_ADD_SCRIPT="$BASE_DIR/scripts/add-stream.sh"
CREATE_DROPLETS_SCRIPT="$BASE_DIR/scripts/create-droplets.sh"
CHILD_ADD_SCRIPT="$BASE_DIR/scripts/add-child-stream.sh"
VENV_DIR="$BASE_DIR/venv"
source "$VENV_DIR/bin/activate"


function run_single_latency_bm() {
    local NUM_DROPLETS=${1}
    local NUM_EVENTS=${2}
    local OTHER_ARGS=${@:3}

    python3 -u "$RUN_SCRIPT" --num-nodes "$NUM_DROPLETS" --num-events "$NUM_EVENTS" --duration "$RUN_DURATION" \
                             --windows "$WINDOW_STRING" --agg-functions "$AGG_STRING" ${OTHER_ARGS}
}

function run_latency_bm() {
    local NUM_DROPLETS=${1}
    local OTHER_ARGS=${@:2}

    local QUARTER=$(expr $NUM_EVENTS_PER_SECOND / 4)

    # 25% throughput
    local QUARTER_THROUGHPUT=$QUARTER
    echo "Running with quarter events/s: $QUARTER_THROUGHPUT"
    run_single_latency_bm $NUM_DROPLETS $QUARTER_THROUGHPUT $OTHER_ARGS

    # 50% throughput
    local HALF_THROUGHPUT=$(expr $QUARTER \* 2)
    echo "Running with half events/s: $HALF_THROUGHPUT"
    run_single_latency_bm $NUM_DROPLETS $HALF_THROUGHPUT $OTHER_ARGS

    # 75% throughput
    local THREE_QUARTER_THROUGHPUT=$(expr $QUARTER \* 3)
    echo "Running with three quarter events/s: $THREE_QUARTER_THROUGHPUT"
    run_single_latency_bm $NUM_DROPLETS $THREE_QUARTER_THROUGHPUT $OTHER_ARGS

    # 100% throughput
    echo "Running with full events/s: $NUM_EVENTS_PER_SECOND"
    run_single_latency_bm $NUM_DROPLETS $NUM_EVENTS_PER_SECOND $OTHER_ARGS
}

# ===================================
# SINGLE-CHILD RUNS
# ===================================

#$CREATE_DROPLETS_SCRIPT 1 1

## Run 1 child, 1 stream
echo "Running 1 child, 1 stream"
run_latency_bm 3

## Add second stream
#echo "Adding second stream"
#$STREAM_ADD_SCRIPT "child-1" "2"
#
## Run 1 child, 2 streams
#echo "Running 1 child, 2 streams"
#run_latency_bm 4
#
## Add third and fourth stream
#echo "Adding third and fourth stream"
#$STREAM_ADD_SCRIPT "child-1" "3"
#$STREAM_ADD_SCRIPT "child-1" "4"
#
## Run 1 child, 4 streams
#echo "Running 1 child, 4 streams"
#run_latency_bm 6
#
## Add four more streams
#$STREAM_ADD_SCRIPT "child-1" "5"
#$STREAM_ADD_SCRIPT "child-1" "6"
#$STREAM_ADD_SCRIPT "child-1" "7"
#$STREAM_ADD_SCRIPT "child-1" "8"
#
## Run 1 child, 8 streams and delete afterwards
#echo "Running 1 child, 8 streams"
#run_latency_bm 10 --delete


# ===================================
# MULTI-CHILD RUNS
# ===================================

#$CREATE_DROPLETS_SCRIPT 2 2
#
## Run 2 children, 2 streams
#echo "Running 2 children, 2 streams"
#run_latency_bm 5 --no-delete
#
## Add third and fourth child/stream
#echo "Adding third and fourth child/stream"
#$CHILD_ADD_SCRIPT 3
#$CHILD_ADD_SCRIPT 4
#
## Run 4 children, 4 streams
#echo "Running 4 children, 4 streams"
#run_latency_bm 9 --no-delete
#
## Add four more child/streams
#$CHILD_ADD_SCRIPT 5
#$CHILD_ADD_SCRIPT 6
#$CHILD_ADD_SCRIPT 7
#$CHILD_ADD_SCRIPT 8
#
## Run 8 child, 8 streams and delete afterwards
#echo "Running 8 children, 8 streams"
#run_latency_bm 17 --delete
