#!/usr/bin/env bash

RUN_DURATION=120
WINDOW_STRING="TUMBLING,1000"
AGG_STRING="MAX"

echo "BENCHMARK WITH: $RUN_DURATION seconds, window: $WINDOW_STRING, agg: $AGG_STRING"

BM_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BASE_DIR="$( cd "$BM_DIR/../.." && pwd )"

RUN_SCRIPT="$BM_DIR/latency.py"
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

# ===================================
# SINGLE-CHILD RUNS
# ===================================

## Run 1 child, 1 stream
echo "Running 1 child, 1 stream"
run_latency_bm 3 105000

# Run 1 child, 2 streams
echo "Running 1 child, 2 streams"
run_latency_bm 4 664000

# Run 1 child, 4 streams
echo "Running 1 child, 4 streams"
run_latency_bm 6 343000

# Run 1 child, 8 streams and delete afterwards
echo "Running 1 child, 8 streams"
run_latency_bm 10 164000


# ===================================
# MULTI-CHILD RUNS
# ===================================

# Run 2 children, 4 streams
echo "Running 2 children, 4 streams"
run_latency_bm 7 1085000

# Run 4 children, 8 streams
echo "Running 4 children, 8 streams"
run_latency_bm 13 695000


