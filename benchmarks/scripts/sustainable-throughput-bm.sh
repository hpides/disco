#!/usr/bin/env bash
set -e

# Usage: ./sustainable-throughput-bm.sh WINDOWS AGGREGATE_FNS

WINDOWS=${1:-"TUMBLING,1000"}
AGG_FNS=${2:-"MAX"}

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

THROUGHPUT_SCRIPT="$FILE_DIR/find-sustainable-throughput.py"
LATENCY_SCRIPT="$FILE_DIR/latency.py"
VENV_DIR="$FILE_DIR/../../venv"
source "$VENV_DIR/bin/activate"


function run_throughput_bm() {
    local NUM_CHILDREN=${1}
    local NUM_STREAMS=${2}

    python3 -u "$THROUGHPUT_SCRIPT" --num-children "$NUM_CHILDREN" --num-streams "$NUM_STREAMS" \
                               --windows "$WINDOWS" --agg-functions "$AGG_FNS"
}

function run_latency_bm() {
    local NUM_CHILDREN=${1}
    local NUM_STREAMS=${2}
    local NUM_EVENTS=${3}

    python3 -u "$LATENCY_SCRIPT" --num-children "$NUM_CHILDREN" --num-streams "$NUM_STREAMS" \
                                 --num-events "$NUM_EVENTS" --windows "$WINDOWS" --agg-functions "$AGG_FNS"
}

function get_throughput() {
    cat "/tmp/last_sustainable_run" | head -n 1
}

function run_bm() {
    local NUM_CHILDREN=${1}
    local NUM_STREAMS=${2}

    run_throughput_bm "$NUM_CHILDREN" "$NUM_STREAMS"
    local throughput=$(get_throughput)
    run_latency_bm "$NUM_CHILDREN" "$NUM_STREAMS" "$throughput"
}

# ===================================
# SINGLE-CHILD RUNS
# ===================================

echo "Running 1 child, 1 stream"
run_bm 1 1

echo "Running 1 child, 2 streams"
run_bm 1 2

echo "Running 1 child, 4 streams"
run_bm 1 4

echo "Running 1 child, 8 streams"
run_bm 1 8

# ===================================
# MULTI-CHILD RUNS
# ===================================

echo "Running 2 children, 2 streams"
run_bm 2 2

echo "Running 2 children, 4 streams"
run_bm 2 4

echo "Running 2 children, 8 streams"
run_bm 2 8

echo "Running 4 children, 4 streams"
run_bm 4 4

echo "Running 4 children, 8 streams"
run_bm 4 8

echo "Running 4 children, 16 streams"
run_bm 4 16

