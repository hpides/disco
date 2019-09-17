#!/usr/bin/env bash

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

PYTON_SCRIPT="$FILE_DIR/find-sustainable-throughput.py"
VENV_DIR="$FILE_DIR/../../venv"
source "$VENV_DIR/bin/activate"

WINDOWS="TUMBLING,1000"
AGG_FNS="MAX"

function single_run() {
    local NUM_CHILDREN=${1}
    local NUM_STREAMS=${2}

    python3 -u "$PYTON_SCRIPT" --num-children "$NUM_CHILDREN" --num-streams "$NUM_STREAMS" \
                               --windows "$WINDOWS" --agg-functions "$AGG_FNS"
}

# ===================================
# SINGLE-CHILD RUNS
# ===================================

echo "Running 1 child, 1 stream"
single_run 1 1

echo "Running 1 child, 2 streams"
single_run 1 2

echo "Running 1 child, 4 streams"
single_run 1 4

echo "Running 1 child, 8 streams"
single_run 1 8

# ===================================
# MULTI-CHILD RUNS
# ===================================

echo "Running 2 children, 2 streams"
single_run 2 2

echo "Running 2 children, 4 streams"
single_run 2 4

echo "Running 4 children, 4 streams"
single_run 4 4

echo "Running 4 children, 8 streams"
single_run 4 8

echo "Running 8 children, 8 streams"
single_run 8 8

echo "Running 8 children, 16 streams"
single_run 8 16
