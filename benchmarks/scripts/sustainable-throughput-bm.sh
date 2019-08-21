#!/usr/bin/env bash

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

PYTON_SCRIPT="$FILE_DIR/find-sustainable-throughput.py"
STREAM_ADD_SCRIPT="$FILE_DIR/../../scripts/add-stream.sh"
CHILD_ADD_SCRIPT="$FILE_DIR/../../scripts/add-child-stream.sh"

VENV_DIR="$FILE_DIR/../../venv"
source "$VENV_DIR/bin/activate"

# ===================================
# SINGLE-CHILD RUNS
# ===================================


# Run 1 child, 1 stream
echo "Running 1 child, 1 stream"
python3 -u "$PYTON_SCRIPT" --num-children 1 --num-streams 1 --no-delete

# Add second stream
echo "Adding second stream"
$STREAM_ADD_SCRIPT "child-1" "2"

# Wait for nodes to set up
sleep 180

# Run 1 child, 2 streams
echo "Running 1 child, 2 streams"
python3 -u "$PYTON_SCRIPT" --num-children 1 --num-streams 2 --no-delete --no-create

# Add third and fourth stream
echo "Adding third and fourth stream"
$STREAM_ADD_SCRIPT "child-1" "3"
$STREAM_ADD_SCRIPT "child-1" "4"

# Wait for nodes to set up
sleep 180

# Run 1 child, 4 streams
echo "Running 1 child, 4 streams"
python3 -u "$PYTON_SCRIPT" --num-children 1 --num-streams 4 --no-create
# TODO: use with delete python3 -u "$PYTON_SCRIPT" --num-children 1 --num-streams 4 --no-delete --no-create

# TODO: Won't work for now because of digital ocean restrictions.
## Add four more streams
#$STREAM_ADD_SCRIPT "child-1" "5"
#$STREAM_ADD_SCRIPT "child-1" "6"
#$STREAM_ADD_SCRIPT "child-1" "7"
#$STREAM_ADD_SCRIPT "child-1" "8"
#
## Run 1 child, 8 streams and delete afterwards
#python3 -u "$PYTON_SCRIPT" --num-children 1 --num-streams 8 --no-create



# ===================================
# MULTI-CHILD RUNS
# ===================================

# Get root IP so children can be addee
ROOT_IP=$(doctl compute droplet list --format="PublicIPv4" --no-header --tag-name="root")

# Run 2 children, 2 streams
echo "Running 2 child, 2 streams"
python3 -u "$PYTON_SCRIPT" --num-children 2 --num-streams 2 --no-delete

# Add third and fourth child/stream
echo "Adding third and fourth child/stream"
$CHILD_ADD_SCRIPT "ROOT_IP" 3
$CHILD_ADD_SCRIPT "ROOT_IP" 4

# Wait for nodes to set up
sleep 180

# Run 4 children, 4 streams
echo "Running 4 child, 4 streams"
python3 -u "$PYTON_SCRIPT" --num-children 4 --num-streams 4  --no-create
# TODO: use with delete python3 -u "$PYTON_SCRIPT" --num-children 4 --num-streams 4 --no-delete --no-create

# TODO: Won't work for now because of digital ocean restrictions.
## Add four more child/streams
#$CHILD_ADD_SCRIPT "ROOT_IP" 5
#$CHILD_ADD_SCRIPT "ROOT_IP" 6
#$CHILD_ADD_SCRIPT "ROOT_IP" 7
#$CHILD_ADD_SCRIPT "ROOT_IP" 8
#
## Run 8 child, 8 streams and delete afterwards
#python3 -u "$PYTON_SCRIPT" --num-children 8 --num-streams 8 --no-create
