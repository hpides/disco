FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

COMMON_FILE="$FILE_DIR/common.sh"
source $COMMON_FILE

INIT_SCRIPT_FILE="$FILE_DIR/init.sh"
SSH_KEY=$(doctl compute ssh-key list --format="ID" --no-header | head -n 1)

ROOT_TAG="root"
CHILD_TAG="child"
STREAM_TAG="stream"

CHILD_PORT=4060
ROOT_CONTROL_PORT=4055
ROOT_WINDOW_PORT=4056

function create_init_script {
    local CLASS_NAME="$1"
    local FILE_NAME=`mktemp`
    local JAVA_ARGS=${@:2}
    cat "$INIT_SCRIPT_FILE" > ${FILE_NAME}
    echo -e "\n" >> ${FILE_NAME}
    echo "echo \"pkill -9 java\" >> ~/run.sh"  >> ${FILE_NAME}
    echo "echo \"sleep 3\" >> ~/run.sh"  >> ${FILE_NAME}
    echo "echo \"echo -e \\\"\nNEW RUN\n=======\\\"\" >> ~/run.sh"  >> ${FILE_NAME}
    echo "echo \"source benchmark_env\" >> ~/run.sh" >> ${FILE_NAME}
    echo "echo \"echo BENCHMARK ARGS: \\\$BENCHMARK_ARGS \" >> ~/run.sh" >> ${FILE_NAME}
    echo "echo \"echo \\\$\\\$ > /tmp/RUN_PID\" >> ~/run.sh" >> ${FILE_NAME}
    echo "echo \"java -cp \\\$CLASSPATH com.github.lawben.disco.executables.$CLASS_NAME ${JAVA_ARGS}" \
                  "\\\$BENCHMARK_ARGS\" >> ~/run.sh" >> ${FILE_NAME}
    echo "chmod +x ~/run.sh" >> ${FILE_NAME}
    echo ${FILE_NAME}
}

function creat_droplet {
    local TAG_NAMES="$1"
    local SCRIPT="$2"
    local DROPLET_NAME="$3"
    local ITERATION=${4:-0}
    local NO_HEADER=false
    if [[ ${ITERATION} -gt 1 ]]; then
        NO_HEADER=true
    fi

    local instance="s-1vcpu-1gb"
    if [[ $DROPLET_NAME == *stream* ]]; then
        instance="s-2vcpu-2gb"
    fi

    doctl compute droplet create ${DROPLET_NAME} --image ubuntu-18-04-x64 \
                                      --size "$instance" \
                                      --region fra1 \
                                      --tag-names "$TAG_NAMES,$DROPLET_NAME" \
                                      --ssh-keys "$SSH_KEY" \
                                      --user-data-file "$SCRIPT" \
                                      --format="Name,VCPUs,Memory,ID" \
                                      --no-header=${NO_HEADER}
}

function create_child() {
    local ROOT_IP=${1}
    local CHILD_ID=${2}
    local ITERATION=${3:-0}

    local CHILD_SETUP_SCRIPT=$(create_init_script DistributedChildMain ${ROOT_IP} ${ROOT_CONTROL_PORT} \
                                ${ROOT_WINDOW_PORT} ${CHILD_PORT} $CHILD_ID)

    creat_droplet "$CHILD_TAG" "$CHILD_SETUP_SCRIPT" "$CHILD_TAG-$CHILD_ID" "$ITERATION"
}

function create_stream() {
    local CHILD_IP=${1}
    local STREAM_ID=${2}
    local ITERATION=${3:-0}

    local STREAM_SETUP_SCRIPT=$(create_init_script SustainableThroughputRunner ${STREAM_ID} \
                                "$CHILD_IP:${CHILD_PORT}")
    creat_droplet "$STREAM_TAG" "$STREAM_SETUP_SCRIPT" "$STREAM_TAG-$STREAM_ID" "$ITERATION"
}
