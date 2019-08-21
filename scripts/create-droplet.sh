FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
INIT_SCRIPT_FILE="$FILE_DIR/init.sh"
SSH_KEY=$(doctl compute ssh-key list --format="ID" --no-header | head -n 1)

ROOT_TAG="root"
CHILD_TAG="child"
STREAM_TAG="stream"

CHILD_PORT=4060
ROOT_CONTROL_PORT=4055
ROOT_WINDOW_PORT=4056

function get_ips {
    local TAG_NAME="$1"
    doctl compute droplet list --format="PublicIPv4" --no-header --tag-name="$TAG_NAME"
}

function wait_for_ips() {
    NUM_PARENTS=${1}
    PARENT_TAG=${2}

    NUM_READY_CHILDREN=0
    while [[ ${NUM_READY_CHILDREN} -lt ${NUM_PARENTS} ]]; do
        let "difference = ${NUM_PARENTS} - ${NUM_READY_CHILDREN}"
        echo -ne "\rWaiting for $difference more '$PARENT_TAG' node(s) to get an IP..."
        sleep 3
        NUM_READY_CHILDREN=$(get_ips "$PARENT_TAG" | wc -l)
    done
    echo
}

function create_init_script {
    local CLASS_NAME="$1"
    local FILE_NAME=`mktemp`
    local JAVA_ARGS=${@:2}
    cat "$INIT_SCRIPT_FILE" > ${FILE_NAME}
    echo -e "\n" >> ${FILE_NAME}
    echo "echo \"source benchmark_env\" >> ~/run.sh" >> ${FILE_NAME}
    echo "echo \"echo BENCHMARK ARGS: \\\$BENCHMARK_ARGS \" >> ~/run.sh" >> ${FILE_NAME}
    echo "echo \"java -cp \\\$CLASSPATH com.github.lawben.disco.executables.$CLASS_NAME ${JAVA_ARGS}" \
                  "\\\$BENCHMARK_ARGS &\" >> ~/run.sh" >> ${FILE_NAME}
    echo "echo \"echo \\\$! > /tmp/RUN_PID\" >> ~/run.sh" >> ${FILE_NAME}
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

    doctl compute droplet create ${DROPLET_NAME} --image ubuntu-18-04-x64 \
                                      --size s-1vcpu-1gb \
                                      --region fra1 \
                                      --tag-names "$TAG_NAMES,$DROPLET_NAME" \
                                      --ssh-keys "$SSH_KEY" \
                                      --user-data-file "$SCRIPT" \
                                      --format="ID,Name" \
                                      --no-header=${NO_HEADER}
}
