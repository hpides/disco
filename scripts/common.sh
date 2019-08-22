KNOWN_HOSTS_FILE="/tmp/known_hosts"

function get_ips {
    local TAG_NAME="$1"
    doctl compute droplet list --format="PublicIPv4" --no-header --tag-name="$TAG_NAME"
}

function get_droplet_list {
    local FORMAT=${1}
    local TAG_NAME=${2}
    doctl compute droplet list --format="$FORMAT" --tag-name="$TAG_NAME" --no-header
}

function get_all_ips {
    get_droplet_list "PublicIPv4"
}

function get_all_names {
    get_droplet_list "Name"
}

function ssh_cmd {
    local ip=${1}
    local cmd=${2}
    ssh -o UserKnownHostsFile=${KNOWN_HOSTS_FILE} "root@$ip" "$cmd 2>&1"
}

function wait_for_ips() {
    NUM_PARENTS=${1}
    PARENT_TAG=${2}

    NUM_READY_PARENTS=$(get_ips "$PARENT_TAG" | grep -E "\d+\.\d+\.\d+.\d+" | wc -l)
    while [[ ${NUM_READY_PARENTS} -lt ${NUM_PARENTS} ]]; do
        let "difference = ${NUM_PARENTS} - ${NUM_READY_PARENTS}"
        echo -ne "\rWaiting for $difference more '$PARENT_TAG' node(s) to get an IP..."
        sleep 3
        NUM_READY_PARENTS=$(get_ips "$PARENT_TAG" | grep -E "\d+\.\d+\.\d+.\d+" | wc -l)
    done
    echo
}
