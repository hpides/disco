#!/usr/bin/env bash

NUM_EXPECTED_DROPLETS=${1}
DELETE_AFTER=${2:-""}

KNOWN_HOSTS_FILE=/tmp/known_hosts

THIS_FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BASE_DIR=$(cd "$THIS_FILE_DIR/../.." && pwd)
RUN_FILES_DIR="$BASE_DIR/benchmark-runs/$(date +"%Y-%d-%m_%Hh%Mm%Ss")"

function get_droplet_list {
    local FORMAT=${1}
    doctl compute droplet list --format="$FORMAT" --no-header
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
    ssh -o UserKnownHostsFile=${KNOWN_HOSTS_FILE} "root@$ip" "$cmd" 2>/dev/null
}

function run_droplet {
    local IP=${1}
    local NAME=${2}
    ssh_cmd ${IP} "~/run.sh" > "$RUN_FILES_DIR/$NAME.log"
}

function check_ready {
    local result=$(ssh_cmd ${1} "ls")
    if [[ ${result} == *"run.sh"* ]]; then
        echo "ready"
    fi
}

#########################
# ACTUAL CODE THAT IS RUN
#########################

mkdir -p ${RUN_FILES_DIR}
echo "Writing logs to $RUN_FILES_DIR"

echo "Getting IPs..."
ALL_IPS=($(get_all_ips))
while [[ ${#ALL_IPS[@]} -lt ${NUM_EXPECTED_DROPLETS} ]]; do
    let "difference = ${NUM_EXPECTED_DROPLETS} - ${#ALL_IPS[@]}"
    echo -ne "\rWaiting for $difference more node(s) to get an IP..."
    sleep 5
    ALL_IPS=($(get_all_ips))
done
echo
echo "All IPs (${#ALL_IPS[@]}): ${ALL_IPS[@]}"
echo

echo "Adding IPs to $KNOWN_HOSTS_FILE"
> ${KNOWN_HOSTS_FILE}
for ip in ${ALL_IPS[@]}; do
    SCAN_OUTPUT=""
    echo "Adding $ip"
    while [[ ${SCAN_OUTPUT} == "" ]]; do
        SCAN_OUTPUT=$(ssh-keyscan -H -t ecdsa-sha2-nistp256 ${ip} 2>/dev/null)
        if [[ ${SCAN_OUTPUT} == "" ]]; then
            sleep 5
        fi
    done
    echo ${SCAN_OUTPUT} >> ${KNOWN_HOSTS_FILE}
done
echo

echo "Waiting for node setup to complete..."
READY_IPS=()
while [[ ${#READY_IPS[@]} -lt ${NUM_EXPECTED_DROPLETS} ]]; do
    UNREADY_IPS=($(echo ${ALL_IPS[@]} ${READY_IPS[@]} | tr ' ' '\n' | sort | uniq -u | tr '\n' ' '))
    echo -ne "\rWaiting for ${#UNREADY_IPS[@]} more node(s) to become ready..."

    for ip in ${UNREADY_IPS[@]}; do
        READY_STATUS=$(check_ready ${ip})
        if [[ ${READY_STATUS} == "ready" ]]; then
            READY_IPS+=(${ip})
        fi
    done
    if [[ ${#READY_IPS[@]} -lt ${NUM_EXPECTED_DROPLETS} ]]; then
        sleep 10
    fi
done
echo -e "\n"


echo "Setup done. Starting \`run.sh\` on all nodes."

ALL_NAMES=($(get_all_names))
for i in ${!ALL_IPS[@]}; do
    run_droplet ${ALL_IPS[$i]} ${ALL_NAMES[$i]} &
done

echo
echo "To view root logs:"
echo "tail -F $RUN_FILES_DIR/root.log"

echo
read -n 1 -p "Press [ENTER] to end script..." dummy

echo
echo "Ending script by killing all PIDs..."
for ip in ${ALL_IPS[@]}; do
    ssh_cmd ${ip} "kill -9 \$(cat /tmp/RUN_PID)"
done

echo "Killed all PIDs."

if [[ ${DELETE_AFTER} == "" ]]; then
    read -p "Delete all droplets? (y/n) " delete_droplets
    if [[ ${delete_droplets} == "y" ]]; then
        DELETE_AFTER="delete"
    fi
fi

if [[ ${DELETE_AFTER} == "delete" ]]; then
    echo "Deleting all droplets..."
    doctl compute droplet delete --force $(doctl compute droplet list --format="ID" --no-header)
fi

echo "Done."
