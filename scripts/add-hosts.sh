#!/usr/bin/env bash

# Usage: ./add-hosts.sh [numDroplets]

NUM_DROPLETS=${1:-0}

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
COMMON_FILE="$FILE_DIR/common.sh"
source $COMMON_FILE


echo "Adding IPs to $KNOWN_HOSTS_FILE"
echo "This may take a while if the nodes were just created."
> ${KNOWN_HOSTS_FILE}

if [[ $NUM_DROPLETS -gt 0 ]]; then
    wait_for_ips $NUM_DROPLETS ""
fi

ALL_IPS=($(get_all_ips))

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
