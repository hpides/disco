import os
import re
import socket
import subprocess
import time

import doctl
import paramiko

KNOWN_HOSTS_FILE = "/tmp/known_hosts"
SSH_PORT = 22

ROOT_TAG = "root"
CHILD_TAG = "child"
STREAM_TAG = "stream"

CHILD_PORT = 4060
ROOT_CONTROL_PORT = 4055
ROOT_WINDOW_PORT = 4056

UTF8 = "utf-8"

STREAM_LOG_PREFIX = "stream"
GENERIC_ERROR_MSG = "Exception"
NODE_REGISTRATION_FAIL = "Could not register at child node"
QUEUE_SIZE_RE = re.compile(r"Current queue size: (\d+)")

THIS_FILE_DIR = os.path.dirname(os.path.realpath(__file__))
SCRIPTS_PATH = os.path.join(THIS_FILE_DIR, "..", "..", "..", "scripts")
READY_CHECK_SCRIPT = os.path.join(SCRIPTS_PATH, "ready-check.sh")
ADD_HOSTS_SCRIPT = os.path.join(SCRIPTS_PATH, "add-hosts.sh")


def ssh_command(ip, command, timeout=None, verbose=False):
    ssh = None
    try:
        ssh, stdout, stderr = _ssh_command(ip, command, timeout=timeout)
        # Wait for command to finish
        output = str(stdout.read(), UTF8)
        status = stdout.channel.recv_exit_status()
        if verbose:
            print(f"Channel return code for command {command} is {status}")
        return output
    except paramiko.SSHException as e:
        print(f"SSHException {e}")
        raise
    except socket.timeout:
        print("SSH Pipe timed out...")
    finally:
        if ssh is not None:
            ssh.close()


def _ssh_command(ip, command, timeout):
    ssh = paramiko.SSHClient()
    ssh.load_host_keys(KNOWN_HOSTS_FILE)
    ssh.connect(ip, username="root")
    _, stdout, stderr = ssh.exec_command(command, timeout=timeout)
    return ssh, stdout, stderr


def get_ip_of_droplet(droplet):
    networks = droplet['networks']
    if 'v4' not in networks:
        return None
    return networks['v4'][0]['ip_address']


def get_droplets(tag_name=None):
    all_droplets = doctl.compute.droplet.list()

    if tag_name is None:
        return all_droplets

    return [droplet for droplet in all_droplets if tag_name in droplet['tags']]


def get_ips(tag_name=None):
    all_droplets = get_droplets(tag_name)
    ips = []
    for droplet in all_droplets:
        ip = get_ip_of_droplet(droplet)
        if ip is not None:
            ips.append(ip)
    return ips


def wait_for_ips(num_nodes, parent_tag=None):
    ready_parents = get_ips(parent_tag)
    num_ready_parents = len(ready_parents)

    while num_ready_parents < num_nodes:
        difference = num_nodes - num_ready_parents
        parent_tag_str = "" if parent_tag is None else f"'{parent_tag}'"
        print(f"\rWaiting for {difference} more {parent_tag_str} node(s) to get an IP...", end="")
        time.sleep(3)
        ready_parents = get_ips(parent_tag)
        num_ready_parents = len(ready_parents)
    print()


def add_hosts(num_nodes=0):
    print(f"Adding IPs to {KNOWN_HOSTS_FILE}")
    print("This may take a while if the nodes were just created.")

    # Clear known_hosts file
    open(KNOWN_HOSTS_FILE, "w").close()

    if num_nodes > 0:
        wait_for_ips(num_nodes)

    all_ips = get_ips()
    for ip in all_ips:
        print(f"Adding {ip}")

        connected = False
        while not connected:
            try:
                transport = paramiko.Transport(ip, SSH_PORT)
                transport.connect()
            except paramiko.SSHException:
                time.sleep(5)
                continue

            connected = True
            key = transport.get_remote_server_key()
            transport.close()

            host_file = paramiko.HostKeys(filename=KNOWN_HOSTS_FILE)
            host_file.add(ip, key.get_name(), key)
            host_file.save(filename=KNOWN_HOSTS_FILE)


def ready_check(num_nodes=0):
    print("Waiting for node setup to complete...")

    if num_nodes > 0:
        wait_for_ips(num_nodes)

    all_ips = get_ips()
    ready_ips = []

    ready_command = "ls"

    while len(ready_ips) < num_nodes:
        unready_ips = set(all_ips) - set(ready_ips)
        print(f"\rWaiting for {len(unready_ips)} more node(s) to become ready...", end="")

        for ip in unready_ips:
            ready_output = ssh_command(ip, ready_command)
            if "run.sh" in ready_output:
                ready_ips.append(ip)

        if len(ready_ips) < num_nodes:
            time.sleep(5)
    print()


def check_complete(timeout, ips):
    print(f"Running on nodes for a maximum of {timeout} seconds...")
    sleep_duration = 10
    max_num_sleeps = timeout // sleep_duration

    complete_command = "kill -0 $(cat /tmp/RUN_PID) 2>&1"
    num_nodes = len(ips)
    ip_set = set(ips)

    num_sleeps = 0
    complete_ips = []
    while len(complete_ips) < num_nodes:
        incomplete_ips = ip_set - set(complete_ips)
        print(f"\rWaiting for {len(incomplete_ips)} more node(s) to complete...", end="")

        for ip in incomplete_ips:
            complete_output = ssh_command(ip, complete_command)
            if "No such process" in complete_output:
                complete_ips.append(ip)

        if len(complete_ips) < num_nodes:
            if num_sleeps == max_num_sleeps:
                difference = num_nodes - len(complete_ips)
                print()
                print(f"{difference} application(s) still running after timeout. They will be terminated.")
                print("This most likely indicates an error or a missing stream/child end message.")
                return incomplete_ips

            time.sleep(sleep_duration)
            num_sleeps += 1

    print()
    print("All applications terminated before the timeout.")
    return []


def wait_for_setup(num_nodes):
    print("Waiting for node setup to complete...")
    print("Adding IPs to known_hosts...")
    add_hosts_command = (ADD_HOSTS_SCRIPT, f"{num_nodes}")
    subprocess.run(add_hosts_command, check=True, capture_output=True)

    print("Checking setup status...")
    ready_check_command = (READY_CHECK_SCRIPT, f"{num_nodes}")
    subprocess.run(ready_check_command, check=True)


def logs_are_unsustainable(log_directory):
    num_streams = 0
    num_stream_with_error = 0
    num_streams_at_min_queue = 0

    for log_file in os.listdir(log_directory):
        log_file_path = os.path.join(log_directory, log_file)
        with open(log_file_path) as f:
            log_contents = f.read()

            if log_file.startswith("main"):
                continue

            if not log_file.startswith(STREAM_LOG_PREFIX):
                # Not a stream file, error here is bad
                if GENERIC_ERROR_MSG in log_contents:
                    print(f" '--> Error in file {log_file_path}. Retry.")
                    return None
                continue

            num_streams += 1
            if GENERIC_ERROR_MSG in log_contents:
                # Found an error while generating
                if NODE_REGISTRATION_FAIL in log_contents:
                    raise RuntimeError(NODE_REGISTRATION_FAIL)
                num_stream_with_error += 1
            else:
                queue_size_matches = QUEUE_SIZE_RE.findall(log_contents)
                if not queue_size_matches:
                    raise RuntimeError("Bad log file. No queue sizes.")
                queue_size = int(queue_size_matches[-1][0])
                if queue_size < 10_000:
                    num_streams_at_min_queue += 1

    # print(f"Scanned {num_streams} streams. {num_stream_with_error} with an error, "
    #       f"{num_streams_at_min_queue} at min queue.")
    if num_stream_with_error == num_streams or num_stream_with_error > 2:
        # Multiple streams are bad
        return True

    if 0 < num_stream_with_error <= num_streams_at_min_queue:
        # Rerun as result is not conclusive
        return None

    # Run was sustainable
    return False
