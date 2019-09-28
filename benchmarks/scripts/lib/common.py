import os
import re
import socket
import time

import paramiko

SSH_PORT = 22

SSH_BASE_DIR = "/home/hadoop/benson"

CHILD_PORT = 4060
ROOT_CONTROL_PORT = 4055
ROOT_WINDOW_PORT = 4056

UTF8 = "utf-8"

STREAM_LOG_PREFIX = "stream"
GENERIC_ERROR_MSG = "Exception"
NODE_REGISTRATION_FAIL = "Could not register at child node"
QUEUE_SIZE_RE = re.compile(r"Current queue size: (\d+)")


def ssh_command(host, command, timeout=None, verbose=False):
    ssh = None
    try:
        ssh, stdout, stderr = _ssh_command(host, command, timeout=timeout)
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


def _ssh_command(host, command, timeout):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username="hadoop")
    _, stdout, stderr = ssh.exec_command(command, timeout=timeout)
    return ssh, stdout, stderr


def check_complete(timeout, hosts):
    print(f"Running on nodes for a maximum of {timeout} seconds...")
    sleep_duration = 10
    max_num_sleeps = timeout // sleep_duration

    complete_command = "kill -0 $(cat /tmp/RUN_PID) 2>&1"
    num_nodes = len(hosts)
    host_set = set(hosts)

    num_sleeps = 0
    complete_hosts = []
    while len(complete_hosts) < num_nodes:
        incomplete_hosts = host_set - set(complete_hosts)
        print(f"\rWaiting for {len(incomplete_hosts)} more node(s) to complete...", end="")

        for host in incomplete_hosts:
            complete_output = ssh_command(host, complete_command)
            if "No such process" in complete_output:
                complete_hosts.append(host)

        if len(complete_hosts) < num_nodes:
            if num_sleeps == max_num_sleeps:
                difference = num_nodes - len(complete_hosts)
                print()
                print(f"{difference} application(s) still running after timeout. They will be terminated.")
                print("This most likely indicates an error or a missing stream/child end message.")
                return incomplete_hosts

            time.sleep(sleep_duration)
            num_sleeps += 1

    print()
    print("All applications terminated before the timeout.")
    return []


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
            if GENERIC_ERROR_MSG in log_contents or "unsustainable" in log_contents:
                # Found an error while generating
                if NODE_REGISTRATION_FAIL in log_contents:
                    raise RuntimeError(NODE_REGISTRATION_FAIL)
                num_stream_with_error += 1
            else:
                queue_size_matches = QUEUE_SIZE_RE.findall(log_contents)
                if not queue_size_matches:
                    raise RuntimeError("Bad log file. No queue sizes.")
                queue_size = int(queue_size_matches[-1][0])
                if queue_size < 25_000:
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
