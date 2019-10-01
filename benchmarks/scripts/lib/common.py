import os
import re
import socket
import time
import random
import string

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


def ssh_command(host, command, timeout=None, verbose=False, user="hadoop"):
    ssh = None
    try:
        ssh, stdout, stderr = _ssh_command(host, command, timeout=timeout, user=user)
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


def _ssh_command(host, command, timeout, user):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    retries = 0
    max_num_retries = 3
    while retries < max_num_retries:
        try:
            ssh.connect(host, username=user)
            break
        except (paramiko.SSHException, OSError) as e:
            retries += 1
            if retries == max_num_retries:
                raise e

    _, stdout, stderr = ssh.exec_command(command, timeout=timeout)
    return ssh, stdout, stderr


def check_complete(timeout, running_thing, complete_fn):
    print(f"Running on nodes for a maximum of {timeout} seconds...")
    sleep_duration = 10
    max_num_sleeps = timeout // sleep_duration

    num_running_things = len(running_thing)
    running_thing_set = set(running_thing)

    num_sleeps = 0
    complete_running_things = []
    while len(complete_running_things) < num_running_things:
        incomplete_running_thing = running_thing_set - set(complete_running_things)
        print(f"\rWaiting for {len(incomplete_running_thing)} more running thing(s) to complete...", end="")

        for running_thing in incomplete_running_thing:
            if complete_fn(running_thing):
                complete_running_things.append(running_thing)

        if len(complete_running_things) < num_running_things:
            if num_sleeps == max_num_sleeps:
                difference = num_running_things - len(complete_running_things)
                print()
                print(f"{difference} application(s) still running after timeout. They will be terminated.")
                print("This most likely indicates an error or a missing stream/child end message.")
                return incomplete_running_thing

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


def is_port_in_use(port):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def assert_valid_node_config(node_config):
    assert len(node_config) >= 2, f"Need at least #children and #streams! Got {node_config}"
    for i in range(len(node_config) - 1):
        if node_config[i] > node_config[i + 1]:
            raise RuntimeError(f"Bad runtime config. Need increasing number per level. {node_config}")
        if node_config[i + 1] % node_config[i] != 0:
            raise RuntimeError(f"Need exact multiple increase per level. {node_config}")


def create_log_dir(log_dir):
    try:
        os.makedirs(log_dir)
        return log_dir
    except OSError:
        new_log_dir = f"{log_dir}_{''.join(random.choices(string.ascii_lowercase, k=3))}"
        os.makedirs(new_log_dir)
        return new_log_dir


def print_run_string(node_config):
    intermediates = "-".join([str(x) for x in node_config[:-2]])
    intermediates_str = (f"{intermediates}" if intermediates else "0") + " intermediates"
    children_str = f"{node_config[-2]} children"
    streams_tr = f"{node_config[-1]} streams"
    print(f"Running {intermediates_str}, {children_str}, {streams_tr}")
