import argparse
import random
import string
import sys
from datetime import datetime
from threading import Thread

from common import *

FILE_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.abspath(os.path.join(FILE_DIR, "..", "..", "..", ".."))

ROOT_HOST = "cloud-12"
# cloud-23 is dead, cloud-30 is partially broken
ALL_HOSTS = [
    'cloud-13', 'cloud-14', 'cloud-15', 'cloud-16', 'cloud-17',
    'cloud-18', 'cloud-19', 'cloud-20', 'cloud-21', 'cloud-22',
                'cloud-24', 'cloud-25', 'cloud-26', 'cloud-27',
    'cloud-28', 'cloud-29',             'cloud-31', 'cloud-32',
    'cloud-33', 'cloud-34', 'cloud-35', 'cloud-36', 'cloud-37'
]


def get_hosts_for_run(num_nodes):
    return ALL_HOSTS[:num_nodes]


def get_log_dir(num_nodes, num_events, duration):
    now = datetime.now()
    run_date_string = now.strftime("%Y-%m-%d-%H%M")
    return f"{BASE_DIR}/benchmark-runs/{run_date_string}_{num_nodes}-" \
           f"nodes_{num_events}-events_{duration}-seconds"


def upload_benchmark_params(host, *args):
    string_args = [str(arg) for arg in args]
    args_str = " ".join(string_args)
    ssh_command(host, f'echo "export BENCHMARK_ARGS=\\\"{args_str}\\\"" >> ~/benchmark_env')


def upload_root_params(num_children, windows, agg_fns):
    fixed_args = ["DistributedRootMain", ROOT_CONTROL_PORT,
                  ROOT_WINDOW_PORT, "/tmp/disco-results"]
    all_args = fixed_args + [num_children, windows, agg_fns]
    upload_benchmark_params(ROOT_HOST, all_args)


def upload_child_params(child_host, child_id, num_streams):
    fixed_args = ["DistributedChildMain", ROOT_HOST, ROOT_CONTROL_PORT,
                  ROOT_WINDOW_PORT, CHILD_PORT]
    all_args = fixed_args + [child_id, num_streams]
    upload_benchmark_params(child_host, all_args)


def upload_stream_params(stream_host, stream_id, parent_host, num_events, duration):
    parent_addr = f"{parent_host}:{CHILD_PORT}"
    fixed_args = ["SustainableThroughputRunner"]
    all_args = fixed_args + [stream_id, parent_addr, num_events, duration]
    upload_benchmark_params(stream_host, all_args)


def run_host(host, name, log_dir, timeout=None):
    ssh_command(host, f"{SSH_BASE_DIR}/run.sh &> {SSH_BASE_DIR}/logs",
                timeout=timeout, verbose=True)

    out_file_path = os.path.join(log_dir, f"{name}.log")
    util_file_path = os.path.join(log_dir, f"util_{name}.log")
    output = ssh_command(host, f"cat {SSH_BASE_DIR}/logs >> {SSH_BASE_DIR}/all_logs"
                               f" && cat {SSH_BASE_DIR}/logs")
    with open(out_file_path, "w") as out_file:
        print(output)
        out_file.write(output)

    util_output = ssh_command(host, "cat util.log")
    with open(util_file_path, "w") as out_file:
        print(util_output)
        out_file.write(util_output)


def run(num_children, num_streams, num_events, duration, windows,
        agg_functions, process_log_dir_pipe=None):
    num_nodes = num_children + num_streams + 1
    log_dir = get_log_dir(num_nodes, num_events, duration)

    try:
        os.makedirs(log_dir)
    except OSError:
        log_dir += f"_{''.join(random.choices(string.ascii_lowercase, k=3))}"
        os.makedirs(log_dir)

    if process_log_dir_pipe is not None:
        sys.stdout = open(os.path.join(log_dir, "main.log"), "w")

    print(f"Writing logs to {log_dir}\n")

    all_hosts = [ROOT_HOST] + get_hosts_for_run(num_children + num_streams)
    print(f"All hosts ({len(all_hosts)}): {' '.join(all_hosts)}")

    print("Uploading benchmark arguments on all nodes...")
    child_hosts = all_hosts[1:num_children + 1]
    assert len(child_hosts) == num_children
    stream_hosts = all_hosts[num_children + 1:]
    assert len(stream_hosts) == num_streams

    named_hosts = []
    upload_root_params(num_children, windows, agg_functions)
    named_hosts.append((ROOT_HOST, "root"))

    num_streams_per_child = num_streams // num_children
    for child_id, child_host in enumerate(child_hosts):
        upload_child_params(child_host, child_id, num_streams_per_child)
        named_hosts.append((child_host, f"child-{child_id}"))

    for stream_id, stream_host in enumerate(stream_hosts):
        parent_host = child_hosts[stream_id % num_children]
        upload_stream_params(stream_host, stream_id, parent_host, num_events, duration)
        named_hosts.append((stream_host, f"streams-{stream_id}"))

    print("Starting `run.sh` on all nodes.\n")
    max_run_duration = duration + 30
    threads = []

    for host, name in named_hosts:
        name = "root"
        thread = Thread(target=run_host,
                        args=(host, name, log_dir, max_run_duration),
                        name=f"thread-{name}")
        thread.start()
        threads.append(thread)

    print("To view root logs:")
    print(f"tail -F {os.path.join(log_dir, 'root.log')}\n")

    uncompleted_ips = check_complete(max_run_duration, all_hosts)
    if uncompleted_ips:
        kill_command = "kill -9 $(cat /tmp/RUN_PID 2> /dev/null) &> dev/null"
        print("Ending script by killing all PIDs...")
        for ip in uncompleted_ips:
            ssh_command(ip, kill_command)

        print(f"Killed remaining {len(uncompleted_ips)} jobs.")
        uncompleted_ips = check_complete(30, uncompleted_ips)
        if uncompleted_ips:
            raise RuntimeError(f"Could not terminate IPs: {uncompleted_ips}")

    for thread in threads:
        thread.join()

    print("Joined all run threads.")

    if process_log_dir_pipe is not None:
        process_log_dir_pipe.send(log_dir)

    return log_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-children", type=int, required=True, dest="num_children")
    parser.add_argument("--num-streams", type=int, required=True, dest="num_streams")
    parser.add_argument("--num-events", type=int, required=True, dest="num_events")
    parser.add_argument("--duration", type=int, required=True, dest="duration")
    parser.add_argument("--windows", type=str, required=True, dest="windows")
    parser.add_argument("--agg-functions", type=str, required=True, dest="agg_functions")

    args = parser.parse_args()
    run(args.num_children, args.num_streams, args.num_events,
        args.duration, args.windows, args.agg_functions)
