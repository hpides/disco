import os
import sys
import time
from datetime import datetime
from multiprocessing.connection import Connection
from threading import Thread
from typing import List

from lib.common import ssh_command, check_complete, SSH_BASE_DIR, \
    ROOT_CONTROL_PORT, ROOT_WINDOW_PORT, CHILD_PORT, assert_valid_node_config, create_log_dir

FILE_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.abspath(os.path.join(FILE_DIR, "..", "..", "..", ".."))

ROOT_HOST = "cloud-13"

# Bad hosts: 12, 14, 16, 23, 30
ALL_HOSTS = [
    'cloud-15', 'cloud-17', 'cloud-18', 'cloud-19', 'cloud-20',
    'cloud-21', 'cloud-22', 'cloud-24', 'cloud-25', 'cloud-26',
    'cloud-27', 'cloud-28', 'cloud-29', 'cloud-31', 'cloud-32',
    'cloud-33', 'cloud-34', 'cloud-35', 'cloud-36', 'cloud-37',
    'cloud-12',  # TODO remove again
    'cloud-14',  # TODO remove again
    'cloud-16',  # TODO remove again
]


def get_hosts_for_run(node_config: List[int]) -> List[List[str]]:
    assert sum(node_config) <= len(ALL_HOSTS), \
        f"Cannot use more than {len(ALL_HOSTS)} nodes. Got {sum(node_config)}."
    node_idx = 0
    hosts = []
    for num_level_nodes in node_config:
        hosts.append(ALL_HOSTS[node_idx:node_idx + num_level_nodes])
        node_idx += num_level_nodes
    return hosts


def get_log_dir(num_nodes, num_events, duration):
    now = datetime.now()
    run_date_string = now.strftime("%Y-%m-%d-%H%M")
    return f"{BASE_DIR}/benchmark-runs/{run_date_string}_{num_nodes}-" \
           f"nodes_{num_events}-events_{duration}-seconds"


def complete_check_fn(host):
    complete_command = "ps aux | grep disco.jar"
    complete_output = ssh_command(host, complete_command)
    return not ("com.github.lawben" in complete_output)


def upload_benchmark_params(host, *args):
    string_args = [str(arg) for arg in args]
    args_str = " ".join(string_args)
    print(f"BENCHMARK_ARGS={args_str}")
    ssh_command(host, f'echo "export BENCHMARK_ARGS=\\\"{args_str}\\\"" >> {SSH_BASE_DIR}/benchmark_env')


def upload_root_params(num_children, windows, agg_fns, is_single_node=False):
    runner_class = "DistributedRootMain" if not is_single_node else "SingleNodeRootMain"
    node_args = (runner_class, ROOT_CONTROL_PORT, ROOT_WINDOW_PORT, "/tmp/disco-results")
    upload_benchmark_params(ROOT_HOST, *node_args, num_children,
                            windows, agg_fns)


def upload_intermediate_params(host, node_id, num_children, parent_host, is_single_node=False):
    runner_class = "DistributedMergeNodeMain" if not is_single_node else "EventForwarderMain"
    node_args = (runner_class, parent_host, ROOT_CONTROL_PORT, ROOT_WINDOW_PORT,
                 ROOT_CONTROL_PORT, ROOT_WINDOW_PORT, num_children, node_id)
    upload_benchmark_params(host, *node_args)


def upload_child_params(child_host, parent_host, child_id, num_streams, is_single_node=False):
    runner_class = "DistributedChildMain" if not is_single_node else "SingleNodeChildMain"
    node_args = (runner_class, parent_host, ROOT_CONTROL_PORT,
                 ROOT_WINDOW_PORT, CHILD_PORT, child_id, num_streams)
    upload_benchmark_params(child_host, *node_args)

# TODO
# def upload_stream_params(stream_host: str, stream_id: int, parent_host: str, num_events: int, duration: int,
#                          agg_fn: str, num_keys: int, is_fixed_events: bool = False):
#     runner_class = "SustainableThroughputRunner" if not is_fixed_events else "InputStreamMain"
#     parent_addr = f"{parent_host}:{CHILD_PORT}"
#     node_args = (runner_class, stream_id, parent_addr, num_events, duration, agg_fn, f"stream{num_keys}")
#     upload_benchmark_params(stream_host, *node_args)


def upload_stream_params(stream_host: str, stream_id: int, parent_host: str, num_events: int, duration: int,
                         agg_fn: str, is_fixed_events: bool = False):
    runner_class = "SustainableThroughputRunner" if not is_fixed_events else "InputStreamMain"
    parent_addr = f"{parent_host}:{CHILD_PORT}"
    node_args = (runner_class, stream_id, parent_addr, num_events, duration, agg_fn, f"session")
    upload_benchmark_params(stream_host, *node_args)


def run_host(host, name, log_dir, timeout=None):
    ssh_command(host, f"{SSH_BASE_DIR}/run.sh &> {SSH_BASE_DIR}/logs",
                timeout=timeout, verbose=True)

    out_file_path = os.path.join(log_dir, f"{name}.log")
    util_file_path = os.path.join(log_dir, f"util_{name}.log")
    output = ssh_command(host, f"cat {SSH_BASE_DIR}/logs")
    with open(out_file_path, "w") as out_file:
        print(output)
        out_file.write(output)

    util_output = ssh_command(host, f"cat {SSH_BASE_DIR}/cpu-util.log")
    with open(util_file_path, "w") as out_file:
        print(util_output)
        out_file.write(util_output)


def run_single_level(num_children: int, num_streams: int, num_events: int, duration: int,
                     windows: str, agg_functions: str, process_log_dir_pipe: Connection = None):
    node_config = [num_children, num_streams]
    return run(node_config, num_events, duration, windows, agg_functions, process_log_dir_pipe=process_log_dir_pipe)


def run(node_config: List[int], num_events: int, duration: int,
        windows: str, agg_functions: str, is_single_node: bool = False,
        is_fixed_events: bool = False, process_log_dir_pipe: Connection = None):
    assert_valid_node_config(node_config)

    num_nodes = sum(node_config) + 1
    log_dir = get_log_dir(num_nodes, num_events, duration)
    log_dir = create_log_dir(log_dir)

    if process_log_dir_pipe is not None:
        sys.stdout = open(os.path.join(log_dir, "main.log"), "w")

    print(f"Writing logs to {log_dir}\n")

    all_hosts = get_hosts_for_run(node_config)
    all_hosts.insert(0, [ROOT_HOST])
    flat_hosts = [host for level in all_hosts for host in level]
    print(f"All hosts ({len(flat_hosts)}): {' '.join(flat_hosts)}")

    print("Uploading benchmark arguments on all nodes...")
    # Upload for root node
    named_hosts = [(ROOT_HOST, "root")]
    upload_root_params(len(all_hosts[1]), windows, agg_functions, is_single_node)

    # Upload for intermediate nodes
    for level_id, level_hosts in enumerate(all_hosts[1:-2], 1):
        parent_nodes = all_hosts[level_id - 1]
        num_parents = len(parent_nodes)
        num_children = len(all_hosts[level_id + 1])
        assert num_children % len(level_hosts) == 0
        num_children_per_node = num_children // len(level_hosts)
        for node_id, level_host in enumerate(level_hosts):
            parent_host = parent_nodes[node_id % num_parents]
            node_level_id = (10 * level_id) + node_id
            upload_intermediate_params(level_host, node_level_id, num_children_per_node, parent_host, is_single_node)
            named_hosts.append((level_host, f"merger-{node_level_id}"))

    parent_nodes = all_hosts[-3]
    child_hosts = all_hosts[-2]
    stream_hosts = all_hosts[-1]
    num_parents = len(parent_nodes)
    num_children = len(child_hosts)
    num_streams = len(stream_hosts)

    # Upload for child nodes
    num_streams_per_child = num_streams // num_children
    for child_id, child_host in enumerate(child_hosts):
        parent_host = parent_nodes[child_id % num_parents]
        upload_child_params(child_host, parent_host, child_id, num_streams_per_child, is_single_node)
        named_hosts.append((child_host, f"child-{child_id}"))

    # Upload for stream nodes
    for stream_id, stream_host in enumerate(stream_hosts):
        parent_host = child_hosts[stream_id % num_children]
        upload_stream_params(stream_host, stream_id, parent_host, num_events, duration,
                             agg_functions, is_fixed_events)
        named_hosts.append((stream_host, f"stream-{stream_id}"))

    print("Starting `run.sh` on all nodes.\n")
    max_run_duration = duration + 30
    threads = []

    for host, name in named_hosts:
        thread = Thread(target=run_host,
                        args=(host, name, log_dir, max_run_duration),
                        name=f"thread-{name}")
        thread.start()
        threads.append(thread)

    # Give nodes time to start running
    time.sleep(5)

    check_complete(max_run_duration, flat_hosts, complete_check_fn)
    kill_command = "pkill -9 -f /home/hadoop/benson/openjdk12/bin/java"
    print("Ending script by killing all PIDs...")
    for host in flat_hosts:
        ssh_command(host, kill_command)

    for thread in threads:
        thread.join()

    print("Joined all run threads.")

    if process_log_dir_pipe is not None:
        process_log_dir_pipe.send(log_dir)

    return log_dir

# TODO: change if needed again for new node_config
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--num-children", type=int, required=True, dest="num_children")
#     parser.add_argument("--num-streams", type=int, required=True, dest="num_streams")
#     parser.add_argument("--num-events", type=int, required=True, dest="num_events")
#     parser.add_argument("--duration", type=int, required=True, dest="duration")
#     parser.add_argument("--windows", type=str, required=True, dest="windows")
#     parser.add_argument("--agg-functions", type=str, required=True, dest="agg_functions")
#
#     args = parser.parse_args()
#     run(args.num_children, args.num_streams, args.num_events,
#         args.duration, args.windows, args.agg_functions)
