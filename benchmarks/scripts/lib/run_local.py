import os
import subprocess
import sys
import zipfile
from datetime import datetime
from multiprocessing.connection import Connection
from subprocess import Popen
from typing import List, Tuple, Iterable
from zipfile import ZipFile

from lib.common import check_complete, is_port_in_use, assert_valid_node_config, create_log_dir

BASE_DIR = "/Users/law/repos/ma/local_bm_runs"

JAVA_CLASSPATH = os.environ["DISCO_CP"]
JAVA_RUN_PREFIX = "com.github.lawben.disco.executables."
JAVA_CMD = ("java", "-cp", JAVA_CLASSPATH)


ROOT_HOST = "localhost"

ALL_AVAILABLE_PORTS = range(4000, 10000)
ALL_PORTS = iter(ALL_AVAILABLE_PORTS)
ALL_FILES = []

RUN_TIMEOUT = 150


def get_next_port() -> int:
    next_port = next(ALL_PORTS)
    while is_port_in_use(next_port):
        print(f"Bad port {next_port}")
        next_port = next(ALL_PORTS)

    return next_port


def get_ports_for_run(node_config: List[int]) -> List[List[Tuple[int, int]]]:
    ports = []
    # -2 because streams do not need ports and children only have one explicit port
    for num_level_nodes in node_config[:-2]:
        level_ports = []
        for _ in range(num_level_nodes):
            control_port = get_next_port()
            window_port = get_next_port()
            level_ports.append((control_port, window_port))
        ports.append(level_ports)

    child_ports = []
    for _ in range(node_config[-2]):
        child_ports.append([get_next_port(), None])
    ports.append(child_ports)

    return ports


def get_log_dir(num_nodes, num_events, duration):
    now = datetime.now()
    run_date_string = now.strftime("%Y-%m-%d-%H%M")
    return f"{BASE_DIR}/{run_date_string}_{num_nodes}-" \
           f"nodes_{num_events}-events_{duration}-seconds"


def _start_runner(name: str, run_class: str, args: Iterable, log_dir: str) -> Popen:
    out_file_path = os.path.join(log_dir, f"{name}.log")
    out_file = open(out_file_path, "w")
    ALL_FILES.append(out_file)
    all_args = (*JAVA_CMD, f"{JAVA_RUN_PREFIX}{run_class}", *args)
    str_args = [str(arg) for arg in all_args]
    return subprocess.Popen(str_args, stdout=out_file, stderr=out_file)


def run_root(port: Tuple[int, int], num_children: int, windows: str, agg_fns: str, log_dir: str, is_single_node=False):
    runner_class = "DistributedRootMain" if not is_single_node else "SingleNodeRootMain"
    control_port, window_port = port
    runner_args = (control_port, window_port, "/tmp/disco-results", num_children, windows, agg_fns)
    return _start_runner("root", runner_class, runner_args, log_dir)


def run_intermediate(node_id: int, num_children: int, parent_port: Tuple[int, int],
                     own_port: Tuple[int, int], log_dir: str, is_single_node=False):
    runner_class = "DistributedMergeNodeMain" if not is_single_node else "EventForwarderMain"
    control_port, window_port = own_port
    parent_control_port, parent_window_port = parent_port
    runner_args = ("localhost", parent_control_port, parent_window_port,
                   control_port, window_port, num_children, node_id)
    return _start_runner(f"intermediate-{node_id}", runner_class, runner_args, log_dir)


def run_child(child_id: int, num_streams: int, parent_port: Tuple[int, int],
              own_port: int, log_dir: str, is_single_node=False):
    runner_class = "DistributedChildMain" if not is_single_node else "SingleNodeChildMain"
    parent_control_port, parent_window_port = parent_port
    runner_args = ("localhost", parent_control_port, parent_window_port, own_port, child_id, num_streams)
    return _start_runner(f"child-{child_id}", runner_class, runner_args, log_dir)


def run_stream(stream_id: int, num_events: int, parent_port: int, duration: int, log_dir: str, is_fixed_events=False):
    runner_class = "SustainableThroughputRunner" if not is_fixed_events else "InputStreamMain"
    parent_addr = f"localhost:{parent_port}"
    runner_args = (stream_id, parent_addr, num_events, duration)
    return _start_runner(f"stream-{stream_id}", runner_class, runner_args, log_dir)


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

    all_ports = get_ports_for_run(node_config)
    all_ports.insert(0, [(get_next_port(), get_next_port())])
    flat_ports = [str(port) for level in all_ports for port_pair in level for port in port_pair if port is not None]
    print(f"All ports ({len(flat_ports)}): {' '.join(flat_ports)}")

    print("Starting network capture...")
    port_string = " || ".join([f"tcp port {port}" for port in flat_ports])
    capture_file = f"{log_dir}/network_capture.pcap"
    capture_args = ("tshark", "-f", port_string, "-i", "lo0", "-w", capture_file)
    network_capture = subprocess.Popen(capture_args, stdout=open(os.devnull, 'w'), stderr=subprocess.STDOUT)

    print("Starting all runners...")
    all_runners: List[Popen] = []

    # Start root runner
    root_runner = run_root(all_ports[0][0], node_config[0], windows, agg_functions, log_dir, is_single_node)
    all_runners.append(root_runner)

    # Upload for intermediate nodes
    for level_id, level_ports in enumerate(all_ports[1:-1], 1):
        parent_ports = all_ports[level_id - 1]
        num_parents = len(parent_ports)
        num_children = len(all_ports[level_id + 1])
        assert num_children % len(level_ports) == 0
        num_children_per_node = num_children // len(level_ports)
        for node_id, level_port in enumerate(level_ports):
            parent_port = parent_ports[node_id % num_parents]
            node_level_id = (10 * level_id) + node_id
            runner = run_intermediate(node_level_id, num_children_per_node, parent_port,
                                      level_port, log_dir, is_single_node)
            all_runners.append(runner)

    parent_port = all_ports[-2]
    child_ports = all_ports[-1]
    num_parents = len(parent_port)
    num_children = node_config[-2]
    num_streams = node_config[-1]

    # Upload for child nodes
    num_streams_per_child = num_streams // num_children
    for child_id, child_port in enumerate(child_ports):
        p_port = parent_port[child_id % num_parents]
        runner = run_child(child_id, num_streams_per_child, p_port, child_port[0], log_dir, is_single_node)
        all_runners.append(runner)

    # Upload for stream nodes
    for stream_id in range(num_streams):
        child_port = child_ports[stream_id % num_children]
        runner = run_stream(stream_id, num_events, child_port[0], duration, log_dir, is_fixed_events)
        all_runners.append(runner)

    # Checking for completion
    max_run_duration = duration + 30
    incomplete_runners = check_complete(max_run_duration, all_runners, lambda r: r.poll() is not None)
    if incomplete_runners:
        print("Ending script by killing all processes...")
        for runner in all_runners:
            runner.kill()

    print("Joined all run runners.")

    print("Ending network capture...")
    network_capture.terminate()

    print("Analyzing network capture...")
    try:
        analyzer = subprocess.run(("capinfos", "-csdizyxuM", capture_file), timeout=120)
        analysis_failed = analyzer.returncode != 0
    except subprocess.TimeoutExpired:
        analysis_failed = True

    if analysis_failed:
        print(f"Analysis of {capture_file} failed. Re-run manually.")

    print("Zipping capture file...")
    with ZipFile(f"{log_dir}/network_capture.zip", "w", zipfile.ZIP_DEFLATED) as zip_f:
        zip_f.write(capture_file, "network_capture.pcap")
    os.remove(capture_file)

    if process_log_dir_pipe is not None:
        process_log_dir_pipe.send(log_dir)

    for f in ALL_FILES:
        f.close()
    ALL_FILES.clear()

    return log_dir
