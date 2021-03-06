import os
import re
import shutil
from argparse import ArgumentParser
from datetime import datetime
from multiprocessing import Pipe, Process
from typing import List

from lib.common import logs_are_unsustainable
from lib.run import run as run_all_main

UTF8 = "utf-8"
THIS_FILE_DIR = os.path.dirname(os.path.realpath(__file__))
LOG_PATH = os.path.abspath(os.path.join(THIS_FILE_DIR, "..", "..", "..", "..", "benchmark-runs"))

RUN_LOG_RE = re.compile(r"Writing logs to (?P<log_location>.*)")
STREAM_LOG_PREFIX = "stream"
GENERIC_ERROR_MSG = "Exception in thread"
NODE_REGISTRATION_FAIL = "Could not register at child node"
QUEUE_SIZE_RE = re.compile(r"Current queue size: (\d+)")

DECOMPOSABLE_SUSTAINABLE_THRESHOLD = 10_000
HOLISTIC_SUSTAINABLE_THRESHOLD = 1_000

DECOMPOSABLE_EXPECTED = 1_000_000
DECOMPOSABLE_MAX = 2_000_000
HOLISTIC_EXPECTED = 15_000
HOLISTIC_MAX = 70_000


RUN_LOGS = []


def move_logs(node_config: List[int]):
    run_date_string = datetime.now().strftime("%Y-%m-%d-%H%M")
    node_str = f"{'_'.join(f'{node}-mergers' for node in node_config[:-2])}"
    num_children = node_config[-2]
    num_streams = node_config[-1]
    child_stream_str = f"{num_children}-children_{num_streams}-streams"
    node_child_sep = "_" if node_str else ""
    run_log_dir = f"sustainable_{run_date_string}_{node_str}{node_child_sep}{child_stream_str}"
    run_log_path = os.path.join(LOG_PATH, run_log_dir)
    os.makedirs(run_log_path)
    for log in RUN_LOGS:
        log_dir_name = os.path.basename(log)
        shutil.move(log, os.path.join(run_log_path, log_dir_name))

    RUN_LOGS.clear()
    print(f"All logs can be found in {run_log_path}.")


def single_sustainability_run(num_events_per_second: int, node_config: List[int],
                              windows: str, agg_functions: str, run_duration: int, is_single_node: bool):
    num_nodes = sum(node_config) + 1  # + 1 for root
    timeout = run_duration + 30
    print(f"Running sustainability test with {num_events_per_second} events/s.")
    process_recv_pipe, process_send_pipe = Pipe(False)
    run_process = Process(target=run_all_main,
                          args=(node_config, num_events_per_second, run_duration, windows, agg_functions),
                          kwargs=({'process_log_dir_pipe': process_send_pipe, 'is_single_node': is_single_node}),
                          name=f"process-run-{num_nodes}-{num_events_per_second}")
    run_process.start()
    try:
        run_process.join(timeout)
    except TimeoutError:
        print("Current run failed. See logs for more details.")
        raise

    directory = process_recv_pipe.recv()
    log_directory = directory
    if log_directory not in RUN_LOGS:
        RUN_LOGS.append(log_directory)
    return logs_are_unsustainable(log_directory)


def sustainability_run(num_events_per_second: int, node_config: List[int],
                       windows: str, agg_functions: str, run_duration: int, is_single_node: bool):
    is_unsustainable = None
    tries = 0
    while is_unsustainable is None and tries < 3:
        is_unsustainable = single_sustainability_run(num_events_per_second, node_config, windows,
                                                     agg_functions, run_duration, is_single_node)
        tries += 1
        if is_unsustainable is None:
            # Error was very different to rest of nodes.
            print(" '--> Result inconclusive, running again...")

    if is_unsustainable is None:
        print(" '--> Result inconclusive again, counting as unsustainable.")
        is_unsustainable = True

    return not is_unsustainable


def find_sustainable_throughput(node_config: List[int], windows: str,
                                agg_functions: str, duration: int, is_single_node: bool):

    expected_max_events_per_stream = DECOMPOSABLE_EXPECTED
    sustainable_threshold = DECOMPOSABLE_SUSTAINABLE_THRESHOLD
    total_max_events = DECOMPOSABLE_MAX

    if "MEDIAN" in agg_functions:
        expected_max_events_per_stream = HOLISTIC_EXPECTED
        sustainable_threshold = HOLISTIC_SUSTAINABLE_THRESHOLD
        total_max_events = HOLISTIC_MAX

    max_events = total_max_events
    min_events = 0
    num_sustainable_events = expected_max_events_per_stream

    print("Trying to find sustainable throughput...")
    while (max_events - min_events > sustainable_threshold
           and num_sustainable_events < max_events):
        is_sustainable = sustainability_run(num_sustainable_events, node_config,
                                            windows, agg_functions, duration, is_single_node)

        if is_sustainable:
            # Try to go higher
            min_events = num_sustainable_events
            num_sustainable_events = (num_sustainable_events + max_events) // 2
            print(f" '--> {min_events} events/s are sustainable.")
        else:
            # Try a lower number of events
            max_events = num_sustainable_events
            num_sustainable_events = (num_sustainable_events + min_events) // 2
            print(f" '--> {max_events} events/s are too many.")

    # Min and max are nearly equal --> min events is sustainable throughput
    print(f"Found sustainable candidate ({min_events} events/s).")
    with open("/tmp/last_sustainable_run", "w") as out_f:
        out_f.write(str(min_events))

    return min_events


def run_throughput(node_config: List[int], duration: int, windows: str, agg_fns: str, is_single_node: bool = False):
    try:
        return find_sustainable_throughput(node_config, windows, agg_fns, duration, is_single_node)
    except Exception as e:
        print(f"Got exception: {e}")
    finally:
        move_logs(node_config)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--nodes", type=int, nargs='+', required=True, dest='nodes')
    parser.add_argument("--windows", type=str, required=True, dest="windows")
    parser.add_argument("--agg-functions", type=str, required=True, dest="agg_functions")
    parser.add_argument("--duration", dest='duration', required=False,
                        type=int, default="120", help="Duration of run in seconds.")

    parser_args = parser.parse_args()
    run_throughput(parser_args.nodes, parser_args.duration,
                   parser_args.windows, parser_args.agg_functions)
