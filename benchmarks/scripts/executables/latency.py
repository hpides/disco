from multiprocessing import Pipe, Process
from typing import List

from lib.common import logs_are_unsustainable
from lib.run import run as run_all_main


def single_latency_run(node_config: List[int], num_events: int, duration: int,
                       windows: str, agg_functions: str, is_single_node: bool):
    num_nodes = sum(node_config) + 1  # + 1 for root
    timeout = duration + 30
    print(f"Running latency test with {num_events} events/s.")
    process_recv_pipe, process_send_pipe = Pipe(False)
    run_process = Process(target=run_all_main,
                          args=(node_config, num_events, duration, windows, agg_functions),
                          kwargs=({'process_log_dir_pipe': process_send_pipe, 'is_single_node': is_single_node}),
                          name=f"process-run-{num_nodes}-{num_events}")
    run_process.start()
    try:
        run_process.join(timeout)
    except TimeoutError:
        print("Current run failed. See logs for more details.")
        raise

    directory = process_recv_pipe.recv()
    log_directory = directory
    return logs_are_unsustainable(log_directory), log_directory


def _latency_run(node_config: List[int], num_events: int, duration: int,
                 windows: str, agg_functions: str, is_single_node: bool):
    is_unsustainable = None
    log_directory = None
    tries = 0
    while (is_unsustainable is None or is_unsustainable) and tries < 3:
        is_unsustainable, log_directory = \
            single_latency_run(node_config, num_events, duration,
                               windows, agg_functions, is_single_node)

        tries += 1
        if is_unsustainable is None:
            # Error was very different to rest of nodes.
            print(" '--> Result inconclusive, running again...")
        if is_unsustainable:
            # Error was very different to rest of nodes.
            print(" '--> Error while running, running again...")

    if is_unsustainable is None:
        print(" '--> Result inconclusive again, counting as unsustainable.")
        return

    print(f"Latencies in dir: {log_directory}")


def run_latency(node_config: List[int], num_events: int, duration: int,
                windows: str, agg_functions: str, is_single_node: bool = False):
    quarter_events = num_events // 4
    print(f"Running with quarter events/s: {quarter_events}")
    _latency_run(node_config, quarter_events,
                 duration, windows, agg_functions, is_single_node)

    half_events = quarter_events * 2
    print(f"Running with half events/s: {half_events}")
    _latency_run(node_config, half_events,
                 duration, windows, agg_functions, is_single_node)

    three_quarter_events = quarter_events * 3
    print(f"Running with three quarter events/s: {three_quarter_events}")
    _latency_run(node_config, three_quarter_events,
                 duration, windows, agg_functions, is_single_node)

    full_events = num_events
    print(f"Running with full events/s: {full_events}")
    _latency_run(node_config, full_events,
                 duration, windows, agg_functions, is_single_node)


# TODO: change if needed again
# if __name__ == '__main__':
#     parser = ArgumentParser()
#     parser.add_argument("--num-children", type=int, required=True, dest='num_children')
#     parser.add_argument("--num-streams", type=int, required=True, dest='num_streams')
#     parser.add_argument("--num-events", type=int, required=True, dest="num_events")
#     parser.add_argument("--duration", type=int, dest='duration', default=120)
#     parser.add_argument("--windows", type=str, required=True, dest="windows")
#     parser.add_argument("--agg-functions", type=str, required=True, dest="agg_functions")
#
#     args = parser.parse_args()
#     run_latency(args.num_children, args.num_streams, args.num_events,
#                 args.duration, args.windows, args.agg_functions)
