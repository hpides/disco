import os
from argparse import ArgumentParser
from multiprocessing import Process, Pipe

from lib.common import wait_for_setup, logs_are_unsustainable
from lib.run import run as run_all_main


def single_latency_run(num_nodes, num_events, duration, windows, agg_functions):
    timeout = duration + 30
    print(f"Running latency test with {num_events} events/s.")

    process_recv_pipe, process_send_pipe = Pipe(False)
    run_process = Process(target=run_all_main,
                          args=(num_nodes, num_events,
                                duration, windows,
                                agg_functions, False, False, process_send_pipe),
                          name=f"process-run-{num_nodes}-{num_events}")
    run_process.start()

    try:
        run_process.join(timeout)
    except TimeoutError:
        print("Current run failed. See logs for more details.")
        raise

    log_directory = process_recv_pipe.recv()
    return logs_are_unsustainable(log_directory), log_directory


def _latency_run(num_nodes, num_events, duration, windows, agg_functions):
    is_unsustainable = None
    log_directory = None
    tries = 0
    while (is_unsustainable is None or is_unsustainable) and tries < 3:
        is_unsustainable, log_directory = \
            single_latency_run(num_nodes, num_events, duration, windows, agg_functions)

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


def run_latency(num_nodes, num_events, duration, windows, agg_functions):
    # Wait for nodes to set up. Otherwise the time out of the runs will kill the setup.
    wait_for_setup(num_nodes)

    quarter_events = num_events // 4
    print(f"Running with quarter events/s: {quarter_events}")
    _latency_run(num_nodes, quarter_events, duration, windows, agg_functions)

    half_events = quarter_events * 2
    print(f"Running with half events/s: {half_events}")
    _latency_run(num_nodes, half_events, duration, windows, agg_functions)

    three_quarter_events = quarter_events * 3
    print(f"Running with three quarter events/s: {three_quarter_events}")
    _latency_run(num_nodes, three_quarter_events, duration, windows, agg_functions)

    full_events = num_events
    print(f"Running with full events/s: {full_events}")
    _latency_run(num_nodes, full_events, duration, windows, agg_functions)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--num-nodes", type=int, required=True, dest="num_nodes")
    parser.add_argument("--num-events", type=int, required=True, dest="num_events")
    parser.add_argument("--duration", type=int, dest='duration', required=True)
    parser.add_argument("--windows", type=str, required=True, dest="windows")
    parser.add_argument("--agg-functions", type=str, required=True, dest="agg_functions")

    args = parser.parse_args()
    run_latency(args.num_nodes, args.num_events, args.duration,
                args.windows, args.agg_functions)
