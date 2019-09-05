import os
import re
import shutil
import subprocess
from argparse import ArgumentParser
from datetime import datetime
from multiprocessing import Process, Pipe

from lib.run import run as run_all_main
from lib.common import wait_for_setup, logs_are_unsustainable

UTF8 = "utf-8"
THIS_FILE_DIR = os.path.dirname(os.path.realpath(__file__))
SCRIPTS_PATH = os.path.join(THIS_FILE_DIR, "..", "..", "scripts")
LOG_PATH = os.path.abspath(os.path.join(THIS_FILE_DIR, "..", "..", "benchmark-runs"))
CREATE_DROPLETS_SCRIPT = os.path.join(SCRIPTS_PATH, "create-droplets.sh")

RUN_LOG_RE = re.compile(r"Writing logs to (?P<log_location>.*)")
STREAM_LOG_PREFIX = "stream"
GENERIC_ERROR_MSG = "Exception in thread"
NODE_REGISTRATION_FAIL = "Could not register at child node"
QUEUE_SIZE_RE = re.compile(r"Current queue size: (\d+)")

SUSTAINABLE_THRESHOLD = 10_000

RUN_LOGS = []


def move_logs(num_children, num_streams):
    now = datetime.now()
    run_date_string = now.strftime("%Y-%m-%d-%H%M")
    run_log_dir = f"sustainable_{run_date_string}_{num_children}-children_{num_streams}-streams"
    run_log_path = os.path.join(LOG_PATH, run_log_dir)
    os.makedirs(run_log_path)
    for log in RUN_LOGS:
        log_dir_name = os.path.basename(log)
        shutil.move(log, os.path.join(run_log_path, log_dir_name))

    print(f"All logs can be found in {run_log_path}.")


def single_sustainability_run(num_events_per_second, num_children, num_streams,
                              windows, agg_functions, run_duration, delete=False):
    num_nodes = num_children + num_streams + 1  # + 1 for root
    short = True

    timeout = 2 * run_duration
    print(f"Running sustainability test with {num_events_per_second} events/s.")

    process_recv_pipe, process_send_pipe = Pipe(False)
    run_process = Process(target=run_all_main,
                          args=(num_nodes, num_events_per_second,
                                run_duration, windows,
                                agg_functions, delete, short, process_send_pipe),
                          name=f"process-run-{num_nodes}-{num_events_per_second}")
    run_process.start()

    try:
        run_process.join(timeout)
    except TimeoutError:
        print("Current run failed. See logs for more details.")
        raise

    log_directory = process_recv_pipe.recv()
    if log_directory not in RUN_LOGS:
        RUN_LOGS.append(log_directory)
    return logs_are_unsustainable(log_directory)


def sustainability_run(num_events_per_second, num_children, num_streams,
                       windows, agg_functions, run_duration, delete=False):
    is_unsustainable = None
    tries = 0
    while is_unsustainable is None and tries < 3:
        is_unsustainable = single_sustainability_run(num_events_per_second,
                                                     num_children, num_streams,
                                                     windows, agg_functions,
                                                     run_duration, delete)
        tries += 1
        if is_unsustainable is None:
            # Error was very different to rest of nodes.
            print(" '--> Result inconclusive, running again...")

    if is_unsustainable is None:
        print(" '--> Result inconclusive again, counting as unsustainable.")
        is_unsustainable = True

    return not is_unsustainable


def find_sustainable_throughput(args):
    num_children = args.num_children
    num_streams = args.num_streams
    should_create_nodes = args.create
    duration = args.duration
    windows = args.windows
    agg_functions = args.agg_functions

    num_nodes = 1 + num_children + num_streams

    if should_create_nodes:
        print("Creating nodes...")
        create_command = (CREATE_DROPLETS_SCRIPT, f"{num_children}", f"{num_streams}")
        subprocess.run(create_command, check=True, timeout=180)
    else:
        print("Using existing node setup.")

    # Wait for nodes to set up. Otherwise the time out of the runs will kill the setup.
    wait_for_setup(num_nodes)

    total_max_events = 2_000_000
    expected_max_events_per_stream = 1_000_000

    max_events = total_max_events
    min_events = 0
    num_sustainable_events = expected_max_events_per_stream

    print("Trying to find sustainable throughput...")
    while (max_events - min_events > SUSTAINABLE_THRESHOLD
           and num_sustainable_events < max_events):
        is_sustainable = sustainability_run(num_sustainable_events,
                                            num_children, num_streams,
                                            windows, agg_functions,
                                            duration)

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


def main(args):
    red = '\033[91m'
    end_color = '\033[0m'
    if not args.delete:
        print(red + "RUNNING IN NO_DELETE MODE! MAKE SURE TO DELETE MANUALLY AFTER USE!" + end_color)

    try:
        find_sustainable_throughput(parser_args)
    except Exception as e:
        print(f"Got exception: {e}")

        if not args.delete:
            return

        print("Deleting droplets...")
        droplet_id_process = subprocess.run(("doctl", "compute", "droplet", "list",
                                             "--format=ID", "--no-header"),
                                            capture_output=True)
        droplet_id_process_output = str(droplet_id_process.stdout, UTF8)
        droplet_ids = droplet_id_process_output.split("\n")
        droplet_ids = [d_id for d_id in droplet_ids if len(d_id) == 9]
        subprocess.run(("doctl", "compute", "droplet", "delete", "--force", *droplet_ids))
    finally:
        move_logs(args.num_children, args.num_streams)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--num-children", dest='num_children', required=True,
                        type=int, help="Number of total children.")
    parser.add_argument("--num-streams", dest='num_streams', required=True,
                        type=int, help="Number of stream per child.")
    parser.add_argument("--windows", type=str, required=True, dest="windows")
    parser.add_argument("--agg-functions", type=str, required=True, dest="agg_functions")
    parser.add_argument("--no-create", dest='create', action='store_false')
    parser.add_argument("--no-delete", dest='delete', action='store_false')
    parser.add_argument("--duration", dest='duration', required=False,
                        type=int, default="120", help="Duration of run in seconds.")
    parser.set_defaults(create=True)
    parser.set_defaults(delete=True)

    parser_args = parser.parse_args()
    main(parser_args)
