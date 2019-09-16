import argparse
import random
import string
import sys
from datetime import datetime
from threading import Thread

from .common import *

FILE_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.abspath(os.path.join(FILE_DIR, "..", "..", ".."))


def get_log_dir(num_nodes, num_events, duration):
    now = datetime.now()
    run_date_string = now.strftime("%Y-%m-%d-%H%M")
    return f"{BASE_DIR}/benchmark-runs/{run_date_string}_{num_nodes}-" \
           f"nodes_{num_events}-events_{duration}-seconds"


def upload_benchmark_params(ip, *args):
    string_args = [str(arg) for arg in args]
    args_str = " ".join(string_args)
    ssh_command(ip, f'echo "export BENCHMARK_ARGS=\\\"{args_str}\\\"" >> ~/benchmark_env')


def run_droplet(droplet, log_dir, timeout=None):
    ip = get_ip_of_droplet(droplet)
    name = droplet['name']
    ssh_command(ip, "~/run.sh &> logs", timeout=timeout, verbose=True)

    out_file_path = os.path.join(log_dir, f"{name}.log")
    util_file_path = os.path.join(log_dir, f"util_{name}.log")
    output = ssh_command(ip, "cat logs >> all_logs && cat logs")
    with open(out_file_path, "w") as out_file:
        print(output)
        out_file.write(output)

    util_output = ssh_command(ip, "cat util.log")
    with open(util_file_path, "w") as out_file:
        print(util_output)
        out_file.write(util_output)


def run(num_nodes, num_events, duration, windows, agg_functions,
        delete, short, process_log_dir_pipe=None):
    log_dir = get_log_dir(num_nodes, num_events, duration)

    try:
        os.makedirs(log_dir)
    except OSError:
        log_dir += f"_{''.join(random.choices(string.ascii_lowercase, k=3))}"
        os.makedirs(log_dir)

    if process_log_dir_pipe is not None:
        sys.stdout = open(os.path.join(log_dir, "main.log"), "w")

    print(f"Writing logs to {log_dir}\n")

    print("Getting IPs...")
    wait_for_ips(num_nodes)

    all_droplets = get_droplets()
    if len(all_droplets) != num_nodes:
        print(f"Did not get enough IPs while waiting. "
              f"Got: {len(all_droplets)}, expected: {num_nodes}.")
        sys.exit(1)

    all_ips = [get_ip_of_droplet(droplet) for droplet in all_droplets]
    print(f"All IPs ({len(all_ips)}): {' '.join(all_ips)}")

    if not short:
        add_hosts(num_nodes)
        ready_check(num_nodes)

    print("Setup done. Uploading benchmark arguments on all nodes...")
    child_ips = get_ips(CHILD_TAG)
    num_children = len(child_ips)

    stream_ips = get_ips(STREAM_TAG)
    num_streams = len(stream_ips)

    root_ip = get_ips(ROOT_TAG)[0]
    upload_benchmark_params(root_ip, num_children, windows, agg_functions)

    num_streams_per_child = num_streams // num_children
    for child_ip in child_ips:
        upload_benchmark_params(child_ip, num_streams_per_child)

    for stream_ip in stream_ips:
        upload_benchmark_params(stream_ip, num_events, duration)

    print("Starting `run.sh` on all nodes.\n")
    max_run_duration = duration + 30
    threads = []
    for droplet in all_droplets:
        droplet_name = droplet['name']

        thread = Thread(target=run_droplet,
                        args=(droplet, log_dir, max_run_duration),
                        name=f"thread-{droplet_name}")
        thread.start()
        threads.append(thread)

    print("To view root logs:")
    print(f"tail -F {os.path.join(log_dir, 'root.log')}\n")

    uncompleted_ips = check_complete(max_run_duration, all_ips)
    if uncompleted_ips:
        kill_command = "pkill -9 java &> /dev/null"
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

    if delete:
        print("Deleting droplets...")
        for droplet in all_droplets:
            doctl.compute.droplet.delete(str(droplet['id']))

    if process_log_dir_pipe is not None:
        process_log_dir_pipe.send(log_dir)

    return log_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-nodes", type=int, required=True, dest="num_nodes")
    parser.add_argument("--num-events", type=int, required=True, dest="num_events")
    parser.add_argument("--duration", type=int, required=True, dest="duration")
    parser.add_argument("--windows", type=str, required=True, dest="windows")
    parser.add_argument("--agg-functions", type=str, required=True, dest="agg_functions")

    parser.add_argument("--delete", dest='delete', action='store_true')
    parser.add_argument("--short", dest='short', action='store_true')
    parser.set_defaults(delete=False)
    parser.set_defaults(short=False)

    args = parser.parse_args()
    run(args.num_nodes, args.num_events, args.duration, args.windows,
        args.agg_functions, args.delete, args.short)
