import os
from typing import List

from lib.common import print_run_string
from lib.run_local import run, BASE_DIR

RUN_DURATION_DISTRIBUTIVE = 240
RUN_DURATION_ALGEBRAIC = 300
RUN_DURATION_HOLISTIC = 500


def run_matrix(node_configs: List[List[int]], num_events: int, windows: List[str],
               agg_fns: List[str], duration: int):
    for agg_fn in agg_fns:
        for window in windows:
            for node_config in node_configs:
                num_streams = node_config[-1]
                events_per_stream = num_events // num_streams
                print(f"BENCHMARK: WINDOWS: {window} - AGG_FNS: {agg_fn} - DISTRIBUTED")
                print_run_string(node_config)
                run(node_config, events_per_stream, duration, window, agg_fn,
                    is_fixed_events=True, is_single_node=False)

                print(f"BENCHMARK: WINDOWS: {window} - AGG_FNS: {agg_fn} - SINGLE_NODE")
                print_run_string(node_config)
                run(node_config, events_per_stream, duration, window, agg_fn,
                    is_fixed_events=True, is_single_node=True)

                dir_size = get_dir_size(BASE_DIR)
                print(f"{BASE_DIR} at {int(dir_size)} MB.")


def run_multi_level():
    node_configs = [
        [1, 1],
        [2, 2],
        [4, 4],
        [8, 8],
        [1, 1, 1],
        [2, 4, 4],
        [3, 6, 6],
        [4, 8, 8],
        [1, 1, 1, 1],
        [2, 4, 8, 8],
        [4, 8, 16, 16],
        [1, 1, 1, 1, 1],
    ]
    windows = [
        "TUMBLING,1000",
        "SLIDING,1000,500",
        "CONCURRENT,10,TUMBLING,20000",
    ]

    partial_node_configs = [
        [1, 1, 1],
        [2, 4, 4],
        [3, 6, 6],
        [4, 8, 8],
        [1, 1, 1, 1],
        [2, 4, 8, 8],
        [4, 8, 16, 16],
        [1, 1, 1, 1, 1],
    ]
    partial_windows = [
        "SLIDING,1000,500",
        "CONCURRENT,10,TUMBLING,20000",
    ]

    max_agg_fns = ["MAX"]
    run_matrix(partial_node_configs, 100_000_000, partial_windows, max_agg_fns, RUN_DURATION_DISTRIBUTIVE)

    avg_agg_fns = ["M_AVG"]
    run_matrix(node_configs, 100_000_000, windows, avg_agg_fns, RUN_DURATION_ALGEBRAIC)

    # Run with less because median is slow
    median_agg_fns = ["M_MEDIAN"]
    run_matrix(node_configs, 10_000_000, windows, median_agg_fns, RUN_DURATION_HOLISTIC)


def get_dir_size(start_path):
    total_size = 0
    for path, _, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(path, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    # Convert bytes to MB
    return total_size / (1024 * 1024)


if __name__ == '__main__':
    run_multi_level()
