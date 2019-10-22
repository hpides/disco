from typing import List

from lib.common import print_run_string
from executables.latency import run_latency
from executables.find_sustainable_throughput import run_throughput

DURATION = 120


def _run_single_benchmark(node_config: List[int], windows: str, agg_fns: str, is_single_node: bool):
    print_run_string(node_config)
    throughput = run_throughput(node_config, DURATION, windows, agg_fns, is_single_node)
    # run_latency(node_config, throughput, DURATION, windows, agg_fns, is_single_node)


def run_benchmark(windows: str, agg_fns: str, node_configs: List[List[int]], is_single_node: bool):
    run_mode_str = "SINGLE_NODE" if is_single_node else "DISTRIBUTED"
    print(f"BENCHMARK: WINDOWS: {windows} - AGG_FNS: {agg_fns} - {run_mode_str}")
    for node_config in node_configs:
        _run_single_benchmark(node_config, windows, agg_fns, is_single_node)


def run_benchmark_matrix(windows: List[str], agg_fns: List[str], node_config: List[List[int]],
                         is_single_node: bool = False):
    for agg_fn in agg_fns:
        for window in windows:
            run_benchmark(window, agg_fn, node_config, is_single_node)


def run_single_node_benchmark_matrix(windows: List[str], agg_fns: List[str], node_config: List[List[int]]):
    run_benchmark_matrix(windows, agg_fns, node_config, is_single_node=True)


def run_all():
    basic_windows = ["TUMBLING,1000"]  # , "SLIDING,1000,500"]  # , "SESSION,100"]
    basic_agg_fns = ["MAX", "M_AVG", "M_MEDIAN"]
    # basic_node_config = [[1, 1], [1, 2], [1, 4], [1, 8], [2, 2], [4, 4], [8, 8]]
    basic_node_config = [[1, 1], [2, 2], [4, 4], [8, 8]]
    run_benchmark_matrix(basic_windows, basic_agg_fns, basic_node_config)
    # run_single_node_benchmark_matrix(basic_windows, basic_agg_fns, basic_node_config)

    ############################

    # concurrent_windows = [
    #     # 1-1000 overlapping tumbling windows
    #     "CONCURRENT,1,TUMBLING,1000",
    #     "CONCURRENT,5,TUMBLING,1000",
    #     "CONCURRENT,10,TUMBLING,2000",
    #     "CONCURRENT,50,TUMBLING,5000",
    #     "CONCURRENT,100,TUMBLING,20000",
    #     "CONCURRENT,500,TUMBLING,20000",
    #     "CONCURRENT,1000,TUMBLING,20000",
    # ]
    # concurrent_node_config = [[1, 1]]
    #
    # max_windows = [
    #     "CONCURRENT,500,TUMBLING,20000",
    #     "CONCURRENT,1000,TUMBLING,20000",
    # ]
    #
    # run_benchmark_matrix(max_windows, ["MAX"], concurrent_node_config)
    # run_single_node_benchmark_matrix(max_windows, ["MAX"], concurrent_node_config)
    #
    # concurrent_agg_fns = ["M_AVG", "M_MEDIAN"]
    # run_benchmark_matrix(concurrent_windows, concurrent_agg_fns, concurrent_node_config)
    # run_single_node_benchmark_matrix(concurrent_windows, concurrent_agg_fns, concurrent_node_config)


if __name__ == '__main__':
    run_all()
