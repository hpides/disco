from typing import List, Tuple

from executables.latency import run_latency
from executables.find_sustainable_throughput import run_throughput

DURATION = 120


def _run_single_benchmark(num_children: int, num_streams: int, windows: str, agg_fns: str):
    throughput = run_throughput(num_children, num_streams, DURATION, windows, agg_fns)
    run_latency(num_children, num_streams, throughput, DURATION, windows, agg_fns)


def run_benchmark(windows: str, agg_fns: str, num_nodes_config: List[Tuple[int, int]]):
    print(f"BENCHMARK: WINDOWS: {windows} - AGG_FNS: {agg_fns}")
    for num_children, num_streams in num_nodes_config:
        child_str = "child" if num_children == 1 else "children"
        stream_str = "stream" if num_streams == 1 else "streams"
        print(f"Running {num_children} {child_str}, {num_streams} {stream_str}")
        _run_single_benchmark(num_children, num_streams, windows, agg_fns)


def run_benchmark_matrix(windows: List[str], agg_fns: List[str], node_config: List[Tuple[int, int]]):
    for window in windows:
        for agg_fn in agg_fns:
            run_benchmark(window, agg_fn, node_config)


def run_all():
    basic_windows = ["TUMBLING,1000", "SLIDING,1000,500"]  # , "SESSION,100"]
    basic_agg_fns = ["MAX", "M_AVG", "M_MEDIAN"]
    basic_node_config = [(1, 1), (1, 2), (1, 4), (1, 8), (2, 2), (4, 4), (8, 8)]
    run_benchmark_matrix(basic_windows, basic_agg_fns, basic_node_config)

    ############################

    tumbling_windows = [
        # 10-10000 equal tumbling windows
        "\n".join([f"TUMBLING,1000,{i}" for i in range(5)]),
        "\n".join([f"TUMBLING,1000,{i}" for i in range(10)]),
        "\n".join([f"TUMBLING,1000,{i}" for i in range(50)]),
        "\n".join([f"TUMBLING,1000,{i}" for i in range(100)]),
        "\n".join([f"TUMBLING,1000,{i}" for i in range(500)]),
        "\n".join([f"TUMBLING,1000,{i}" for i in range(1000)]),
        "\n".join([f"TUMBLING,1000,{i}" for i in range(5000)]),
        "\n".join([f"TUMBLING,1000,{i}" for i in range(10000)])
    ]
    tumbling_agg_fns = ["MAX", "M_MEDIAN"]
    tumbling_node_config = [(1, 1), (2, 2), (4, 4), (8, 8)]
    run_benchmark_matrix(tumbling_windows, tumbling_agg_fns, tumbling_node_config)


if __name__ == '__main__':
    run_all()
