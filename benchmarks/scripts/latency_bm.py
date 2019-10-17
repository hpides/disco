from typing import List

from lib.common import print_run_string
from executables.latency import run_latency

DURATION = 120


def run_single_benchmark(windows: str, agg_fns: str, node_config: List[int], throughput: int):
    print(f"BENCHMARK: WINDOWS: {windows} - AGG_FNS: {agg_fns} - SINGLE_NODE")
    print_run_string(node_config)
    run_latency(node_config, throughput, DURATION, windows, agg_fns, True)


def run_dist_benchmark(windows: str, agg_fns: str, node_config: List[int], throughput: int):
    print(f"BENCHMARK: WINDOWS: {windows} - AGG_FNS: {agg_fns} - DISTRIBUTED")
    print_run_string(node_config)
    run_latency(node_config, throughput, DURATION, windows, agg_fns, False)


def run_all():
    dist_matrix = [
        ("TUMBLING,1000", "MAX", [1, 1], (1023437, 875000)),
        ("TUMBLING,1000", "MAX", [2, 2], (1062500, 492187)),
        ("TUMBLING,1000", "MAX", [4, 4], (976562, 250000)),
        ("TUMBLING,1000", "MAX", [8, 8], (929687, 125000)),
        ("TUMBLING,1000", "M_MEDIAN", [1, 1], (28359, 23593)),
        ("TUMBLING,1000", "M_MEDIAN", [2, 2], (26250, 12187)),
        ("TUMBLING,1000", "M_MEDIAN", [4, 4], (26250, 7500)),
        ("TUMBLING,1000", "M_MEDIAN", [8, 8], (23437, 4687)),
    ]

    for window, agg_fn, node_config, throughput in dist_matrix:
        run_dist_benchmark(window, agg_fn, node_config, throughput[0])
        run_single_benchmark(window, agg_fn, node_config, throughput[1])


if __name__ == '__main__':
    run_all()
