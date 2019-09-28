from lib.run import run


def run_matrix(node_config, num_events, windows, agg_fns):
    for agg_fn in agg_fns:
        for window in windows:
            run(node_config, num_events, 120, window, agg_fn)


def run_multi_level():
    node_configs = [
        [2, 2],
        [4, 4],
        [8, 8],
        [2, 4, 4],
        [3, 6, 6],
        [4, 8, 8],
        [1, 1, 1, 1],
    ]
    windows = [
        "TUMBLING,1000",
        "SLIDING,1000,500",
        "CONCURRENT,10,TUMBLING,1000",
        "CONCURRENT,100,TUMBLING,1000",
    ]
    agg_fns = ["MAX", "M_AVG", "M_MEDIAN"]
    run_matrix(node_configs, 1500000, windows, agg_fns)


if __name__ == '__main__':
    run_multi_level()
