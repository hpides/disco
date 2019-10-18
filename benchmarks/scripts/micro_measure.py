from lib.run import run_single_level, run


def micro_measure():
    run_single_level(1, 1, 500000, 120, "TUMBLING,1000", "MAX")

    # run([1, 1, 1, 1], 1000, 120, "TUMBLING,1000", "MAX")
    # run([1, 1, 1, 1], 1000, 120, "TUMBLING,1000", "MAX", is_single_node=False)
    # run([1, 1, 1, 1], 1000, 120, "TUMBLING,1000", "MAX", is_single_node=True)
    # run([1, 1, 1, 1], 1000, 120, "TUMBLING,1000", "MAX", is_single_node=False, is_fixed_events=False)
    # run([1, 1, 1, 1], 1000, 120, "TUMBLING,1000", "MAX", is_single_node=False, is_fixed_events=True)
    # run([1, 1, 1, 1], 1000, 120, "TUMBLING,1000", "MAX", is_single_node=True, is_fixed_events=True)

if __name__ == '__main__':
    micro_measure()
