from lib.run import run_single_level


def micro_measure():
    run_single_level(1, 1, 1000000, 120, "TUMBLING,1000", "MAX")
    run_single_level(1, 1,  900000, 120, "TUMBLING,1000", "M_AVG")
    run_single_level(1, 1,   30000, 120, "TUMBLING,1000", "M_MEDIAN")


if __name__ == '__main__':
    micro_measure()
