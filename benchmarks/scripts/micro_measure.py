from lib.run import run


def micro_measure():
    run(1, 1, 1500000, 120, "TUMBLING,1000", "MAX")
    run(1, 1, 1500000, 120, "TUMBLING,1000", "M_AVG")
    run(1, 1,   75000, 120, "TUMBLING,1000", "M_MEDIAN")


if __name__ == '__main__':
    micro_measure()
