import re

#[RESULT] FINAL WINDOW: WindowAggregateId{windowId=1, windowStartTimestamp=155000, windowEndTimestamp=156000} --> 581549
WINDOW_RE = re.compile(r"WindowAggregateId{windowId=(\d+), windowStartTimestamp=(\d+), windowEndTimestamp=(\d+)} --> (\d+)")


class WindowAggregate:
    def __init__(self, window_id, window_start, window_end, aggregate):
        self.window_id = window_id
        self.window_start = window_start
        self.window_end = window_end
        self.aggregate = aggregate

    def __str__(self):
        return "WindowAggregateId(" \
            f"windowId={self.window_id}, " \
            f"windowStartTimestamp={self.window_start}, " \
            f"windowEndTimestamp={self.window_end}, " \
            f"aggregate={self.aggregate}" \
            ")"

    def __repr__(self):
        return str(self)


def parse_log(log_file):
    windows = []
    with open(log_file) as log:
        for line in log:
            if not line.startswith("[RESULT] FINAL WINDOW:"):
                continue

            match = WINDOW_RE.search(line)
            if match is None:
                print(f"Bad line: {line}")
                continue

            window_id = int(match.group(1))
            window_start = int(match.group(2))
            window_end = int(match.group(3))
            aggregate = int(match.group(4))
            window_aggregate = WindowAggregate(window_id, window_start,
                                               window_end, aggregate)
            windows.append(window_aggregate)

    return windows
