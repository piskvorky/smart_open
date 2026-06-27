#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import sys
import time

import smart_open

open_fn = smart_open.open
# open_fn = open  # noqa: ERA001


def report_time_iterate_rows(file_name, report_every=100000):
    """Iterate ``file_name`` line by line, printing throughput every N rows."""
    start = time.time()
    last = start
    with open_fn(file_name, "r") as f:
        for i, _line in enumerate(f, start=1):
            if not (i % report_every):
                current = time.time()
                time_taken = current - last
                print(
                    f"Time taken for {report_every} rows: {time_taken:.2f} seconds, "
                    f"{report_every / time_taken:.2f} rows/s"
                )
                last = current
    total = time.time() - start
    print(f"Total: {i} rows, {total:.2f} seconds, {i / total:.2f} rows/s")


report_time_iterate_rows(sys.argv[1])
