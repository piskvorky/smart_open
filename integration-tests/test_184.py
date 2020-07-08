# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import sys
import time

import smart_open

open_fn = smart_open.smart_open
# open_fn = open


def report_time_iterate_rows(file_name, report_every=100000):
    start = time.time()
    last = start
    with open_fn(file_name, 'r') as f:
        for i, line in enumerate(f, start=1):
            if not (i % report_every):
                current = time.time()
                time_taken = current - last
                print('Time taken for %d rows: %.2f seconds, %.2f rows/s' % (
                    report_every, time_taken, report_every / time_taken))
                last = current
    total = time.time() - start
    print('Total: %d rows, %.2f seconds, %.2f rows/s' % (
        i, total, i / total))


report_time_iterate_rows(sys.argv[1])
