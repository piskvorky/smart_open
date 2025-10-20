# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Common functionality for concurrent processing. The main entry point is :func:`create_pool`."""

import concurrent.futures
import contextlib
import logging

logger = logging.getLogger(__name__)


class ConcurrentFuturesPool(object):
    """A class that mimics multiprocessing.pool.Pool but uses concurrent futures instead of processes."""
    def __init__(self, max_workers):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    def imap_unordered(self, function, items):
        futures = [self.executor.submit(function, item) for item in items]
        for future in concurrent.futures.as_completed(futures):
            yield future.result()

    def terminate(self):
        self.executor.shutdown(wait=True)


@contextlib.contextmanager
def create_pool(processes=1):  # arg is called processes due to historical reasons
    logger.info("creating concurrent futures pool with %i workers", processes)
    pool = ConcurrentFuturesPool(max_workers=processes)
    yield pool
    pool.terminate()
