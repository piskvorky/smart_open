#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Common functionality for concurrent processing.

The main entry point is :class:`ThreadPoolExecutor`, which extends the
standard library executor with a lazy ``imap`` method.
"""

from __future__ import annotations

import logging
from collections import deque
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

logger = logging.getLogger(__name__)


class ThreadPoolExecutor(_ThreadPoolExecutor):
    """Subclass with a lazy consuming imap method."""

    def imap(
        self,
        fn: Callable[..., Any],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        queued_tasks_per_worker: int = 2,
    ) -> Iterator[Any]:
        """Ordered imap that consumes iterables just-in-time.

        References:
            https://gist.github.com/ddelange/c98b05437f80e4b16bf4fc20fde9c999

        Args:
            fn: Function to apply.
            *iterables: One (or more) iterable(s) to pass to fn (using zip) as positional argument(s).
            timeout: Per-future result retrieval timeout in seconds.
            queued_tasks_per_worker: Amount of additional items per worker to fetch from iterables to
                    fill the queue: this determines the total queue size.
                Setting 0 will result in a true just-in-time behaviour: when a worker finishes a task,
                    it waits until a result is consumed from the imap generator, at which point next()
                    is called on the input iterable(s) and a new task is submitted.
                Default 2 ensures there is always some work to pick up. Note that at imap startup,
                    the queue will fill up before the first yield occurs.

        Yields:
            Results of ``fn`` applied to items from ``iterables``, in input order.

        Example:
            long_generator = itertools.count()
            with ThreadPoolExecutor(42) as pool:
                result_generator = pool.imap(fn, long_generator)
                for result in result_generator:
                    print(result)
        """
        futures, maxlen = deque(), self._max_workers * (queued_tasks_per_worker + 1)
        popleft, append, submit = futures.popleft, futures.append, self.submit

        def get() -> Any:
            """Block until the next task is done and return the result."""
            return popleft().result(timeout)

        for args in zip(*iterables, strict=False):
            append(submit(fn, *args))
            if len(futures) == maxlen:
                yield get()

        while futures:
            yield get()
