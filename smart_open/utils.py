# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Helper functions for documentation, etc."""

import inspect
import logging

logger = logging.getLogger(__name__)


def inspect_kwargs(kallable):
    #
    # inspect.getargspec got deprecated in Py3.4, and calling it spews
    # deprecation warnings that we'd prefer to avoid.  Unfortunately, older
    # versions of Python (<3.3) did not have inspect.signature, so we need to
    # handle them the old-fashioned getargspec way.
    #
    try:
        signature = inspect.signature(kallable)
    except AttributeError:
        try:
            args, varargs, keywords, defaults = inspect.getargspec(kallable)
        except TypeError:
            #
            # Happens under Py2.7 with mocking.
            #
            return {}

        if not defaults:
            return {}
        supported_keywords = args[-len(defaults):]
        return dict(zip(supported_keywords, defaults))
    else:
        return {
            name: param.default
            for name, param in signature.parameters.items()
            if param.default != inspect.Parameter.empty
        }


def check_kwargs(kallable, kwargs):
    """Check which keyword arguments the callable supports.

    Parameters
    ----------
    kallable: callable
        A function or method to test
    kwargs: dict
        The keyword arguments to check.  If the callable doesn't support any
        of these, a warning message will get printed.

    Returns
    -------
    dict
        A dictionary of argument names and values supported by the callable.
    """
    supported_keywords = sorted(inspect_kwargs(kallable))
    unsupported_keywords = [k for k in sorted(kwargs) if k not in supported_keywords]
    supported_kwargs = {k: v for (k, v) in kwargs.items() if k in supported_keywords}

    if unsupported_keywords:
        logger.warning('ignoring unsupported keyword arguments: %r', unsupported_keywords)

    return supported_kwargs


def clamp(value, minval, maxval):
    """Clamp a numeric value to a specific range.

    Parameters
    ----------
    value: numeric
        The value to clamp.

    minval: numeric
        The lower bound.

    maxval: numeric
        The upper bound.

    Returns
    -------
    numeric
        The clamped value.  It will be in the range ``[minval, maxval]``.

    """
    return max(min(value, maxval), minval)


def make_range_string(start, stop=None):
    """Create a byte range specifier in accordance with RFC-2616.

    Parameters
    ----------
    start: int
        The start of the byte range

    stop: int, optional
        The end of the byte range.  If unspecified, indicates EOF.

    Returns
    -------
    str
        A byte range specifier.

    """
    #
    # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    #
    if stop is None:
        return 'bytes=%d-' % start
    return 'bytes=%d-%d' % (start, stop)
