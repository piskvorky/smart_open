#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Utilities for streaming to/from several file-like data storages.

Supports S3 / HDFS / local filesystem / compressed files, and many more,
using a simple, Pythonic API.

The streaming makes heavy use of generators and pipes, to avoid loading
full file contents into memory, allowing work with arbitrarily large files.

The main functions are:

* `open()`, which opens the given file for reading/writing
* `parse_uri()`
* `register_compressor()`, which registers callbacks for transparent compressor handling

"""

import contextlib
import logging
from importlib.metadata import PackageNotFoundError, version

with contextlib.suppress(PackageNotFoundError):
    __version__ = version("smart_open")
#
# Prevent regression of #474 and #475
#
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

from .compression import register_compressor  # noqa: E402  # logger setup precedes imports (see #474)
from .smart_open_lib import open, parse_uri  # noqa: E402  # logger setup precedes imports (see #474)

__all__ = [
    "open",
    "parse_uri",
    "register_compressor",
]
