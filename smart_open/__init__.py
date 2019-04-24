#
# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""
Utilities for streaming to/from several file-like data storages: S3 / HDFS / local
filesystem / compressed files, and many more, using a simple, Pythonic API.

The streaming makes heavy use of generators and pipes, to avoid loading
full file contents into memory, allowing work with arbitrarily large files.

The main functions are:

* `open()`, which opens the given file for reading/writing
* `s3_iter_bucket()`, which goes over all keys in an S3 bucket in parallel
* `register_compressor()`, which registers callbacks for transparent compressor handling

"""

import logging
import os.path

from .smart_open_lib import open, smart_open, register_compressor
from .s3 import iter_bucket as s3_iter_bucket
__all__ = ['open', 'smart_open', 's3_iter_bucket', 'register_compressor']


def _get_version():
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(curr_dir, 'VERSION')) as fin:
        return fin.read().strip()


__version__ = _get_version()

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
