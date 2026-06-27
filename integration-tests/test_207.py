#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import os
import sys
import tempfile

try:
    import numpy as np
except ImportError:
    print("You really need numpy to proceed with this test")
    sys.exit(1)

import smart_open


def tofile():
    """Write a small numpy array to a temp file and return its path."""
    dt = np.dtype([("time", [("min", int), ("sec", int)]), ("temp", float)])
    x = np.zeros((1,), dtype=dt)

    with tempfile.NamedTemporaryFile(prefix="test_207", suffix=".dat", delete=False) as fout:
        x.tofile(fout.name)
        return fout.name


def test_fromfile():
    """Reading a numpy ``.dat`` file through smart_open round-trips successfully."""
    try:
        path = tofile()
        with smart_open.open(path, "rb") as fin:
            np.fromfile(fin)
    finally:
        os.unlink(path)  # noqa: PTH108  # paired with tempfile.NamedTemporaryFile.name
