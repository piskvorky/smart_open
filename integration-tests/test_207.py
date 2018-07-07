import os
import sys

try:
    import numpy as np
except ImportError:
    print("You really need numpy to proceed with this test")
    sys.exit(1)

import smart_open


def tofile():
    dt = np.dtype([('time', [('min', int), ('sec', int)]), ('temp', float)])
    x = np.zeros((1,), dtype=dt)

    fname = "test.dat"
    x.tofile(fname)
    return fname


try:
    path = tofile()
    with smart_open.smart_open(path, 'rb') as fin:
        loaded = np.fromfile(fin)
    print("OK")
finally:
    os.unlink(path)
