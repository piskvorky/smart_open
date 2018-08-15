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
    dt = np.dtype([('time', [('min', int), ('sec', int)]), ('temp', float)])
    x = np.zeros((1,), dtype=dt)

    with tempfile.NamedTemporaryFile(prefix='test_207', suffix='.dat', delete=False) as fout:
        x.tofile(fout.name)
        return fout.name


def test():
    try:
        path = tofile()
        with smart_open.smart_open(path, 'rb') as fin:
            loaded = np.fromfile(fin)
        return 0
    finally:
        os.unlink(path)
    return 1


if __name__ == '__main__':
    sys.exit(test())
