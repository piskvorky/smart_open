import io
import sys

import smart_open

s3_url, local_path = sys.argv[1:]
with smart_open.smart_open(s3_url, 'rb') as fin, open(local_path, 'wb') as fout:
    while True:
        buf = fin.read(io.DEFAULT_BUFFER_SIZE)
        if not buf:
            break
        fout.write(buf)
