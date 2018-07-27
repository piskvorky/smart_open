import io
import json
import logging
import os
import os.path as P
import subprocess
import warnings

import avro.io
import avro.datafile
import smart_open
import six

with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    import pandas as pn

logging.basicConfig(level=logging.ERROR)

if six.PY3:
    assert False, 'this code only runs on Py2.7'

_S3_URL = os.environ.get('SO_S3_URL')
assert _S3_URL is not None, 'please set the SO_S3_URL environment variable'

_NUMROWS = os.environ.get('SO_NUMROWS')
if _NUMROWS is not None:
    _NUMROWS = int(_NUMROWS)


def gen_schema(data):
    schema = {
        'type': 'record', 'name': 'data', 'namespace': 'namespace',
        'fields': [
            {'name': field, 'type': ['null', 'string'], 'default': None}
            for field in data.columns
        ]
    }
    return json.dumps(schema, indent=4)


if not P.isfile('index_2018.csv'):
    os.system('aws s3 cp s3://irs-form-990/index_2018.csv .')

with open('index_2018.csv') as fin:
    data = pn.read_csv(fin, header=0, error_bad_lines=False,
                       nrows=_NUMROWS, dtype='str').fillna('NA')

avroSchemaOut = gen_schema(data)

# with open('schema.out', 'wb') as fout:
#     fout.write(avroSchemaOut)

output_url = _S3_URL + '/issue_209/out.avro'


def write_avro(foutd):
    schema = avro.schema.parse(avroSchemaOut)
    dictRes = data.to_dict(orient='records')
    writer = avro.datafile.DataFileWriter(foutd, avro.io.DatumWriter(), schema)
    for ll, row in enumerate(dictRes):
        writer.append(row)


with smart_open.smart_open('local.avro', 'wb') as foutd:
    logging.critical('writing to %r', foutd)
    write_avro(foutd)


if False:
    #
    # This is a sanity check.  We're effectively writing to disk, and then
    # writing from disk to S3 via smart_open
    #
    with smart_open.smart_open(output_url, 'wb') as foutd:
        logging.critical('writing to %r', foutd)
        with open('local.avro', 'rb') as fin:
            while True:
                buf = fin.read(io.DEFAULT_BUFFER_SIZE)
                if not buf:
                    break
                foutd.write(buf)
    os.system('aws s3 cp %s remote.avro' % output_url)
    subprocess.check_call(['diff', 'local.avro', 'remote.avro'])
    print('sanity check OK')


if False:
    #
    # Mirrors the way avro writes to S3 via smart_open
    #
    BUFLEN = [4, 1, 1, 11, 1, 1, 680, 1, 10, 1, 4, 1, 16, 1, 1, 1, 1, 1, 64064, 16]
    assert sum(BUFLEN) == len(open('local.avro').read())


    with smart_open.smart_open(output_url, 'wb') as foutd:
        logging.critical('writing to %r', foutd)
        with open('local.avro', 'rb') as fin:
            for buflen in BUFLEN:
                buf = fin.read(buflen)
                if not buf:
                    break
                foutd.write(buf)
    os.system('aws s3 cp %s remote.avro' % output_url)
    subprocess.check_call(['diff', 'local.avro', 'remote.avro'])
    print('sanity check 2 OK')

#
# This is the real test.  We're writing to S3 on the fly.  We somehow end up
# with a different file.
#
with smart_open.smart_open(output_url, 'wb') as foutd:
    logging.critical('writing to %r', foutd)
    write_avro(foutd)
os.system('aws s3 cp %s remote.avro' % output_url)

try:
    subprocess.check_call(['diff', 'local.avro', 'remote.avro'])
except subprocess.CalledProcessError:
    print('test NG')
else:
    print('test OK')
