# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import json
import logging
import os
import os.path as P
import subprocess
import urlparse
import warnings

import avro.io
import avro.datafile
import boto3
import mock
import moto
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


def maybe_mock_s3(func):
    if os.environ.get('SO_ENABLE_MOCKS') == "1":
        return moto.mock_s3(func)
    else:
        return func


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

num_csv_rows = len(data.index)
avroSchemaOut = gen_schema(data)

output_url = _S3_URL + '/issue_209/out.avro'


@mock.patch(
    'avro.datafile.DataFileWriter.generate_sync_marker',
    mock.Mock(return_value=b'0123456789abcdef'),
)
def write_avro_context_manager(foutd):
    schema = avro.schema.parse(avroSchemaOut)
    dictRes = data.to_dict(orient='records')
    with avro.datafile.DataFileWriter(foutd, avro.io.DatumWriter(), schema) as writer:
        for ll, row in enumerate(dictRes):
            writer.append(row)


@mock.patch(
    'avro.datafile.DataFileWriter.generate_sync_marker',
    mock.Mock(return_value=b'0123456789abcdef'),
)
def write_avro_manual_close(foutd):
    schema = avro.schema.parse(avroSchemaOut)
    dictRes = data.to_dict(orient='records')
    writer = avro.datafile.DataFileWriter(foutd, avro.io.DatumWriter(), schema)
    for ll, row in enumerate(dictRes):
        writer.append(row)
    writer.close()


#
# The above two functions appear to work identically.
#
write_avro = write_avro_context_manager


with open('local.avro', 'wb') as foutd:
    logging.critical('writing to %r', foutd)
    write_avro(foutd)

with smart_open.smart_open('local-so.avro', 'wb') as foutd:
    logging.critical('writing to %r', foutd)
    write_avro(foutd)
subprocess.check_call(['diff', 'local.avro', 'local-so.avro'])
print('sanity check OK')


def split_s3_url(url):
    parsed = urlparse.urlparse(url)
    return parsed.netloc, parsed.path[1:]


def read_avro(fin):
    reader = avro.datafile.DataFileReader(fin, avro.io.DatumReader())
    return list(reader)


def diff(file1, file2):
    with open(file1, 'rb') as fin:
        records1 = read_avro(fin)
    with open(file2, 'rb') as fin:
        records2 = read_avro(fin)
    if len(records1) != num_csv_rows:
        print('%s contains %r records, but I expected %r' % (file1, len(records1), num_csv_rows))
    if len(records2) != num_csv_rows:
        print('%s contains %r records, but I expected %r' % (file2, len(records1), num_csv_rows))
    return 0 if records1 == records2 else 1


@maybe_mock_s3
def run():
    bucket_name, key_name = split_s3_url(output_url)
    logging.critical(
        'output_url: %r bucket_name: %r key_name: %r',
        output_url, bucket_name, key_name,
    )

    s3 = boto3.resource('s3')
    if os.environ.get('SO_ENABLE_MOCKS') == "1":
        s3.create_bucket(Bucket=bucket_name)

    with smart_open.smart_open(output_url, 'wb') as foutd:
        logging.critical('writing to %r', foutd)
        write_avro(foutd)

    with open('remote.avro', 'wb') as fout:
        fout.write(s3.Object(bucket_name, key_name).get()['Body'].read())

    if diff('local.avro', 'remote.avro'):
        print('test NG')
    else:
        print('test OK')

    subprocess.check_call(['diff', 'local.avro', 'remote.avro'])
    print('binary check OK')


run()
