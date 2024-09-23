# -*- coding: utf-8 -*-
import io
import os

import obs
from obs import ObsClient

import smart_open
from smart_open.obs import parse_uri

_OBS_URL = os.environ.get('SO_OBS_URL')

assert _OBS_URL is not None, 'please set the SO_OBS_URL environment variable'

assert os.environ.get('OBS_ACCESS_KEY_ID') is not None, \
    'please set the OBS_ACCESS_KEY_ID environment variable'
assert os.environ.get('OBS_SECRET_ACCESS_KEY') is not None, \
    'please set the OBS_SECRET_ACCESS_KEY environment variable'


def _clear_bucket(obs_client: obs.ObsClient, bucket_id: str):
    objects = obs_client.listObjects(bucketName=bucket_id)
    for content in objects.body.contents:
        print(content.get('key'))
        _delete_object(obs_client=obs_client,
                       bucket_id=bucket_id,
                       object_key=content.get('key'))


def _delete_object(obs_client: obs.ObsClient, bucket_id: str, object_key: str):
    try:
        resp = obs_client.deleteObject(bucketName=bucket_id, objectKey=object_key)
        if resp.status < 300:
            print('requestId:', resp.requestId)
            print('deleteMarker:', resp.body.deleteMarker)
    except Exception as ex:
        print(ex)


def initialize_bucket():
    parsed = parse_uri(_OBS_URL)
    server = f'https://{parsed.get("server")}'
    bucket_id = parsed.get('bucket_id')
    obs_client = ObsClient(server=server, security_provider_policy='ENV')
    _clear_bucket(obs_client=obs_client, bucket_id=bucket_id)


def write_read(key, content, write_mode, read_mode, **kwargs):
    with smart_open.open(key, write_mode, **kwargs) as fout:
        fout.write(content)
    with smart_open.open(key, read_mode, **kwargs) as fin:
        return fin.read()


def read_length_prefixed_messages(key, read_mode, **kwargs):
    result = io.BytesIO()

    with smart_open.open(key, read_mode, **kwargs) as fin:
        length_byte = fin.read(1)
        while len(length_byte):
            result.write(length_byte)
            msg = fin.read(ord(length_byte))
            result.write(msg)
            length_byte = fin.read(1)
    return result.getvalue()


def test_obs_readwrite_binary(benchmark):
    initialize_bucket()

    key = _OBS_URL + '/sanity.txt'
    binary = 'с гранатою в кармане, с чекою в руке'.encode()
    actual = benchmark(write_read, key, binary, 'wb', 'rb')
    assert actual == binary


def test_obs_readwrite_binary_gzip(benchmark):
    initialize_bucket()

    key = _OBS_URL + '/sanity.txt.gz'
    binary = 'не чайки здесь запели на знакомом языке'.encode()
    actual = benchmark(write_read, key, binary, 'wb', 'rb')
    assert actual == binary


def test_obs_performance(benchmark):
    initialize_bucket()

    one_megabyte = io.BytesIO()
    for _ in range(1024 * 128):
        one_megabyte.write(b'01234567')
    one_megabyte = one_megabyte.getvalue()

    key = _OBS_URL + '/performance.txt'
    actual = benchmark(write_read, key, one_megabyte, 'wb', 'rb')
    assert actual == one_megabyte


def test_obs_performance_gz(benchmark):
    initialize_bucket()

    one_megabyte = io.BytesIO()
    for _ in range(1024 * 128):
        one_megabyte.write(b'01234567')
    one_megabyte = one_megabyte.getvalue()

    key = _OBS_URL + '/performance.txt.gz'
    actual = benchmark(write_read, key, one_megabyte, 'wb', 'rb')
    assert actual == one_megabyte


def test_obs_performance_small_reads(benchmark):
    initialize_bucket()

    ONE_MIB = 1024 ** 2
    one_megabyte_of_msgs = io.BytesIO()
    msg = b'\x0f' + b'0123456789abcde'  # a length-prefixed "message"
    for _ in range(0, ONE_MIB, len(msg)):
        one_megabyte_of_msgs.write(msg)
    one_megabyte_of_msgs = one_megabyte_of_msgs.getvalue()

    key = _OBS_URL + '/many_reads_performance.bin'

    with smart_open.open(key, 'wb') as fout:
        fout.write(one_megabyte_of_msgs)

    actual = benchmark(read_length_prefixed_messages, key, 'rb', buffering=ONE_MIB)
    assert actual == one_megabyte_of_msgs
