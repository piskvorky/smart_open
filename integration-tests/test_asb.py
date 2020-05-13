# -*- coding: utf-8 -*-
import io
import os

import azure.storage.blob

import smart_open

_ASB_CONTAINER = os.environ.get('SO_ASB_CONTAINER')
assert _ASB_CONTAINER is not None, 'please set the SO_ASB_URL environment variable'


def initialize_bucket():
    blob_service_client = azure.storage.blob.BlobServiceClient()
    container_client = blob_service_client.get_container_client(_ASB_CONTAINER)
    container_client.create_container()
    blobs = container_client.list_blobs()
    for blob in blobs:
        blob.delete()


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


def test_asb_readwrite_text(benchmark):
    initialize_bucket()

    key = _ASB_CONTAINER + '/sanity.txt'
    text = 'с гранатою в кармане, с чекою в руке'
    actual = benchmark(write_read, key, text, 'w', 'r', encoding='utf-8')
    assert actual == text


def test_asb_readwrite_text_gzip(benchmark):
    initialize_bucket()

    key = _ASB_CONTAINER + '/sanity.txt.gz'
    text = 'не чайки здесь запели на знакомом языке'
    actual = benchmark(write_read, key, text, 'w', 'r', encoding='utf-8')
    assert actual == text


def test_asb_readwrite_binary(benchmark):
    initialize_bucket()

    key = _ASB_CONTAINER + '/sanity.txt'
    binary = b'this is a test'
    actual = benchmark(write_read, key, binary, 'wb', 'rb')
    assert actual == binary


def test_asb_readwrite_binary_gzip(benchmark):
    initialize_bucket()

    key = _ASB_CONTAINER + '/sanity.txt.gz'
    binary = b'this is a test'
    actual = benchmark(write_read, key, binary, 'wb', 'rb')
    assert actual == binary


def test_asb_performance(benchmark):
    initialize_bucket()

    one_megabyte = io.BytesIO()
    for _ in range(1024*128):
        one_megabyte.write(b'01234567')
    one_megabyte = one_megabyte.getvalue()

    key = _ASB_CONTAINER + '/performance.txt'
    actual = benchmark(write_read, key, one_megabyte, 'wb', 'rb')
    assert actual == one_megabyte


def test_asb_performance_gz(benchmark):
    initialize_bucket()

    one_megabyte = io.BytesIO()
    for _ in range(1024*128):
        one_megabyte.write(b'01234567')
    one_megabyte = one_megabyte.getvalue()

    key = _ASB_CONTAINER + '/performance.txt.gz'
    actual = benchmark(write_read, key, one_megabyte, 'wb', 'rb')
    assert actual == one_megabyte


def test_asb_performance_small_reads(benchmark):
    initialize_bucket()

    ONE_MIB = 1024**2
    one_megabyte_of_msgs = io.BytesIO()
    msg = b'\x0f' + b'0123456789abcde'  # a length-prefixed "message"
    for _ in range(0, ONE_MIB, len(msg)):
        one_megabyte_of_msgs.write(msg)
    one_megabyte_of_msgs = one_megabyte_of_msgs.getvalue()

    key = _ASB_CONTAINER + '/many_reads_performance.bin'

    with smart_open.open(key, 'wb') as fout:
        fout.write(one_megabyte_of_msgs)

    actual = benchmark(read_length_prefixed_messages, key, 'rb', buffering=ONE_MIB)
    assert actual == one_megabyte_of_msgs
