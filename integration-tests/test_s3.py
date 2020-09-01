# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

from __future__ import unicode_literals
import contextlib
import io
import os
import subprocess
import uuid

import smart_open

_S3_BUCKET_NAME = os.environ.get('SO_BUCKET_NAME')
assert _S3_BUCKET_NAME is not None, 'please set the SO_BUCKET_NAME environment variable'

_SO_KEY = os.environ.get('SO_KEY')
assert _SO_KEY is not None, 'please set the SO_KEY environment variable'


@contextlib.contextmanager
def temporary():
    """Yields a URL than can be used for temporary writing.

    Removes all content under the URL when exiting.
    """
    key = '%s/%s' % (_SO_KEY, uuid.uuid4().hex)
    uri = 's3://%s/%s' % (_S3_BUCKET_NAME, key)
    yield uri
    subprocess.check_call(['aws', 's3', 'rm', '--recursive', uri])


def _test_case(function):
    def inner(benchmark):
        with temporary() as uri:
            return function(benchmark, uri)
    return inner


def write_read(uri, content, write_mode, read_mode, encoding=None, s3_upload=None, **kwargs):
    with smart_open.smart_open(
            uri, write_mode, encoding=encoding,
            s3_upload=s3_upload, **kwargs) as fout:
        fout.write(content)
    with smart_open.smart_open(uri, read_mode, encoding=encoding, **kwargs) as fin:
        actual = fin.read()
    return actual


def read_length_prefixed_messages(uri, read_mode, encoding=None, **kwargs):
    with smart_open.smart_open(uri, read_mode, encoding=encoding, **kwargs) as fin:
        actual = b''
        length_byte = fin.read(1)
        while len(length_byte):
            actual += length_byte
            msg = fin.read(ord(length_byte))
            actual += msg
            length_byte = fin.read(1)
    return actual


@_test_case
def test_s3_readwrite_text(benchmark, uri):
    text = 'с гранатою в кармане, с чекою в руке'
    actual = benchmark(write_read, uri, text, 'w', 'r', 'utf-8')

    assert actual == text


@_test_case
def test_s3_readwrite_text_gzip(benchmark, uri):
    text = 'не чайки здесь запели на знакомом языке'
    actual = benchmark(write_read, uri, text, 'w', 'r', 'utf-8')
    assert actual == text


@_test_case
def test_s3_readwrite_binary(benchmark, uri):
    binary = b'this is a test'
    actual = benchmark(write_read, uri, binary, 'wb', 'rb')

    assert actual == binary


@_test_case
def test_s3_readwrite_binary_gzip(benchmark, uri):
    binary = b'this is a test'
    actual = benchmark(write_read, uri, binary, 'wb', 'rb')

    assert actual == binary


@_test_case
def test_s3_performance(benchmark, uri):
    one_megabyte = io.BytesIO()
    for _ in range(1024*128):
        one_megabyte.write(b'01234567')
    one_megabyte = one_megabyte.getvalue()

    actual = benchmark(write_read, uri, one_megabyte, 'wb', 'rb')

    assert actual == one_megabyte


@_test_case
def test_s3_performance_gz(benchmark, uri):
    one_megabyte = io.BytesIO()
    for _ in range(1024*128):
        one_megabyte.write(b'01234567')
    one_megabyte = one_megabyte.getvalue()

    actual = benchmark(write_read, uri, one_megabyte, 'wb', 'rb')

    assert actual == one_megabyte


@_test_case
def test_s3_performance_small_reads(benchmark, uri):
    one_mib = 1024**2
    one_megabyte_of_msgs = io.BytesIO()
    msg = b'\x0f' + b'0123456789abcde'  # a length-prefixed "message"
    for _ in range(0, one_mib, len(msg)):
        one_megabyte_of_msgs.write(msg)
    one_megabyte_of_msgs = one_megabyte_of_msgs.getvalue()

    with smart_open.smart_open(uri, 'wb') as fout:
        fout.write(one_megabyte_of_msgs)

    actual = benchmark(read_length_prefixed_messages, uri, 'rb', buffer_size=one_mib)

    assert actual == one_megabyte_of_msgs


@_test_case
def test_s3_encrypted_file(benchmark, uri):
    text = 'с гранатою в кармане, с чекою в руке'
    actual = benchmark(write_read, uri, text, 'w', 'r', 'utf-8', s3_upload={
        'ServerSideEncryption': 'AES256'
    })

    assert actual == text
