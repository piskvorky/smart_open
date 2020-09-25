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
import random
import subprocess
import string

import boto3
import smart_open

_BUCKET = os.environ.get('SO_BUCKET')
assert _BUCKET is not None, 'please set the SO_BUCKET environment variable'

_KEY = os.environ.get('SO_KEY')
assert _KEY is not None, 'please set the SO_KEY environment variable'


#
# https://stackoverflow.com/questions/13484726/safe-enough-8-character-short-unique-random-string
#
def _random_string(length=8):
    alphabet = string.ascii_lowercase + string.digits
    return ''.join(random.choices(alphabet, k=length))


@contextlib.contextmanager
def temporary():
    """Yields a URL than can be used for temporary writing.

    Removes all content under the URL when exiting.
    """
    key = '%s/%s' % (_KEY, _random_string())
    yield 's3://%s/%s' % (_BUCKET, key)
    boto3.resource('s3').Bucket(_BUCKET).objects.filter(Prefix=key).delete()


def _test_case(function):
    def inner(benchmark):
        with temporary() as uri:
            return function(benchmark, uri)
    return inner


def write_read(uri, content, write_mode, read_mode, encoding=None, s3_upload=None, **kwargs):
    write_params = dict(kwargs)
    write_params.update(s3_upload=s3_upload)
    with smart_open.open(uri, write_mode, encoding=encoding, transport_params=write_params) as fout:
        fout.write(content)
    with smart_open.open(uri, read_mode, encoding=encoding, transport_params=kwargs) as fin:
        actual = fin.read()
    return actual


def read_length_prefixed_messages(uri, read_mode, encoding=None, **kwargs):
    with smart_open.open(uri, read_mode, encoding=encoding, transport_params=kwargs) as fin:
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

    with smart_open.open(uri, 'wb') as fout:
        fout.write(one_megabyte_of_msgs)

    actual = benchmark(read_length_prefixed_messages, uri, 'rb', buffer_size=one_mib)
    assert actual == one_megabyte_of_msgs


@_test_case
def test_s3_encrypted_file(benchmark, uri):
    text = 'с гранатою в кармане, с чекою в руке'
    s3_upload = {'ServerSideEncryption': 'AES256'}
    actual = benchmark(write_read, uri, text, 'w', 'r', 'utf-8', s3_upload=s3_upload)
    assert actual == text
