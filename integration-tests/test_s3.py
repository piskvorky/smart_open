# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

from __future__ import unicode_literals
import io
import os
import subprocess

import smart_open

_S3_URL = os.environ.get('SO_S3_URL')
assert _S3_URL is not None, 'please set the SO_S3_URL environment variable'


def initialize_bucket():
    subprocess.check_call(['aws', 's3', 'rm', '--recursive', _S3_URL])


def write_read(key, content, write_mode, read_mode, encoding=None, s3_upload=None, **kwargs):
    with smart_open.smart_open(
            key, write_mode, encoding=encoding,
            s3_upload=s3_upload, **kwargs) as fout:
        fout.write(content)
    with smart_open.smart_open(key, read_mode, encoding=encoding, **kwargs) as fin:
        actual = fin.read()
    return actual


def read_length_prefixed_messages(key, read_mode, encoding=None, **kwargs):
    with smart_open.smart_open(key, read_mode, encoding=encoding, **kwargs) as fin:
        actual = b''
        length_byte = fin.read(1)
        while len(length_byte):
            actual += length_byte
            msg = fin.read(ord(length_byte))
            actual += msg
            length_byte = fin.read(1)
    return actual


def test_s3_readwrite_text(benchmark):
    initialize_bucket()

    key = _S3_URL + '/sanity.txt'
    text = 'с гранатою в кармане, с чекою в руке'
    actual = benchmark(write_read, key, text, 'w', 'r', 'utf-8')
    assert actual == text


def test_s3_readwrite_text_gzip(benchmark):
    initialize_bucket()

    key = _S3_URL + '/sanity.txt.gz'
    text = 'не чайки здесь запели на знакомом языке'
    actual = benchmark(write_read, key, text, 'w', 'r', 'utf-8')
    assert actual == text


def test_s3_readwrite_binary(benchmark):
    initialize_bucket()

    key = _S3_URL + '/sanity.txt'
    binary = b'this is a test'
    actual = benchmark(write_read, key, binary, 'wb', 'rb')
    assert actual == binary


def test_s3_readwrite_binary_gzip(benchmark):
    initialize_bucket()

    key = _S3_URL + '/sanity.txt.gz'
    binary = b'this is a test'
    actual = benchmark(write_read, key, binary, 'wb', 'rb')
    assert actual == binary


def test_s3_performance(benchmark):
    initialize_bucket()

    one_megabyte = io.BytesIO()
    for _ in range(1024*128):
        one_megabyte.write(b'01234567')
    one_megabyte = one_megabyte.getvalue()

    key = _S3_URL + '/performance.txt'
    actual = benchmark(write_read, key, one_megabyte, 'wb', 'rb')
    assert actual == one_megabyte


def test_s3_performance_gz(benchmark):
    initialize_bucket()

    one_megabyte = io.BytesIO()
    for _ in range(1024*128):
        one_megabyte.write(b'01234567')
    one_megabyte = one_megabyte.getvalue()

    key = _S3_URL + '/performance.txt.gz'
    actual = benchmark(write_read, key, one_megabyte, 'wb', 'rb')
    assert actual == one_megabyte


def test_s3_performance_small_reads(benchmark):
    initialize_bucket()

    ONE_MIB = 1024**2
    one_megabyte_of_msgs = io.BytesIO()
    msg = b'\x0f' + b'0123456789abcde'  # a length-prefixed "message"
    for _ in range(0, ONE_MIB, len(msg)):
        one_megabyte_of_msgs.write(msg)
    one_megabyte_of_msgs = one_megabyte_of_msgs.getvalue()

    key = _S3_URL + '/many_reads_performance.bin'

    with smart_open.smart_open(key, 'wb') as fout:
        fout.write(one_megabyte_of_msgs)

    actual = benchmark(read_length_prefixed_messages, key, 'rb', buffer_size=ONE_MIB)
    assert actual == one_megabyte_of_msgs


def test_s3_encrypted_file(benchmark):
    initialize_bucket()

    key = _S3_URL + '/sanity.txt'
    text = 'с гранатою в кармане, с чекою в руке'
    actual = benchmark(write_read, key, text, 'w', 'r', 'utf-8', s3_upload={
        'ServerSideEncryption': 'AES256'
    })
    assert actual == text
