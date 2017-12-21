# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import io
import os
import subprocess

import smart_open

_S3_URL = os.environ.get('SO_S3_URL')
assert _S3_URL is not None, 'please set the SO_S3_URL environment variable'


def initialize_bucket():
    subprocess.check_call(['aws', 's3', 'rm', '--recursive', _S3_URL])


def write_read(key, content, write_mode, read_mode):
    with smart_open.smart_open(key, write_mode) as fout:
        fout.write(content)
    with smart_open.smart_open(key, read_mode) as fin:
        actual = fin.read()
    return actual


def test_s3_readwrite_text(benchmark):
    initialize_bucket()

    key = _S3_URL + '/sanity.txt'
    text = 'с гранатою в кармане, с чекою в руке'
    actual = benchmark(write_read, key, text, 'w', 'r')
    assert actual == text


def test_s3_readwrite_text_gzip(benchmark):
    initialize_bucket()

    key = _S3_URL + '/sanity.txt.gz'
    text = 'не чайки здесь запели на знакомом языке'
    actual = benchmark(write_read, key, text, 'w', 'r')
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


def test_deliberately_fail_travis_build(benchmark):
    assert False
