# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import gzip
import os
import os.path as P
import subprocess
from unittest import mock
import sys

import pytest

import smart_open.hdfs

CURR_DIR = P.dirname(P.abspath(__file__))

if sys.platform.startswith("win"):
    pytest.skip("these tests don't work under Windows", allow_module_level=True)


#
# We want our mocks to emulate the real implementation as close as possible,
# so we use a Popen call during each test.  If we mocked using io.BytesIO, then
# it is possible the mocks would behave differently to what we expect in real
# use.
#
# Since these tests use cat, they will not work in an environment without cat,
# such as Windows.  The main line of this test submodule contains a simple
# cat implementation.  We need this because Windows' analog, type, does
# weird stuff with line endings (inserts CRLF).  Also, I don't know of a way
# to get type to echo standard input.
#
def cat(path=None):
    command = [sys.executable, P.abspath(__file__)]
    if path:
        command.append(path)
    return subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)


CAP_PATH = P.join(CURR_DIR, 'test_data', 'crime-and-punishment.txt')
with open(CAP_PATH, encoding='utf-8') as fin:
    CRIME_AND_PUNISHMENT = fin.read()


def test_sanity_read_bytes():
    with open(CAP_PATH, 'rb') as fin:
        lines = [line for line in fin]
    assert len(lines) == 3


def test_sanity_read_text():
    with open(CAP_PATH, 'r', encoding='utf-8') as fin:
        text = fin.read()

    expected = 'В начале июля, в чрезвычайно жаркое время'
    assert text[:len(expected)] == expected


@pytest.mark.parametrize('schema', [('hdfs', ), ('viewfs', )])
def test_read(schema):
    with mock.patch('subprocess.Popen', return_value=cat(CAP_PATH)):
        reader = smart_open.hdfs.CliRawInputBase(f'{schema}://dummy/url')
        as_bytes = reader.read()

    #
    # Not 100% sure why this is necessary on Windows platforms, but the
    # tests fail without it.  It may be a bug, but I don't have time to
    # investigate right now.
    #
    as_text = as_bytes.decode('utf-8').replace(os.linesep, '\n')
    assert as_text == CRIME_AND_PUNISHMENT


@pytest.mark.parametrize('schema', [('hdfs', ), ('viewfs', )])
def test_read_75(schema):
    with mock.patch('subprocess.Popen', return_value=cat(CAP_PATH)):
        reader = smart_open.hdfs.CliRawInputBase(f'{schema}://dummy/url')
        as_bytes = reader.read(75)

    as_text = as_bytes.decode('utf-8').replace(os.linesep, '\n')
    assert as_text == CRIME_AND_PUNISHMENT[:len(as_text)]


@pytest.mark.parametrize('schema', [('hdfs', ), ('viewfs', )])
def test_unzip(schema):
    with mock.patch('subprocess.Popen', return_value=cat(CAP_PATH + '.gz')):
        with gzip.GzipFile(fileobj=smart_open.hdfs.CliRawInputBase(f'{schema}://dummy/url')) as fin:
            as_bytes = fin.read()

    as_text = as_bytes.decode('utf-8')
    assert as_text == CRIME_AND_PUNISHMENT


@pytest.mark.parametrize('schema', [('hdfs', ), ('viewfs', )])
def test_context_manager(schema):
    with mock.patch('subprocess.Popen', return_value=cat(CAP_PATH)):
        with smart_open.hdfs.CliRawInputBase(f'{schema}://dummy/url') as fin:
            as_bytes = fin.read()

    as_text = as_bytes.decode('utf-8').replace('\r\n', '\n')
    assert as_text == CRIME_AND_PUNISHMENT


@pytest.mark.parametrize('schema', [('hdfs', ), ('viewfs', )])
def test_write(schema):
    expected = 'мы в ответе за тех, кого приручили'
    mocked_cat = cat()

    with mock.patch('subprocess.Popen', return_value=mocked_cat):
        with smart_open.hdfs.CliRawOutputBase(f'{schema}://dummy/url') as fout:
            fout.write(expected.encode('utf-8'))

    actual = mocked_cat.stdout.read().decode('utf-8')
    assert actual == expected


@pytest.mark.parametrize('schema', [('hdfs', ), ('viewfs', )])
def test_write_zip(schema):
    expected = 'мы в ответе за тех, кого приручили'
    mocked_cat = cat()

    with mock.patch('subprocess.Popen', return_value=mocked_cat):
        with smart_open.hdfs.CliRawOutputBase(f'{schema}://dummy/url') as fout:
            with gzip.GzipFile(fileobj=fout, mode='wb') as gz_fout:
                gz_fout.write(expected.encode('utf-8'))

    with gzip.GzipFile(fileobj=mocked_cat.stdout) as fin:
        actual = fin.read().decode('utf-8')

    assert actual == expected


def main():
    try:
        path = sys.argv[1]
    except IndexError:
        bytez = sys.stdin.buffer.read()
    else:
        with open(path, 'rb') as fin:
            bytez = fin.read()

    sys.stdout.buffer.write(bytez)
    sys.stdout.flush()


if __name__ == '__main__':
    main()
