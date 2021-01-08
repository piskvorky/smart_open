# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
from __future__ import print_function
from __future__ import unicode_literals

import gzip
import os.path as P
import subprocess
import unittest

import mock

import smart_open.hdfs

import sys

CURR_DIR = P.dirname(P.abspath(__file__))


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


class CliRawInputBaseTest(unittest.TestCase):
    def setUp(self):
        self.path = P.join(CURR_DIR, 'test_data', 'crime-and-punishment.txt')
        with open(self.path) as fin:
            self.expected = fin.read()
        self.cat = cat(self.path)

    def test_read(self):
        with mock.patch('subprocess.Popen', return_value=self.cat):
            reader = smart_open.hdfs.CliRawInputBase('hdfs://dummy/url')
            as_bytes = reader.read()

        as_text = as_bytes.decode('utf-8')
        assert as_text == self.expected

    def test_read_75(self):
        with mock.patch('subprocess.Popen', return_value=self.cat):
            reader = smart_open.hdfs.CliRawInputBase('hdfs://dummy/url')
            as_bytes = reader.read(75)

        as_text = as_bytes.decode('utf-8')
        assert as_text == self.expected[:len(as_text)]

    def test_unzip(self):
        path = P.join(CURR_DIR, 'test_data', 'crime-and-punishment.txt.gz')

        with mock.patch('subprocess.Popen', return_value=cat(path)):
            with gzip.GzipFile(fileobj=smart_open.hdfs.CliRawInputBase('hdfs://dummy/url')) as fin:
                as_bytes = fin.read()

        as_text = as_bytes.decode('utf-8')
        assert as_text == self.expected

    def test_context_manager(self):
        with mock.patch('subprocess.Popen', return_value=self.cat):
            with smart_open.hdfs.CliRawInputBase('hdfs://dummy/url') as fin:
                as_bytes = fin.read()

        as_text = as_bytes.decode('utf-8').replace('\r\n', '\n')
        assert as_text == self.expected


class CliRawOutputBaseTest(unittest.TestCase):
    def test_write(self):
        expected = 'мы в ответе за тех, кого приручили'
        mocked_cat = cat()

        with mock.patch('subprocess.Popen', return_value=mocked_cat):
            with smart_open.hdfs.CliRawOutputBase('hdfs://dummy/url') as fout:
                fout.write(expected.encode('utf-8'))

        actual = mocked_cat.stdout.read().decode('utf-8')
        assert actual == expected

    def test_zip(self):
        expected = 'мы в ответе за тех, кого приручили'
        mocked_cat = cat()

        with mock.patch('subprocess.Popen', return_value=mocked_cat):
            with smart_open.hdfs.CliRawOutputBase('hdfs://dummy/url') as fout:
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
