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
import unittest
import sys

import mock

import smart_open.hdfs

#
# Workaround for https://bugs.python.org/issue37380
#
if sys.version_info[:2] == (3, 6):
    subprocess._cleanup = lambda: None

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

        #
        # We have to specify the encoding explicitly, because different
        # platforms like Windows may be using something other than unicode
        # by default.
        #
        with open(self.path, encoding='utf-8') as fin:
            self.expected = fin.read()
        self.cat = cat(self.path)

    def test_read(self):
        with mock.patch('subprocess.Popen', return_value=self.cat):
            reader = smart_open.hdfs.CliRawInputBase('hdfs://dummy/url')
            as_bytes = reader.read()

        #
        # Not 100% sure why this is necessary on Windows platforms, but the
        # tests fail without it.  It may be a bug, but I don't have time to
        # investigate right now.
        #
        as_text = as_bytes.decode('utf-8').replace(os.linesep, '\n')
        assert as_text == self.expected

    def test_read_75(self):
        with mock.patch('subprocess.Popen', return_value=self.cat):
            reader = smart_open.hdfs.CliRawInputBase('hdfs://dummy/url')
            as_bytes = reader.read(75)

        as_text = as_bytes.decode('utf-8').replace(os.linesep, '\n')
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


class SanityTest(unittest.TestCase):
    def test_read_bytes(self):
        path = P.join(CURR_DIR, 'test_data', 'crime-and-punishment.txt')
        with open(path, 'rb') as fin:
            lines = [line for line in fin]
        assert len(lines) == 3

    def test_read_text(self):
        path = P.join(CURR_DIR, 'test_data', 'crime-and-punishment.txt')
        with open(path, 'r', encoding='utf-8') as fin:
            text = fin.read()

        expected = 'В начале июля, в чрезвычайно жаркое время'
        assert text[:len(expected)] == expected


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
