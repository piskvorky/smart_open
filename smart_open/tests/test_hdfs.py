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

from sys import platform

CURR_DIR = P.dirname(P.abspath(__file__))


#
# We want our mocks to emulate the real implementation as close as possible,
# so we use a Popen call during each test.  If we mocked using io.BytesIO, then
# it is possible the mocks would behave differently to what we expect in real
# use.
#
# Since these tests use cat, they will not work in an environment without cat,
# such as Windows.
#
class CliRawInputBaseTest(unittest.TestCase):
    def _setUp(self, test_file):
        if platform == "linux" or platform == "linux2":
            path = P.join(CURR_DIR, 'test_data/'+test_file)
            self.cat = subprocess.Popen(['cat', path], stdout=subprocess.PIPE)
        elif platform == "win32":
            path = P.join(CURR_DIR, 'test_data\\'+test_file)
            self.cat = subprocess.Popen(['type', path], stdout=subprocess.PIPE, shell=True)

    def test_read(self):
        self._setUp(test_file = 'crime-and-punishment.txt')

        with mock.patch('subprocess.Popen', return_value=self.cat):
            reader = smart_open.hdfs.CliRawInputBase('hdfs://dummy/url')
            as_bytes = reader.read()

        as_text = as_bytes.decode('utf-8')
        self.assertTrue(as_text.startswith('В начале июля, в чрезвычайно жаркое время'))
        self.assertTrue(as_text.endswith('улизнуть, чтобы никто не видал.'))

    def test_read_100(self):
        self._setUp(test_file='crime-and-punishment.txt')

        with mock.patch('subprocess.Popen', return_value=self.cat):
            reader = smart_open.hdfs.CliRawInputBase('hdfs://dummy/url')
            as_bytes = reader.read(75)

        as_text = as_bytes.decode('utf-8')
        expected = 'В начале июля, в чрезвычайно жаркое время'
        self.assertEqual(expected, as_text)

    def test_unzip(self):
        self._setUp(test_file='crime-and-punishment.txt.gz')

        with mock.patch('subprocess.Popen', return_value=self.cat):
            with gzip.GzipFile(fileobj=smart_open.hdfs.CliRawInputBase('hdfs://dummy/url')) as fin:
                as_bytes = fin.read()

        as_text = as_bytes.decode('utf-8')
        self.assertTrue(as_text.startswith('В начале июля, в чрезвычайно жаркое время'))
        self.assertTrue(as_text.endswith('улизнуть, чтобы никто не видал.'))

    def test_context_manager(self):
        self._setUp(test_file='crime-and-punishment.txt')

        with mock.patch('subprocess.Popen', return_value=self.cat):
            with smart_open.hdfs.CliRawInputBase('hdfs://dummy/url') as fin:
                as_bytes = fin.read()

        as_text = as_bytes.decode('utf-8')
        self.assertTrue(as_text.startswith('В начале июля, в чрезвычайно жаркое время'))
        self.assertTrue(as_text.endswith('улизнуть, чтобы никто не видал.'))

@unittest.skipIf(platform == 'win32',reason="does not run on windows")
class CliRawOutputBaseTest(unittest.TestCase):
    def test_write(self):
        cat = subprocess.Popen(['cat'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
        as_text = 'мы в ответе за тех, кого приручили'

        with mock.patch('subprocess.Popen', return_value=self.cat):
            with smart_open.hdfs.CliRawOutputBase('hdfs://dummy/url') as fout:
                fout.write(as_text.encode('utf-8'))

        actual = cat.stdout.read().decode('utf-8')
        self.assertEqual(as_text, actual)

    def test_zip(self):
        cat = subprocess.Popen(['cat'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        as_text = 'мы в ответе за тех, кого приручили'

        with mock.patch('subprocess.Popen', return_value=cat):
            with smart_open.hdfs.CliRawOutputBase('hdfs://dummy/url') as fout:
                with gzip.GzipFile(fileobj=fout, mode='wb') as gz_fout:
                    gz_fout.write(as_text.encode('utf-8'))

        with gzip.GzipFile(fileobj=cat.stdout) as fin:
            actual = fin.read().decode('utf-8')
        self.assertEqual(as_text, actual)
