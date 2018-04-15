#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals

import logging
import subprocess
import unittest
import os.path as P
import time

import six

import smart_open

PORT = 8008
GZIP_MAGIC = b'\x1f\x8b'
CURR_DIR = P.dirname(P.abspath(__file__))
ROOT_DIR = P.dirname(CURR_DIR)
TEST_DATA_DIR = P.join(ROOT_DIR, 'smart_open/tests/test_data')


assert P.isfile(P.join(TEST_DATA_DIR, 'crime-and-punishment.txt'))


def startup_server(port=PORT, cwd=TEST_DATA_DIR):
    command = ['python', '-m', 'SimpleHTTPServer', str(port)]
    sub = subprocess.Popen(command, cwd=cwd)
    return sub


class ReadTest(unittest.TestCase):
    def setUp(self):
        self.sub = startup_server()
        time.sleep(1)

    def tearDown(self):
        self.sub.terminate()
        time.sleep(1)

    def test_right_subdirectory(self):
        url = 'http://localhost:%d' % PORT
        with smart_open.smart_open(url, encoding='utf-8') as fin:
            body = fin.read()
        self.assertTrue('crime-and-punishment.txt' in body)

    def test_read_text(self):
        url = 'http://localhost:%d/crime-and-punishment.txt' % PORT
        with smart_open.smart_open(url, encoding='utf-8') as fin:
            text = fin.read()
        self.assertTrue(text.startswith('В начале июля, в чрезвычайно жаркое время,'))
        self.assertTrue(text.endswith('улизнуть, чтобы никто не видал.\n'))

    def test_read_binary(self):
        url = 'http://localhost:%d/crime-and-punishment.txt' % PORT
        with smart_open.smart_open(url, 'rb') as fin:
            text = fin.read()
        self.assertTrue(text.startswith('В начале июля, в чрезвычайно'.encode('utf-8')))
        self.assertTrue(text.endswith('улизнуть, чтобы никто не видал.\n'.encode('utf-8')))

    def test_read_gzip_text(self):
        url = 'http://localhost:%d/crime-and-punishment.txt.gz' % PORT
        with smart_open.smart_open(url, encoding='utf-8') as fin:
            text = fin.read()
        self.assertTrue(text.startswith('В начале июля, в чрезвычайно жаркое время,'))
        self.assertTrue(text.endswith('улизнуть, чтобы никто не видал.\n'))

    def test_read_gzip_binary(self):
        url = 'http://localhost:%d/crime-and-punishment.txt.gz' % PORT
        with smart_open.smart_open(url, 'rb', ignore_extension=True) as fin:
            binary = fin.read()
        self.assertTrue(binary.startswith(GZIP_MAGIC))


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    unittest.main()
