# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
from __future__ import unicode_literals

import logging
import unittest

import smart_open

GZIP_MAGIC = b'\x1f\x8b'
BASE_URL = ('https://raw.githubusercontent.com/RaRe-Technologies/smart_open/'
            'master/smart_open/tests/test_data/')


class ReadTest(unittest.TestCase):
    def test_read_text(self):
        url = BASE_URL + 'crime-and-punishment.txt'
        with smart_open.smart_open(url, encoding='utf-8') as fin:
            text = fin.read()
        self.assertTrue(text.startswith('В начале июля, в чрезвычайно жаркое время,'))
        self.assertTrue(text.endswith('улизнуть, чтобы никто не видал.\n'))

    def test_read_binary(self):
        url = BASE_URL + 'crime-and-punishment.txt'
        with smart_open.smart_open(url, 'rb') as fin:
            text = fin.read()
        self.assertTrue(text.startswith('В начале июля, в чрезвычайно'.encode('utf-8')))
        self.assertTrue(text.endswith('улизнуть, чтобы никто не видал.\n'.encode('utf-8')))

    def test_read_gzip_text(self):
        url = BASE_URL + 'crime-and-punishment.txt.gz'
        with smart_open.smart_open(url, encoding='utf-8') as fin:
            text = fin.read()
        self.assertTrue(text.startswith('В начале июля, в чрезвычайно жаркое время,'))
        self.assertTrue(text.endswith('улизнуть, чтобы никто не видал.\n'))

    def test_read_gzip_binary(self):
        url = BASE_URL + 'crime-and-punishment.txt.gz'
        with smart_open.smart_open(url, 'rb', ignore_extension=True) as fin:
            binary = fin.read()
        self.assertTrue(binary.startswith(GZIP_MAGIC))


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    unittest.main()
