# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import unittest
import urllib.parse

import pytest

import smart_open.utils


class ClampTest(unittest.TestCase):
    def test_low(self):
        self.assertEqual(smart_open.utils.clamp(5, 0, 10), 5)

    def test_high(self):
        self.assertEqual(smart_open.utils.clamp(11, 0, 10), 10)

    def test_out_of_range(self):
        self.assertEqual(smart_open.utils.clamp(-1, 0, 10), 0)


def test_check_kwargs():
    import smart_open.s3
    kallable = smart_open.s3.open
    kwargs = {'client': 'foo', 'unsupported': 'bar', 'client_kwargs': 'boaz'}
    supported = smart_open.utils.check_kwargs(kallable, kwargs)
    assert supported == {'client': 'foo', 'client_kwargs': 'boaz'}


@pytest.mark.parametrize(
    'url,expected',
    [
        ('s3://bucket/key', ('s3', 'bucket', '/key', '', '')),
        ('s3://bucket/key?', ('s3', 'bucket', '/key?', '', '')),
        ('s3://bucket/???', ('s3', 'bucket', '/???', '', '')),
        ('https://host/path?foo=bar', ('https', 'host', '/path', 'foo=bar', '')),
    ]
)
def test_safe_urlsplit(url, expected):
    actual = smart_open.utils.safe_urlsplit(url)
    assert actual == urllib.parse.SplitResult(*expected)
