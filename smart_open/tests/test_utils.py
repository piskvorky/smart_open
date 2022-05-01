# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import urllib.parse

import pytest

import smart_open.utils


@pytest.mark.parametrize(
    'value,minval,maxval,expected',
    [
        (5, 0, 10, 5),
        (11, 0, 10, 10),
        (-1, 0, 10, 0),
        (10, 0, None, 10),
        (-10, 0, None, 0),
    ]
)
def test_clamp(value, minval, maxval, expected):
    assert smart_open.utils.clamp(value, minval=minval, maxval=maxval) == expected


@pytest.mark.parametrize(
    'value,params,expected',
    [
        (10, {}, 10),
        (-10, {}, 0),
        (-10, {'minval': -5}, -5),
        (10, {'maxval': 5}, 5),
    ]
)
def test_clamp_defaults(value, params, expected):
    assert smart_open.utils.clamp(value, **params) == expected


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


def test_find_entry_points():
    # Installed through setup.py tests requirements
    eps = smart_open.utils.find_entry_points("pytest11")
    eps_names = {ep.name for ep in eps}
    assert "rerunfailures" in eps_names

    # Part of setuptools
    eps = smart_open.utils.find_entry_points("distutils.commands")
    assert len(eps) > 0
