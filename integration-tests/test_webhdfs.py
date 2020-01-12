# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""
Sample code for WebHDFS integration tests.
To run it working WebHDFS in your network is needed - simply
set _SO_WEBHDFS_BASE_URL env variable to webhdfs url you have
write access to.

For example on Amazon EMR WebHDFS is accessible on driver port 14000, so
it may look like:

$ export SO_WEBHDFS_BASE_URL=webhdfs://hadoop@your-emr-driver:14000/tmp/
$ py.test integration-tests/test_webhdfs.py
"""
import json
import os
import smart_open
from smart_open.webhdfs import WebHdfsException
import pytest

_SO_WEBHDFS_BASE_URL = os.environ.get("SO_WEBHDFS_BASE_URL")
assert (
    _SO_WEBHDFS_BASE_URL is not None
), "please set the SO_WEBHDFS_BASE_URL environment variable"


def make_url(path):
    return "{base_url}/{path}".format(
        base_url=_SO_WEBHDFS_BASE_URL.rstrip("/"), path=path.lstrip("/")
    )


def test_write_and_read():
    with smart_open.open(make_url("test2.txt"), "w") as f:
        f.write("write_test\n")
    with smart_open.open(make_url("test2.txt"), "r") as f:
        assert f.read() == "write_test\n"


def test_binary_write_and_read():
    with smart_open.open(make_url("test3.txt"), "wb") as f:
        f.write(b"binary_write_test\n")
    with smart_open.open(make_url("test3.txt"), "rb") as f:
        assert f.read() == b"binary_write_test\n"


def test_not_found():
    with pytest.raises(WebHdfsException) as exc_info:
        with smart_open.open(make_url("not_existing"), "r") as f:
            assert f.read()
    assert exc_info.value.status_code == 404


def test_quoted_path():
    with smart_open.open(make_url("test_%40_4.txt"), "w") as f:
        f.write("write_test\n")

    with smart_open.open(make_url("?op=LISTSTATUS"), "r") as f:
        data = json.load(f)
        filenames = [
            entry["pathSuffix"] for entry in data["FileStatuses"]["FileStatus"]
        ]
        assert "test_@_4.txt" in filenames
