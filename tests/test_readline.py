#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import io
from unittest import mock

import pytest
import responses

import smart_open

LINE = b"x" * 20_000 + b"\n"
DATA = b"first\n" + LINE + b"next\n"


class CountingReader(io.BufferedReader):
    """Source stream that counts read calls, so byte-at-a-time line reading shows up."""

    reads = 0

    def read(self, size=-1):
        """Count the call and read."""
        self.reads += 1
        return super().read(size)


@pytest.fixture
def source():
    """Return the source stream behind the transport."""
    return CountingReader(io.BytesIO(DATA))


def check_lines(fin, source):
    """Read lines after a partial read, with and without a size limit, in a few source reads."""
    assert fin.read(1) == b"f"
    assert fin.readline() == b"irst\n"
    assert fin.readline(3) == b"xxx"
    assert fin.readline() == LINE[3:]
    assert list(fin) == [b"next\n"]
    assert source.reads < 10  # reading byte by byte takes over 20000


@responses.activate
def test_http(source):
    """Read lines over HTTP and keep the position for seeking."""
    responses.add(responses.GET, "http://host/file", body=source)
    with smart_open.open("http://host/file", "rb") as fin:
        check_lines(fin, source)
        assert fin.tell() == len(DATA)


@responses.activate
def test_webhdfs(source):
    """Read lines over WebHDFS."""
    responses.add(responses.GET, "http://host:1/webhdfs/v1/file", body=source)
    with smart_open.open("webhdfs://host:1/file", "rb") as fin:
        check_lines(fin, source)


def test_hdfs(source):
    """Read lines from the ``hdfs dfs -cat`` subprocess."""
    with (
        mock.patch("subprocess.Popen", return_value=mock.Mock(stdout=source)),
        smart_open.open("hdfs://host/file", "rb") as fin,
    ):
        check_lines(fin, source)
