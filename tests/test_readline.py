#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import io
from unittest import mock

import boto3
import pytest
import responses

import smart_open
from tests.test_azure import FakeBlobServiceClient

# See https://github.com/piskvorky/smart_open/issues/800
# This supports moto 4 & 5 until v4 is no longer used by distros.
try:
    from moto import mock_s3
except ImportError:
    from moto import mock_aws as mock_s3

LINE = b"x" * 20_000 + b"\n"
DATA = b"first\n" + LINE + b"next\n"
BUFFER_SIZE = 4096  # smaller than LINE, so reading LINE refills the buffer


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


def check_lines(fin):
    """Read lines after a partial read, with and without a size limit."""
    assert fin.read(1) == b"f"
    assert fin.readline() == b"irst\n"
    assert fin.readline(3) == b"xxx"
    assert fin.readline(BUFFER_SIZE + 1) == b"x" * (BUFFER_SIZE + 1)
    assert fin.readline() == LINE[BUFFER_SIZE + 4 :]
    assert list(fin) == [b"next\n"]


@responses.activate
def test_http(source):
    """Read lines over HTTP in chunks, and keep the position for seeking."""
    responses.add(responses.GET, "http://host/file", body=source)
    with smart_open.open("http://host/file", "rb", transport_params={"buffer_size": BUFFER_SIZE}) as fin:
        check_lines(fin)
        assert fin.tell() == len(DATA)
    assert source.reads < 100  # reading byte by byte takes over 20000


@responses.activate
def test_webhdfs(source):
    """Read lines over WebHDFS in chunks, and refuse once closed."""
    responses.add(responses.GET, "http://host:1/webhdfs/v1/file", body=source)
    with smart_open.open("webhdfs://host:1/file", "rb") as fin:
        check_lines(fin)
    assert source.reads < 100  # reading byte by byte takes over 20000
    with pytest.raises(ValueError, match="closed file"):
        fin.readline()


def test_hdfs(source):
    """Read lines from the ``hdfs dfs -cat`` subprocess in chunks, and refuse once closed."""
    with (
        mock.patch("subprocess.Popen", return_value=mock.Mock(stdout=source)),
        smart_open.open("hdfs://host/file", "rb") as fin,
    ):
        check_lines(fin)
    assert source.reads < 100  # reading byte by byte takes over 20000
    with pytest.raises(ValueError, match="closed file"):
        fin.readline()


@mock_s3
def test_s3():
    """Read lines from S3, and keep the position for seeking."""
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="bucket")
    client.put_object(Bucket="bucket", Key="key", Body=DATA)
    params = {"client": client, "buffer_size": BUFFER_SIZE}
    with smart_open.open("s3://bucket/key", "rb", transport_params=params) as fin:
        check_lines(fin)
        assert fin.tell() == len(DATA)


def test_azure():
    """Read lines from Azure Blob Storage, and keep the position for seeking."""
    client = FakeBlobServiceClient("https://account.blob.core.windows.net")
    client.create_container("container").upload_blob("blob", DATA)
    params = {"client": client, "buffer_size": BUFFER_SIZE}
    with smart_open.open("azure://container/blob", "rb", transport_params=params) as fin:
        check_lines(fin)
        assert fin.tell() == len(DATA)
