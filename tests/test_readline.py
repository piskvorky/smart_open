#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import io
from unittest import mock

import boto3
import responses

import smart_open
from tests.test_azure import FakeBlobServiceClient

# See https://github.com/piskvorky/smart_open/issues/800
# This supports moto 4 & 5 until v4 is no longer used by distros.
try:
    from moto import mock_s3
except ImportError:
    from moto import mock_aws as mock_s3

BUFFER_SIZE = 4096
LINE = b"x" * (io.DEFAULT_BUFFER_SIZE + BUFFER_SIZE) + b"\n"  # longer than any transport's read chunk
DATA = b"first\n" + LINE + b"next"  # no trailing newline, so the last line ends at EOF


def check_lines(fin):
    """Read lines after a partial read, with and without a size limit, in chunks rather than read(1) per byte."""
    with mock.patch.object(fin, "read", wraps=fin.read) as read:
        assert fin.read(1) == b"f"
        assert fin.readline(0) == b""
        assert fin.readline() == b"irst\n"
        assert fin.readline(3) == b"xxx"
        assert fin.readline(len(LINE) - 4) == LINE[3:-1]  # the limit spans a refill
        assert fin.readline() == b"\n"
        assert fin.read(2) == b"ne"
        assert list(fin) == [b"xt"]
    assert read.call_count < 100  # io.IOBase.readline falls back to read(1) per byte


@responses.activate
def test_http():
    """Read lines over HTTP in chunks, and keep the position for seeking."""
    responses.add(responses.GET, "http://host/file", body=DATA)
    with smart_open.open("http://host/file", "rb", transport_params={"buffer_size": BUFFER_SIZE}) as fin:
        check_lines(fin)
        assert fin.tell() == len(DATA)


@responses.activate
def test_webhdfs():
    """Read lines over WebHDFS in chunks."""
    responses.add(responses.GET, "http://host:1/webhdfs/v1/file", body=DATA)
    with smart_open.open("webhdfs://host:1/file", "rb") as fin:
        check_lines(fin)


def test_hdfs():
    """Read lines from the ``hdfs dfs -cat`` subprocess in chunks."""
    stdout = io.BufferedReader(io.BytesIO(DATA))  # Popen(stdout=PIPE) gives a BufferedReader
    with (
        mock.patch("subprocess.Popen", return_value=mock.Mock(stdout=stdout)),
        smart_open.open("hdfs://host/file", "rb") as fin,
    ):
        check_lines(fin)


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
