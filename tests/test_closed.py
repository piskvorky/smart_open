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

CLOSED = r"^I/O operation on closed file\.$"  # what io.IOBase raises
DATA = b"line\n"
READ = [
    lambda f: f.read(),
    lambda f: f.read(1),
    lambda f: f.read1(1),
    lambda f: f.readinto(bytearray(1)),
    lambda f: f.readline(),
    list,
]
SEEK = [lambda f: f.seek(0), lambda f: f.tell()]
TELL = [lambda f: f.tell()]
WRITE = [lambda f: f.write(b"x"), lambda f: f.flush()]


def check_closed(f, calls):
    """Close twice, then expect every call to raise the ValueError of a closed io.IOBase."""
    f.close()
    f.close()  # closing twice is a no-op
    assert f.closed
    for call in calls:
        with pytest.raises(ValueError, match=CLOSED):
            call(f)


@pytest.fixture
def s3_client():
    """Return a moto S3 client with a bucket holding DATA under key."""
    with mock_s3():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        client.put_object(Bucket="bucket", Key="key", Body=DATA)
        yield client


@pytest.fixture
def azure_client():
    """Return a fake Azure client with a container holding DATA under blob."""
    client = FakeBlobServiceClient("https://account.blob.core.windows.net")
    client.create_container("container").upload_blob("blob", DATA)
    return client


@responses.activate
def test_http_reader():
    """Closed HTTP reader."""
    responses.add(responses.GET, "http://host/file", body=DATA)
    check_closed(smart_open.open("http://host/file", "rb"), READ + SEEK)


@responses.activate
def test_webhdfs_reader():
    """Closed WebHDFS reader."""
    responses.add(responses.GET, "http://host:1/webhdfs/v1/file", body=DATA)
    check_closed(smart_open.open("webhdfs://host:1/file", "rb"), READ)


@responses.activate
def test_webhdfs_writer():
    """Closed WebHDFS writer, which uploads its last part only once."""
    redirect = {"location": "http://node/file"}
    for method, status in ((responses.PUT, 201), (responses.POST, 200)):
        responses.add(method, "http://host:1/webhdfs/v1/file", status=307, headers=redirect)
        responses.add(method, "http://node/file", status=status)
    fout = smart_open.open("webhdfs://host:1/file", "wb")
    fout.write(DATA)
    check_closed(fout, WRITE)
    assert len(responses.calls) == 4  # create the file, then append DATA once


def test_hdfs_reader():
    """Closed HDFS reader."""
    with mock.patch("subprocess.Popen", return_value=mock.Mock(stdout=io.BufferedReader(io.BytesIO(DATA)))):
        check_closed(smart_open.open("hdfs://host/file", "rb"), READ)


def test_hdfs_writer():
    """Closed HDFS writer."""
    with mock.patch(
        "subprocess.Popen", return_value=mock.Mock(stdin=io.BytesIO(), wait=mock.Mock(return_value=0))
    ):
        check_closed(smart_open.open("hdfs://host/file", "wb"), WRITE)


def test_s3_reader(s3_client):
    """Closed S3 reader."""
    params = {"client": s3_client}
    check_closed(smart_open.open("s3://bucket/key", "rb", transport_params=params), READ + SEEK)


def test_s3_multipart_writer(s3_client):
    """Closed S3 multipart writer."""
    params = {"client": s3_client}
    check_closed(smart_open.open("s3://bucket/key", "wb", transport_params=params), WRITE + TELL)


def test_s3_singlepart_writer(s3_client):
    """Closed S3 single part writer."""
    params = {"client": s3_client, "multipart_upload": False}
    check_closed(smart_open.open("s3://bucket/key", "wb", transport_params=params), WRITE + SEEK)


def test_azure_reader(azure_client):
    """Closed Azure reader."""
    params = {"client": azure_client}
    check_closed(smart_open.open("azure://container/blob", "rb", transport_params=params), READ + SEEK)


def test_azure_writer(azure_client):
    """Closed Azure writer."""
    params = {"client": azure_client}
    check_closed(smart_open.open("azure://container/new", "wb", transport_params=params), WRITE + TELL)


def test_azure_append_writer(azure_client):
    """Closed Azure append writer."""
    params = {"client": azure_client}
    check_closed(smart_open.open("azure://container/new", "ab", transport_params=params), WRITE + TELL)
