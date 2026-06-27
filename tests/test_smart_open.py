#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

import bz2
import contextlib
import csv
import functools
import gzip
import hashlib
import io
import logging
import os
import tempfile
import unittest
from unittest import mock

import boto3
import pytest
import responses

from smart_open.compression import INFER_FROM_EXTENSION, NO_COMPRESSION

# See https://github.com/piskvorky/smart_open/issues/800
# This supports moto 4 & 5 until v4 is no longer used by distros.
try:
    from moto import mock_s3
except ImportError:
    from moto import mock_aws as mock_s3

import smart_open
from smart_open import smart_open_lib, webhdfs
from smart_open.smart_open_lib import _patch_pathlib, patch_pathlib

from .test_s3 import patch_invalid_range_response

logger = logging.getLogger(__name__)

CURR_DIR = os.path.abspath(os.path.dirname(__file__))  # noqa: PTH100, PTH120  # test fixture abspath; test fixture dirname
SAMPLE_TEXT = "Hello, world!"
SAMPLE_BYTES = SAMPLE_TEXT.encode("utf-8")

_resource = functools.partial(boto3.resource, region_name="us-east-1")


#
# For Windows platforms, under which tempfile.NamedTemporaryFile has some
# unwanted quirks.
#
# https://docs.python.org/3.14/library/tempfile.html#tempfile.NamedTemporaryFile
# https://stackoverflow.com/a/58955530
#
@contextlib.contextmanager
def named_temporary_file(mode="w+b", prefix=None, suffix=None, *, delete=True):
    """Named temporary file."""
    filename = io.StringIO()
    if prefix:
        filename.write(prefix)
    filename.write(os.urandom(8).hex())
    if suffix:
        filename.write(suffix)
    pathname = os.path.join(tempfile.gettempdir(), filename.getvalue())  # noqa: PTH118  # test fixture path join
    with open(pathname, mode) as f:  # noqa: PTH123  # test fixture path open
        yield f

    if delete:
        try:
            os.unlink(pathname)  # noqa: PTH108  # test fixture unlink
        except PermissionError:
            #
            # This can happen on Windows for unknown reasons.
            #
            logger.exception("failed to unlink temporary file")


def test_compression_kwargs_changes_gzip_output_size():
    """Confirm compression_kwargs is forwarded: compresslevel=1 must be larger than =9 for .gz."""
    payload = b"hello world " * 10_000
    with named_temporary_file(suffix=".gz") as low_tmp, named_temporary_file(suffix=".gz") as high_tmp:
        with smart_open.open(low_tmp.name, "wb", compression_kwargs={"compresslevel": 1}) as fout:
            fout.write(payload)
        with smart_open.open(high_tmp.name, "wb", compression_kwargs={"compresslevel": 9}) as fout:
            fout.write(payload)
        assert os.path.getsize(low_tmp.name) > os.path.getsize(high_tmp.name)  # noqa: PTH202  # test fixture getsize


def test_compression_extensions():
    """Compression extensions."""
    for extension in smart_open.compression.get_supported_extensions():
        with named_temporary_file(suffix=extension) as tmp:
            with smart_open.open(tmp.name, "wt") as fout:
                print("hello world", file=fout)
                print("this is a test", file=fout)

            with smart_open.open(tmp.name, "rt") as fin:
                got = list(fin)

        assert got == ["hello world\n", "this is a test\n"], f"Error for {extension=}, mode='wt'"

        with named_temporary_file(suffix=extension) as tmp:
            with smart_open.open(tmp.name, "w") as fout:
                fout.write("hello world\n")
                fout.write("this is a test\n")

            with smart_open.open(tmp.name, "r") as fin:
                got = list(fin)

        assert got == ["hello world\n", "this is a test\n"], f"Error for {extension=}, mode='w'"

        with named_temporary_file(suffix=extension) as tmp:
            with smart_open.open(tmp.name, "wb") as fout:
                fout.write(b"hello world\n")
                fout.write(b"this is a test\n")

            with smart_open.open(tmp.name, "rb") as fin:
                got = list(fin)

        assert got == [b"hello world\n", b"this is a test\n"], f"Error for {extension=}, mode='wb'"


class ParseUriTest(unittest.TestCase):
    """Test ParseUri class."""

    def test_scheme(self):
        """Do URIs schemes parse correctly?"""
        # supported schemes
        for scheme in (
            "s3",
            "s3a",
            "s3n",
            "hdfs",
            "file",
            "http",
            "https",
            "gcs",
            "gs",
            "azure",
        ):
            parsed_uri = smart_open_lib._parse_uri(
                scheme + "://mybucket/mykey"
            )  # test reaches into private state
            assert parsed_uri.scheme == scheme

        # unsupported scheme => NotImplementedError
        with pytest.raises(NotImplementedError):
            smart_open_lib._parse_uri("foobar://mybucket/mykey")  # test reaches into private state
        # unknown scheme => default_scheme
        parsed_uri = smart_open_lib._parse_uri("blah blah")  # test reaches into private state
        assert parsed_uri.scheme == "file"

    def test_s3_uri(self):
        """Do S3 URIs parse correctly?"""
        # correct uri without credentials
        parsed_uri = smart_open_lib._parse_uri("s3://mybucket/mykey")  # test reaches into private state
        assert parsed_uri.scheme == "s3"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.key_id == "mykey"
        assert parsed_uri.access_id is None
        assert parsed_uri.access_secret is None

    def test_s3_uri_contains_slash(self):
        """S3 uri contains slash."""
        parsed_uri = smart_open_lib._parse_uri("s3://mybucket/mydir/mykey")  # test reaches into private state
        assert parsed_uri.scheme == "s3"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.key_id == "mydir/mykey"
        assert parsed_uri.access_id is None
        assert parsed_uri.access_secret is None

    def test_s3_uri_with_credentials(self):
        """S3 uri with credentials."""
        parsed_uri = smart_open_lib._parse_uri(
            "s3://ACCESSID456:acces/sse_cr-et@mybucket/mykey"
        )  # test reaches into private state
        assert parsed_uri.scheme == "s3"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.key_id == "mykey"
        assert parsed_uri.access_id == "ACCESSID456"
        assert parsed_uri.access_secret == "acces/sse_cr-et"  # fixture password literal

    def test_s3_uri_with_credentials2(self):
        """S3 uri with credentials2."""
        parsed_uri = smart_open_lib._parse_uri(
            "s3://accessid:access/secret@mybucket/mykey"
        )  # test reaches into private state
        assert parsed_uri.scheme == "s3"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.key_id == "mykey"
        assert parsed_uri.access_id == "accessid"
        assert parsed_uri.access_secret == "access/secret"  # fixture password literal

    def test_s3_uri_has_atmark_in_key_name(self):
        """S3 uri has atmark in key name."""
        parsed_uri = smart_open_lib._parse_uri(
            "s3://accessid:access/secret@mybucket/my@ke@y"
        )  # test reaches into private state
        assert parsed_uri.scheme == "s3"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.key_id == "my@ke@y"
        assert parsed_uri.access_id == "accessid"
        assert parsed_uri.access_secret == "access/secret"  # fixture password literal

    #
    # Nb. should never happen in theory, but if it does, we should avoid crashing
    #
    def test_s3_uri_has_colon_in_secret(self):
        """S3 uri has colon in secret."""
        parsed_uri = smart_open_lib._parse_uri(
            "s3://accessid:access/secret:totally@mybucket/my@ke@y"
        )  # test reaches into private state
        assert parsed_uri.scheme == "s3"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.key_id == "my@ke@y"
        assert parsed_uri.access_id == "accessid"
        assert parsed_uri.access_secret == "access/secret:totally"  # fixture password literal

    def test_s3_handles_fragments(self):
        """S3 handles fragments."""
        uri_str = "s3://bucket-name/folder/picture #1.jpg"
        parsed_uri = smart_open_lib._parse_uri(uri_str)  # test reaches into private state
        assert parsed_uri.key_id == "folder/picture #1.jpg"

    def test_s3_handles_querystring(self):
        """S3 handles querystring."""
        uri_str = "s3://bucket-name/folder/picture1.jpg?bar"
        parsed_uri = smart_open_lib._parse_uri(uri_str)  # test reaches into private state
        assert parsed_uri.key_id == "folder/picture1.jpg?bar"

    def test_webhdfs_uri_to_http(self):
        """Webhdfs uri to http."""
        parsed_uri = smart_open_lib._parse_uri(
            "webhdfs://host:14000/path/file"
        )  # test reaches into private state
        actual = webhdfs.convert_to_http_uri(parsed_uri)
        expected = "http://host:14000/webhdfs/v1/path/file"
        assert actual == expected

    def test_webhdfs_uri_to_http_with_query(self):
        """Webhdfs uri to http with query."""
        parsed_uri = smart_open_lib._parse_uri(
            "webhdfs://host:14000/path/file?a=1"
        )  # test reaches into private state
        actual = webhdfs.convert_to_http_uri(parsed_uri)
        expected = "http://host:14000/webhdfs/v1/path/file?a=1"
        assert actual == expected

    def test_webhdfs_uri_to_http_with_user(self):
        """Webhdfs uri to http with user."""
        parsed_uri = smart_open_lib._parse_uri(
            "webhdfs://user@host:14000/path"
        )  # test reaches into private state
        actual = webhdfs.convert_to_http_uri(parsed_uri)
        expected = "http://host:14000/webhdfs/v1/path?user.name=user"
        assert actual == expected

    def test_webhdfs_uri_to_http_with_user_and_query(self):
        """Webhdfs uri to http with user and query."""
        parsed_uri = smart_open_lib._parse_uri(
            "webhdfs://user@host:14000/path?a=1"
        )  # test reaches into private state
        actual = webhdfs.convert_to_http_uri(parsed_uri)
        expected = "http://host:14000/webhdfs/v1/path?a=1&user.name=user"
        assert actual == expected

    def test_uri_from_issue_223_works(self):
        """Uri from issue 223 works."""
        uri = "s3://:@omax-mis/twilio-messages-media/final/MEcd7c36e75f87dc6dd9e33702cdcd8fb6"
        parsed_uri = smart_open_lib._parse_uri(uri)  # test reaches into private state
        assert parsed_uri.scheme == "s3"
        assert parsed_uri.bucket_id == "omax-mis"
        assert parsed_uri.key_id == "twilio-messages-media/final/MEcd7c36e75f87dc6dd9e33702cdcd8fb6"
        assert parsed_uri.access_id == ""
        assert parsed_uri.access_secret == ""

    def test_s3_uri_with_colon_in_key_name(self):
        """Correctly parse the s3 url if there is a colon in the key or dir."""
        parsed_uri = smart_open_lib._parse_uri(
            "s3://mybucket/mydir/my:key"
        )  # test reaches into private state
        assert parsed_uri.scheme == "s3"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.key_id == "mydir/my:key"
        assert parsed_uri.access_id is None
        assert parsed_uri.access_secret is None

    def test_s3_uri_with_at_symbol_in_key_name0(self):
        """Correctly parse the s3 url if there is an @ symbol (and colon) in the key or dir."""
        parsed_uri = smart_open_lib._parse_uri(
            "s3://mybucket/mydir:my@key"
        )  # test reaches into private state
        assert parsed_uri.scheme == "s3"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.key_id == "mydir:my@key"
        assert parsed_uri.access_id is None
        assert parsed_uri.access_secret is None

    def test_s3_uri_with_at_symbol_in_key_name1(self):
        """Correctly parse the s3 url if there is an @ symbol (and colon) in the key or dir."""
        parsed_uri = smart_open_lib._parse_uri(
            "s3://mybucket/my:dir@my/key"
        )  # test reaches into private state
        assert parsed_uri.scheme == "s3"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.key_id == "my:dir@my/key"
        assert parsed_uri.access_id is None
        assert parsed_uri.access_secret is None

    def test_s3_uri_contains_question_mark(self):
        """S3 uri contains question mark."""
        parsed_uri = smart_open_lib._parse_uri(
            "s3://mybucket/mydir/mykey?param"
        )  # test reaches into private state
        assert parsed_uri.scheme == "s3"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.key_id == "mydir/mykey?param"
        assert parsed_uri.version_id is None

    def test_s3_uri_version_id(self):
        """Does ?versionId=... in an S3 URI populate version_id?"""
        parsed_uri = smart_open_lib._parse_uri(
            "s3://mybucket/mydir/mykey?versionId=v1"
        )  # test reaches into private state
        assert parsed_uri.scheme == "s3"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.key_id == "mydir/mykey"
        assert parsed_uri.version_id == "v1"

    def test_s3_uri_version_id_with_other_params(self):
        """Are non-versionId query params preserved on the key?"""
        parsed_uri = smart_open_lib._parse_uri(
            "s3://mybucket/mykey?versionId=v1&other=keep"
        )  # test reaches into private state
        assert parsed_uri.key_id == "mykey?other=keep"
        assert parsed_uri.version_id == "v1"

        parsed_uri = smart_open_lib._parse_uri(
            "s3://mybucket/mykey?other=keep&versionId=v1"
        )  # test reaches into private state
        assert parsed_uri.key_id == "mykey?other=keep"
        assert parsed_uri.version_id == "v1"

    def test_s3_uri_no_version_id_when_no_match(self):
        """``versionId`` is only extracted when the key matches exactly."""
        parsed_uri = smart_open_lib._parse_uri(
            "s3://mybucket/mykey?versionIdLookalike=foo"
        )  # test reaches into private state
        assert parsed_uri.key_id == "mykey?versionIdLookalike=foo"
        assert parsed_uri.version_id is None

    def test_leading_slash_local_file(self):
        """Leading slash local file."""
        path = "/home/misha/hello.txt"
        uri = smart_open_lib._parse_uri(path)  # test reaches into private state
        assert uri.scheme == "file"
        assert uri.uri_path == path

        uri = smart_open_lib._parse_uri("//" + path)  # test reaches into private state
        assert uri.scheme == "file"
        assert uri.uri_path == "//" + path

    def test_ssh(self):
        """Ssh."""
        as_string = "ssh://user@host:1234/path/to/file"
        uri = smart_open_lib._parse_uri(as_string)  # test reaches into private state
        assert uri.scheme == "ssh"
        assert uri.uri_path == "/path/to/file"
        assert uri.user == "user"
        assert uri.host == "host"
        assert uri.port == 1234  # test uses inline magic value
        assert uri.password is None

    def test_ssh_with_pass(self):
        """Ssh with pass."""
        as_string = "ssh://user:pass@host:1234/path/to/file"
        uri = smart_open_lib._parse_uri(as_string)  # test reaches into private state
        assert uri.scheme == "ssh"
        assert uri.uri_path == "/path/to/file"
        assert uri.user == "user"
        assert uri.host == "host"
        assert uri.port == 1234  # test uses inline magic value
        assert uri.password == "pass"  # fixture password literal

    def test_scp(self):
        """Scp."""
        as_string = "scp://user@host:/path/to/file"
        uri = smart_open_lib._parse_uri(as_string)  # test reaches into private state
        assert uri.scheme == "scp"
        assert uri.uri_path == "/path/to/file"
        assert uri.user == "user"
        assert uri.host == "host"
        assert uri.port is None
        assert uri.password is None

    def test_scp_with_pass(self):
        """Scp with pass."""
        as_string = "scp://user:pass@host:/path/to/file"
        uri = smart_open_lib._parse_uri(as_string)  # test reaches into private state
        assert uri.scheme == "scp"
        assert uri.uri_path == "/path/to/file"
        assert uri.user == "user"
        assert uri.host == "host"
        assert uri.port is None
        assert uri.password == "pass"  # fixture password literal

    def test_sftp(self):
        """Sftp."""
        as_string = "sftp://host/path/to/file"
        uri = smart_open_lib._parse_uri(as_string)  # test reaches into private state
        assert uri.scheme == "sftp"
        assert uri.uri_path == "/path/to/file"
        assert uri.user is None
        assert uri.host == "host"
        assert uri.port is None
        assert uri.password is None

    def test_sftp_with_user_and_pass(self):
        """Sftp with user and pass."""
        as_string = "sftp://user:pass@host:2222/path/to/file"
        uri = smart_open_lib._parse_uri(as_string)  # test reaches into private state
        assert uri.scheme == "sftp"
        assert uri.uri_path == "/path/to/file"
        assert uri.user == "user"
        assert uri.host == "host"
        assert uri.port == 2222  # test uses inline magic value
        assert uri.password == "pass"  # fixture password literal

    def test_ssh_complex_password_with_colon(self):
        """Ssh complex password with colon."""
        as_string = "sftp://user:some:complex@password$$@host:2222/path/to/file"
        uri = smart_open_lib._parse_uri(as_string)  # test reaches into private state
        assert uri.password == "some:complex@password$$"  # fixture password literal

    def test_gcs_uri(self):
        """Do GCS URIs parse correctly?"""
        # correct uri without credentials
        parsed_uri = smart_open_lib._parse_uri("gcs://mybucket/myblob")  # test reaches into private state
        assert parsed_uri.scheme == "gcs"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.blob_id == "myblob"

    def test_gcs_uri_contains_slash(self):
        """Gcs uri contains slash."""
        parsed_uri = smart_open_lib._parse_uri(
            "gcs://mybucket/mydir/myblob"
        )  # test reaches into private state
        assert parsed_uri.scheme == "gcs"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.blob_id == "mydir/myblob"

    def test_gcs_uri_contains_question_mark(self):
        """Gcs uri contains question mark."""
        parsed_uri = smart_open_lib._parse_uri(
            "gcs://mybucket/mydir/myblob?param"
        )  # test reaches into private state
        assert parsed_uri.scheme == "gcs"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.blob_id == "mydir/myblob?param"

    def test_gs_uri_backwards_compat(self):
        """``gs://`` keeps working as a backwards-compat alias of ``gcs://``."""
        parsed_uri = smart_open_lib._parse_uri(
            "gs://mybucket/mydir/myblob"
        )  # test reaches into private state
        assert parsed_uri.scheme == "gs"
        assert parsed_uri.bucket_id == "mybucket"
        assert parsed_uri.blob_id == "mydir/myblob"

    def test_azure_blob_uri(self):
        """Do Azure Blob URIs parse correctly?"""
        # correct uri without credentials
        parsed_uri = smart_open_lib._parse_uri(
            "azure://mycontainer/myblob"
        )  # test reaches into private state
        assert parsed_uri.scheme == "azure"
        assert parsed_uri.container_id == "mycontainer"
        assert parsed_uri.blob_id == "myblob"

    def test_azure_blob_uri_root_container(self):
        """Azure blob uri root container."""
        parsed_uri = smart_open_lib._parse_uri("azure://myblob")  # test reaches into private state
        assert parsed_uri.scheme == "azure"
        assert parsed_uri.container_id == "$root"
        assert parsed_uri.blob_id == "myblob"

    def test_azure_blob_uri_contains_slash(self):
        """Azure blob uri contains slash."""
        parsed_uri = smart_open_lib._parse_uri(
            "azure://mycontainer/mydir/myblob"
        )  # test reaches into private state
        assert parsed_uri.scheme == "azure"
        assert parsed_uri.container_id == "mycontainer"
        assert parsed_uri.blob_id == "mydir/myblob"

    def test_pathlib_monkeypatch(self):
        """Pathlib monkeypatch."""
        from smart_open.smart_open_lib import pathlib

        assert pathlib.Path.open != smart_open.open

        with patch_pathlib():
            assert pathlib.Path.open == smart_open.open

        assert pathlib.Path.open != smart_open.open

        obj = patch_pathlib()
        assert pathlib.Path.open == smart_open.open

        _patch_pathlib(obj.old_impl)
        assert pathlib.Path.open != smart_open.open

    def test_pathlib_monkeypatch_read_gz(self):
        """Pathlib monkeypatch read gz."""
        from smart_open.smart_open_lib import pathlib

        path = pathlib.Path(CURR_DIR) / "test_data" / "crime-and-punishment.txt.gz"

        # Check that standard implementation can't work with gzip
        with path.open("r") as infile, pytest.raises(Exception):  # noqa: B017, PT011  # legacy broad pytest.raises
            lines = infile.readlines()

        # Check that our implementation works with gzip
        obj = patch_pathlib()
        try:
            with path.open("r", encoding="utf-8") as infile:
                lines = infile.readlines()
            assert len(lines) == 3  # test uses inline magic value
        finally:
            _patch_pathlib(obj.old_impl)


class SmartOpenHttpTest(unittest.TestCase):
    """Test reading from HTTP connections in various ways."""

    @mock.patch("smart_open.ssh.open", return_value=open(__file__))  # noqa: PTH123, SIM115  # test fixture path open
    def test_read_ssh(self, mock_open):
        """Is SSH line iterator called correctly?"""
        obj = smart_open.open(
            "ssh://ubuntu:pass@ip_address:1022/some/path/lines.txt",
            mode="rb",
            transport_params={"hello": "world"},
        )
        obj.__iter__()
        mock_open.assert_called_with(
            "/some/path/lines.txt",
            "rb",
            host="ip_address",
            user="ubuntu",
            password="pass",  # fixture password kwarg
            port=1022,
        )

    @responses.activate
    def test_http_read(self):
        """Does http read method work correctly."""
        responses.add(responses.GET, "http://127.0.0.1/index.html", body="line1\nline2")
        smart_open_object = smart_open.open("http://127.0.0.1/index.html", "rb")
        assert smart_open_object.read().decode("utf-8") == "line1\nline2"

    @responses.activate
    def test_https_readline(self):
        """Does https readline method work correctly."""
        responses.add(responses.GET, "https://127.0.0.1/index.html", body="line1\u2028still line1\nline2")
        smart_open_object = smart_open.open("https://127.0.0.1/index.html", "rb")
        assert smart_open_object.readline().decode("utf-8") == "line1\u2028still line1\n"
        smart_open_object = smart_open.open("https://127.0.0.1/index.html", "r", encoding="utf-8")
        assert smart_open_object.readline() == "line1\u2028still line1\n"

    @responses.activate
    def test_http_pass(self):
        """Does http authentication work correctly."""
        responses.add(responses.GET, "http://127.0.0.1/index.html", body="line1\nline2")
        tp = {"user": "me", "password": "pass"}
        smart_open.open("http://127.0.0.1/index.html", transport_params=tp)
        assert len(responses.calls) == 1
        actual_request = responses.calls[0].request
        assert "Authorization" in actual_request.headers
        assert actual_request.headers["Authorization"].startswith("Basic ")

    @responses.activate
    def test_http_cert(self):
        """Does cert parameter get passed to requests."""
        responses.add(responses.GET, "http://127.0.0.1/index.html", body="line1\nline2")
        cert_path = "/path/to/my/cert.pem"
        tp = {"cert": cert_path}
        smart_open.open("http://127.0.0.1/index.html", transport_params=tp)
        assert len(responses.calls) == 1
        actual_request = responses.calls[0].request
        assert cert_path == actual_request.req_kwargs["cert"]

    @responses.activate
    def _test_compressed_http(self, suffix, *, query):
        """Can open <suffix> via http?"""
        assert suffix in (".gz", ".bz2")

        raw_data = b"Hello World Compressed." * 10000
        compressed_data = gzip_compress(raw_data) if suffix == ".gz" else bz2.compress(raw_data)

        responses.add(responses.GET, "http://127.0.0.1/data" + suffix, body=compressed_data)
        url = "http://127.0.0.1/data{}{}".format(suffix, "?some_param=some_val" if query else "")
        smart_open_object = smart_open.open(url, "rb")

        assert smart_open_object.read() == raw_data

    def test_http_gz(self):
        """Can open gzip via http?"""
        self._test_compressed_http(".gz", query=False)

    def test_http_bz2(self):
        """Can open bzip2 via http?"""
        self._test_compressed_http(".bz2", query=False)

    def test_http_gz_query(self):
        """Can open gzip via http with a query appended to URI?"""
        self._test_compressed_http(".gz", query=True)

    def test_http_bz2_query(self):
        """Can open bzip2 via http with a query appended to URI?"""
        self._test_compressed_http(".bz2", query=True)


class RealFileSystemTests(unittest.TestCase):
    """Tests that touch the file system via temporary files."""

    def setUp(self):
        """SetUp."""
        with named_temporary_file(prefix="test", delete=False) as fout:
            fout.write(SAMPLE_BYTES)
            self.temp_file = fout.name

    def tearDown(self):
        """TearDown."""
        os.unlink(self.temp_file)  # noqa: PTH108  # test fixture unlink

    def test_rt(self):
        """Rt."""
        with smart_open.open(self.temp_file, "rt") as fin:
            data = fin.read()
        assert data == SAMPLE_TEXT

    def test_wt(self):
        """Wt."""
        #
        # The file already contains SAMPLE_TEXT, so write something different.
        #
        text = "nippon budokan"
        with smart_open.open(self.temp_file, "wt") as fout:
            fout.write(text)

        with smart_open.open(self.temp_file, "rt") as fin:
            data = fin.read()
        assert data == text

    def test_ab(self):
        """Ab."""
        with smart_open.open(self.temp_file, "ab") as fout:
            fout.write(SAMPLE_BYTES)
        with smart_open.open(self.temp_file, "rb") as fin:
            data = fin.read()
        assert data == SAMPLE_BYTES * 2

    def test_aplus(self):
        """Aplus."""
        with smart_open.open(self.temp_file, "a+") as fout:
            fout.write(SAMPLE_TEXT)
        with smart_open.open(self.temp_file, "rt") as fin:
            text = fin.read()
        assert text == SAMPLE_TEXT * 2

    def test_at(self):
        """At."""
        with smart_open.open(self.temp_file, "at") as fout:
            fout.write(SAMPLE_TEXT)
        with smart_open.open(self.temp_file, "rt") as fin:
            text = fin.read()
        assert text == SAMPLE_TEXT * 2

    def test_atplus(self):
        """Atplus."""
        with smart_open.open(self.temp_file, "at+") as fout:
            fout.write(SAMPLE_TEXT)
        with smart_open.open(self.temp_file, "rt") as fin:
            text = fin.read()
        assert text == SAMPLE_TEXT * 2


class CompressionRealFileSystemTests(RealFileSystemTests):
    """Same as RealFileSystemTests but with a compressed file."""

    def setUp(self):
        """SetUp."""
        with named_temporary_file(prefix="test", suffix=".zst", delete=False) as fout:
            self.temp_file = fout.name
        with smart_open.open(self.temp_file, "wb") as fout:
            fout.write(SAMPLE_BYTES)

    def test_aplus(self):
        """Aplus."""
        # transparent (de)compression unsupported for mode 'ab+'

    def test_atplus(self):
        """Atplus."""
        # transparent (de)compression unsupported for mode 'ab+'


#
# What exactly to patch here differs on _how_ we're opening the file.
# See the _shortcut_open function for details.
#
_IO_OPEN = "io.open"
_BUILTIN_OPEN = "smart_open.smart_open_lib._builtin_open"


class SmartOpenReadTest(unittest.TestCase):
    """Test reading from files under various schemes."""

    def test_shortcut(self):
        """Shortcut."""
        fpath = os.path.join(CURR_DIR, "test_data/crime-and-punishment.txt")  # noqa: PTH118  # test fixture path join
        with mock.patch("smart_open.smart_open_lib._builtin_open") as mock_open:
            smart_open.open(fpath, "r").read()
        mock_open.assert_called_with(fpath, "r", buffering=-1)

    def test_open_binary(self):
        """Open binary."""
        fpath = os.path.join(CURR_DIR, "test_data/cp852.tsv.txt")  # noqa: PTH118  # test fixture path join
        with open(fpath, "rb") as fin:  # noqa: PTH123  # test fixture path open
            expected = fin.read()
        with smart_open.open(fpath, "rb") as fin:
            actual = fin.read()
        assert expected == actual

    def test_open_with_keywords(self):
        """This test captures Issue #142."""
        fpath = os.path.join(CURR_DIR, "test_data/cp852.tsv.txt")  # noqa: PTH118  # test fixture path join
        with open(fpath, encoding="cp852") as fin:  # noqa: PTH123  # test fixture path open
            expected = fin.read()
        with smart_open.open(fpath, encoding="cp852") as fin:
            actual = fin.read()
        assert expected == actual

    def test_open_with_keywords_explicit_r(self):
        """Open with keywords explicit r."""
        fpath = os.path.join(CURR_DIR, "test_data/cp852.tsv.txt")  # noqa: PTH118  # test fixture path join
        with open(fpath, encoding="cp852") as fin:  # noqa: PTH123  # test fixture path open
            expected = fin.read()
        with smart_open.open(fpath, mode="r", encoding="cp852") as fin:
            actual = fin.read()
        assert expected == actual

    def test_open_and_read_pathlib_path(self):
        """If ``pathlib.Path`` is available we should be able to open and read."""
        from smart_open.smart_open_lib import pathlib

        fpath = os.path.join(CURR_DIR, "test_data/cp852.tsv.txt")  # noqa: PTH118  # test fixture path join
        with open(fpath, "rb") as fin:  # noqa: PTH123  # test fixture path open
            expected = fin.read().decode("cp852")
        with smart_open.open(pathlib.Path(fpath), mode="r", encoding="cp852", newline="") as fin:
            actual = fin.read()
        assert expected == actual

    @mock_s3
    def test_read_never_returns_none(self):
        """Read should never return None."""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="mybucket")

        test_string = "ветер по морю гуляет..."
        with smart_open.open("s3://mybucket/mykey", "wb") as fout:
            fout.write(test_string.encode("utf8"))

        r = smart_open.open("s3://mybucket/mykey", "rb")
        assert r.read() == test_string.encode("utf-8")
        assert r.read() == b""
        assert r.read() == b""

    @mock_s3
    def test_read_newline_none(self):
        """The ``newline`` open() parameter for reading must match the stdlib semantics."""
        _resource("s3").create_bucket(Bucket="mybucket")
        # Unicode line separator and various others must never split lines
        test_file = "line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\nlast line"
        with smart_open.open("s3://mybucket/mykey", "wb") as fout:
            fout.write(test_file.encode("utf-8"))
        # No newline parameter means newline=None i.e. universal newline mode with all
        # line endings translated to '\n'
        with smart_open.open("s3://mybucket/mykey", "r", encoding="utf-8") as fin:
            assert list(fin) == ["line\u2028 LF\n", "line\x1c CR\n", "line\x85 CRLF\n", "last line"]

    @mock_s3
    def test_read_newline_empty(self):
        """Read newline empty."""
        _resource("s3").create_bucket(Bucket="mybucket")
        test_file = "line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\nlast line"
        with smart_open.open("s3://mybucket/mykey", "wb") as fout:
            fout.write(test_file.encode("utf-8"))
        # If newline='' universal newline mode is enabled but line separators are not replaced
        with smart_open.open("s3://mybucket/mykey", "r", encoding="utf-8", newline="") as fin:
            assert list(fin) == ["line\u2028 LF\n", "line\x1c CR\r", "line\x85 CRLF\r\n", "last line"]

    @mock_s3
    def test_read_newline_cr(self):
        """Read newline cr."""
        _resource("s3").create_bucket(Bucket="mybucket")
        test_file = "line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\nlast line"
        with smart_open.open("s3://mybucket/mykey", "wb") as fout:
            fout.write(test_file.encode("utf-8"))
        # If newline='\r' only CR splits lines
        with smart_open.open("s3://mybucket/mykey", "r", encoding="utf-8", newline="\r") as fin:
            assert list(fin) == ["line\u2028 LF\nline\x1c CR\r", "line\x85 CRLF\r", "\nlast line"]

    @mock_s3
    def test_read_newline_lf(self):
        """Read newline lf."""
        _resource("s3").create_bucket(Bucket="mybucket")
        test_file = "line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\nlast line"
        with smart_open.open("s3://mybucket/mykey", "wb") as fout:
            fout.write(test_file.encode("utf-8"))
        # If newline='\n' only LF splits lines
        with smart_open.open("s3://mybucket/mykey", "r", encoding="utf-8", newline="\n") as fin:
            assert list(fin) == ["line\u2028 LF\n", "line\x1c CR\rline\x85 CRLF\r\n", "last line"]

    @mock_s3
    def test_read_newline_crlf(self):
        """Read newline crlf."""
        _resource("s3").create_bucket(Bucket="mybucket")
        test_file = "line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\nlast line"
        with smart_open.open("s3://mybucket/mykey", "wb") as fout:
            fout.write(test_file.encode("utf-8"))
        # If newline='\r\n' only CRLF splits lines
        with smart_open.open("s3://mybucket/mykey", "r", encoding="utf-8", newline="\r\n") as fin:
            assert list(fin) == ["line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\n", "last line"]

    @mock_s3
    def test_read_newline_slurp(self):
        """Read newline slurp."""
        _resource("s3").create_bucket(Bucket="mybucket")
        test_file = "line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\nlast line"
        with smart_open.open("s3://mybucket/mykey", "wb") as fout:
            fout.write(test_file.encode("utf-8"))
        # Even reading the whole file with read() must replace newlines
        with smart_open.open("s3://mybucket/mykey", "r", encoding="utf-8", newline=None) as fin:
            assert fin.read() == "line\u2028 LF\nline\x1c CR\nline\x85 CRLF\nlast line"

    @mock_s3
    def test_read_newline_binary(self):
        """Read newline binary."""
        _resource("s3").create_bucket(Bucket="mybucket")
        test_file = "line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\nlast line"
        with smart_open.open("s3://mybucket/mykey", "wb") as fout:
            fout.write(test_file.encode("utf-8"))
        # If the file is opened in binary mode only LF splits lines
        with smart_open.open("s3://mybucket/mykey", "rb") as fin:
            assert list(fin) == [
                "line\u2028 LF\n".encode(),
                "line\x1c CR\rline\x85 CRLF\r\n".encode(),
                b"last line",
            ]

    @mock_s3
    def test_write_newline_none(self):
        """The ``newline`` open() parameter for writing must match the stdlib semantics."""
        _resource("s3").create_bucket(Bucket="mybucket")
        # Unicode line separator and various others must never split lines
        test_file = "line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\nlast line"
        # No newline parameter means newline=None, all LF are translatest to os.linesep
        with smart_open.open("s3://mybucket/mykey", "w", encoding="utf-8") as fout:
            fout.write(test_file)
        with smart_open.open("s3://mybucket/mykey", "rb") as fin:
            assert (
                fin.read().decode("utf-8")
                == "line\u2028 LF" + os.linesep + "line\x1c CR\rline\x85 CRLF\r" + os.linesep + "last line"
            )

    @mock_s3
    def test_write_newline_empty(self):
        """Write newline empty."""
        _resource("s3").create_bucket(Bucket="mybucket")
        test_file = "line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\nlast line"
        # If newline='' nothing is changed
        with smart_open.open("s3://mybucket/mykey", "w", encoding="utf-8", newline="") as fout:
            fout.write(test_file)
        with smart_open.open("s3://mybucket/mykey", "rb") as fin:
            assert fin.read().decode("utf-8") == "line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\nlast line"

    @mock_s3
    def test_write_newline_lf(self):
        """Write newline lf."""
        _resource("s3").create_bucket(Bucket="mybucket")
        test_file = "line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\nlast line"
        # If newline='\n' nothing is changed
        with smart_open.open("s3://mybucket/mykey", "w", encoding="utf-8", newline="\n") as fout:
            fout.write(test_file)
        with smart_open.open("s3://mybucket/mykey", "rb") as fin:
            assert fin.read().decode("utf-8") == "line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\nlast line"

    @mock_s3
    def test_write_newline_cr(self):
        """Write newline cr."""
        _resource("s3").create_bucket(Bucket="mybucket")
        test_file = "line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\nlast line"
        # If newline='\r' all LF are replaced by CR
        with smart_open.open("s3://mybucket/mykey", "w", encoding="utf-8", newline="\r") as fout:
            fout.write(test_file)
        with smart_open.open("s3://mybucket/mykey", "rb") as fin:
            assert fin.read().decode("utf-8") == "line\u2028 LF\rline\x1c CR\rline\x85 CRLF\r\rlast line"

    @mock_s3
    def test_write_newline_crlf(self):
        """Write newline crlf."""
        _resource("s3").create_bucket(Bucket="mybucket")
        test_file = "line\u2028 LF\nline\x1c CR\rline\x85 CRLF\r\nlast line"
        # If newline='\r\n' all LF are replaced by CRLF
        with smart_open.open("s3://mybucket/mykey", "w", encoding="utf-8", newline="\r\n") as fout:
            fout.write(test_file)
        with smart_open.open("s3://mybucket/mykey", "rb") as fin:
            assert fin.read().decode("utf-8") == "line\u2028 LF\r\nline\x1c CR\rline\x85 CRLF\r\r\nlast line"

    @mock_s3
    def test_readline(self):
        """Does readline() return the correct file content?"""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="mybucket")
        test_string = "hello žluťoučký\u2028world!\nhow are you?".encode()
        with smart_open.open("s3://mybucket/mykey", "wb") as fout:
            fout.write(test_string)

        reader = smart_open.open("s3://mybucket/mykey", "rb")
        assert reader.readline() == "hello žluťoučký\u2028world!\n".encode()

    @mock_s3
    def test_readline_iter(self):
        """Does __iter__ return the correct file content?"""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="mybucket")
        lines = ["всем\u2028привет!\n", "что нового?"]  # noqa: RUF001  # fixture deliberately uses Cyrillic characters
        with smart_open.open("s3://mybucket/mykey", "wb") as fout:
            fout.write("".join(lines).encode("utf-8"))

        reader = smart_open.open("s3://mybucket/mykey", "rb")

        actual_lines = [line.decode("utf-8") for line in reader]
        assert len(actual_lines) == 2  # test uses inline magic value
        assert lines[0] == actual_lines[0]
        assert lines[1] == actual_lines[1]

    @mock_s3
    def test_readline_eof(self):
        """Does readline() return empty string on EOF?"""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="mybucket")
        with smart_open.open("s3://mybucket/mykey", "wb"):
            pass

        with patch_invalid_range_response("0"):
            reader = smart_open.open("s3://mybucket/mykey", "rb")

            assert reader.readline() == b""
            assert reader.readline() == b""
            assert reader.readline() == b""

    @mock_s3
    def test_s3_iter_lines(self):
        """Does s3_iter_lines give correct content?"""
        # create fake bucket and fake key
        s3 = _resource("s3")
        s3.create_bucket(Bucket="mybucket")
        test_string = "hello žluťoučký\u2028world!\nhow are you?".encode()
        with smart_open.open("s3://mybucket/mykey", "wb") as fin:
            fin.write(test_string)

        # call s3_iter_lines and check output
        reader = smart_open.open("s3://mybucket/mykey", "rb")
        output = list(reader)
        assert len(output) == 2  # test uses inline magic value
        assert b"".join(output) == test_string

    # TODO: add more complex test for file://  # noqa: TD002, TD003  # legacy TODO
    @mock.patch("smart_open.smart_open_lib._builtin_open")
    def test_file(self, mock_smart_open):
        """Is file:// line iterator called correctly?"""
        prefix = "file://"
        full_path = "/tmp/test.txt"  # tmp path in test
        read_mode = "rb"
        smart_open_object = smart_open.open(prefix + full_path, read_mode)
        smart_open_object.__iter__()
        # called with the correct path?
        mock_smart_open.assert_called_with(full_path, read_mode, buffering=-1)

        full_path = "/tmp/test#hash##more.txt"  # tmp path in test
        read_mode = "rb"
        smart_open_object = smart_open.open(prefix + full_path, read_mode)
        smart_open_object.__iter__()
        # called with the correct path?
        mock_smart_open.assert_called_with(full_path, read_mode, buffering=-1)

        full_path = "aa#aa"
        read_mode = "rb"
        smart_open_object = smart_open.open(full_path, read_mode)
        smart_open_object.__iter__()
        # called with the correct path?
        mock_smart_open.assert_called_with(full_path, read_mode, buffering=-1)

        short_path = "~/tmp/test.txt"
        full_path = os.path.expanduser(short_path)  # noqa: PTH111  # test fixture expanduser

    @mock.patch(_BUILTIN_OPEN)
    def test_file_errors(self, mock_smart_open):
        """File errors."""
        prefix = "file://"
        full_path = "/tmp/test.txt"  # tmp path in test
        read_mode = "r"
        short_path = "~/tmp/test.txt"
        full_path = os.path.expanduser(short_path)  # noqa: PTH111  # test fixture expanduser
        smart_open_object = smart_open.open(prefix + short_path, read_mode, errors="strict")
        smart_open_object.__iter__()
        # called with the correct expanded path?
        mock_smart_open.assert_called_with(full_path, read_mode, buffering=-1, errors="strict")

    @mock.patch(_BUILTIN_OPEN)
    def test_file_buffering(self, mock_smart_open):
        """File buffering."""
        smart_open_object = smart_open.open("/tmp/somefile", "rb", buffering=0)  # tmp path in test
        smart_open_object.__iter__()
        # called with the correct expanded path?
        mock_smart_open.assert_called_with("/tmp/somefile", "rb", buffering=0)  # tmp path in test

    @mock.patch(_BUILTIN_OPEN)
    def test_file_buffering2(self, mock_smart_open):
        """File buffering2."""
        smart_open_object = smart_open.open("/tmp/somefile", "rb", 0)  # tmp path in test
        smart_open_object.__iter__()
        # called with the correct expanded path?
        mock_smart_open.assert_called_with("/tmp/somefile", "rb", buffering=0)  # tmp path in test

    # couldn't find any project for mocking up HDFS data
    # TODO: we want to test also a content of the files, not just fnc call params  # noqa: TD002, TD003  # legacy TODO
    @mock.patch("smart_open.hdfs.subprocess")
    def test_hdfs(self, mock_subprocess):
        """Is HDFS line iterator called correctly?"""
        mock_subprocess.PIPE.return_value = "test"
        smart_open_object = smart_open.open("hdfs:///tmp/test.txt")
        smart_open_object.__iter__()
        # called with the correct params?
        mock_subprocess.Popen.assert_called_with(
            ["hdfs", "dfs", "-cat", "/tmp/test.txt"],  # tmp path in test
            stdout=mock_subprocess.PIPE,
        )

        # netloc host - pass the full URI so the CLI routes to the right cluster
        smart_open_object = smart_open.open("hdfs://host/tmp/test.txt")
        smart_open_object.__iter__()
        mock_subprocess.Popen.assert_called_with(
            ["hdfs", "dfs", "-cat", "hdfs://host/tmp/test.txt"],
            stdout=mock_subprocess.PIPE,
        )

        # netloc host:port - pass the full URI so the CLI routes to the right cluster
        smart_open_object = smart_open.open("hdfs://host:8020/tmp/test.txt")
        smart_open_object.__iter__()
        mock_subprocess.Popen.assert_called_with(
            ["hdfs", "dfs", "-cat", "hdfs://host:8020/tmp/test.txt"],
            stdout=mock_subprocess.PIPE,
        )

    @responses.activate
    def test_webhdfs(self):
        """Is webhdfs line iterator called correctly."""
        responses.add(responses.GET, "http://127.0.0.1:8440/webhdfs/v1/path/file", body="line1\nline2")
        smart_open_object = smart_open.open("webhdfs://127.0.0.1:8440/path/file", "rb")
        iterator = iter(smart_open_object)
        assert next(iterator).decode("utf-8") == "line1\n"
        assert next(iterator).decode("utf-8") == "line2"

    @responses.activate
    def test_webhdfs_encoding(self):
        """Is HDFS line iterator called correctly?"""
        input_url = "webhdfs://127.0.0.1:8440/path/file"
        actual_url = "http://127.0.0.1:8440/webhdfs/v1/path/file"
        text = "не для меня прийдёт весна, не для меня дон разольётся"
        body = text.encode("utf-8")
        responses.add(responses.GET, actual_url, body=body)

        actual = smart_open.open(input_url, encoding="utf-8").read()
        assert text == actual

    @responses.activate
    def test_webhdfs_read(self):
        """Does webhdfs read method work correctly."""
        responses.add(responses.GET, "http://127.0.0.1:8440/webhdfs/v1/path/file", body="line1\nline2")
        smart_open_object = smart_open.open("webhdfs://127.0.0.1:8440/path/file", "rb")
        assert smart_open_object.read().decode("utf-8") == "line1\nline2"

    @mock_s3
    def test_s3_iter_moto(self):
        """Are S3 files iterated over correctly?"""
        # a list of strings to test with
        expected = [b"*" * 5 * 1024**2] + [b"0123456789"] * 1024 + [b"test"]

        # create fake bucket and fake key
        s3 = _resource("s3")
        s3.create_bucket(Bucket="mybucket")

        tp = {"s3_min_part_size": 5 * 1024**2}
        with smart_open.open("s3://mybucket/mykey", "wb", transport_params=tp) as fout:
            # write a single huge line (=full multipart upload)
            fout.write(expected[0] + b"\n")

            # write lots of small lines
            for _lineno, line in enumerate(expected[1:-1]):
                fout.write(line + b"\n")

            # ...and write the last line too, no newline at the end
            fout.write(expected[-1])

        # connect to fake s3 and read from the fake key we filled above
        smart_open_object = smart_open.open("s3://mybucket/mykey", "rb")
        output = [line.rstrip(b"\n") for line in smart_open_object]
        assert output == expected

        # same thing but using a context manager
        with smart_open.open("s3://mybucket/mykey", "rb") as smart_open_object:
            output = [line.rstrip(b"\n") for line in smart_open_object]
            assert output == expected

    @mock_s3
    def test_s3_read_moto(self):
        """Are S3 files read correctly?"""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="mybucket")

        # write some bogus key so we can check it below
        content = "hello wořld\nhow are you?".encode()
        with smart_open.open("s3://mybucket/mykey", "wb") as fout:
            fout.write(content)

        smart_open_object = smart_open.open("s3://mybucket/mykey", "rb")
        assert content[:6] == smart_open_object.read(6)
        assert content[6:14] == smart_open_object.read(8)  # ř is 2 bytes

        assert content[14:] == smart_open_object.read()  # read the rest

    @mock_s3
    def test_s3_seek_moto(self):
        """Does seeking in S3 files work correctly?"""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="mybucket")

        # write some bogus key so we can check it below
        content = "hello wořld\nhow are you?".encode()
        with smart_open.open("s3://mybucket/mykey", "wb") as fout:
            fout.write(content)

        smart_open_object = smart_open.open("s3://mybucket/mykey", "rb")
        assert content[:6] == smart_open_object.read(6)
        assert content[6:14] == smart_open_object.read(8)  # ř is 2 bytes

        smart_open_object.seek(0)
        assert content == smart_open_object.read()  # no size given => read whole file

        smart_open_object.seek(0)
        assert content == smart_open_object.read(-1)  # same thing

    @mock_s3
    def test_s3_tell(self):
        """Does tell() work when S3 file is opened for text writing?"""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="mybucket")

        with smart_open.open("s3://mybucket/mykey", "w") as fout:
            fout.write("test")
            # Note that tell() in general returns an opaque number for text files.
            # See https://docs.python.org/3/library/io.html#io.TextIOBase.tell
            assert fout.tell() == 4  # test uses inline magic value


class SmartOpenS3KwargsTest(unittest.TestCase):
    """Tests for Smart Open S3Kwargs."""

    @mock.patch("boto3.client")
    def test_no_kwargs(self, mock_client):
        """No kwargs."""
        smart_open.open("s3://mybucket/mykey", transport_params={"defer_seek": True})
        mock_client.assert_called_with("s3", config=mock_client.call_args.kwargs["config"])

    @mock.patch("boto3.client")
    def test_credentials(self, mock_client):
        """Credentials."""
        smart_open.open("s3://access_id:access_secret@mybucket/mykey", transport_params={"defer_seek": True})
        mock_client.assert_called_with(
            "s3",
            aws_access_key_id="access_id",
            aws_secret_access_key="access_secret",  # fixture password kwarg
            config=mock_client.call_args.kwargs["config"],
        )

    @mock.patch("boto3.client")
    def test_host(self, mock_client):
        """Host."""
        tp = {
            "client_kwargs": {
                "S3.Client": {"endpoint_url": "http://aa.domain.com"},
            },
            "defer_seek": True,
        }
        smart_open.open("s3://access_id:access_secret@mybucket/mykey", transport_params=tp)
        mock_client.assert_called_with(
            "s3",
            aws_access_key_id="access_id",
            aws_secret_access_key="access_secret",  # fixture password kwarg
            endpoint_url="http://aa.domain.com",
            config=mock_client.call_args.kwargs["config"],
        )

    @mock.patch("boto3.client")
    def test_s3_upload(self, mock_client):
        """S3 upload."""
        tp = {
            "client_kwargs": {
                "S3.Client.create_multipart_upload": {
                    "ServerSideEncryption": "AES256",
                    "ContentType": "application/json",
                }
            }
        }
        smart_open.open("s3://bucket/key", "wb", transport_params=tp)
        mock_client.return_value.create_multipart_upload.assert_called_with(
            Bucket="bucket",
            Key="key",
            ServerSideEncryption="AES256",
            ContentType="application/json",
        )


class SmartOpenTest(unittest.TestCase):
    """Test reading and writing from/into files."""

    def setUp(self):
        """SetUp."""
        self.as_text = "куда идём мы с пятачком - большой большой секрет"  # noqa: RUF001  # fixture deliberately uses Cyrillic characters
        self.as_bytes = self.as_text.encode("utf-8")
        self.stringio = io.StringIO(self.as_text)
        self.bytesio = io.BytesIO(self.as_bytes)

    def test_file_mode_mock(self):
        """Are file:// open modes passed correctly?"""
        # correct read modes
        #
        # We always open files in binary mode first, but engage
        # encoders/decoders as necessary.  Instead of checking how the file
        # _initially_ got opened, we now also check the end result: if the
        # contents got decoded correctly.
        #

    def test_text(self):
        """Text."""
        with (
            mock.patch(_BUILTIN_OPEN, mock.Mock(return_value=self.stringio)) as mock_open,
            smart_open.open("blah", "r", encoding="utf-8") as fin,
        ):
            assert fin.read() == self.as_text
            mock_open.assert_called_with("blah", "r", buffering=-1, encoding="utf-8")

    def test_binary(self):
        """Binary."""
        with (
            mock.patch(_BUILTIN_OPEN, mock.Mock(return_value=self.bytesio)) as mock_open,
            smart_open.open("blah", "rb") as fin,
        ):
            assert fin.read() == self.as_bytes
            mock_open.assert_called_with("blah", "rb", buffering=-1)

    def test_expanded_path(self):
        """Expanded path."""
        short_path = "~/blah"
        full_path = os.path.expanduser(short_path)  # noqa: PTH111  # test fixture expanduser
        with (
            mock.patch(_BUILTIN_OPEN, mock.Mock(return_value=self.stringio)) as mock_open,
            smart_open.open(short_path, "rb"),
        ):
            mock_open.assert_called_with(full_path, "rb", buffering=-1)

    def test_incorrect(self):
        """Incorrect."""
        # incorrect file mode
        with pytest.raises(NotImplementedError):
            smart_open.open("s3://bucket/key", "x")
        # correct write modes, incorrect scheme
        with pytest.raises(NotImplementedError):
            smart_open.open("hdfs:///blah.txt", "wb+")
        with pytest.raises(NotImplementedError):
            smart_open.open("http:///blah.txt", "w")
        with pytest.raises(NotImplementedError):
            smart_open.open("s3://bucket/key", "wb+")

    def test_write_utf8(self):
        """Write utf8."""
        # correct write mode, correct file:// URI
        with (
            mock.patch(_BUILTIN_OPEN, mock.Mock(return_value=self.stringio)) as mock_open,
            smart_open.open("blah", "w", encoding="utf-8") as fout,
        ):
            mock_open.assert_called_with("blah", "w", buffering=-1, encoding="utf-8")
            fout.write(self.as_text)

    def test_write_utf8_absolute_path(self):
        """Write utf8 absolute path."""
        with (
            mock.patch(_BUILTIN_OPEN, mock.Mock(return_value=self.stringio)) as mock_open,
            smart_open.open("/some/file.txt", "w", encoding="utf-8") as fout,
        ):
            mock_open.assert_called_with("/some/file.txt", "w", buffering=-1, encoding="utf-8")
            fout.write(self.as_text)

    def test_append_utf8(self):
        """Append utf8."""
        with (
            mock.patch(_BUILTIN_OPEN, mock.Mock(return_value=self.stringio)) as mock_open,
            smart_open.open("/some/file.txt", "w+", encoding="utf-8") as fout,
        ):
            mock_open.assert_called_with("/some/file.txt", "w+", buffering=-1, encoding="utf-8")
            fout.write(self.as_text)

    def test_append_binary_absolute_path(self):
        """Append binary absolute path."""
        with (
            mock.patch(_BUILTIN_OPEN, mock.Mock(return_value=self.bytesio)) as mock_open,
            smart_open.open("/some/file.txt", "wb+") as fout,
        ):
            mock_open.assert_called_with("/some/file.txt", "wb+", buffering=-1)
            fout.write(self.as_bytes)

    def test_newline(self):
        """Newline."""
        with mock.patch(_BUILTIN_OPEN, mock.Mock(return_value=self.bytesio)) as mock_open:
            smart_open.open("/some/file.txt", "wb+", newline="\n")
            mock_open.assert_called_with("/some/file.txt", "wb+", buffering=-1, newline="\n")

    def test_newline_csv(self):
        """Newline csv."""
        #
        # See https://github.com/piskvorky/smart_open/issues/477
        #
        rows = [{"name": "alice\u2028beatrice", "color": "aqua"}, {"name": "bob", "color": "blue"}]
        expected = "name,color\r\nalice\u2028beatrice,aqua\r\nbob,blue\r\n"

        with named_temporary_file(mode="w") as tmp:
            # The csv module recommends using newline='' when opening files and letting
            # the csv writer handle line endings. By default it uses the 'excel' dialect which
            # emits \r\n as line terminator.
            with smart_open.open(tmp.name, "w+", encoding="utf-8", newline="") as fout:
                out = csv.DictWriter(fout, fieldnames=["name", "color"])
                out.writeheader()
                out.writerows(rows)

            with open(tmp.name, encoding="utf-8", newline="") as fin:  # noqa: PTH123  # test fixture path open
                content = fin.read()

        assert content == expected

    @mock.patch("boto3.client")
    def test_s3_mode_mock(self, mock_client):
        """Are s3:// open modes passed correctly?"""
        # correct write mode, correct s3 URI
        transport_params = {
            "client_kwargs": {
                "S3.Client": {"endpoint_url": "http://s3.amazonaws.com"},
            }
        }
        smart_open.open("s3://mybucket/mykey", "w", transport_params=transport_params)
        mock_client.assert_called_with(
            "s3",
            endpoint_url="http://s3.amazonaws.com",
            config=mock_client.call_args.kwargs["config"],
        )

    @mock.patch("smart_open.hdfs.subprocess")
    def test_hdfs(self, mock_subprocess):
        """Is HDFS write called correctly."""
        smart_open_object = smart_open.open("hdfs:///tmp/test.txt", "wb")
        smart_open_object.write("test")
        # called with the correct params?
        mock_subprocess.Popen.assert_called_with(
            ["hdfs", "dfs", "-put", "-f", "-", "/tmp/test.txt"],  # tmp path in test
            stdin=mock_subprocess.PIPE,
        )

        # netloc host - pass the full URI so the CLI routes to the right cluster
        smart_open_object = smart_open.open("hdfs://host/tmp/test.txt", "wb")
        smart_open_object.write("test")
        mock_subprocess.Popen.assert_called_with(
            ["hdfs", "dfs", "-put", "-f", "-", "hdfs://host/tmp/test.txt"], stdin=mock_subprocess.PIPE
        )

        # netloc host:port - pass the full URI so the CLI routes to the right cluster
        smart_open_object = smart_open.open("hdfs://host:8020/tmp/test.txt", "wb")
        smart_open_object.write("test")
        mock_subprocess.Popen.assert_called_with(
            ["hdfs", "dfs", "-put", "-f", "-", "hdfs://host:8020/tmp/test.txt"], stdin=mock_subprocess.PIPE
        )

    @mock_s3
    def test_s3_modes_moto(self):
        """Do s3:// open modes work correctly?"""
        # fake bucket and key
        s3 = _resource("s3")
        s3.create_bucket(Bucket="mybucket")
        raw_data = b"second test"

        # correct write mode, correct s3 URI
        with smart_open.open("s3://mybucket/newkey", "wb") as fout:
            logger.debug("fout: %r", fout)
            fout.write(raw_data)

        logger.debug("write successfully completed")

        output = list(smart_open.open("s3://mybucket/newkey", "rb"))

        assert output == [raw_data]

    @mock_s3
    def test_s3_metadata_write(self):
        """S3 metadata write."""
        # Read local file fixture
        path = os.path.join(CURR_DIR, "test_data/crime-and-punishment.txt.gz")  # noqa: PTH118  # test fixture path join
        data = ""
        with smart_open.open(path, "rb") as fd:
            data = fd.read()

        # Create a test bucket
        s3 = _resource("s3")
        s3.create_bucket(Bucket="mybucket")

        tp = {
            "client_kwargs": {
                "S3.Client.create_multipart_upload": {
                    "ContentType": "text/plain",
                    "ContentEncoding": "gzip",
                }
            }
        }

        # Write data, with multipart_upload options
        write_stream = smart_open.open(
            "s3://mybucket/crime-and-punishment.txt.gz",
            "wb",
            transport_params=tp,
        )
        with write_stream as fout:
            fout.write(data)

        key = s3.Object("mybucket", "crime-and-punishment.txt.gz")
        assert "text/plain" in key.content_type
        assert key.content_encoding == "gzip"

    @mock_s3
    def test_write_bad_encoding_strict(self):
        """Should abort on encoding error."""
        text = "欲しい気持ちが成長しすぎて"

        with (
            pytest.raises(UnicodeEncodeError),
            named_temporary_file("wb", delete=True) as infile,
            smart_open.open(infile.name, "w", encoding="koi8-r", errors="strict") as fout,
        ):
            fout.write(text)

    @mock_s3
    def test_write_bad_encoding_replace(self):
        """Should replace characters that failed to encode."""
        text = "欲しい気持ちが成長しすぎて"
        expected = "?" * len(text)

        with named_temporary_file("wb", delete=True) as infile:
            with smart_open.open(infile.name, "w", encoding="koi8-r", errors="replace") as fout:
                fout.write(text)
            with smart_open.open(infile.name, "r", encoding="koi8-r") as fin:
                actual = fin.read()

        assert expected == actual


class WebHdfsWriteTest(unittest.TestCase):
    """Test writing into webhdfs files."""

    @responses.activate
    def test_initialize_write(self):
        """Initialize write."""

        def request_callback(_):
            resp_body = ""
            headers = {"location": "http://127.0.0.1:8440/file"}
            return 307, headers, resp_body

        responses.add_callback(
            responses.PUT,
            "http://127.0.0.1:8440/webhdfs/v1/path/file",
            callback=request_callback,
        )
        responses.add(
            responses.PUT,
            "http://127.0.0.1:8440/file",
            status=201,
        )
        smart_open.open("webhdfs://127.0.0.1:8440/path/file", "wb")

        assert len(responses.calls) == 2  # test uses inline magic value
        path, params = responses.calls[0].request.url.split("?")
        assert path == "http://127.0.0.1:8440/webhdfs/v1/path/file"
        assert params in {"overwrite=True&op=CREATE", "op=CREATE&overwrite=True"}
        assert responses.calls[1].request.url == "http://127.0.0.1:8440/file"

    @responses.activate
    def test_write(self):
        """Write."""

        def request_callback(_):
            resp_body = ""
            headers = {"location": "http://127.0.0.1:8440/file"}
            return 307, headers, resp_body

        responses.add_callback(
            responses.PUT,
            "http://127.0.0.1:8440/webhdfs/v1/path/file",
            callback=request_callback,
        )
        responses.add(responses.PUT, "http://127.0.0.1:8440/file", status=201)
        smart_open_object = smart_open.open("webhdfs://127.0.0.1:8440/path/file", "wb")

        def write_callback(request):
            assert request.body == "žluťoučký koníček".encode()
            headers = {}
            return 200, headers, ""

        test_string = "žluťoučký koníček".encode()
        responses.add_callback(
            responses.POST,
            "http://127.0.0.1:8440/webhdfs/v1/path/file",
            callback=request_callback,
        )
        responses.add_callback(
            responses.POST,
            "http://127.0.0.1:8440/file",
            callback=write_callback,
        )
        smart_open_object.write(test_string)
        smart_open_object.close()

        assert len(responses.calls) == 4  # test uses inline magic value
        assert responses.calls[2].request.url == "http://127.0.0.1:8440/webhdfs/v1/path/file?op=APPEND"
        assert responses.calls[3].request.url == "http://127.0.0.1:8440/file"


_DECOMPRESSED_DATA = "не слышны в саду даже шорохи".encode()
_MOCK_TIME = mock.Mock(return_value=1620256567)


def gzip_compress(data, filename=None):
    """Gzip compress."""
    #
    # gzip.compress is sensitive to the current time and the destination filename.
    # This function fixes those variables for consistent compression results.
    #
    buf = io.BytesIO()
    buf.name = filename
    with mock.patch("time.time", _MOCK_TIME), gzip.GzipFile(fileobj=buf, mode="w") as gz:
        gz.write(data)
    return buf.getvalue()


class CompressionFormatTest(unittest.TestCase):
    """Test transparent (de)compression."""

    def write_read_assertion(self, suffix):
        """Write read assertion."""
        with named_temporary_file(suffix=suffix) as tmp:
            with smart_open.open(tmp.name, "wb") as fout:
                fout.write(SAMPLE_BYTES)

            with open(tmp.name, "rb") as fin:  # noqa: PTH123  # test fixture path open
                assert fin.read() != SAMPLE_BYTES  # is the content really compressed? (built-in fails)

            with smart_open.open(tmp.name, "rb") as fin:
                assert fin.read() == SAMPLE_BYTES  # ... smart_open correctly opens and decompresses

    def test_open_gz(self):
        """Can open gzip?"""
        fpath = os.path.join(CURR_DIR, "test_data/crlf_at_1k_boundary.warc.gz")  # noqa: PTH118  # test fixture path join
        with smart_open.open(fpath, "rb") as infile:
            data = infile.read()
        m = hashlib.md5(data, usedforsecurity=False)
        assert m.hexdigest() == "18473e60f8c7c98d29d65bf805736a0d", "Failed to read gzip"

    def test_write_read_gz(self):
        """Can write and read gzip?"""
        self.write_read_assertion(".gz")

    def test_write_read_bz2(self):
        """Can write and read bz2?"""
        self.write_read_assertion(".bz2")

    def test_gzip_text(self):
        """Gzip text."""
        with named_temporary_file(suffix=".gz") as f:
            with smart_open.open(f.name, "wt") as fout:
                fout.write("hello world")

            with smart_open.open(f.name, "rt") as fin:
                assert fin.read() == "hello world"


class MultistreamsBZ2Test(unittest.TestCase):
    """Test that multistream bzip2 compressed files can be read."""

    # note: these tests are derived from the Python 3.x tip bz2 tests.

    TEXT_LINES = (
        b"root:x:0:0:root:/root:/bin/bash\n",
        b"bin:x:1:1:bin:/bin:\n",
        b"daemon:x:2:2:daemon:/sbin:\n",
        b"adm:x:3:4:adm:/var/adm:\n",
        b"lp:x:4:7:lp:/var/spool/lpd:\n",
        b"sync:x:5:0:sync:/sbin:/bin/sync\n",
        b"shutdown:x:6:0:shutdown:/sbin:/sbin/shutdown\n",
        b"halt:x:7:0:halt:/sbin:/sbin/halt\n",
        b"mail:x:8:12:mail:/var/spool/mail:\n",
        b"news:x:9:13:news:/var/spool/news:\n",
        b"uucp:x:10:14:uucp:/var/spool/uucp:\n",
        b"operator:x:11:0:operator:/root:\n",
        b"games:x:12:100:games:/usr/games:\n",
        b"gopher:x:13:30:gopher:/usr/lib/gopher-data:\n",
        b"ftp:x:14:50:FTP User:/var/ftp:/bin/bash\n",
        b"nobody:x:65534:65534:Nobody:/home:\n",
        b"postfix:x:100:101:postfix:/var/spool/postfix:\n",
        b"niemeyer:x:500:500::/home/niemeyer:/bin/bash\n",
        b"postgres:x:101:102:PostgreSQL Server:/var/lib/pgsql:/bin/bash\n",
        b"mysql:x:102:103:MySQL server:/var/lib/mysql:/bin/bash\n",
        b"www:x:103:104::/var/www:/bin/false\n",
    )

    TEXT = b"".join(TEXT_LINES)

    DATA = (
        b"BZh91AY&SY.\xc8N\x18\x00\x01>_\x80\x00\x10@\x02\xff\xf0\x01\x07n\x00?\xe7\xff\xe00\x01\x99\xaa\x00"
        b"\xc0\x03F\x86\x8c#&\x83F\x9a\x03\x06\xa6\xd0\xa6\x93M\x0fQ\xa7\xa8\x06\x804hh\x12$\x11\xa4i4\xf14S"
        b"\xd2<Q\xb5\x0fH\xd3\xd4\xdd\xd5\x87\xbb\xf8\x94\r\x8f\xafI\x12\xe1\xc9\xf8/E\x00pu\x89\x12]\xc9"
        b"\xbbDL\nQ\x0e\t1\x12\xdf\xa0\xc0\x97\xac2O9\x89\x13\x94\x0e\x1c7\x0ed\x95I\x0c\xaaJ\xa4\x18L\x10"
        b"\x05#\x9c\xaf\xba\xbc/\x97\x8a#C\xc8\xe1\x8cW\xf9\xe2\xd0\xd6M\xa7\x8bXa<e\x84t\xcbL\xb3\xa7\xd9"
        b"\xcd\xd1\xcb\x84.\xaf\xb3\xab\xab\xad`n}\xa0lh\tE,\x8eZ\x15\x17VH>\x88\xe5\xcd9gd6\x0b\n\xe9\x9b"
        b'\xd5\x8a\x99\xf7\x08.K\x8ev\xfb\xf7xw\xbb\xdf\xa1\x92\xf1\xdd|/";\xa2\xba\x9f\xd5\xb1#A\xb6\xf6'
        b'\xb3o\xc9\xc5y\\\xebO\xe7\x85\x9a\xbc\xb6f8\x952\xd5\xd7"%\x89>V,\xf7\xa6z\xe2\x9f\xa3\xdf\x11'
        b'\x11"\xd6E)I\xa9\x13^\xca\xf3r\xd0\x03U\x922\xf26\xec\xb6\xed\x8b\xc3U\x13\x9d\xc5\x170\xa4\xfa^'
        b"\x92\xacDF\x8a\x97\xd6\x19\xfe\xdd\xb8\xbd\x1a\x9a\x19\xa3\x80ankR\x8b\xe5\xd83]\xa9\xc6\x08"
        b'\x82f\xf6\xb9"6l$\xb8j@\xc0\x8a\xb0l1..\xbak\x83ls\x15\xbc\xf4\xc1\x13\xbe\xf8E\xb8\x9d\r\xa8\x9dk'
        b"\x84\xd3n\xfa\xacQ\x07\xb1%y\xaav\xb4\x08\xe0z\x1b\x16\xf5\x04\xe9\xcc\xb9\x08z\x1en7.G\xfc]\xc9"
        b"\x14\xe1B@\xbb!8`"
    )

    def create_temp_bz2(self, streams=1):
        """Create temp bz2."""
        with named_temporary_file("wb", suffix=".bz2", delete=False) as f:
            f.write(self.DATA * streams)
        return f.name

    def cleanup_temp_bz2(self, test_file):
        """Cleanup temp bz2."""
        if os.path.isfile(test_file):  # noqa: PTH113  # test fixture isfile
            os.unlink(test_file)  # noqa: PTH108  # test fixture unlink

    def test_can_read_multistream_bz2(self):
        """Can read multistream bz2."""
        from bz2 import BZ2File

        test_file = self.create_temp_bz2(streams=5)
        with BZ2File(test_file) as bz2f:
            assert bz2f.read() == self.TEXT * 5
        self.cleanup_temp_bz2(test_file)

    def test_file_smart_open_can_read_multistream_bz2(self):
        """File smart open can read multistream bz2."""
        test_file = self.create_temp_bz2(streams=5)
        with smart_open_lib.open(test_file, "rb") as bz2f:
            assert bz2f.read() == self.TEXT * 5
        self.cleanup_temp_bz2(test_file)


class S3OpenTest(unittest.TestCase):
    """Tests for S3Open."""

    @mock_s3
    def test_r(self):
        """Reading a UTF string should work."""
        text = "физкульт-привет!"

        s3 = _resource("s3")
        s3.create_bucket(Bucket="bucket")
        key = s3.Object("bucket", "key")
        key.put(Body=text.encode("utf-8"))

        with smart_open.open("s3://bucket/key", "rb") as fin:
            assert fin.read() == text.encode("utf-8")

        with smart_open.open("s3://bucket/key", "r", encoding="utf-8") as fin:
            assert fin.read() == text

    def test_bad_mode(self):
        """Bad mode should raise and exception."""
        uri = smart_open_lib._parse_uri("s3://bucket/key")  # test reaches into private state
        with pytest.raises(NotImplementedError):
            smart_open.open(uri, "x")

    @mock_s3
    def test_rw_encoding(self):
        """Should read and write text, respecting encodings, etc."""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="bucket")

        key = "s3://bucket/key"
        text = "расцветали яблони и груши"

        with smart_open.open(key, "w", encoding="koi8-r") as fout:
            fout.write(text)

        with smart_open.open(key, "r", encoding="koi8-r") as fin:
            assert text == fin.read()

        with smart_open.open(key, "rb") as fin:
            assert text.encode("koi8-r") == fin.read()

        with smart_open.open(key, "r", encoding="euc-jp") as fin, pytest.raises(UnicodeDecodeError):
            fin.read()
        with smart_open.open(key, "r", encoding="euc-jp", errors="replace") as fin:
            fin.read()

    @mock_s3
    def test_rw_gzip(self):
        """Should read/write gzip files, implicitly and explicitly."""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="bucket")
        key = "s3://bucket/key.gz"

        text = "не слышны в саду даже шорохи"
        with smart_open.open(key, "wb") as fout:
            fout.write(text.encode("utf-8"))

        #
        # Check that what we've created is a gzip.
        #
        with smart_open.open(key, "rb", compression="disable") as fin:
            gz = gzip.GzipFile(fileobj=fin)
            assert gz.read().decode("utf-8") == text

        #
        # We should be able to read it back as well.
        #
        with smart_open.open(key, "rb") as fin:
            assert fin.read().decode("utf-8") == text

    @mock_s3
    @mock.patch("smart_open.utils.inspect_kwargs", mock.Mock(return_value={}))
    def test_gzip_write_mode(self):
        """Should always open in binary mode when writing through a codec."""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="bucket")

        with mock.patch("smart_open.s3.open", return_value=open(__file__, "rb")) as mock_open:  # noqa: PTH123  # test fixture path open
            smart_open.open("s3://bucket/key.gz", "wb")
            mock_open.assert_called_with("bucket", "key.gz", "wb")

    @mock_s3
    @mock.patch("smart_open.utils.inspect_kwargs", mock.Mock(return_value={}))
    def test_gzip_read_mode(self):
        """Should always open in binary mode when reading through a codec."""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="bucket")
        key = "s3://bucket/key.gz"

        text = "если-б я был султан и имел трёх жён, то тройной красотой был бы окружён"  # noqa: RUF001  # fixture deliberately uses Cyrillic characters
        with smart_open.open(key, "wb") as fout:
            fout.write(text.encode("utf-8"))

        with mock.patch("smart_open.s3.open", return_value=open(__file__)) as mock_open:  # noqa: PTH123  # test fixture path open
            smart_open.open(key, "r")
            mock_open.assert_called_with("bucket", "key.gz", "rb")

    @mock_s3
    def test_read_encoding(self):
        """Should open the file with the correct encoding, explicit text read."""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="bucket")
        key = "s3://bucket/key.txt"
        text = "это знала ева, это знал адам, колеса любви едут прямо по нам"
        with smart_open.open(key, "wb") as fout:
            fout.write(text.encode("koi8-r"))
        with smart_open.open(key, "r", encoding="koi8-r") as fin:
            actual = fin.read()
        assert text == actual

    @mock_s3
    def test_read_encoding_implicit_text(self):
        """Should open the file with the correct encoding, implicit text read."""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="bucket")
        key = "s3://bucket/key.txt"
        text = "это знала ева, это знал адам, колеса любви едут прямо по нам"
        with smart_open.open(key, "wb") as fout:
            fout.write(text.encode("koi8-r"))
        with smart_open.open(key, encoding="koi8-r") as fin:
            actual = fin.read()
        assert text == actual

    @mock_s3
    def test_write_encoding(self):
        """Should open the file for writing with the correct encoding."""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="bucket")
        key = "s3://bucket/key.txt"
        text = "какая боль, какая боль, аргентина - ямайка, 5-0"

        with smart_open.open(key, "w", encoding="koi8-r") as fout:
            fout.write(text)
        with smart_open.open(key, encoding="koi8-r") as fin:
            actual = fin.read()
        assert text == actual

    @mock_s3
    def test_write_bad_encoding_strict(self):
        """Should open the file for writing with the correct encoding."""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="bucket")
        key = "s3://bucket/key.txt"
        text = "欲しい気持ちが成長しすぎて"

        with (
            pytest.raises(UnicodeEncodeError),
            smart_open.open(key, "w", encoding="koi8-r", errors="strict") as fout,
        ):
            fout.write(text)

    @mock_s3
    def test_write_bad_encoding_replace(self):
        """Should open the file for writing with the correct encoding."""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="bucket")
        key = "s3://bucket/key.txt"
        text = "欲しい気持ちが成長しすぎて"
        expected = "?" * len(text)

        with smart_open.open(key, "w", encoding="koi8-r", errors="replace") as fout:
            fout.write(text)
        with smart_open.open(key, encoding="koi8-r") as fin:
            actual = fin.read()
        assert expected == actual

    @mock_s3
    def test_write_text_gzip(self):
        """Should open the file for writing with the correct encoding."""
        s3 = _resource("s3")
        s3.create_bucket(Bucket="bucket")
        key = "s3://bucket/key.txt.gz"
        text = "какая боль, какая боль, аргентина - ямайка, 5-0"

        with smart_open.open(key, "w", encoding="utf-8") as fout:
            fout.write(text)
        with smart_open.open(key, "r", encoding="utf-8") as fin:
            actual = fin.read()
        assert text == actual

    @mock.patch("smart_open.s3.Reader")
    def test_transport_params_is_not_mutable(self, mock_open):
        """Transport params is not mutable."""
        smart_open.open("s3://access_key:secret_key@bucket/key")
        actual = mock_open.call_args_list[0][1]["client_kwargs"]
        expected = {
            "S3.Client": {
                "aws_access_key_id": "access_key",
                "aws_secret_access_key": "secret_key",
            }
        }
        assert actual == expected

        smart_open.open("s3://bucket/key")
        actual = mock_open.call_args_list[1][1].get("client_kwargs")
        assert actual is None

    @mock.patch("smart_open.s3.Reader")
    def test_version_id_from_url_is_forwarded(self, mock_open):
        """The ?versionId=... query param is forwarded as version_id to s3.open()."""
        smart_open.open("s3://bucket/key?versionId=v123")
        assert mock_open.call_args[1]["version_id"] == "v123"

    @mock.patch("smart_open.s3.Reader")
    def test_version_id_in_transport_params_wins(self, mock_open):
        """transport_params['version_id'] takes precedence over the URL value."""
        smart_open.open(
            "s3://bucket/key?versionId=from-url",
            transport_params={"version_id": "from-tp"},
        )
        assert mock_open.call_args[1]["version_id"] == "from-tp"


def function(a, b, c, foo="bar", baz="boz"):
    """Function."""


class CheckKwargsTest(unittest.TestCase):
    """Tests for Check Kwargs."""

    def test(self):
        """Test."""
        kwargs = {"foo": 123, "bad": False}
        expected = {"foo": 123}
        actual = smart_open.utils.check_kwargs(function, kwargs)
        assert expected == actual


def initialize_bucket():
    """Initialize bucket."""
    s3 = _resource("s3")
    bucket = s3.create_bucket(Bucket="bucket")
    bucket.wait_until_exists()

    bucket.Object("gzipped").put(Body=gzip_compress(_DECOMPRESSED_DATA))
    bucket.Object("bzipped").put(Body=bz2.compress(_DECOMPRESSED_DATA))


@mock_s3
@mock.patch("time.time", _MOCK_TIME)
def test_s3_gzip_compress_sanity():
    """Does our gzip_compress function actually produce gzipped data?"""
    initialize_bucket()
    assert gzip.decompress(gzip_compress(_DECOMPRESSED_DATA)) == _DECOMPRESSED_DATA


@mock_s3
@mock.patch("time.time", _MOCK_TIME)
@pytest.mark.parametrize(
    ("url", "_compression"),
    [
        ("s3://bucket/gzipped", ".gz"),
        ("s3://bucket/bzipped", ".bz2"),
    ],
)
def test_s3_read_explicit(url, _compression):
    """Can we read using the explicitly specified compression?"""
    initialize_bucket()
    with smart_open.open(url, "rb", compression=_compression) as fin:
        assert fin.read() == _DECOMPRESSED_DATA


@mock_s3
@mock.patch("time.time", _MOCK_TIME)
@pytest.mark.parametrize(
    ("_compression", "expected"),
    [
        (".gz", gzip_compress(_DECOMPRESSED_DATA, "key")),
        (".bz2", bz2.compress(_DECOMPRESSED_DATA)),
    ],
)
def test_s3_write_explicit(_compression, expected):
    """Can we write using the explicitly specified compression?"""
    initialize_bucket()

    with smart_open.open("s3://bucket/key", "wb", compression=_compression) as fout:
        fout.write(_DECOMPRESSED_DATA)

    with smart_open.open("s3://bucket/key", "rb", compression=NO_COMPRESSION) as fin:
        assert fin.read() == expected


@mock_s3
@mock.patch("time.time", _MOCK_TIME)
@pytest.mark.parametrize(
    ("url", "_compression", "expected"),
    [
        ("s3://bucket/key.gz", ".gz", gzip_compress(_DECOMPRESSED_DATA, "key.gz")),
        ("s3://bucket/key.bz2", ".bz2", bz2.compress(_DECOMPRESSED_DATA)),
    ],
)
def test_s3_write_implicit(url, _compression, expected):
    """Can we determine the compression from the file extension?"""
    initialize_bucket()

    with smart_open.open(url, "wb", compression=INFER_FROM_EXTENSION) as fout:
        fout.write(_DECOMPRESSED_DATA)

    with smart_open.open(url, "rb", compression=NO_COMPRESSION) as fin:
        assert fin.read() == expected


@mock_s3
@mock.patch("time.time", _MOCK_TIME)
@pytest.mark.parametrize(
    ("url", "_compression", "expected"),
    [
        ("s3://bucket/key.gz", ".gz", gzip_compress(_DECOMPRESSED_DATA, "key.gz")),
        ("s3://bucket/key.bz2", ".bz2", bz2.compress(_DECOMPRESSED_DATA)),
    ],
)
def test_s3_disable_compression(url, _compression, expected):
    """Can we handle the compression parameter when reading/writing?"""
    initialize_bucket()

    with smart_open.open(url, "wb") as fout:
        fout.write(_DECOMPRESSED_DATA)

    with smart_open.open(url, "rb", compression="disable") as fin:
        assert fin.read() == expected


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("r", "rb"),
        ("r+", "rb+"),
        ("rt", "rb"),
        ("rt+", "rb+"),
        ("r+t", "rb+"),
        ("w", "wb"),
        ("w+", "wb+"),
        ("wt", "wb"),
        ("wt+", "wb+"),
        ("w+t", "wb+"),
        ("a", "ab"),
        ("a+", "ab+"),
        ("at", "ab"),
        ("at+", "ab+"),
        ("a+t", "ab+"),
    ],
)
def test_get_binary_mode(mode, expected):
    """Get binary mode."""
    actual = smart_open.smart_open_lib._get_binary_mode(mode)  # test reaches into private state
    assert actual == expected


@pytest.mark.parametrize(
    "mode",
    [
        ("rw",),
        ("rwa",),
        ("rbt",),
        ("r++",),
        ("+",),
        ("x",),
    ],
)
def test_get_binary_mode_bad(mode):
    """Get binary mode bad."""
    with pytest.raises(ValueError):  # noqa: PT011  # legacy broad pytest.raises
        smart_open.smart_open_lib._get_binary_mode(mode)  # test reaches into private state


@pytest.mark.skipif(os.name == "nt", reason="this test does not work on Windows")
def test_read_file_descriptor():
    """Read file descriptor."""
    with smart_open.open(__file__) as fin:
        expected = fin.read()

    fd = os.open(__file__, os.O_RDONLY)
    with smart_open.open(fd) as fin:
        actual = fin.read()

    assert actual == expected


@pytest.mark.skipif(os.name == "nt", reason="this test does not work on Windows")
def test_write_file_descriptor():
    """Write file descriptor."""
    with named_temporary_file() as tmp:
        with smart_open.open(os.open(tmp.name, os.O_WRONLY), "wt") as fout:
            fout.write("hello world")

        with smart_open.open(tmp.name, "rt") as fin:
            assert fin.read() == "hello world"


@mock_s3
def test_to_boto3():
    """To boto3."""
    resource = _resource("s3")
    resource.create_bucket(Bucket="mybucket")
    #
    # If we don't specify encoding explicitly, the platform-dependent encoding
    # will be used, and it may not necessarily support Unicode, breaking this
    # test under Windows on github actions.
    #
    with smart_open.open("s3://mybucket/key.txt", "wt", encoding="utf-8") as fout:
        fout.write("я бегу по вызженной земле, гермошлем захлопнув на ходу")  # noqa: RUF001  # fixture deliberately uses Cyrillic characters
        obj = fout.to_boto3(resource)
    assert obj.bucket_name == "mybucket"
    assert obj.key == "key.txt"


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=logging.DEBUG)
    unittest.main()
