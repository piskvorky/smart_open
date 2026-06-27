#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Integration tests ported from our old unit tests.

Before running these tests against a real bucket, make sure its initialized
with initialize_s3_bucket.py.

"""

import gzip
import io
import unittest
import uuid
import warnings
from pathlib import Path

import boto3
import pytest
from initialize_s3_bucket import CONTENTS

import smart_open
import smart_open.concurrency
import smart_open.constants

BUCKET_NAME = "smartopen-integration-tests"


def setUpModule():
    """Sanity-check that the test bucket has been initialized."""
    assert boto3.resource("s3").Bucket(BUCKET_NAME).creation_date, "see initialize_s3_bucket.py"


def ignore_resource_warnings():
    """Silence SSL socket ResourceWarning emitted by boto3."""
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")


class ReaderTest(unittest.TestCase):
    """Tests for smart_open's S3 Reader."""

    def setUp(self):
        """Silence noisy SSL resource warnings before each test."""
        ignore_resource_warnings()

    def test_iter(self):
        """Are S3 files iterated over correctly?"""
        key_name = "hello.txt"
        expected = CONTENTS[key_name].split(b"\n")

        fin = smart_open.s3.Reader(BUCKET_NAME, key_name)
        actual = [line.rstrip(b"\n") for line in fin]
        assert expected == actual

    def test_iter_context_manager(self):
        """Are S3 files iterated over correctly when used as a context manager?"""
        # same thing but using a context manager
        key_name = "hello.txt"
        expected = CONTENTS[key_name].split(b"\n")

        with smart_open.s3.Reader(BUCKET_NAME, key_name) as fin:
            actual = [line.rstrip(b"\n") for line in fin]
        assert expected == actual

    def test_read(self):
        """Are S3 files read correctly?"""
        key_name = "hello.txt"
        expected = CONTENTS[key_name]

        fin = smart_open.s3.Reader(BUCKET_NAME, key_name)
        assert expected[:6] == fin.read(6)
        assert expected[6:14] == fin.read(8)  # ř is 2 bytes
        assert expected[14:] == fin.read()  # read the rest

    def test_seek_beginning(self):
        """Does seeking to the beginning of S3 files work correctly?"""
        key_name = "hello.txt"
        expected = CONTENTS[key_name]

        fin = smart_open.s3.Reader(BUCKET_NAME, key_name)
        assert expected[:6] == fin.read(6)
        assert expected[6:14] == fin.read(8)  # ř is 2 bytes

        fin.seek(0)
        assert expected == fin.read()  # no size given => read whole file

        fin.seek(0)
        assert expected == fin.read(-1)  # same thing

    def test_seek_start(self):
        """Does seeking from the start of S3 files work correctly?"""
        fin = smart_open.s3.Reader(BUCKET_NAME, "hello.txt")
        seek = fin.seek(6)
        assert seek == 6  # byte offset
        assert fin.tell() == 6  # byte offset
        assert fin.read(6) == "wořld".encode()

    def test_seek_current(self):
        """Does seeking from the middle of S3 files work correctly?"""
        fin = smart_open.s3.Reader(BUCKET_NAME, "hello.txt")
        assert fin.read(5) == b"hello"
        seek = fin.seek(1, whence=smart_open.constants.WHENCE_CURRENT)
        assert seek == 6  # byte offset
        assert fin.read(6) == "wořld".encode()

    def test_seek_end(self):
        """Does seeking from the end of S3 files work correctly?"""
        key_name = "hello.txt"
        expected = CONTENTS[key_name]

        fin = smart_open.s3.Reader(BUCKET_NAME, key_name)
        seek = fin.seek(-4, whence=smart_open.constants.WHENCE_END)
        assert seek == len(expected) - 4
        assert fin.read() == b"you?"

    def test_detect_eof(self):
        """Does reading to end report the same position as seek(0, END)?"""
        key_name = "hello.txt"
        expected = CONTENTS[key_name]

        fin = smart_open.s3.Reader(BUCKET_NAME, key_name)
        fin.read()
        eof = fin.tell()
        assert eof == len(expected)
        fin.seek(0, whence=smart_open.constants.WHENCE_END)
        assert eof == fin.tell()

    def test_read_gzip(self):
        """Reading a gzip object through smart_open then GzipFile decompresses correctly."""
        key_name = "hello.txt.gz"

        with gzip.GzipFile(fileobj=io.BytesIO(CONTENTS[key_name])) as fin:
            expected = fin.read()

        with (
            smart_open.s3.Reader(BUCKET_NAME, key_name) as fin,
            gzip.GzipFile(fileobj=fin) as zipfile,
        ):
            actual = zipfile.read()

        assert expected == actual

    def test_readline(self):
        """readline() advances tell() and full iteration yields all lines."""
        key_name = "multiline.txt"
        expected = CONTENTS[key_name]

        with smart_open.s3.Reader(BUCKET_NAME, key_name) as fin:
            fin.readline()
            assert fin.tell() == expected.index(b"\n") + 1

            fin.seek(0)
            actual = list(fin)
            assert fin.tell() == len(expected)

        expected = [b"englishman\n", b"in\n", b"new\n", b"york\n"]
        assert expected == actual

    def test_readline_tiny_buffer(self):
        """Iteration yields the same lines even with a very small read buffer."""
        key_name = "multiline.txt"
        expected = CONTENTS[key_name]

        with smart_open.s3.Reader(BUCKET_NAME, key_name, buffer_size=8) as fin:
            actual = list(fin)

        expected = [b"englishman\n", b"in\n", b"new\n", b"york\n"]
        assert expected == actual

    def test_read0_does_not_return_data(self):
        """Reader.read(0) returns an empty bytes object."""
        with smart_open.s3.Reader(BUCKET_NAME, "hello.txt") as fin:
            data = fin.read(0)

        assert data == b""

    def test_to_boto3(self):
        """Reader.to_boto3 returns a boto3 Object that reads identical bytes."""
        key_name = "multiline.txt"
        expected = CONTENTS[key_name]

        with smart_open.s3.Reader(BUCKET_NAME, key_name) as fin:
            returned_obj = fin.to_boto3(boto3.resource("s3"))

        boto3_body = returned_obj.get()["Body"].read()
        assert expected == boto3_body


def read_key(key):
    """Read ``key`` from the test bucket via boto3 and return its bytes."""
    return boto3.resource("s3").Object(BUCKET_NAME, key).get()["Body"].read()


class WriterTest(unittest.TestCase):
    """Tests for smart_open's S3 MultipartWriter."""

    def setUp(self):
        """Pick a unique key per test to avoid cross-talk between parallel runs."""
        self.key = "writer-test/" + uuid.uuid4().hex

    def tearDown(self):
        """Delete the per-test object so the bucket stays tidy."""
        boto3.resource("s3").Object(BUCKET_NAME, self.key).delete()

    def test_write(self):
        """Does writing into s3 work correctly?"""
        test_string = "žluťoučký koníček".encode()

        with smart_open.s3.MultipartWriter(BUCKET_NAME, self.key) as fout:
            fout.write(test_string)

        data = read_key(self.key)
        assert data == test_string

    def test_multipart(self):
        """Does s3 multipart chunking work correctly?"""
        data_dir = Path(__file__).parent / ".." / "tests" / "test_data"
        with (data_dir / "crime-and-punishment.txt").open("rb") as fin:
            crime = fin.read()
        data = b""
        ps = 5 * 1024 * 1024
        while len(data) < ps:
            data += crime

        title = "Преступление и наказание\n\n".encode()
        to_be_continued = "\n\n... продолжение следует ...\n\n".encode()

        key = "WriterTest.test_multipart"
        with smart_open.s3.MultipartWriter(BUCKET_NAME, key, part_size=ps) as fout:
            #
            # Write some data without triggering an upload
            #
            fout.write(title)
            assert fout._total_parts == 0  # asserting writer internals
            assert fout._buf.tell() == 48  # expected buffer size

            #
            # Trigger a part upload
            #
            fout.write(data)
            assert fout._total_parts == 1  # asserting writer internals
            assert fout._buf.tell() == 661  # expected buffer size

            #
            # Write _without_ triggering a part upload
            #
            fout.write(to_be_continued)
            assert fout._total_parts == 1  # asserting writer internals
            assert fout._buf.tell() == 710  # expected buffer size

        #
        # We closed the writer, so the final part must have been uploaded
        #
        assert fout._buf.tell() == 0  # asserting writer internals
        assert fout._total_parts == 2  # expected part count

        #
        # read back the same key and check its content
        #
        with smart_open.s3.open(BUCKET_NAME, key, "rb") as fin:
            got = fin.read()
        want = title + data + to_be_continued
        assert want == got

    def test_empty_key(self):
        """Does writing no data cause key with an empty value to be created?"""
        smart_open_write = smart_open.s3.MultipartWriter(BUCKET_NAME, self.key)
        with smart_open_write:
            pass

        # read back the same key and check its content
        data = read_key(self.key)
        assert data == b""

    def test_buffered_writer_wrapper_works(self):
        """Ensure that we can wrap a smart_open s3 stream in a BufferedWriter."""
        expected = "не думай о секундах свысока"  # noqa: RUF001  # Cyrillic fixture

        with (
            smart_open.s3.MultipartWriter(BUCKET_NAME, self.key) as fout,
            io.BufferedWriter(fout) as sub_out,
        ):
            sub_out.write(expected.encode("utf-8"))

        text = read_key(self.key).decode("utf-8")
        assert expected == text

    def test_double_close(self):
        """Calling close() twice should be a no-op the second time."""
        text = "там за туманами, вечными, пьяными".encode()
        fout = smart_open.s3.open(BUCKET_NAME, self.key, "wb")
        fout.write(text)
        fout.close()
        fout.close()

        result = read_key(self.key)
        assert result == text

    def test_flush_close(self):
        """Calling flush() before close() does not break the upload."""
        text = "там за туманами, вечными, пьяными".encode()
        fout = smart_open.s3.open(BUCKET_NAME, self.key, "wb")
        fout.write(text)
        fout.flush()
        fout.close()

        result = read_key(self.key)
        assert result == text


class IterBucketTest(unittest.TestCase):
    """Tests for smart_open.s3.iter_bucket."""

    def setUp(self):
        """Capture the expected (key, value) pairs from the test fixtures."""
        self.expected = [(key, value) for (key, value) in CONTENTS.items() if key.startswith("iter_bucket/")]
        self.expected.sort()

    def test_multithreading(self):
        """iter_bucket returns every object under the prefix when multi-threaded."""
        actual = list(smart_open.s3.iter_bucket(BUCKET_NAME, prefix="iter_bucket"))
        assert len(self.expected) == len(actual)
        assert self.expected == sorted(actual)

    def test_accept_key(self):
        """iter_bucket's accept_key filter restricts the returned objects."""
        expected = [(key, value) for (key, value) in self.expected if "4" in key]
        actual = list(
            smart_open.s3.iter_bucket(BUCKET_NAME, prefix="iter_bucket", accept_key=lambda key: "4" in key)
        )
        assert len(expected) == len(actual)
        assert expected == sorted(actual)


@pytest.mark.parametrize("workers", [1, 4, 8, 16, 64])
def test_workers(workers):
    """iter_bucket returns the same data regardless of the worker count."""
    expected = sorted([(key, value) for (key, value) in CONTENTS.items() if key.startswith("iter_bucket/")])
    actual = sorted(smart_open.s3.iter_bucket(BUCKET_NAME, prefix="iter_bucket", workers=workers))
    assert len(expected) == len(actual)
    assert expected == actual
