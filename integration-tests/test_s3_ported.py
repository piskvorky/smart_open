# -*- coding: utf-8 -*-
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

import contextlib
import gzip
import io
import six
import unittest
import uuid
import warnings

import boto3
from parameterizedtestcase import ParameterizedTestCase as PTestCase

import smart_open
import smart_open.concurrency
import smart_open.constants
from initialize_s3_bucket import CONTENTS

BUCKET_NAME = 'smartopen-integration-tests'


def setUpModule():
    assert boto3.resource('s3').Bucket(BUCKET_NAME).creation_date, 'see initialize_s3_bucket.py'


def ignore_resource_warnings():
    #
    # https://github.com/boto/boto3/issues/454
    # Py2 doesn't have ResourceWarning, so do nothing.
    #
    if six.PY2:
        return
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")  # noqa


class ReaderTest(unittest.TestCase):
    def setUp(self):
        ignore_resource_warnings()

    def test_iter(self):
        """Are S3 files iterated over correctly?"""
        key_name = 'hello.txt'
        expected = CONTENTS[key_name].split(b'\n')

        fin = smart_open.s3.Reader(BUCKET_NAME, key_name)
        actual = [line.rstrip(b'\n') for line in fin]
        self.assertEqual(expected, actual)

    def test_iter_context_manager(self):
        # same thing but using a context manager
        key_name = 'hello.txt'
        expected = CONTENTS[key_name].split(b'\n')

        with smart_open.s3.Reader(BUCKET_NAME, key_name) as fin:
            actual = [line.rstrip(b'\n') for line in fin]
        self.assertEqual(expected, actual)

    def test_read(self):
        """Are S3 files read correctly?"""
        key_name = 'hello.txt'
        expected = CONTENTS[key_name]

        fin = smart_open.s3.Reader(BUCKET_NAME, key_name)
        self.assertEqual(expected[:6], fin.read(6))
        self.assertEqual(expected[6:14], fin.read(8))  # ř is 2 bytes
        self.assertEqual(expected[14:], fin.read())  # read the rest

    def test_seek_beginning(self):
        """Does seeking to the beginning of S3 files work correctly?"""
        key_name = 'hello.txt'
        expected = CONTENTS[key_name]

        fin = smart_open.s3.Reader(BUCKET_NAME, key_name)
        self.assertEqual(expected[:6], fin.read(6))
        self.assertEqual(expected[6:14], fin.read(8))  # ř is 2 bytes

        fin.seek(0)
        self.assertEqual(expected, fin.read())  # no size given => read whole file

        fin.seek(0)
        self.assertEqual(expected, fin.read(-1))  # same thing

    def test_seek_start(self):
        """Does seeking from the start of S3 files work correctly?"""
        fin = smart_open.s3.Reader(BUCKET_NAME, 'hello.txt')
        seek = fin.seek(6)
        self.assertEqual(seek, 6)
        self.assertEqual(fin.tell(), 6)
        self.assertEqual(fin.read(6), u'wořld'.encode('utf-8'))

    def test_seek_current(self):
        """Does seeking from the middle of S3 files work correctly?"""
        fin = smart_open.s3.Reader(BUCKET_NAME, 'hello.txt')
        self.assertEqual(fin.read(5), b'hello')
        seek = fin.seek(1, whence=smart_open.constants.WHENCE_CURRENT)
        self.assertEqual(seek, 6)
        self.assertEqual(fin.read(6), u'wořld'.encode('utf-8'))

    def test_seek_end(self):
        """Does seeking from the end of S3 files work correctly?"""
        key_name = 'hello.txt'
        expected = CONTENTS[key_name]

        fin = smart_open.s3.Reader(BUCKET_NAME, key_name)
        seek = fin.seek(-4, whence=smart_open.constants.WHENCE_END)
        self.assertEqual(seek, len(expected) - 4)
        self.assertEqual(fin.read(), b'you?')

    def test_detect_eof(self):
        key_name = 'hello.txt'
        expected = CONTENTS[key_name]

        fin = smart_open.s3.Reader(BUCKET_NAME, key_name)
        fin.read()
        eof = fin.tell()
        self.assertEqual(eof, len(expected))
        fin.seek(0, whence=smart_open.constants.WHENCE_END)
        self.assertEqual(eof, fin.tell())

    def test_read_gzip(self):
        key_name = 'hello.txt.gz'

        with gzip.GzipFile(fileobj=io.BytesIO(CONTENTS[key_name])) as fin:
            expected = fin.read()

        with smart_open.s3.Reader(BUCKET_NAME, key_name) as fin:
            with gzip.GzipFile(fileobj=fin) as zipfile:
                actual = zipfile.read()

        self.assertEqual(expected, actual)

    def test_readline(self):
        key_name = 'multiline.txt'
        expected = CONTENTS[key_name]

        with smart_open.s3.Reader(BUCKET_NAME, key_name) as fin:
            fin.readline()
            self.assertEqual(fin.tell(), expected.index(b'\n')+1)

            fin.seek(0)
            actual = list(fin)
            self.assertEqual(fin.tell(), len(expected))

        expected = [b'englishman\n', b'in\n', b'new\n', b'york\n']
        self.assertEqual(expected, actual)

    def test_readline_tiny_buffer(self):
        key_name = 'multiline.txt'
        expected = CONTENTS[key_name]

        with smart_open.s3.Reader(BUCKET_NAME, key_name, buffer_size=8) as fin:
            actual = list(fin)

        expected = [b'englishman\n', b'in\n', b'new\n', b'york\n']
        self.assertEqual(expected, actual)

    def test_read0_does_not_return_data(self):
        with smart_open.s3.Reader(BUCKET_NAME, 'hello.txt') as fin:
            data = fin.read(0)

        self.assertEqual(data, b'')

    def test_to_boto3(self):
        key_name = 'multiline.txt'
        expected = CONTENTS[key_name]

        with smart_open.s3.Reader(BUCKET_NAME, key_name) as fin:
            returned_obj = fin.to_boto3()

        boto3_body = returned_obj.get()['Body'].read()
        self.assertEqual(expected, boto3_body)


def read_key(key):
    return boto3.resource('s3').Object(BUCKET_NAME, key).get()['Body'].read()


class WriterTest(unittest.TestCase):
    def setUp(self):
        #
        # Write to a unique key each time to avoid cross-talk between
        # simultaneous test runs.
        #
        self.key = 'writer-test/' + uuid.uuid4().hex

    def tearDown(self):
        boto3.resource('s3').Object(BUCKET_NAME, self.key).delete()

    def test_write(self):
        """Does writing into s3 work correctly?"""
        test_string = u"žluťoučký koníček".encode('utf8')

        with smart_open.s3.MultipartWriter(BUCKET_NAME, self.key) as fout:
            fout.write(test_string)

        data = read_key(self.key)
        self.assertEqual(data, test_string)

    def test_multipart(self):
        """Does s3 multipart chunking work correctly?"""
        with smart_open.s3.MultipartWriter(BUCKET_NAME, self.key, min_part_size=10) as fout:
            fout.write(b"test")
            self.assertEqual(fout._buf.tell(), 4)

            fout.write(b"test\n")
            self.assertEqual(fout._buf.tell(), 9)
            self.assertEqual(fout._total_parts, 0)

            fout.write(b"test")
            self.assertEqual(fout._buf.tell(), 0)
            self.assertEqual(fout._total_parts, 1)

        data = read_key(self.key)
        self.assertEqual(data, b"testtest\ntest")

    def test_empty_key(self):
        """Does writing no data cause key with an empty value to be created?"""
        smart_open_write = smart_open.s3.MultipartWriter(BUCKET_NAME, self.key)
        with smart_open_write as fout:  # noqa
            pass

        # read back the same key and check its content
        data = read_key(self.key)
        self.assertEqual(data, b'')

    def test_buffered_writer_wrapper_works(self):
        """
        Ensure that we can wrap a smart_open s3 stream in a BufferedWriter, which
        passes a memoryview object to the underlying stream in python >= 2.7
        """
        expected = u'не думай о секундах свысока'

        with smart_open.s3.MultipartWriter(BUCKET_NAME, self.key) as fout:
            with io.BufferedWriter(fout) as sub_out:
                sub_out.write(expected.encode('utf-8'))

        text = read_key(self.key).decode('utf-8')
        self.assertEqual(expected, text)

    def test_double_close(self):
        text = u'там за туманами, вечными, пьяными'.encode('utf-8')
        fout = smart_open.s3.open(BUCKET_NAME, self.key, 'wb')
        fout.write(text)
        fout.close()
        fout.close()

        result = read_key(self.key)
        self.assertEqual(result, text)

    def test_flush_close(self):
        text = u'там за туманами, вечными, пьяными'.encode('utf-8')
        fout = smart_open.s3.open(BUCKET_NAME, self.key, 'wb')
        fout.write(text)
        fout.flush()
        fout.close()

        result = read_key(self.key)
        self.assertEqual(result, text)


@contextlib.contextmanager
def force(multiprocessing=False, concurrent_futures=False):
    assert not (multiprocessing and concurrent_futures)
    old_multiprocessing = smart_open.concurrency._MULTIPROCESSING
    old_concurrent_futures = smart_open.concurrency._CONCURRENT_FUTURES
    smart_open.concurrency._MULTIPROCESSING = multiprocessing
    smart_open.concurrency._CONCURRENT_FUTURES = concurrent_futures

    yield

    smart_open.concurrency._MULTIPROCESSING = old_multiprocessing
    smart_open.concurrency._CONCURRENT_FUTURES = old_concurrent_futures


class IterBucketTest(PTestCase):
    def setUp(self):
        self.expected = [
            (key, value)
            for (key, value) in CONTENTS.items()
            if key.startswith('iter_bucket/')
        ]
        self.expected.sort()

    def test_singleprocess(self):
        with force():
            actual = list(smart_open.s3.iter_bucket(BUCKET_NAME, prefix='iter_bucket'))

        self.assertEqual(len(self.expected), len(actual))
        self.assertEqual(self.expected, sorted(actual))

    @unittest.skipIf(not smart_open.concurrency._MULTIPROCESSING, 'multiprocessing unavailable')
    def test_multiprocess(self):
        with force(multiprocessing=True):
            actual = list(smart_open.s3.iter_bucket(BUCKET_NAME, prefix='iter_bucket'))

        self.assertEqual(len(self.expected), len(actual))
        self.assertEqual(self.expected, sorted(actual))

    @unittest.skipIf(not smart_open.concurrency._CONCURRENT_FUTURES, 'concurrent.futures unavailable')
    def test_concurrent_futures(self):
        with force(concurrent_futures=True):
            actual = list(smart_open.s3.iter_bucket(BUCKET_NAME, prefix='iter_bucket'))

        self.assertEqual(len(self.expected), len(actual))
        self.assertEqual(self.expected, sorted(actual))

    def test_accept_key(self):
        expected = [(key, value) for (key, value) in self.expected if '4' in key]
        actual = list(
            smart_open.s3.iter_bucket(
                BUCKET_NAME,
                prefix='iter_bucket',
                accept_key=lambda key: '4' in key
            )
        )
        self.assertEqual(len(expected), len(actual))
        self.assertEqual(expected, sorted(actual))

    @PTestCase.parameterize(('workers',), [(x,) for x in (1, 4, 8, 16, 64)])
    def test_workers(self, workers):
        actual = list(smart_open.s3.iter_bucket(BUCKET_NAME, prefix='iter_bucket', workers=workers))
        self.assertEqual(len(self.expected), len(actual))
        self.assertEqual(self.expected, sorted(actual))


class DownloadKeyTest(unittest.TestCase):
    def test(self):
        key_name = 'hello.txt'
        expected = (key_name, CONTENTS[key_name])

        actual = smart_open.s3._download_key(key_name, bucket_name=BUCKET_NAME)
        self.assertEqual(expected, actual)
