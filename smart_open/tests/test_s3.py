# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
from collections import defaultdict
import gzip
import io
import logging
import os
import time
import unittest
import warnings
from contextlib import contextmanager
from unittest.mock import patch
import sys

import boto3
import botocore.client
import botocore.endpoint
import mock
import moto

import smart_open
import smart_open.s3

# To reduce spurious errors due to S3's eventually-consistent behavior
# we create this bucket once before running these tests and then
# remove it when we're done.  The bucket has a random name so that we
# can run multiple instances of this suite in parallel and not have
# them conflict with one another. Travis, for example, runs the Python
# 2.7, 3.6, and 3.7 suites concurrently.
BUCKET_NAME = 'test-smartopen'
KEY_NAME = 'test-key'
WRITE_KEY_NAME = 'test-write-key'
ENABLE_MOTO_SERVER = os.environ.get("SO_ENABLE_MOTO_SERVER") == "1"

#
# This is a hack to keep moto happy
# See https://github.com/spulec/moto/issues/1941
#
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"


logger = logging.getLogger(__name__)


@moto.mock_s3
def setUpModule():
    '''Called once by unittest when initializing this module.  Sets up the
    test S3 bucket.

    '''
    bucket = boto3.resource('s3').create_bucket(Bucket=BUCKET_NAME)
    bucket.wait_until_exists()


def cleanup_bucket():
    for key in boto3.resource('s3').Bucket(BUCKET_NAME).objects.all():
        key.delete()


def put_to_bucket(contents, num_attempts=12, sleep_time=5):
    logger.debug('%r', locals())

    #
    # In real life, it can take a few seconds for the bucket to become ready.
    # If we try to write to the key while the bucket while it isn't ready, we
    # will get a ClientError: NoSuchBucket.
    #
    for attempt in range(num_attempts):
        try:
            boto3.resource('s3').Object(BUCKET_NAME, KEY_NAME).put(Body=contents)
            return
        except botocore.exceptions.ClientError as err:
            logger.error('caught %r, retrying', err)
            time.sleep(sleep_time)

    assert False, 'failed to write to bucket %s after %d attempts' % (BUCKET_NAME, num_attempts)


def ignore_resource_warnings():
    #
    # https://github.com/boto/boto3/issues/454
    # Py2 doesn't have ResourceWarning, so do nothing.
    #
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")  # noqa


@contextmanager
def patch_invalid_range_response(actual_size):
    """ Work around a bug in moto (https://github.com/spulec/moto/issues/2981) where the
     API response doesn't match when requesting an invalid range of bytes from an S3 GetObject. """
    _real_get = smart_open.s3._get

    def mock_get(*args, **kwargs):
        try:
            return _real_get(*args, **kwargs)
        except IOError as ioe:
            error_response = smart_open.s3._unwrap_ioerror(ioe)
            if error_response and error_response.get('Message') == 'Requested Range Not Satisfiable':
                error_response['ActualObjectSize'] = actual_size
                error_response['Code'] = 'InvalidRange'
                error_response['Message'] = 'The requested range is not satisfiable'
            raise

    with patch('smart_open.s3._get', new=mock_get):
        yield


class BaseTest(unittest.TestCase):
    @contextmanager
    def assertApiCalls(self, **expected_api_calls):
        """ Track calls to S3 in self.api_calls by patching botocore.endpoint.Endpoint.make_request. """
        _real_make_request = botocore.endpoint.Endpoint.make_request
        api_calls = defaultdict(int)

        def mock_make_request(self, operation_model, *args, **kwargs):
            api_calls[operation_model.name] += 1
            return _real_make_request(self, operation_model, *args, **kwargs)

        patcher = patch('botocore.endpoint.Endpoint.make_request', new=mock_make_request)
        patcher.start()
        try:
            yield api_calls
            self.assertDictEqual(expected_api_calls, api_calls)
        finally:
            patcher.stop()


@unittest.skipUnless(
    ENABLE_MOTO_SERVER,
    'The test case needs a Moto server running on the local 5000 port.'
)
class SeekableRawReaderTest(unittest.TestCase):

    def setUp(self):
        self._body = b'123456'
        self._local_resource = boto3.resource('s3', endpoint_url='http://localhost:5000')
        self._local_resource.Bucket(BUCKET_NAME).create()
        self._local_resource.Object(BUCKET_NAME, KEY_NAME).put(Body=self._body)

    def tearDown(self):
        self._local_resource.Object(BUCKET_NAME, KEY_NAME).delete()
        self._local_resource.Bucket(BUCKET_NAME).delete()

    def test_read_from_a_closed_body(self):
        obj = self._local_resource.Object(BUCKET_NAME, KEY_NAME)
        reader = smart_open.s3._SeekableRawReader(obj)
        self.assertEqual(reader.read(1), b'1')
        reader._body.close()
        self.assertEqual(reader.read(2), b'23')


class CrapStream(io.BytesIO):
    """Raises an exception on every second read call."""
    def __init__(self, *args, modulus=2, **kwargs):
        super().__init__(*args, **kwargs)
        self._count = 0
        self._modulus = modulus

    def read(self, size=-1):
        self._count += 1
        if self._count % self._modulus == 0:
            raise botocore.exceptions.BotoCoreError()
        the_bytes = super().read(size)
        return the_bytes


class CrapObject:
    def __init__(self, data, modulus=2):
        self._datasize = len(data)
        self._body = CrapStream(data, modulus=modulus)
        self.bucket_name, self.key = 'crap', 'object'

    def get(self, *args, **kwargs):
        return {
            'ActualObjectSize': self._datasize,
            'ContentLength': self._datasize,
            'ContentRange': 'bytes 0-%d/%d' % (self._datasize, self._datasize),
            'Body': self._body,
            'ResponseMetadata': {'RetryAttempts': 1},
        }


class IncrementalBackoffTest(unittest.TestCase):
    def test_every_read_fails(self):
        reader = smart_open.s3._SeekableRawReader(CrapObject(b'hello', 1))
        with mock.patch('time.sleep') as mock_sleep:
            with self.assertRaises(IOError):
                reader.read()

            #
            # Make sure our incremental backoff is actually happening here.
            #
            mock_sleep.assert_has_calls([mock.call(s) for s in (1, 2, 4, 8, 16)])

    def test_every_second_read_fails(self):
        """Can we read from a stream that raises exceptions from time to time?"""
        reader = smart_open.s3._SeekableRawReader(CrapObject(b'hello'))
        with mock.patch('time.sleep') as mock_sleep:
            assert reader.read(1) == b'h'
            mock_sleep.assert_not_called()

            assert reader.read(1) == b'e'
            mock_sleep.assert_called_with(1)
            mock_sleep.reset_mock()

            assert reader.read(1) == b'l'
            mock_sleep.reset_mock()

            assert reader.read(1) == b'l'
            mock_sleep.assert_called_with(1)
            mock_sleep.reset_mock()

            assert reader.read(1) == b'o'
            mock_sleep.assert_called_with(1)
            mock_sleep.reset_mock()


@moto.mock_s3
class SeekableBufferedInputBaseTest(BaseTest):
    def setUp(self):
        # lower the multipart upload size, to speed up these tests
        self.old_min_part_size = smart_open.s3.DEFAULT_MIN_PART_SIZE
        smart_open.s3.DEFAULT_MIN_PART_SIZE = 5 * 1024**2

        ignore_resource_warnings()

        super().setUp()

    def tearDown(self):
        smart_open.s3.DEFAULT_MIN_PART_SIZE = self.old_min_part_size
        cleanup_bucket()

    def test_iter(self):
        """Are S3 files iterated over correctly?"""
        # a list of strings to test with
        expected = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=expected)

        # connect to fake s3 and read from the fake key we filled above
        with self.assertApiCalls(GetObject=1):
            fin = smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME)
            output = [line.rstrip(b'\n') for line in fin]
        self.assertEqual(output, expected.split(b'\n'))

    def test_iter_context_manager(self):
        # same thing but using a context manager
        expected = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=expected)
        with self.assertApiCalls(GetObject=1):
            with smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME) as fin:
                output = [line.rstrip(b'\n') for line in fin]
        self.assertEqual(output, expected.split(b'\n'))

    def test_read(self):
        """Are S3 files read correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=content)
        logger.debug('content: %r len: %r', content, len(content))

        with self.assertApiCalls(GetObject=1):
            fin = smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME)
            self.assertEqual(content[:6], fin.read(6))
            self.assertEqual(content[6:14], fin.read(8))  # ř is 2 bytes
            self.assertEqual(content[14:], fin.read())  # read the rest

    def test_seek_beginning(self):
        """Does seeking to the beginning of S3 files work correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=content)

        with self.assertApiCalls(GetObject=1):
            fin = smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME)
            self.assertEqual(content[:6], fin.read(6))
            self.assertEqual(content[6:14], fin.read(8))  # ř is 2 bytes

        with self.assertApiCalls(GetObject=1):
            fin.seek(0)
            self.assertEqual(content, fin.read())  # no size given => read whole file

        with self.assertApiCalls(GetObject=1):
            fin.seek(0)
            self.assertEqual(content, fin.read(-1))  # same thing

    def test_seek_start(self):
        """Does seeking from the start of S3 files work correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=content)

        with self.assertApiCalls(GetObject=1):
            fin = smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME, defer_seek=True)
            seek = fin.seek(6)
            self.assertEqual(seek, 6)
            self.assertEqual(fin.tell(), 6)
            self.assertEqual(fin.read(6), u'wořld'.encode('utf-8'))

    def test_seek_current(self):
        """Does seeking from the middle of S3 files work correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=content)

        with self.assertApiCalls(GetObject=1):
            fin = smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME)
            self.assertEqual(fin.read(5), b'hello')

        with self.assertApiCalls(GetObject=1):
            seek = fin.seek(1, whence=smart_open.constants.WHENCE_CURRENT)
            self.assertEqual(seek, 6)
            self.assertEqual(fin.read(6), u'wořld'.encode('utf-8'))

    def test_seek_end(self):
        """Does seeking from the end of S3 files work correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=content)

        with self.assertApiCalls(GetObject=1):
            fin = smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME, defer_seek=True)
            seek = fin.seek(-4, whence=smart_open.constants.WHENCE_END)
            self.assertEqual(seek, len(content) - 4)
            self.assertEqual(fin.read(), b'you?')

    def test_seek_past_end(self):
        content = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=content)

        with self.assertApiCalls(GetObject=1), patch_invalid_range_response(str(len(content))):
            fin = smart_open.s3.Reader(BUCKET_NAME, KEY_NAME, defer_seek=True)
            seek = fin.seek(60)
            self.assertEqual(seek, len(content))

    def test_detect_eof(self):
        content = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=content)

        with self.assertApiCalls(GetObject=1):
            fin = smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME)
            fin.read()
            eof = fin.tell()
            self.assertEqual(eof, len(content))
            fin.seek(0, whence=smart_open.constants.WHENCE_END)
            self.assertEqual(eof, fin.tell())
            fin.seek(eof)
            self.assertEqual(eof, fin.tell())

    def test_read_gzip(self):
        expected = u'раcцветали яблони и груши, поплыли туманы над рекой...'.encode('utf-8')
        buf = io.BytesIO()
        buf.close = lambda: None  # keep buffer open so that we can .getvalue()
        with gzip.GzipFile(fileobj=buf, mode='w') as zipfile:
            zipfile.write(expected)
        put_to_bucket(contents=buf.getvalue())

        #
        # Make sure we're reading things correctly.
        #
        with smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME) as fin:
            self.assertEqual(fin.read(), buf.getvalue())

        #
        # Make sure the buffer we wrote is legitimate gzip.
        #
        sanity_buf = io.BytesIO(buf.getvalue())
        with gzip.GzipFile(fileobj=sanity_buf) as zipfile:
            self.assertEqual(zipfile.read(), expected)

        logger.debug('starting actual test')
        with self.assertApiCalls(GetObject=1):
            with smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME) as fin:
                with gzip.GzipFile(fileobj=fin) as zipfile:
                    actual = zipfile.read()

        self.assertEqual(expected, actual)

    def test_readline(self):
        content = b'englishman\nin\nnew\nyork\n'
        put_to_bucket(contents=content)

        with self.assertApiCalls(GetObject=2):
            with smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME) as fin:
                fin.readline()
                self.assertEqual(fin.tell(), content.index(b'\n')+1)

                fin.seek(0)
                actual = list(fin)
                self.assertEqual(fin.tell(), len(content))

        expected = [b'englishman\n', b'in\n', b'new\n', b'york\n']
        self.assertEqual(expected, actual)

    def test_readline_tiny_buffer(self):
        content = b'englishman\nin\nnew\nyork\n'
        put_to_bucket(contents=content)

        with self.assertApiCalls(GetObject=1):
            with smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME, buffer_size=8) as fin:
                actual = list(fin)

        expected = [b'englishman\n', b'in\n', b'new\n', b'york\n']
        self.assertEqual(expected, actual)

    def test_read0_does_not_return_data(self):
        content = b'englishman\nin\nnew\nyork\n'
        put_to_bucket(contents=content)

        with self.assertApiCalls():
            # set defer_seek to verify that read(0) doesn't trigger an unnecessary API call
            with smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME, defer_seek=True) as fin:
                data = fin.read(0)

        self.assertEqual(data, b'')

    def test_to_boto3(self):
        contents = b'the spice melange\n'
        put_to_bucket(contents=contents)

        with self.assertApiCalls():
            # set defer_seek to verify that to_boto3() doesn't trigger an unnecessary API call
            with smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME, defer_seek=True) as fin:
                returned_obj = fin.to_boto3()

        boto3_body = returned_obj.get()['Body'].read()
        self.assertEqual(contents, boto3_body)

    def test_binary_iterator(self):
        expected = u"выйду ночью в поле с конём".encode('utf-8').split(b' ')
        put_to_bucket(contents=b"\n".join(expected))
        with self.assertApiCalls(GetObject=1):
            with smart_open.s3.open(BUCKET_NAME, KEY_NAME, 'rb') as fin:
                actual = [line.rstrip() for line in fin]
        self.assertEqual(expected, actual)

    def test_defer_seek(self):
        content = b'englishman\nin\nnew\nyork\n'
        put_to_bucket(contents=content)

        with self.assertApiCalls():
            fin = smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME, defer_seek=True)
        with self.assertApiCalls(GetObject=1):
            self.assertEqual(fin.read(), content)

        with self.assertApiCalls():
            fin = smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, KEY_NAME, defer_seek=True)
        with self.assertApiCalls(GetObject=1):
            fin.seek(10)
            self.assertEqual(fin.read(), content[10:])

    def test_read_empty_file(self):
        put_to_bucket(contents=b'')

        with self.assertApiCalls(GetObject=1), patch_invalid_range_response('0'):
            with smart_open.s3.Reader(BUCKET_NAME, KEY_NAME) as fin:
                data = fin.read()

        self.assertEqual(data, b'')


@moto.mock_s3
class MultipartWriterTest(unittest.TestCase):
    """
    Test writing into s3 files.

    """
    def setUp(self):
        ignore_resource_warnings()

    def tearDown(self):
        cleanup_bucket()

    def test_write_01(self):
        """Does writing into s3 work correctly?"""
        test_string = u"žluťoučký koníček".encode('utf8')

        # write into key
        with smart_open.s3.MultipartWriter(BUCKET_NAME, WRITE_KEY_NAME) as fout:
            fout.write(test_string)

        # read key and test content
        output = list(smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, "rb"))

        self.assertEqual(output, [test_string])

    def test_write_01a(self):
        """Does s3 write fail on incorrect input?"""
        try:
            with smart_open.s3.MultipartWriter(BUCKET_NAME, WRITE_KEY_NAME) as fin:
                fin.write(None)
        except TypeError:
            pass
        else:
            self.fail()

    def test_write_02(self):
        """Does s3 write unicode-utf8 conversion work?"""
        smart_open_write = smart_open.s3.MultipartWriter(BUCKET_NAME, WRITE_KEY_NAME)
        smart_open_write.tell()
        logger.info("smart_open_write: %r", smart_open_write)
        with smart_open_write as fout:
            fout.write(u"testžížáč".encode("utf-8"))
            self.assertEqual(fout.tell(), 14)

    def test_write_03(self):
        """Does s3 multipart chunking work correctly?"""
        # write
        smart_open_write = smart_open.s3.MultipartWriter(
            BUCKET_NAME, WRITE_KEY_NAME, min_part_size=10
        )
        with smart_open_write as fout:
            fout.write(b"test")
            self.assertEqual(fout._buf.tell(), 4)

            fout.write(b"test\n")
            self.assertEqual(fout._buf.tell(), 9)
            self.assertEqual(fout._total_parts, 0)

            fout.write(b"test")
            self.assertEqual(fout._buf.tell(), 0)
            self.assertEqual(fout._total_parts, 1)

        # read back the same key and check its content
        output = list(smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, 'rb'))
        self.assertEqual(output, [b"testtest\n", b"test"])

    def test_write_04(self):
        """Does writing no data cause key with an empty value to be created?"""
        smart_open_write = smart_open.s3.MultipartWriter(BUCKET_NAME, WRITE_KEY_NAME)
        with smart_open_write as fout:  # noqa
            pass

        # read back the same key and check its content
        with patch_invalid_range_response('0'):
            output = list(smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, 'rb'))

        self.assertEqual(output, [])

    def test_gzip(self):
        expected = u'а не спеть ли мне песню... о любви'.encode('utf-8')
        with smart_open.s3.MultipartWriter(BUCKET_NAME, WRITE_KEY_NAME) as fout:
            with gzip.GzipFile(fileobj=fout, mode='w') as zipfile:
                zipfile.write(expected)

        with smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, WRITE_KEY_NAME) as fin:
            with gzip.GzipFile(fileobj=fin) as zipfile:
                actual = zipfile.read()

        self.assertEqual(expected, actual)

    def test_buffered_writer_wrapper_works(self):
        """
        Ensure that we can wrap a smart_open s3 stream in a BufferedWriter, which
        passes a memoryview object to the underlying stream in python >= 2.7
        """
        expected = u'не думай о секундах свысока'

        with smart_open.s3.MultipartWriter(BUCKET_NAME, WRITE_KEY_NAME) as fout:
            with io.BufferedWriter(fout) as sub_out:
                sub_out.write(expected.encode('utf-8'))

        with smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, 'rb') as fin:
            with io.TextIOWrapper(fin, encoding='utf-8') as text:
                actual = text.read()

        self.assertEqual(expected, actual)

    def test_nonexisting_bucket(self):
        expected = u"выйду ночью в поле с конём".encode('utf-8')
        with self.assertRaises(ValueError):
            with smart_open.s3.open('thisbucketdoesntexist', 'mykey', 'wb') as fout:
                fout.write(expected)

    def test_read_nonexisting_key(self):
        with self.assertRaises(IOError):
            with smart_open.s3.open(BUCKET_NAME, 'my_nonexisting_key', 'rb') as fin:
                fin.read()

    def test_double_close(self):
        text = u'там за туманами, вечными, пьяными'.encode('utf-8')
        fout = smart_open.s3.open(BUCKET_NAME, 'key', 'wb')
        fout.write(text)
        fout.close()
        fout.close()

    def test_flush_close(self):
        text = u'там за туманами, вечными, пьяными'.encode('utf-8')
        fout = smart_open.s3.open(BUCKET_NAME, 'key', 'wb')
        fout.write(text)
        fout.flush()
        fout.close()

    def test_to_boto3(self):
        contents = b'the spice melange\n'

        with smart_open.s3.open(BUCKET_NAME, KEY_NAME, 'wb') as fout:
            fout.write(contents)
            returned_obj = fout.to_boto3()

        boto3_body = returned_obj.get()['Body'].read()
        self.assertEqual(contents, boto3_body)


@moto.mock_s3
class SinglepartWriterTest(unittest.TestCase):
    """
    Test writing into s3 files using single part upload.

    """
    def setUp(self):
        ignore_resource_warnings()

    def tearDown(self):
        cleanup_bucket()

    def test_write_01(self):
        """Does writing into s3 work correctly?"""
        test_string = u"žluťoučký koníček".encode('utf8')

        # write into key
        with smart_open.s3.SinglepartWriter(BUCKET_NAME, WRITE_KEY_NAME) as fout:
            fout.write(test_string)

        # read key and test content
        output = list(smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, "rb"))

        self.assertEqual(output, [test_string])

    def test_write_01a(self):
        """Does s3 write fail on incorrect input?"""
        try:
            with smart_open.s3.SinglepartWriter(BUCKET_NAME, WRITE_KEY_NAME) as fin:
                fin.write(None)
        except TypeError:
            pass
        else:
            self.fail()

    def test_write_02(self):
        """Does s3 write unicode-utf8 conversion work?"""
        test_string = u"testžížáč".encode("utf-8")

        smart_open_write = smart_open.s3.SinglepartWriter(BUCKET_NAME, WRITE_KEY_NAME)
        smart_open_write.tell()
        logger.info("smart_open_write: %r", smart_open_write)
        with smart_open_write as fout:
            fout.write(test_string)
            self.assertEqual(fout.tell(), 14)

    def test_write_04(self):
        """Does writing no data cause key with an empty value to be created?"""
        smart_open_write = smart_open.s3.SinglepartWriter(BUCKET_NAME, WRITE_KEY_NAME)
        with smart_open_write as fout:  # noqa
            pass

        # read back the same key and check its content
        with patch_invalid_range_response('0'):
            output = list(smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, 'rb'))
        self.assertEqual(output, [])

    def test_buffered_writer_wrapper_works(self):
        """
        Ensure that we can wrap a smart_open s3 stream in a BufferedWriter, which
        passes a memoryview object to the underlying stream in python >= 2.7
        """
        expected = u'не думай о секундах свысока'

        with smart_open.s3.SinglepartWriter(BUCKET_NAME, WRITE_KEY_NAME) as fout:
            with io.BufferedWriter(fout) as sub_out:
                sub_out.write(expected.encode('utf-8'))

        with smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, 'rb') as fin:
            with io.TextIOWrapper(fin, encoding='utf-8') as text:
                actual = text.read()

        self.assertEqual(expected, actual)

    def test_nonexisting_bucket(self):
        expected = u"выйду ночью в поле с конём".encode('utf-8')
        with self.assertRaises(ValueError):
            with smart_open.s3.open('thisbucketdoesntexist', 'mykey', 'wb', multipart_upload=False) as fout:
                fout.write(expected)

    def test_double_close(self):
        text = u'там за туманами, вечными, пьяными'.encode('utf-8')
        fout = smart_open.s3.open(BUCKET_NAME, 'key', 'wb', multipart_upload=False)
        fout.write(text)
        fout.close()
        fout.close()

    def test_flush_close(self):
        text = u'там за туманами, вечными, пьяными'.encode('utf-8')
        fout = smart_open.s3.open(BUCKET_NAME, 'key', 'wb', multipart_upload=False)
        fout.write(text)
        fout.flush()
        fout.close()


ARBITRARY_CLIENT_ERROR = botocore.client.ClientError(error_response={}, operation_name='bar')


@moto.mock_s3
class IterBucketTest(unittest.TestCase):
    def setUp(self):
        ignore_resource_warnings()

    def tearDown(self):
        cleanup_bucket()

    @unittest.skipIf(sys.platform == 'win32', reason="does not run on windows")
    def test_iter_bucket(self):
        populate_bucket()
        results = list(smart_open.s3.iter_bucket(BUCKET_NAME))
        self.assertEqual(len(results), 10)

    def test_deprecated_top_level_s3_iter_bucket(self):
        populate_bucket()
        with self.assertLogs(smart_open.logger.name, level='WARN') as cm:
            # invoking once will generate a warning
            smart_open.s3_iter_bucket(BUCKET_NAME)
            # invoking again will not (to reduce spam)
            smart_open.s3_iter_bucket(BUCKET_NAME)
            # verify only one output
            assert len(cm.output) == 1
            # verify the suggested new import is in the warning
            assert "from smart_open.s3 import iter_bucket as s3_iter_bucket" in cm.output[0]

    @unittest.skipIf(sys.platform == 'win32', reason="does not run on windows")
    def test_accepts_boto3_bucket(self):
        populate_bucket()
        bucket = boto3.resource('s3').Bucket(BUCKET_NAME)
        results = list(smart_open.s3.iter_bucket(bucket))
        self.assertEqual(len(results), 10)

    def test_list_bucket(self):
        num_keys = 10
        populate_bucket()
        keys = list(smart_open.s3._list_bucket(BUCKET_NAME))
        self.assertEqual(len(keys), num_keys)

        expected = ['key_%d' % x for x in range(num_keys)]
        self.assertEqual(sorted(keys), sorted(expected))

    def test_list_bucket_long(self):
        num_keys = 1010
        populate_bucket(num_keys=num_keys)
        keys = list(smart_open.s3._list_bucket(BUCKET_NAME))
        self.assertEqual(len(keys), num_keys)

        expected = ['key_%d' % x for x in range(num_keys)]
        self.assertEqual(sorted(keys), sorted(expected))


@moto.mock_s3
@unittest.skipIf(not smart_open.concurrency._CONCURRENT_FUTURES, 'concurrent.futures unavailable')
@unittest.skipIf(sys.platform == 'win32', reason="does not run on windows")
class IterBucketConcurrentFuturesTest(unittest.TestCase):
    def setUp(self):
        self.old_flag_multi = smart_open.concurrency._MULTIPROCESSING
        smart_open.concurrency._MULTIPROCESSING = False
        ignore_resource_warnings()

    def tearDown(self):
        smart_open.concurrency._MULTIPROCESSING = self.old_flag_multi
        cleanup_bucket()

    def test(self):
        num_keys = 101
        populate_bucket(num_keys=num_keys)
        keys = list(smart_open.s3.iter_bucket(BUCKET_NAME))
        self.assertEqual(len(keys), num_keys)

        expected = [('key_%d' % x, b'%d' % x) for x in range(num_keys)]
        self.assertEqual(sorted(keys), sorted(expected))


@moto.mock_s3
@unittest.skipIf(not smart_open.concurrency._MULTIPROCESSING, 'multiprocessing unavailable')
@unittest.skipIf(sys.platform == 'win32', reason="does not run on windows")
class IterBucketMultiprocessingTest(unittest.TestCase):
    def setUp(self):
        self.old_flag_concurrent = smart_open.concurrency._CONCURRENT_FUTURES
        smart_open.concurrency._CONCURRENT_FUTURES = False
        ignore_resource_warnings()

    def tearDown(self):
        smart_open.concurrency._CONCURRENT_FUTURES = self.old_flag_concurrent
        cleanup_bucket()

    def test(self):
        num_keys = 101
        populate_bucket(num_keys=num_keys)
        keys = list(smart_open.s3.iter_bucket(BUCKET_NAME))
        self.assertEqual(len(keys), num_keys)

        expected = [('key_%d' % x, b'%d' % x) for x in range(num_keys)]
        self.assertEqual(sorted(keys), sorted(expected))


@moto.mock_s3
class IterBucketSingleProcessTest(unittest.TestCase):
    def setUp(self):
        self.old_flag_multi = smart_open.concurrency._MULTIPROCESSING
        self.old_flag_concurrent = smart_open.concurrency._CONCURRENT_FUTURES
        smart_open.concurrency._MULTIPROCESSING = False
        smart_open.concurrency._CONCURRENT_FUTURES = False

        ignore_resource_warnings()

    def tearDown(self):
        smart_open.concurrency._MULTIPROCESSING = self.old_flag_multi
        smart_open.concurrency._CONCURRENT_FUTURES = self.old_flag_concurrent
        cleanup_bucket()

    def test(self):
        num_keys = 101
        populate_bucket(num_keys=num_keys)
        keys = list(smart_open.s3.iter_bucket(BUCKET_NAME))
        self.assertEqual(len(keys), num_keys)

        expected = [('key_%d' % x, b'%d' % x) for x in range(num_keys)]
        self.assertEqual(sorted(keys), sorted(expected))


#
# This has to be a separate test because we cannot run it against real S3
# (we don't want to expose our real S3 credentials).
#
@moto.mock_s3
class IterBucketCredentialsTest(unittest.TestCase):
    def test(self):
        num_keys = 10
        populate_bucket(num_keys=num_keys)
        result = list(
            smart_open.s3.iter_bucket(
                BUCKET_NAME,
                workers=None,
                aws_access_key_id='access_id',
                aws_secret_access_key='access_secret'
            )
        )
        self.assertEqual(len(result), num_keys)


@moto.mock_s3
class DownloadKeyTest(unittest.TestCase):
    def setUp(self):
        ignore_resource_warnings()

    def tearDown(self):
        cleanup_bucket()

    def test_happy(self):
        contents = b'hello'
        put_to_bucket(contents=contents)
        expected = (KEY_NAME, contents)
        actual = smart_open.s3._download_key(KEY_NAME, bucket_name=BUCKET_NAME)
        self.assertEqual(expected, actual)

    def test_intermittent_error(self):
        contents = b'hello'
        put_to_bucket(contents=contents)
        expected = (KEY_NAME, contents)
        side_effect = [ARBITRARY_CLIENT_ERROR, ARBITRARY_CLIENT_ERROR, contents]
        with mock.patch('smart_open.s3._download_fileobj', side_effect=side_effect):
            actual = smart_open.s3._download_key(KEY_NAME, bucket_name=BUCKET_NAME)
        self.assertEqual(expected, actual)

    def test_persistent_error(self):
        contents = b'hello'
        put_to_bucket(contents=contents)
        side_effect = [ARBITRARY_CLIENT_ERROR, ARBITRARY_CLIENT_ERROR,
                       ARBITRARY_CLIENT_ERROR, ARBITRARY_CLIENT_ERROR]
        with mock.patch('smart_open.s3._download_fileobj', side_effect=side_effect):
            self.assertRaises(botocore.client.ClientError, smart_open.s3._download_key,
                              KEY_NAME, bucket_name=BUCKET_NAME)

    def test_intermittent_error_retries(self):
        contents = b'hello'
        put_to_bucket(contents=contents)
        expected = (KEY_NAME, contents)
        side_effect = [ARBITRARY_CLIENT_ERROR, ARBITRARY_CLIENT_ERROR,
                       ARBITRARY_CLIENT_ERROR, ARBITRARY_CLIENT_ERROR, contents]
        with mock.patch('smart_open.s3._download_fileobj', side_effect=side_effect):
            actual = smart_open.s3._download_key(KEY_NAME, bucket_name=BUCKET_NAME, retries=4)
        self.assertEqual(expected, actual)

    def test_propagates_other_exception(self):
        contents = b'hello'
        put_to_bucket(contents=contents)
        with mock.patch('smart_open.s3._download_fileobj', side_effect=ValueError):
            self.assertRaises(ValueError, smart_open.s3._download_key,
                              KEY_NAME, bucket_name=BUCKET_NAME)


@moto.mock_s3
class OpenTest(unittest.TestCase):
    def setUp(self):
        ignore_resource_warnings()

    def tearDown(self):
        cleanup_bucket()

    def test_read_never_returns_none(self):
        """read should never return None."""
        test_string = u"ветер по морю гуляет..."
        with smart_open.s3.open(BUCKET_NAME, KEY_NAME, "wb") as fout:
            fout.write(test_string.encode('utf8'))

        r = smart_open.s3.open(BUCKET_NAME, KEY_NAME, "rb")
        self.assertEqual(r.read(), test_string.encode("utf-8"))
        self.assertEqual(r.read(), b"")
        self.assertEqual(r.read(), b"")


def populate_bucket(num_keys=10):
    s3 = boto3.resource('s3')
    for key_number in range(num_keys):
        key_name = 'key_%d' % key_number
        s3.Object(BUCKET_NAME, key_name).put(Body=str(key_number))


class RetryIfFailedTest(unittest.TestCase):
    def test_success(self):
        partial = mock.Mock(return_value=1)
        result = smart_open.s3._retry_if_failed(partial, attempts=3, sleep_seconds=0)
        self.assertEqual(result, 1)
        self.assertEqual(partial.call_count, 1)

    def test_failure(self):
        partial = mock.Mock(side_effect=ValueError)
        exceptions = (ValueError, )

        with self.assertRaises(IOError):
            smart_open.s3._retry_if_failed(partial, attempts=3, sleep_seconds=0, exceptions=exceptions)

        self.assertEqual(partial.call_count, 3)


@moto.mock_s3()
def test_resource_propagation_singlepart():
    """Does the resource parameter make it from the caller to Boto3?"""
    #
    # Not sure why we need to create the bucket here, as setUpModule should
    # have done that for us by now.
    #
    session = boto3.Session()
    resource = session.resource('s3')
    bucket = resource.create_bucket(Bucket=BUCKET_NAME)
    bucket.wait_until_exists()

    with smart_open.s3.open(
        BUCKET_NAME,
        WRITE_KEY_NAME,
        mode='wb',
        resource=resource,
        multipart_upload=False,
    ) as writer:
        assert writer._resource == resource
        assert id(writer._resource) == id(resource)


@moto.mock_s3()
def test_resource_propagation_multipart():
    """Does the resource parameter make it from the caller to Boto3?"""
    session = boto3.Session()
    resource = session.resource('s3')
    bucket = resource.create_bucket(Bucket=BUCKET_NAME)
    bucket.wait_until_exists()

    with smart_open.s3.open(
        BUCKET_NAME,
        WRITE_KEY_NAME,
        mode='wb',
        resource=resource,
        multipart_upload=True,
    ) as writer:
        assert writer._resource == resource
        assert id(writer._resource) == id(resource)


@moto.mock_s3()
def test_resource_propagation_reader():
    """Does the resource parameter make it from the caller to Boto3?"""
    session = boto3.Session()
    resource = session.resource('s3')
    bucket = resource.create_bucket(Bucket=BUCKET_NAME)
    bucket.wait_until_exists()

    with smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, mode='wb') as writer:
        writer.write(b'hello world')

    with smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, mode='rb', resource=resource) as reader:
        assert reader._resource == resource
        assert id(reader._resource) == id(resource)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    unittest.main()
