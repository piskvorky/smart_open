# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
from collections import defaultdict
import functools
import gzip
import io
import logging
import os
import tempfile
import unittest
import warnings
from contextlib import contextmanager
from unittest import mock
import sys

import boto3
import botocore.client
import botocore.endpoint
import botocore.exceptions
import pytest

# See https://github.com/piskvorky/smart_open/issues/800
# This supports moto 4 & 5 until v4 is no longer used by distros.
try:
    from moto import mock_s3
except ImportError:
    from moto import mock_aws as mock_s3

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

_resource = functools.partial(boto3.resource, region_name='us-east-1')


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
            if actual_size is None:
                error_response.pop('ActualObjectSize', None)
            raise

    with mock.patch('smart_open.s3._get', new=mock_get):
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

        patcher = mock.patch('botocore.endpoint.Endpoint.make_request', new=mock_make_request)
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
        self._local_client = boto3.client('s3', endpoint_url='http://localhost:5000')

    def tearDown(self):
        self._local_resource.Object(BUCKET_NAME, KEY_NAME).delete()
        self._local_resource.Bucket(BUCKET_NAME).delete()

    def test_read_from_a_closed_body(self):
        reader = smart_open.s3._SeekableRawReader(self._local_client, BUCKET_NAME, KEY_NAME)
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


class CrapClient:
    def __init__(self, data, modulus=2):
        self._datasize = len(data)
        self._body = CrapStream(data, modulus=modulus)

    def get_object(self, *args, **kwargs):
        return {
            'ActualObjectSize': self._datasize,
            'ContentLength': self._datasize,
            'ContentRange': 'bytes 0-%d/%d' % (self._datasize, self._datasize),
            'Body': self._body,
            'ResponseMetadata': {'RetryAttempts': 1, 'HTTPStatusCode': 206},
        }


class IncrementalBackoffTest(unittest.TestCase):
    def test_every_read_fails(self):
        reader = smart_open.s3._SeekableRawReader(CrapClient(b'hello', 1), 'bucket', 'key')
        with mock.patch('time.sleep') as mock_sleep:
            with self.assertRaises(IOError):
                reader.read()

            #
            # Make sure our incremental backoff is actually happening here.
            #
            mock_sleep.assert_has_calls([mock.call(s) for s in (1, 2, 4, 8, 16)])

    def test_every_second_read_fails(self):
        """Can we read from a stream that raises exceptions from time to time?"""
        reader = smart_open.s3._SeekableRawReader(CrapClient(b'hello'), 'bucket', 'key')
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


@mock_s3
class ReaderTest(BaseTest):
    def setUp(self):
        # lower the multipart upload size, to speed up these tests
        self.old_min_part_size = smart_open.s3.DEFAULT_PART_SIZE
        smart_open.s3.DEFAULT_PART_SIZE = 5 * 1024**2

        ignore_resource_warnings()

        super().setUp()

        s3 = _resource('s3')
        s3.create_bucket(Bucket=BUCKET_NAME).wait_until_exists()

        self.body = u"hello wořld\nhow are you?".encode('utf8')
        s3.Object(BUCKET_NAME, KEY_NAME).put(Body=self.body)

    def tearDown(self):
        smart_open.s3.DEFAULT_PART_SIZE = self.old_min_part_size

    def test_iter(self):
        """Are S3 files iterated over correctly?"""
        # connect to fake s3 and read from the fake key we filled above
        with self.assertApiCalls(GetObject=1):
            fin = smart_open.s3.Reader(BUCKET_NAME, KEY_NAME)
            output = [line.rstrip(b'\n') for line in fin]
        self.assertEqual(output, self.body.split(b'\n'))

    def test_iter_context_manager(self):
        # same thing but using a context manager
        _resource('s3').create_bucket(Bucket=BUCKET_NAME).wait_until_exists()

        with self.assertApiCalls(GetObject=1):
            with smart_open.s3.Reader(BUCKET_NAME, KEY_NAME) as fin:
                output = [line.rstrip(b'\n') for line in fin]
        self.assertEqual(output, self.body.split(b'\n'))

    def test_read(self):
        """Are S3 files read correctly?"""
        with self.assertApiCalls(GetObject=1):
            fin = smart_open.s3.Reader(BUCKET_NAME, KEY_NAME)
            self.assertEqual(self.body[:6], fin.read(6))
            self.assertEqual(self.body[6:14], fin.read(8))  # ř is 2 bytes
            self.assertEqual(self.body[14:], fin.read())  # read the rest

    def test_seek_beginning(self):
        """Does seeking to the beginning of S3 files work correctly?"""
        with self.assertApiCalls(GetObject=1):
            fin = smart_open.s3.Reader(BUCKET_NAME, KEY_NAME)
            self.assertEqual(self.body[:6], fin.read(6))
            self.assertEqual(self.body[6:14], fin.read(8))  # ř is 2 bytes

        with self.assertApiCalls(GetObject=1):
            fin.seek(0)
            self.assertEqual(self.body, fin.read())  # no size given => read whole file

        with self.assertApiCalls(GetObject=1):
            fin.seek(0)
            self.assertEqual(self.body, fin.read(-1))  # same thing

    def test_seek_start(self):
        """Does seeking from the start of S3 files work correctly?"""
        with self.assertApiCalls(GetObject=1):
            fin = smart_open.s3.Reader(BUCKET_NAME, KEY_NAME, defer_seek=True)
            seek = fin.seek(6)
            self.assertEqual(seek, 6)
            self.assertEqual(fin.tell(), 6)
            self.assertEqual(fin.read(6), u'wořld'.encode('utf-8'))

    def test_seek_current(self):
        """Does seeking from the middle of S3 files work correctly?"""
        with self.assertApiCalls(GetObject=1):
            fin = smart_open.s3.Reader(BUCKET_NAME, KEY_NAME)
            self.assertEqual(fin.read(5), b'hello')

        with self.assertApiCalls(GetObject=1):
            seek = fin.seek(1, whence=smart_open.constants.WHENCE_CURRENT)
            self.assertEqual(seek, 6)
            self.assertEqual(fin.read(6), u'wořld'.encode('utf-8'))

    def test_seek_end(self):
        """Does seeking from the end of S3 files work correctly?"""
        with self.assertApiCalls(GetObject=1):
            fin = smart_open.s3.Reader(BUCKET_NAME, KEY_NAME, defer_seek=True)
            seek = fin.seek(-4, whence=smart_open.constants.WHENCE_END)
            self.assertEqual(seek, len(self.body) - 4)
            self.assertEqual(fin.read(), b'you?')

    def test_seek_past_end(self):
        with self.assertApiCalls(GetObject=1), patch_invalid_range_response(str(len(self.body))):
            fin = smart_open.s3.Reader(BUCKET_NAME, KEY_NAME, defer_seek=True)
            seek = fin.seek(60)
            self.assertEqual(seek, len(self.body))

    def test_detect_eof(self):
        with self.assertApiCalls(GetObject=1):
            fin = smart_open.s3.Reader(BUCKET_NAME, KEY_NAME)
            fin.read()
            eof = fin.tell()
            self.assertEqual(eof, len(self.body))
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

        _resource('s3').Object(BUCKET_NAME, KEY_NAME).put(Body=buf.getvalue())

        #
        # Make sure we're reading things correctly.
        #
        with smart_open.s3.Reader(BUCKET_NAME, KEY_NAME) as fin:
            self.assertEqual(fin.read(), buf.getvalue())

        #
        # Make sure the buffer we wrote is legitimate gzip.
        #
        sanity_buf = io.BytesIO(buf.getvalue())
        with gzip.GzipFile(fileobj=sanity_buf) as zipfile:
            self.assertEqual(zipfile.read(), expected)

        logger.debug('starting actual test')
        with self.assertApiCalls(GetObject=1):
            with smart_open.s3.Reader(BUCKET_NAME, KEY_NAME) as fin:
                with gzip.GzipFile(fileobj=fin) as zipfile:
                    actual = zipfile.read()

        self.assertEqual(expected, actual)

    def test_readline(self):
        content = b'englishman\nin\nnew\nyork\n'
        _resource('s3').Object(BUCKET_NAME, KEY_NAME).put(Body=content)

        with self.assertApiCalls(GetObject=2):
            with smart_open.s3.Reader(BUCKET_NAME, KEY_NAME) as fin:
                fin.readline()
                self.assertEqual(fin.tell(), content.index(b'\n')+1)

                fin.seek(0)
                actual = list(fin)
                self.assertEqual(fin.tell(), len(content))

        expected = [b'englishman\n', b'in\n', b'new\n', b'york\n']
        self.assertEqual(expected, actual)

    def test_readline_tiny_buffer(self):
        content = b'englishman\nin\nnew\nyork\n'
        _resource('s3').Object(BUCKET_NAME, KEY_NAME).put(Body=content)

        with self.assertApiCalls(GetObject=1):
            with smart_open.s3.Reader(BUCKET_NAME, KEY_NAME, buffer_size=8) as fin:
                actual = list(fin)

        expected = [b'englishman\n', b'in\n', b'new\n', b'york\n']
        self.assertEqual(expected, actual)

    def test_read0_does_not_return_data(self):
        with self.assertApiCalls():
            # set defer_seek to verify that read(0) doesn't trigger an unnecessary API call
            with smart_open.s3.Reader(BUCKET_NAME, KEY_NAME, defer_seek=True) as fin:
                data = fin.read(0)

        self.assertEqual(data, b'')

    def test_to_boto3(self):
        with self.assertApiCalls():
            # set defer_seek to verify that to_boto3() doesn't trigger an unnecessary API call
            with smart_open.s3.Reader(BUCKET_NAME, KEY_NAME, defer_seek=True) as fin:
                returned_obj = fin.to_boto3(_resource('s3'))

        boto3_body = returned_obj.get()['Body'].read()
        self.assertEqual(self.body, boto3_body)

    def test_binary_iterator(self):
        expected = u"выйду ночью в поле с конём".encode('utf-8').split(b' ')
        _resource('s3').Object(BUCKET_NAME, KEY_NAME).put(Body=b'\n'.join(expected))

        # test the __iter__ method
        with self.assertApiCalls(GetObject=1):
            with smart_open.s3.open(BUCKET_NAME, KEY_NAME, 'rb') as fin:
                actual = [line.rstrip() for line in fin]
        self.assertEqual(expected, actual)

        # test the __next__ method
        with self.assertApiCalls(GetObject=1):
            with smart_open.s3.open(BUCKET_NAME, KEY_NAME, 'rb') as fin:
                first = next(fin).rstrip()
        self.assertEqual(expected[0], first)

    def test_text_iterator(self):
        expected = u"выйду ночью в поле с конём".split(' ')
        uri = f's3://{BUCKET_NAME}/{KEY_NAME}.gz'

        with smart_open.open(uri, 'w', encoding='utf-8') as fout:
            fout.write('\n'.join(expected))

        # test the __iter__ method
        with self.assertApiCalls(GetObject=1):
            with smart_open.open(uri, 'r', encoding='utf-8') as fin:
                actual = [line.rstrip() for line in fin]
        self.assertEqual(expected, actual)

        # test the __next__ method
        with self.assertApiCalls(GetObject=1):
            with smart_open.open(uri, 'r', encoding='utf-8') as fin:
                first = next(fin).rstrip()
        self.assertEqual(expected[0], first)

    def test_defer_seek(self):
        content = b'englishman\nin\nnew\nyork\n'
        _resource('s3').Object(BUCKET_NAME, KEY_NAME).put(Body=content)

        with self.assertApiCalls():
            fin = smart_open.s3.Reader(BUCKET_NAME, KEY_NAME, defer_seek=True)
        with self.assertApiCalls(GetObject=1):
            self.assertEqual(fin.read(), content)

        with self.assertApiCalls():
            fin = smart_open.s3.Reader(BUCKET_NAME, KEY_NAME, defer_seek=True)
        with self.assertApiCalls(GetObject=1):
            fin.seek(10)
            self.assertEqual(fin.read(), content[10:])

    def test_read_empty_file(self):
        _resource('s3').Object(BUCKET_NAME, KEY_NAME).put(Body=b'')

        with self.assertApiCalls(GetObject=1), patch_invalid_range_response('0'):
            with smart_open.s3.Reader(BUCKET_NAME, KEY_NAME) as fin:
                data = fin.read()

        self.assertEqual(data, b'')

    def test_read_empty_file_no_actual_size(self):
        _resource('s3').Object(BUCKET_NAME, KEY_NAME).put(Body=b'')

        with self.assertApiCalls(GetObject=2), patch_invalid_range_response(None):
            with smart_open.s3.Reader(BUCKET_NAME, KEY_NAME) as fin:
                data = fin.read()

        self.assertEqual(data, b'')


@mock_s3
class MultipartWriterTest(unittest.TestCase):
    """
    Test writing into s3 files.

    """
    def setUp(self):
        ignore_resource_warnings()

        _resource('s3').create_bucket(Bucket=BUCKET_NAME).wait_until_exists()

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

    #
    # Nb. Under Windows, the byte offsets are different for some reason
    #
    @pytest.mark.skipif(condition=sys.platform == 'win32', reason="does not run on windows")
    def test_write_03(self):
        """Does s3 multipart chunking work correctly?"""
        #
        # generate enough test data for a single multipart upload part.
        # We need this because moto behaves like S3; it refuses to upload
        # parts smaller than 5MB.
        #
        data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        with open(os.path.join(data_dir, "crime-and-punishment.txt"), "rb") as fin:
            crime = fin.read()
        data = b''
        ps = 5 * 1024 * 1024
        while len(data) < ps:
            data += crime

        title = "Преступление и наказание\n\n".encode()
        to_be_continued = "\n\n... продолжение следует ...\n\n".encode()

        with smart_open.s3.MultipartWriter(BUCKET_NAME, WRITE_KEY_NAME, part_size=ps) as fout:
            #
            # Write some data without triggering an upload
            #
            fout.write(title)
            assert fout._total_parts == 0
            assert fout._buf.tell() == 48

            #
            # Trigger a part upload
            #
            fout.write(data)
            assert fout._total_parts == 1
            assert fout._buf.tell() == 661

            #
            # Write _without_ triggering a part upload
            #
            fout.write(to_be_continued)
            assert fout._total_parts == 1
            assert fout._buf.tell() == 710

        #
        # We closed the writer, so the final part must have been uploaded
        #
        assert fout._buf.tell() == 0
        assert fout._total_parts == 2

        #
        # read back the same key and check its content
        #
        with smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, 'rb') as fin:
            got = fin.read()
        want = title + data + to_be_continued
        assert want == got

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

        with smart_open.s3.Reader(BUCKET_NAME, WRITE_KEY_NAME) as fin:
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
            returned_obj = fout.to_boto3(_resource('s3'))

        boto3_body = returned_obj.get()['Body'].read()
        self.assertEqual(contents, boto3_body)

    def test_writebuffer(self):
        """Does the MultipartWriter support writing to a custom buffer?"""
        contents = b'get ready for a surprise'

        with tempfile.NamedTemporaryFile(mode='rb+') as f:
            with smart_open.s3.MultipartWriter(BUCKET_NAME, WRITE_KEY_NAME, writebuffer=f) as fout:
                fout.write(contents)

            with smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, 'rb') as fin:
                actual = fin.read()

            assert actual == contents

    def test_write_gz_using_context_manager(self):
        """Does s3 multipart upload create a compressed file using context manager?"""
        contents = b'get ready for a surprise'
        with smart_open.open(
                f's3://{BUCKET_NAME}/{WRITE_KEY_NAME}.gz',
                mode="wb",
                transport_params={
                    "multipart_upload": True,
                    "min_part_size": 10,
                }
        ) as fout:
            fout.write(contents)

        with smart_open.open(f's3://{BUCKET_NAME}/{WRITE_KEY_NAME}.gz', 'rb') as fin:
            actual = fin.read()

        assert actual == contents

    def test_write_gz_not_using_context_manager(self):
        """Does s3 multipart upload create a compressed file not using context manager but close()?"""
        contents = b'get ready for a surprise'
        fout = smart_open.open(
            f's3://{BUCKET_NAME}/{WRITE_KEY_NAME}.gz',
            mode="wb",
            transport_params={
                "multipart_upload": True,
                "min_part_size": 10,
            }
        )
        fout.write(contents)
        fout.close()

        with smart_open.open(f's3://{BUCKET_NAME}/{WRITE_KEY_NAME}.gz', 'rb') as fin:
            actual = fin.read()

        assert actual == contents

    def test_write_gz_with_error(self):
        """Does s3 multipart upload abort for a failed compressed file upload?"""
        with self.assertRaises(ValueError):
            with smart_open.open(
                    f's3://{BUCKET_NAME}/{WRITE_KEY_NAME}',
                    mode="wb",
                    compression='.gz',
                    transport_params={
                        "multipart_upload": True,
                        "min_part_size": 10,
                    }
            ) as fout:
                fout.write(b"test12345678test12345678")
                fout.write(b"test\n")

                # FileLikeWrapper.__exit__ should cause a MultipartWriter.terminate()
                raise ValueError("some error")

        # no multipart upload was committed:
        # smart_open.s3.MultipartWriter.__exit__ was called
        with self.assertRaises(OSError) as cm:
            smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, 'rb')

        assert 'The specified key does not exist.' in cm.exception.args[0]

    def test_write_text_with_error(self):
        """Does s3 multipart upload abort for a failed text file upload?"""
        with self.assertRaises(ValueError):
            with smart_open.open(
                    f's3://{BUCKET_NAME}/{WRITE_KEY_NAME}',
                    mode="w",
                    transport_params={
                        "multipart_upload": True,
                        "min_part_size": 10,
                    }
            ) as fout:
                fout.write("test12345678test12345678")
                fout.write("test\n")

                # TextIOWrapper.__exit__ should not cause a self.buffer.close()
                # FileLikeWrapper.__exit__ should cause a MultipartWriter.terminate()
                raise ValueError("some error")

        # no multipart upload was committed:
        # smart_open.s3.MultipartWriter.__exit__ was called
        with self.assertRaises(OSError) as cm:
            smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, 'rb')

        assert 'The specified key does not exist.' in cm.exception.args[0]


@mock_s3
class SinglepartWriterTest(unittest.TestCase):
    """
    Test writing into s3 files using single part upload.

    """
    def setUp(self):
        ignore_resource_warnings()

        _resource('s3').create_bucket(Bucket=BUCKET_NAME).wait_until_exists()

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

    def test_writebuffer(self):
        """Does the SinglepartWriter support writing to a custom buffer?"""
        contents = b'get ready for a surprise'

        with tempfile.NamedTemporaryFile(mode='rb+') as f:
            with smart_open.s3.SinglepartWriter(BUCKET_NAME, WRITE_KEY_NAME, writebuffer=f) as fout:
                fout.write(contents)

            with smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, 'rb') as fin:
                actual = fin.read()

            assert actual == contents

    def test_seekable(self):
        """Test that SinglepartWriter is seekable."""
        expected = b'  34'

        with smart_open.s3.SinglepartWriter(BUCKET_NAME, WRITE_KEY_NAME) as fout:
            fout.write(b'1234')
            self.assertEqual(len(expected), fout.tell())
            fout.seek(0)
            self.assertEqual(0, fout.tell())
            fout.write(b'  ')
            self.assertEqual(2, fout.tell())

        with self.assertRaises(ValueError, msg="I/O operation on closed file"):
            fout.seekable()

        with smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, 'rb') as fin:
            actual = fin.read()

        self.assertEqual(expected, actual)

    def test_truncate(self):
        """Test that SinglepartWriter.truncate works."""
        expected = u'не думай о секундах свысока'

        with smart_open.s3.SinglepartWriter(BUCKET_NAME, WRITE_KEY_NAME) as fout:
            fout.write(expected.encode('utf-8'))
            fout.write(b'42')
            fout.truncate(len(expected.encode('utf-8')))

        with smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, 'rb') as fin:
            with io.TextIOWrapper(fin, encoding='utf-8') as text:
                actual = text.read()

        self.assertEqual(expected, actual)

    def test_str(self):
        with smart_open.s3.open(BUCKET_NAME, 'key', 'wb', multipart_upload=False) as fout:
            assert str(fout) == "smart_open.s3.SinglepartWriter('test-smartopen', 'key')"

    def test_ensure_no_side_effects_on_exception(self):
        class WriteError(Exception):
            pass

        s3_resource = _resource("s3")
        obj = s3_resource.Object(BUCKET_NAME, KEY_NAME)

        # wrap in closure to ease writer dereferencing
        def _run():
            with smart_open.s3.open(BUCKET_NAME, obj.key, "wb", multipart_upload=False) as fout:
                fout.write(b"this should not be written")
                raise WriteError

        try:
            _run()
        except WriteError:
            pass
        finally:
            self.assertRaises(s3_resource.meta.client.exceptions.NoSuchKey, obj.get)


ARBITRARY_CLIENT_ERROR = botocore.client.ClientError(error_response={}, operation_name='bar')


@mock_s3
class IterBucketTest(unittest.TestCase):
    def setUp(self):
        ignore_resource_warnings()
        _resource('s3').create_bucket(Bucket=BUCKET_NAME).wait_until_exists()

    @pytest.mark.skipif(condition=sys.platform == 'win32', reason="does not run on windows")
    @pytest.mark.xfail(
        condition=sys.platform == 'darwin',
        reason="MacOS uses spawn rather than fork for multiprocessing",
    )
    def test_iter_bucket(self):
        populate_bucket()
        results = list(smart_open.s3.iter_bucket(BUCKET_NAME))
        self.assertEqual(len(results), 10)

    @pytest.mark.skipif(condition=sys.platform == 'win32', reason="does not run on windows")
    @pytest.mark.xfail(
        condition=sys.platform == 'darwin',
        reason="MacOS uses spawn rather than fork for multiprocessing",
    )
    def test_iter_bucket_404(self):
        populate_bucket()

        def throw_404_error_for_key_4(*args):
            if args[1] == "key_4":
                raise botocore.exceptions.ClientError(
                    error_response={"Error": {"Code": "404", "Message": "Not Found"}},
                    operation_name="HeadObject",
                )
            else:
                return [0]

        with mock.patch("smart_open.s3._download_fileobj", side_effect=throw_404_error_for_key_4):
            results = list(smart_open.s3.iter_bucket(BUCKET_NAME))
            self.assertEqual(len(results), 9)

    @pytest.mark.skipif(condition=sys.platform == 'win32', reason="does not run on windows")
    @pytest.mark.xfail(
        condition=sys.platform == 'darwin',
        reason="MacOS uses spawn rather than fork for multiprocessing",
    )
    def test_iter_bucket_non_404(self):
        populate_bucket()
        with mock.patch("smart_open.s3._download_fileobj", side_effect=ARBITRARY_CLIENT_ERROR):
            with pytest.raises(botocore.exceptions.ClientError):
                list(smart_open.s3.iter_bucket(BUCKET_NAME))

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

    @pytest.mark.skipif(condition=sys.platform == 'win32', reason="does not run on windows")
    @pytest.mark.xfail(
        condition=sys.platform == 'darwin',
        reason="MacOS uses spawn rather than fork for multiprocessing",
    )
    def test_accepts_boto3_bucket(self):
        populate_bucket()
        bucket = _resource('s3').Bucket(BUCKET_NAME)
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


@mock_s3
@pytest.mark.skipif(
    condition=not smart_open.concurrency._CONCURRENT_FUTURES,
    reason='concurrent.futures unavailable',
)
@pytest.mark.skipif(condition=sys.platform == 'win32', reason="does not run on windows")
@pytest.mark.xfail(
    condition=sys.platform == 'darwin',
    reason="MacOS uses spawn rather than fork for multiprocessing",
)
class IterBucketConcurrentFuturesTest(unittest.TestCase):
    def setUp(self):
        self.old_flag_multi = smart_open.concurrency._MULTIPROCESSING
        smart_open.concurrency._MULTIPROCESSING = False
        ignore_resource_warnings()

        _resource('s3').create_bucket(Bucket=BUCKET_NAME).wait_until_exists()

    def tearDown(self):
        smart_open.concurrency._MULTIPROCESSING = self.old_flag_multi

    def test(self):
        num_keys = 101
        populate_bucket(num_keys=num_keys)
        keys = list(smart_open.s3.iter_bucket(BUCKET_NAME))
        self.assertEqual(len(keys), num_keys)

        expected = [('key_%d' % x, b'%d' % x) for x in range(num_keys)]
        self.assertEqual(sorted(keys), sorted(expected))


@mock_s3
@pytest.mark.skipif(
    condition=not smart_open.concurrency._MULTIPROCESSING,
    reason='multiprocessing unavailable',
)
@pytest.mark.skipif(condition=sys.platform == 'win32', reason="does not run on windows")
@pytest.mark.xfail(
    condition=sys.platform == 'darwin',
    reason="MacOS uses spawn rather than fork for multiprocessing",
)
class IterBucketMultiprocessingTest(unittest.TestCase):
    def setUp(self):
        self.old_flag_concurrent = smart_open.concurrency._CONCURRENT_FUTURES
        smart_open.concurrency._CONCURRENT_FUTURES = False
        ignore_resource_warnings()

        _resource('s3').create_bucket(Bucket=BUCKET_NAME).wait_until_exists()

    def tearDown(self):
        smart_open.concurrency._CONCURRENT_FUTURES = self.old_flag_concurrent

    def test(self):
        num_keys = 101
        populate_bucket(num_keys=num_keys)
        keys = list(smart_open.s3.iter_bucket(BUCKET_NAME))
        self.assertEqual(len(keys), num_keys)

        expected = [('key_%d' % x, b'%d' % x) for x in range(num_keys)]
        self.assertEqual(sorted(keys), sorted(expected))


@mock_s3
class IterBucketSingleProcessTest(unittest.TestCase):
    def setUp(self):
        self.old_flag_multi = smart_open.concurrency._MULTIPROCESSING
        self.old_flag_concurrent = smart_open.concurrency._CONCURRENT_FUTURES
        smart_open.concurrency._MULTIPROCESSING = False
        smart_open.concurrency._CONCURRENT_FUTURES = False

        ignore_resource_warnings()

        _resource('s3').create_bucket(Bucket=BUCKET_NAME).wait_until_exists()

    def tearDown(self):
        smart_open.concurrency._MULTIPROCESSING = self.old_flag_multi
        smart_open.concurrency._CONCURRENT_FUTURES = self.old_flag_concurrent

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
@mock_s3
class IterBucketCredentialsTest(unittest.TestCase):
    def test(self):
        _resource('s3').create_bucket(Bucket=BUCKET_NAME).wait_until_exists()
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


@mock_s3
class DownloadKeyTest(unittest.TestCase):
    def setUp(self):
        ignore_resource_warnings()

        s3 = _resource('s3')
        bucket = s3.create_bucket(Bucket=BUCKET_NAME)
        bucket.wait_until_exists()

        self.body = b'hello'
        s3.Object(BUCKET_NAME, KEY_NAME).put(Body=self.body)

    def test_happy(self):
        expected = (KEY_NAME, self.body)
        actual = smart_open.s3._download_key(KEY_NAME, bucket_name=BUCKET_NAME)
        self.assertEqual(expected, actual)

    def test_intermittent_error(self):
        expected = (KEY_NAME, self.body)
        side_effect = [ARBITRARY_CLIENT_ERROR, ARBITRARY_CLIENT_ERROR, self.body]
        with mock.patch('smart_open.s3._download_fileobj', side_effect=side_effect):
            actual = smart_open.s3._download_key(KEY_NAME, bucket_name=BUCKET_NAME)
        self.assertEqual(expected, actual)

    def test_persistent_error(self):
        side_effect = [ARBITRARY_CLIENT_ERROR, ARBITRARY_CLIENT_ERROR,
                       ARBITRARY_CLIENT_ERROR, ARBITRARY_CLIENT_ERROR]
        with mock.patch('smart_open.s3._download_fileobj', side_effect=side_effect):
            self.assertRaises(botocore.client.ClientError, smart_open.s3._download_key,
                              KEY_NAME, bucket_name=BUCKET_NAME)

    def test_intermittent_error_retries(self):
        expected = (KEY_NAME, self.body)
        side_effect = [ARBITRARY_CLIENT_ERROR, ARBITRARY_CLIENT_ERROR,
                       ARBITRARY_CLIENT_ERROR, ARBITRARY_CLIENT_ERROR, self.body]
        with mock.patch('smart_open.s3._download_fileobj', side_effect=side_effect):
            actual = smart_open.s3._download_key(KEY_NAME, bucket_name=BUCKET_NAME, retries=4)
        self.assertEqual(expected, actual)

    def test_propagates_other_exception(self):
        with mock.patch('smart_open.s3._download_fileobj', side_effect=ValueError):
            self.assertRaises(ValueError, smart_open.s3._download_key,
                              KEY_NAME, bucket_name=BUCKET_NAME)


@mock_s3
class OpenTest(unittest.TestCase):
    def setUp(self):
        ignore_resource_warnings()
        _resource('s3').create_bucket(Bucket=BUCKET_NAME).wait_until_exists()

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
    s3 = _resource('s3')
    for key_number in range(num_keys):
        key_name = 'key_%d' % key_number
        s3.Object(BUCKET_NAME, key_name).put(Body=str(key_number))


class RetryIfFailedTest(unittest.TestCase):
    def setUp(self):
        self.retry = smart_open.s3.Retry()
        self.retry.attempts = 3
        self.retry.sleep_seconds = 0

    def test_success(self):
        partial = mock.Mock(return_value=1)
        result = self.retry._do(partial)
        self.assertEqual(result, 1)
        self.assertEqual(partial.call_count, 1)

    def test_failure_exception(self):
        partial = mock.Mock(side_effect=ValueError)
        self.retry.exceptions = {ValueError: 'Let us retry ValueError'}
        with self.assertRaises(IOError):
            self.retry._do(partial)
        self.assertEqual(partial.call_count, 3)

    def test_failure_client_error(self):
        partial = mock.Mock(
            side_effect=botocore.exceptions.ClientError(
                {'Error': {'Code': 'NoSuchUpload'}}, 'NoSuchUpload'
            )
        )
        with self.assertRaises(IOError):
            self.retry._do(partial)
        self.assertEqual(partial.call_count, 3)


@mock_s3
def test_client_propagation_singlepart():
    """Does the client parameter make it from the caller to Boto3?"""
    #
    # Not sure why we need to create the bucket here, as setUpModule should
    # have done that for us by now.
    #
    session = boto3.Session()
    _resource('s3').create_bucket(Bucket=BUCKET_NAME).wait_until_exists()

    client = session.client('s3')

    with smart_open.s3.open(
        BUCKET_NAME,
        WRITE_KEY_NAME,
        mode='wb',
        client=client,
        multipart_upload=False,
    ) as writer:
        assert writer._client.client == client
        assert id(writer._client.client) == id(client)


@mock_s3
def test_client_propagation_multipart():
    """Does the resource parameter make it from the caller to Boto3?"""
    session = boto3.Session()
    _resource('s3').create_bucket(Bucket=BUCKET_NAME).wait_until_exists()

    client = session.client('s3')

    with smart_open.s3.open(
        BUCKET_NAME,
        WRITE_KEY_NAME,
        mode='wb',
        client=client,
        multipart_upload=True,
    ) as writer:
        assert writer._client.client == client
        assert id(writer._client.client) == id(client)


@mock_s3
def test_resource_propagation_reader():
    """Does the resource parameter make it from the caller to Boto3?"""
    session = boto3.Session()
    resource = session.resource('s3', region_name='us-east-1')
    bucket = resource.create_bucket(Bucket=BUCKET_NAME)
    bucket.wait_until_exists()

    client = session.client('s3')

    with smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, mode='wb') as writer:
        writer.write(b'hello world')

    with smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, mode='rb', client=client) as reader:
        assert reader._client.client == client
        assert id(reader._client.client) == id(client)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    unittest.main()
