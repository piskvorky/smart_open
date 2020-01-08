# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import gzip
import inspect
import io
import logging
import os
import time
import uuid
import unittest
try:
    from unittest import mock
except ImportError:
    import mock
import warnings

import google.cloud
import google.api_core.exceptions
import six

import smart_open

BUCKET_NAME = 'test-smartopen-{}'.format(uuid.uuid4().hex)
BLOB_NAME = 'test-blob'
WRITE_BLOB_NAME = 'test-write-blob'
DISABLE_MOCKS = os.environ.get('SO_DISABLE_MOCKS') == "1"

RESUMABLE_SESSION_URI_TEMPLATE = (
    'https://www.googleapis.com/upload/storage/v1/b/'
    '{bucket}'
    '/o?uploadType=resumable&upload_id='
    '{upload_id}'
)

logger = logging.getLogger(__name__)


def ignore_resource_warnings():
    if six.PY2:
        return
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")  # noqa


class FakeBucket(object):
    def __init__(self, client, name=None):
        self.client = client  # type: FakeClient
        self.name = name
        self.blobs = []
        self._exists = True

    def blob(self, blob_id):
        blob = next((x for x in self.blobs if x.name == blob_id), None)
        if blob is None:
            blob = FakeBlob(blob_id, self)
        return blob

    def delete(self):
        self.client._delete_bucket(self)
        self._exists = False

    def exists(self):
        return self._exists

    def get_blob(self, blob_id):
        blob = next((x for x in self.blobs if x.name == blob_id), None)
        if blob is None:
            raise google.cloud.exceptions.NotFound('Blob {} not found'.format(blob_id))
        return blob

    def list_blobs(self):
        return self.blobs

    def _delete_blob(self, blob):
        self.blobs.remove(blob)


class FakeBlob(object):
    def __init__(self, name, bucket, create=True):
        self.name = name
        self._bucket = bucket  # type: FakeBucket
        self._exists = False
        self.__contents = io.BytesIO()

        if create:
            self._create_if_not_exists()

    def create_resumable_upload_session(self):
        resumeable_upload_url = RESUMABLE_SESSION_URI_TEMPLATE % dict(
            bucket=self._bucket.name,
            upload_id=uuid.uuid4().hex,
        )
        self._bucket.client._uploads[resumeable_upload_url] = self
        return resumeable_upload_url

    def delete(self):
        self._bucket._delete_blob(self)
        self._created = False

    def download_as_string(self, start=None, end=None):
        if start is None:
            start = 0
        if end is None:
            end = self.__contents.tell()
        self.__contents.seek(start)
        return self.__contents.read(end - start)

    def exists(self, client=None):
        return self._exists

    def upload_from_string(self, str):
        self.__contents.write(str)

    def write(self, data):
        self.__contents.write(data)

    @property
    def bucket(self):
        return self._bucket

    @property
    def size(self):
        if self.__contents.tell() == 0:
            return None
        return self.__contents.tell()

    def _create_if_not_exists(self):
        if self not in self._bucket.blobs:
            self._bucket.blobs.append(self)
        self._exists = True


class FakeCredentials(object):
    def __init__(self, client):
        self.client = client  # type: FakeClient

    def before_request(self, *args, **kwargs):
        pass


class FakeClient(object):
    def __init__(self, credentials=None):
        if credentials is None:
            credentials = FakeCredentials(self)
        self._credentials = credentials  # type: FakeCredentials
        self._uploads = {}
        self.__buckets = []

    def bucket(self, bucket_id):
        bucket = next((x for x in self.__buckets if x.name == bucket_id), None)
        if bucket is None:
            raise google.cloud.exceptions.NotFound('Bucket {} not found'.format(bucket_id))
        return bucket

    def create_bucket(self, bucket_id):
        bucket = FakeBucket(self, bucket_id)
        self.__buckets.append(bucket)
        return bucket

    def get_bucket(self, bucket_id):
        return self.bucket(bucket_id)

    def _delete_bucket(self, bucket):
        self.__buckets.remove(bucket)


class FakeBlobUpload(object):
    def __init__(self, url, blob):
        self.url = url
        self.blob = blob  # type: FakeBlob


class FakeResponse(object):
    def __init__(self, status_code=200):
        self.status_code = status_code


class FakeAuthorizedSession(object):
    def __init__(self, credentials):
        self._credentials = credentials  # type: FakeCredentials

    def delete(self, upload_url):
        blob = self._credentials.client._uploads[upload_url]
        blob.delete()
        self._credentials.client._uploads.pop(upload_url)

    def put(self, url, data=None, headers=None):
        if data is not None:
            self._credentials.client._uploads[url].write(data.read())
        return FakeResponse()

    def _blob_with_url(self, url, client):
        # type: (str, FakeClient) -> None
        return client._uploads.get(url)


if DISABLE_MOCKS:
    storage_client = google.cloud.storage.Client()
else:
    storage_client = FakeClient()


def get_bucket():
    return storage_client.bucket(BUCKET_NAME)


def get_blob():
    bucket = get_bucket()
    return bucket.blob(BLOB_NAME)


def cleanup_bucket():
    bucket = get_bucket()

    blobs = bucket.list_blobs()
    for blob in blobs:
        blob.delete()


def put_to_bucket(contents, num_attempts=12, sleep_time=5):
    logger.debug('%r', locals())

    #
    # In real life, it can take a few seconds for the bucket to become ready.
    # If we try to write to the key while the bucket while it isn't ready, we
    # will get a StorageError: NotFound.
    #
    for attempt in range(num_attempts):
        try:
            blob = get_blob()
            blob.upload_from_string(contents)
            return
        except google.cloud.exceptions.NotFound as err:
            logger.error('caught %r, retrying', err)
            time.sleep(sleep_time)

    assert False, 'failed to create bucket %s after %d attempts' % (BUCKET_NAME, num_attempts)


def mock_gcs(class_or_func):
    """Mock all methods of a class or a function."""
    if inspect.isclass(class_or_func):
        for attr in class_or_func.__dict__:
            if callable(getattr(class_or_func, attr)):
                setattr(class_or_func, attr, mock_gcs_func(getattr(class_or_func, attr)))
        return class_or_func
    else:
        return mock_gcs_func(class_or_func)


def mock_gcs_func(func):
    """Mock the function and provide additional required arguments."""
    def inner(*args, **kwargs):
        with mock.patch(
            'google.cloud.storage.Client',
            return_value=storage_client,
        ), \
            mock.patch(
            'smart_open.gcs.google_requests.AuthorizedSession',
            return_value=FakeAuthorizedSession(storage_client._credentials),
        ):
            assert callable(func), 'you didn\'t provide a function!'
            try:  # is it a method that needs a self arg?
                self_arg = inspect.signature(func).self
                func(self_arg, *args, **kwargs)
            except AttributeError:
                func(*args, **kwargs)
    return inner


def maybe_mock_gcs(func):
    if DISABLE_MOCKS:
        return func
    else:
        return mock_gcs(func)


@maybe_mock_gcs
def setUpModule():  # noqa
    """Called once by unittest when initializing this module.  Set up the
    test GCS bucket.
    """
    storage_client.create_bucket(BUCKET_NAME)


@maybe_mock_gcs
def tearDownModule():  # noqa
    """Called once by unittest when tearing down this module.  Empty and
    removes the test GCS bucket.
    """
    try:
        bucket = get_bucket()
        bucket.delete()
    except google.cloud.exceptions.NotFound:
        pass


@maybe_mock_gcs
class SeekableBufferedInputBaseTest(unittest.TestCase):
    def setUp(self):
        # lower the multipart upload size, to speed up these tests
        self.old_min_buffer_size = smart_open.gcs.DEFAULT_BUFFER_SIZE
        smart_open.gcs.DEFAULT_BUFFER_SIZE = 5 * 1024**2

        ignore_resource_warnings()

    def tearDown(self):
        cleanup_bucket()

    def test_iter(self):
        """Are GCS files iterated over correctly?"""
        expected = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=expected)

        # connect to fake GCS and read from the fake key we filled above
        fin = smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, BLOB_NAME)
        output = [line.rstrip(b'\n') for line in fin]
        self.assertEqual(output, expected.split(b'\n'))

    def test_iter_context_manager(self):
        # same thing but using a context manager
        expected = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=expected)
        with smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, BLOB_NAME) as fin:
            output = [line.rstrip(b'\n') for line in fin]
            self.assertEqual(output, expected.split(b'\n'))

    def test_read(self):
        """Are GCS files read correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=content)
        logger.debug('content: %r len: %r', content, len(content))

        fin = smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, BLOB_NAME)
        self.assertEqual(content[:6], fin.read(6))
        self.assertEqual(content[6:14], fin.read(8))  # ř is 2 bytes
        self.assertEqual(content[14:], fin.read())  # read the rest

    def test_seek_beginning(self):
        """Does seeking to the beginning of GCS files work correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=content)

        fin = smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, BLOB_NAME)
        self.assertEqual(content[:6], fin.read(6))
        self.assertEqual(content[6:14], fin.read(8))  # ř is 2 bytes

        fin.seek(0)
        self.assertEqual(content, fin.read())  # no size given => read whole file

        fin.seek(0)
        self.assertEqual(content, fin.read(-1))  # same thing

    def test_seek_start(self):
        """Does seeking from the start of GCS files work correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=content)

        fin = smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, BLOB_NAME)
        seek = fin.seek(6)
        self.assertEqual(seek, 6)
        self.assertEqual(fin.tell(), 6)
        self.assertEqual(fin.read(6), u'wořld'.encode('utf-8'))

    def test_seek_current(self):
        """Does seeking from the middle of GCS files work correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=content)

        fin = smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, BLOB_NAME)
        self.assertEqual(fin.read(5), b'hello')
        seek = fin.seek(1, whence=smart_open.gcs.CURRENT)
        self.assertEqual(seek, 6)
        self.assertEqual(fin.read(6), u'wořld'.encode('utf-8'))

    def test_seek_end(self):
        """Does seeking from the end of GCS files work correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=content)

        fin = smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, BLOB_NAME)
        seek = fin.seek(-4, whence=smart_open.gcs.END)
        self.assertEqual(seek, len(content) - 4)
        self.assertEqual(fin.read(), b'you?')

    def test_detect_eof(self):
        content = u"hello wořld\nhow are you?".encode('utf8')
        put_to_bucket(contents=content)

        fin = smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, BLOB_NAME)
        fin.read()
        eof = fin.tell()
        self.assertEqual(eof, len(content))
        fin.seek(0, whence=smart_open.gcs.END)
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
        with smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, BLOB_NAME) as fin:
            self.assertEqual(fin.read(), buf.getvalue())

        #
        # Make sure the buffer we wrote is legitimate gzip.
        #
        sanity_buf = io.BytesIO(buf.getvalue())
        with gzip.GzipFile(fileobj=sanity_buf) as zipfile:
            self.assertEqual(zipfile.read(), expected)

        logger.debug('starting actual test')
        with smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, BLOB_NAME) as fin:
            with gzip.GzipFile(fileobj=fin) as zipfile:
                actual = zipfile.read()

        self.assertEqual(expected, actual)

    def test_readline(self):
        content = b'englishman\nin\nnew\nyork\n'
        put_to_bucket(contents=content)

        with smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, BLOB_NAME) as fin:
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

        with smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, BLOB_NAME, buffering=8) as fin:
            actual = list(fin)

        expected = [b'englishman\n', b'in\n', b'new\n', b'york\n']
        self.assertEqual(expected, actual)

    def test_read0_does_not_return_data(self):
        content = b'englishman\nin\nnew\nyork\n'
        put_to_bucket(contents=content)

        with smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, BLOB_NAME) as fin:
            data = fin.read(0)

        self.assertEqual(data, b'')

    def test_read_past_end(self):
        content = b'englishman\nin\nnew\nyork\n'
        put_to_bucket(contents=content)

        with smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, BLOB_NAME) as fin:
            data = fin.read(100)

        self.assertEqual(data, content)


@maybe_mock_gcs
class BufferedOutputBaseTest(unittest.TestCase):
    """
    Test writing into GCS files.

    """
    def setUp(self):
        ignore_resource_warnings()

    def tearDown(self):
        cleanup_bucket()

    def test_write_01(self):
        """Does writing into GCS work correctly?"""
        test_string = u"žluťoučký koníček".encode('utf8')

        with smart_open.gcs.BufferedOutputBase(BUCKET_NAME, WRITE_BLOB_NAME) as fout:
            fout.write(test_string)

        output = list(smart_open.open("gs://{}/{}".format(BUCKET_NAME, WRITE_BLOB_NAME), "rb"))

        self.assertEqual(output, [test_string])

    def test_write_01a(self):
        """Does gcs write fail on incorrect input?"""
        try:
            with smart_open.gcs.BufferedOutputBase(BUCKET_NAME, WRITE_BLOB_NAME) as fin:
                fin.write(None)
        except TypeError:
            pass
        else:
            self.fail()

    def test_write_02(self):
        """Does gcs write unicode-utf8 conversion work?"""
        smart_open_write = smart_open.gcs.BufferedOutputBase(BUCKET_NAME, WRITE_BLOB_NAME)
        smart_open_write.tell()
        logger.info("smart_open_write: %r", smart_open_write)
        with smart_open_write as fout:
            fout.write(u"testžížáč".encode("utf-8"))
            self.assertEqual(fout.tell(), 14)

    def test_write_03(self):
        """Does gcs multipart chunking work correctly?"""
        # write
        smart_open_write = smart_open.gcs.BufferedOutputBase(
            BUCKET_NAME, WRITE_BLOB_NAME, min_part_size=256 * 1024
        )
        with smart_open_write as fout:
            fout.write(b"t" * 262141)
            self.assertEqual(fout._current_part.tell(), 262141)

            fout.write(b"t\n")
            self.assertEqual(fout._current_part.tell(), 262143)
            self.assertEqual(fout._total_parts, 0)

            fout.write(b"t")
            self.assertEqual(fout._current_part.tell(), 0)
            self.assertEqual(fout._total_parts, 1)

        # read back the same key and check its content
        output = list(smart_open.open("gs://{}/{}".format(BUCKET_NAME, WRITE_BLOB_NAME)))
        self.assertEqual(output, ["t" * 262142 + '\n', "t"])

    def test_write_04(self):
        """Does writing no data cause key with an empty value to be created?"""
        smart_open_write = smart_open.gcs.BufferedOutputBase(BUCKET_NAME, WRITE_BLOB_NAME)
        with smart_open_write as fout:  # noqa
            pass

        # read back the same key and check its content
        output = list(smart_open.open("gs://{}/{}".format(BUCKET_NAME, WRITE_BLOB_NAME)))

        self.assertEqual(output, [])

    def test_gzip(self):
        expected = u'а не спеть ли мне песню... о любви'.encode('utf-8')
        with smart_open.gcs.BufferedOutputBase(BUCKET_NAME, WRITE_BLOB_NAME) as fout:
            with gzip.GzipFile(fileobj=fout, mode='w') as zipfile:
                zipfile.write(expected)

        with smart_open.gcs.SeekableBufferedInputBase(BUCKET_NAME, WRITE_BLOB_NAME) as fin:
            with gzip.GzipFile(fileobj=fin) as zipfile:
                actual = zipfile.read()

        self.assertEqual(expected, actual)

    def test_buffered_writer_wrapper_works(self):
        """
        Ensure that we can wrap a smart_open gcs stream in a BufferedWriter, which
        passes a memoryview object to the underlying stream in python >= 2.7
        """
        expected = u'не думай о секундах свысока'

        with smart_open.gcs.BufferedOutputBase(BUCKET_NAME, WRITE_BLOB_NAME) as fout:
            with io.BufferedWriter(fout) as sub_out:
                sub_out.write(expected.encode('utf-8'))

        with smart_open.open("gs://{}/{}".format(BUCKET_NAME, WRITE_BLOB_NAME), 'rb') as fin:
            with io.TextIOWrapper(fin, encoding='utf-8') as text:
                actual = text.read()

        self.assertEqual(expected, actual)

    def test_binary_iterator(self):
        expected = u"выйду ночью в поле с конём".encode('utf-8').split(b' ')
        put_to_bucket(contents=b"\n".join(expected))
        with smart_open.gcs.open(BUCKET_NAME, BLOB_NAME, 'rb') as fin:
            actual = [line.rstrip() for line in fin]
        self.assertEqual(expected, actual)

    def test_nonexisting_bucket(self):
        expected = u"выйду ночью в поле с конём".encode('utf-8')
        with self.assertRaises(google.api_core.exceptions.NotFound):
            with smart_open.gcs.open('thisbucketdoesntexist', 'mykey', 'wb') as fout:
                fout.write(expected)

    def test_read_nonexisting_key(self):
        with self.assertRaises(google.api_core.exceptions.NotFound):
            with smart_open.gcs.open(BUCKET_NAME, 'my_nonexisting_key', 'rb') as fin:
                fin.read()

    def test_double_close(self):
        text = u'там за туманами, вечными, пьяными'.encode('utf-8')
        fout = smart_open.gcs.open(BUCKET_NAME, 'key', 'wb')
        fout.write(text)
        fout.close()
        fout.close()

    def test_flush_close(self):
        text = u'там за туманами, вечными, пьяными'.encode('utf-8')
        fout = smart_open.gcs.open(BUCKET_NAME, 'key', 'wb')
        fout.write(text)
        fout.flush()
        fout.close()

    def test_terminate(self):
        text = u'там за туманами, вечными, пьяными'.encode('utf-8')
        fout = smart_open.gcs.open(BUCKET_NAME, 'key', 'wb')
        fout.write(text)
        fout.terminate()

        with self.assertRaises(google.api_core.exceptions.NotFound):
            with smart_open.gcs.open(BUCKET_NAME, 'key', 'rb') as fin:
                fin.read()


@maybe_mock_gcs
class OpenTest(unittest.TestCase):
    def setUp(self):
        ignore_resource_warnings()

    def tearDown(self):
        cleanup_bucket()

    def test_read_never_returns_none(self):
        """read should never return None."""
        test_string = u"ветер по морю гуляет..."
        with smart_open.gcs.open(BUCKET_NAME, BLOB_NAME, "wb") as fout:
            fout.write(test_string.encode('utf8'))

        r = smart_open.gcs.open(BUCKET_NAME, BLOB_NAME, "rb")
        self.assertEqual(r.read(), test_string.encode("utf-8"))
        self.assertEqual(r.read(), b"")
        self.assertEqual(r.read(), b"")


class MakeRangeStringTest(unittest.TestCase):
    def test_no_stop(self):
        start, stop = 1, None
        self.assertEqual(smart_open.gcs._make_range_string(start, stop), 'bytes 1-/*')

    def test_stop(self):
        start, stop = 1, 2
        self.assertEqual(smart_open.gcs._make_range_string(start, stop), 'bytes 1-2/*')


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    unittest.main()
