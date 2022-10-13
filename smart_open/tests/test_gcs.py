# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

import io
import logging
import os
import uuid
import unittest
from unittest import mock
import warnings
from collections import OrderedDict

import google.cloud
import google.api_core.exceptions

import smart_open
import smart_open.constants

BUCKET_NAME = 'test-smartopen-{}'.format(uuid.uuid4().hex)
BLOB_NAME = 'test-blob'
WRITE_BLOB_NAME = 'test-write-blob'
DISABLE_MOCKS = os.environ.get('SO_DISABLE_GCS_MOCKS') == "1"

logger = logging.getLogger(__name__)


def ignore_resource_warnings():
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")  # noqa


class FakeBucket(object):
    def __init__(self, client, name=None):
        self.client = client  # type: FakeClient
        self.name = name
        self.blobs = OrderedDict()
        self._exists = True

        #
        # This is simpler than creating a backend and metaclass to store the state of every bucket created
        #
        self.client.register_bucket(self)

    def blob(self, blob_id, **kwargs):
        return self.blobs.get(blob_id, FakeBlob(blob_id, self, **kwargs))

    def delete(self):
        self.client.delete_bucket(self)
        self._exists = False
        for blob in list(self.blobs.values()):
            blob.delete()

    def exists(self):
        return self._exists

    def get_blob(self, blob_id):
        try:
            return self.blobs[blob_id]
        except KeyError as e:
            raise google.cloud.exceptions.NotFound('Blob {} not found'.format(blob_id)) from e

    def list_blobs(self):
        return list(self.blobs.values())

    def delete_blob(self, blob):
        del self.blobs[blob.name]

    def register_blob(self, blob):
        if blob.name not in self.blobs.keys():
            self.blobs[blob.name] = blob

    def register_upload(self, upload):
        self.client.register_upload(upload)


class FakeBucketTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()
        self.bucket = FakeBucket(self.client, 'test-bucket')

    def test_blob_registers_with_bucket(self):
        blob_id = 'blob.txt'
        expected = FakeBlob(blob_id, self.bucket)
        actual = self.bucket.blob(blob_id)
        self.assertEqual(actual, expected)

    def test_blob_alternate_constuctor(self):
        blob_id = 'blob.txt'
        expected = self.bucket.blob(blob_id)
        actual = self.bucket.list_blobs()[0]
        self.assertEqual(actual, expected)

    def test_delete(self):
        blob_id = 'blob.txt'
        blob = FakeBlob(blob_id, self.bucket)
        self.bucket.delete()
        self.assertFalse(self.bucket.exists())
        self.assertFalse(blob.exists())

    def test_get_multiple_blobs(self):
        blob_one_id = 'blob_one.avro'
        blob_two_id = 'blob_two.parquet'
        blob_one = self.bucket.blob(blob_one_id)
        blob_two = self.bucket.blob(blob_two_id)
        actual_first_blob = self.bucket.get_blob(blob_one_id)
        actual_second_blob = self.bucket.get_blob(blob_two_id)
        self.assertEqual(actual_first_blob, blob_one)
        self.assertEqual(actual_second_blob, blob_two)

    def test_get_nonexistent_blob(self):
        with self.assertRaises(google.cloud.exceptions.NotFound):
            self.bucket.get_blob('test-blob')

    def test_list_blobs(self):
        blob_one = self.bucket.blob('blob_one.avro')
        blob_two = self.bucket.blob('blob_two.parquet')
        actual = self.bucket.list_blobs()
        expected = [blob_one, blob_two]
        self.assertEqual(actual, expected)


class FakeBlob(object):
    def __init__(self, name, bucket, **kwargs):
        self.name = name
        self._bucket = bucket  # type: FakeBucket
        self._exists = False
        self.__contents = io.BytesIO()
        self.__contents.close = lambda: None
        self._create_if_not_exists()

        self.open = mock.Mock(side_effect=self._mock_open)

    def _mock_open(self, mode, *args, **kwargs):
        if mode.startswith('r'):
            self.__contents.seek(0)
        return self.__contents

    def delete(self):
        self._bucket.delete_blob(self)
        self._exists = False

    def exists(self, client=None):
        return self._exists

    def write(self, data):
        self.upload_from_string(data)

    @property
    def bucket(self):
        return self._bucket

    @property
    def size(self):
        if self.__contents.tell() == 0:
            return None
        return self.__contents.tell()

    def _create_if_not_exists(self):
        self._bucket.register_blob(self)
        self._exists = True


class FakeClient(object):
    def __init__(self):
        self.__buckets = OrderedDict()

    def bucket(self, bucket_id):
        try:
            return self.__buckets[bucket_id]
        except KeyError as e:
            raise google.cloud.exceptions.NotFound('Bucket %s not found' % bucket_id) from e

    def create_bucket(self, bucket_id):
        bucket = FakeBucket(self, bucket_id)
        return bucket

    def get_bucket(self, bucket_id):
        return self.bucket(bucket_id)

    def register_bucket(self, bucket):
        if bucket.name in self.__buckets:
            raise google.cloud.exceptions.Conflict('Bucket %s already exists' % bucket.name)
        self.__buckets[bucket.name] = bucket

    def delete_bucket(self, bucket):
        del self.__buckets[bucket.name]

    def register_upload(self, upload):
        self.uploads[upload.url] = upload


class FakeClientTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()

    def test_nonexistent_bucket(self):
        with self.assertRaises(google.cloud.exceptions.NotFound):
            self.client.bucket('test-bucket')

    def test_bucket(self):
        bucket_id = 'test-bucket'
        bucket = FakeBucket(self.client, bucket_id)
        actual = self.client.bucket(bucket_id)
        self.assertEqual(actual, bucket)

    def test_duplicate_bucket(self):
        bucket_id = 'test-bucket'
        FakeBucket(self.client, bucket_id)
        with self.assertRaises(google.cloud.exceptions.Conflict):
            FakeBucket(self.client, bucket_id)

    def test_create_bucket(self):
        bucket_id = 'test-bucket'
        bucket = self.client.create_bucket(bucket_id)
        actual = self.client.get_bucket(bucket_id)
        self.assertEqual(actual, bucket)


def get_test_bucket(client):
    return client.bucket(BUCKET_NAME)


def cleanup_test_bucket(client):
    bucket = get_test_bucket(client)

    blobs = bucket.list_blobs()
    for blob in blobs:
        blob.delete()


class OpenTest(unittest.TestCase):
    def setUp(self):
        if DISABLE_MOCKS:
            self.client = google.cloud.storage.Client()
        else:
            self.client = FakeClient()
            self.mock_gcs = mock.patch('smart_open.gcs.google.cloud.storage.Client').start()
            self.mock_gcs.return_value = self.client

        self.client.create_bucket(BUCKET_NAME)

        ignore_resource_warnings()

    def tearDown(self):
        cleanup_test_bucket(self.client)
        bucket = get_test_bucket(self.client)
        bucket.delete()

        if not DISABLE_MOCKS:
            self.mock_gcs.stop()

    def test_read_never_returns_none(self):
        """read should never return None."""
        test_string = u"ветер по морю гуляет..."
        with smart_open.gcs.open(BUCKET_NAME, BLOB_NAME, "wb") as fout:
            fout.write(test_string.encode('utf8'))

        r = smart_open.gcs.open(BUCKET_NAME, BLOB_NAME, "rb")
        self.assertEqual(r.read(), test_string.encode("utf-8"))
        self.assertEqual(r.read(), b"")
        self.assertEqual(r.read(), b"")

    def test_round_trip(self):
        test_string = u"ветер по морю гуляет..."
        url = 'gs://%s/%s' % (BUCKET_NAME, BLOB_NAME)
        with smart_open.open(url, "w", encoding='utf-8') as fout:
            fout.write(test_string)

        with smart_open.open(url, encoding='utf-8') as fin:
            actual = fin.read()

        self.assertEqual(test_string, actual)


class WriterTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()
        self.mock_gcs = mock.patch('smart_open.gcs.google.cloud.storage.Client').start()
        self.mock_gcs.return_value = self.client

        self.client.create_bucket(BUCKET_NAME)

    def tearDown(self):
        cleanup_test_bucket(self.client)
        bucket = get_test_bucket(self.client)
        bucket.delete()
        self.mock_gcs.stop()

    def test_property_passthrough(self):
        blob_properties = {'content_type': 'text/utf-8'}

        smart_open.gcs.Writer(BUCKET_NAME, BLOB_NAME, blob_properties=blob_properties)

        b = self.client.bucket(BUCKET_NAME).get_blob(BLOB_NAME)

        for k, v in blob_properties.items():
            self.assertEqual(getattr(b, k), v)

    def test_default_open_kwargs(self):
        smart_open.gcs.Writer(BUCKET_NAME, BLOB_NAME)

        self.client.bucket(BUCKET_NAME).get_blob(BLOB_NAME) \
            .open.assert_called_once_with('wb', **smart_open.gcs._DEFAULT_WRITE_OPEN_KWARGS)

    def test_open_kwargs_passthrough(self):
        open_kwargs = {'ignore_flush': True, 'property': 'value', 'something': 2}

        smart_open.gcs.Writer(BUCKET_NAME, BLOB_NAME, blob_open_kwargs=open_kwargs)

        self.client.bucket(BUCKET_NAME).get_blob(BLOB_NAME) \
            .open.assert_called_once_with('wb', **open_kwargs)

    def test_non_existing_bucket(self):
        with self.assertRaises(google.cloud.exceptions.NotFound):
            smart_open.gcs.Writer('unknown_bucket', BLOB_NAME)

    def test_will_warn_for_conflict(self):
        # Add a terminate() to simulate that being added to the underlying google-cloud-storage library
        original_mo = FakeBlob._mock_open

        def fake_open_with_terminate(*args, **kwargs):
            original_output = original_mo(*args, **kwargs)
            original_output.terminate = lambda: None
            return original_output

        FakeBlob._mock_open = fake_open_with_terminate

        with self.assertRaises(RuntimeWarning):
            smart_open.gcs.Writer(BUCKET_NAME, BLOB_NAME)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    unittest.main()
