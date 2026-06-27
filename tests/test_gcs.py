#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

import io
import logging
import os
import unittest
import uuid
import warnings
from collections import OrderedDict
from unittest import mock

import google.api_core.exceptions
import google.cloud
import pytest

import smart_open
import smart_open.constants

BUCKET_NAME = f"test-smartopen-{uuid.uuid4().hex}"
BLOB_NAME = "test-blob"
WRITE_BLOB_NAME = "test-write-blob"
DISABLE_MOCKS = os.environ.get("SO_DISABLE_GCS_MOCKS") == "1"

logger = logging.getLogger(__name__)


def ignore_resource_warnings():
    """Ignore resource warnings."""
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")


class FakeBucket:
    """In-memory fake of Bucket."""

    def __init__(self, client, name=None):
        self.client = client  # type: FakeClient
        self.name = name
        self.blobs = OrderedDict()
        self._exists = True

        #
        # This is simpler than creating a backend and metaclass to store the state of every bucket created
        #
        self.client.register_bucket(self)

        self.get_blob = mock.Mock(side_effect=self._get_blob)

    def blob(self, blob_id, **kwargs):
        """Blob."""
        return self.blobs.get(blob_id, FakeBlob(blob_id, self, **kwargs))

    def delete(self):
        """Delete."""
        self.client.delete_bucket(self)
        self._exists = False
        for blob in list(self.blobs.values()):
            blob.delete()

    def exists(self):
        """Exists."""
        return self._exists

    def _get_blob(self, blob_id, **kwargs):
        try:
            return self.blobs[blob_id]
        except KeyError as e:
            msg = f"Blob {blob_id} not found"
            raise google.cloud.exceptions.NotFound(msg) from e

    def list_blobs(self):
        """List blobs."""
        return list(self.blobs.values())

    def delete_blob(self, blob):
        """Delete blob."""
        del self.blobs[blob.name]

    def register_blob(self, blob):
        """Register blob."""
        if blob.name not in self.blobs:
            self.blobs[blob.name] = blob

    def register_upload(self, upload):
        """Register upload."""
        self.client.register_upload(upload)


class FakeBucketTest(unittest.TestCase):
    """Tests for Fake Bucket."""

    def setUp(self):
        """SetUp."""
        self.client = FakeClient()
        self.bucket = FakeBucket(self.client, "test-bucket")

    def test_blob_registers_with_bucket(self):
        """Blob registers with bucket."""
        blob_id = "blob.txt"
        expected = FakeBlob(blob_id, self.bucket)
        actual = self.bucket.blob(blob_id)
        assert actual == expected

    def test_blob_alternate_constuctor(self):
        """Blob alternate constuctor."""
        blob_id = "blob.txt"
        expected = self.bucket.blob(blob_id)
        actual = self.bucket.list_blobs()[0]
        assert actual == expected

    def test_delete(self):
        """Delete."""
        blob_id = "blob.txt"
        blob = FakeBlob(blob_id, self.bucket)
        self.bucket.delete()
        assert not self.bucket.exists()
        assert not blob.exists()

    def test_get_multiple_blobs(self):
        """Get multiple blobs."""
        blob_one_id = "blob_one.avro"
        blob_two_id = "blob_two.parquet"
        blob_one = self.bucket.blob(blob_one_id)
        blob_two = self.bucket.blob(blob_two_id)
        actual_first_blob = self.bucket.get_blob(blob_one_id)
        actual_second_blob = self.bucket.get_blob(blob_two_id)
        assert actual_first_blob == blob_one
        assert actual_second_blob == blob_two

    def test_get_nonexistent_blob(self):
        """Get nonexistent blob."""
        with pytest.raises(google.cloud.exceptions.NotFound):
            self.bucket.get_blob("test-blob")

    def test_list_blobs(self):
        """List blobs."""
        blob_one = self.bucket.blob("blob_one.avro")
        blob_two = self.bucket.blob("blob_two.parquet")
        actual = self.bucket.list_blobs()
        expected = [blob_one, blob_two]
        assert actual == expected


class FakeBlob:
    """In-memory fake of Blob."""

    def __init__(self, name, bucket, **kwargs):
        self.name = name
        self._bucket = bucket  # type: FakeBucket
        self._exists = False
        self.__contents = io.BytesIO()
        self.__contents.close = lambda: None
        self._create_if_not_exists()

        self.open = mock.Mock(side_effect=self._mock_open)

    def _mock_open(self, mode, *args, **kwargs):
        if mode.startswith("r"):
            self.__contents.seek(0)
        return self.__contents

    def delete(self):
        """Delete."""
        self._bucket.delete_blob(self)
        self._exists = False

    def exists(self, client=None):
        """Exists."""
        return self._exists

    def write(self, data):
        """Write."""
        self.upload_from_string(data)

    @property
    def bucket(self):
        """Bucket."""
        return self._bucket

    @property
    def size(self):
        """Size."""
        if self.__contents.tell() == 0:
            return None
        return self.__contents.tell()

    def _create_if_not_exists(self):
        self._bucket.register_blob(self)
        self._exists = True


class FakeClient:
    """In-memory fake of Client."""

    def __init__(self):
        self.__buckets = OrderedDict()

    def bucket(self, bucket_id):
        """Bucket."""
        try:
            return self.__buckets[bucket_id]
        except KeyError as e:
            msg = f"Bucket {bucket_id} not found"
            raise google.cloud.exceptions.NotFound(msg) from e

    def create_bucket(self, bucket_id):
        """Create bucket."""
        return FakeBucket(self, bucket_id)

    def get_bucket(self, bucket_id):
        """Get bucket."""
        return self.bucket(bucket_id)

    def register_bucket(self, bucket):
        """Register bucket."""
        if bucket.name in self.__buckets:
            msg = f"Bucket {bucket.name} already exists"
            raise google.cloud.exceptions.Conflict(msg)
        self.__buckets[bucket.name] = bucket

    def delete_bucket(self, bucket):
        """Delete bucket."""
        del self.__buckets[bucket.name]

    def register_upload(self, upload):
        """Register upload."""
        self.uploads[upload.url] = upload


class FakeClientTest(unittest.TestCase):
    """Tests for Fake Client."""

    def setUp(self):
        """SetUp."""
        self.client = FakeClient()

    def test_nonexistent_bucket(self):
        """Nonexistent bucket."""
        with pytest.raises(google.cloud.exceptions.NotFound):
            self.client.bucket("test-bucket")

    def test_bucket(self):
        """Bucket."""
        bucket_id = "test-bucket"
        bucket = FakeBucket(self.client, bucket_id)
        actual = self.client.bucket(bucket_id)
        assert actual == bucket

    def test_duplicate_bucket(self):
        """Duplicate bucket."""
        bucket_id = "test-bucket"
        FakeBucket(self.client, bucket_id)
        with pytest.raises(google.cloud.exceptions.Conflict):
            FakeBucket(self.client, bucket_id)

    def test_create_bucket(self):
        """Create bucket."""
        bucket_id = "test-bucket"
        bucket = self.client.create_bucket(bucket_id)
        actual = self.client.get_bucket(bucket_id)
        assert actual == bucket


def get_test_bucket(client):
    """Get test bucket."""
    return client.bucket(BUCKET_NAME)


def cleanup_test_bucket(client):
    """Cleanup test bucket."""
    bucket = get_test_bucket(client)

    blobs = bucket.list_blobs()
    for blob in blobs:
        blob.delete()


class OpenTest(unittest.TestCase):
    """Tests for Open."""

    def setUp(self):
        """SetUp."""
        if DISABLE_MOCKS:
            self.client = google.cloud.storage.Client()
        else:
            self.client = FakeClient()
            self.mock_gcs = mock.patch("smart_open.gcs.google.cloud.storage.Client").start()
            self.mock_gcs.return_value = self.client

        self.client.create_bucket(BUCKET_NAME)

        ignore_resource_warnings()

    def tearDown(self):
        """TearDown."""
        cleanup_test_bucket(self.client)
        bucket = get_test_bucket(self.client)
        bucket.delete()

        if not DISABLE_MOCKS:
            self.mock_gcs.stop()

    def test_read_never_returns_none(self):
        """Read should never return None."""
        test_string = "ветер по морю гуляет..."
        with smart_open.gcs.open(BUCKET_NAME, BLOB_NAME, "wb") as fout:
            fout.write(test_string.encode("utf8"))

        r = smart_open.gcs.open(BUCKET_NAME, BLOB_NAME, "rb")
        assert r.read() == test_string.encode("utf-8")
        assert r.read() == b""
        assert r.read() == b""

    def test_round_trip(self):
        """Round trip."""
        test_string = "ветер по морю гуляет..."
        url = f"gcs://{BUCKET_NAME}/{BLOB_NAME}"
        with smart_open.open(url, "w", encoding="utf-8") as fout:
            fout.write(test_string)

        with smart_open.open(url, encoding="utf-8") as fin:
            actual = fin.read()

        assert test_string == actual


class WriterTest(unittest.TestCase):
    """Tests for Writer."""

    def setUp(self):
        """SetUp."""
        self.client = FakeClient()
        self.mock_gcs = mock.patch("smart_open.gcs.google.cloud.storage.Client").start()
        self.mock_gcs.return_value = self.client

        self.client.create_bucket(BUCKET_NAME)

    def tearDown(self):
        """TearDown."""
        cleanup_test_bucket(self.client)
        bucket = get_test_bucket(self.client)
        bucket.delete()
        self.mock_gcs.stop()

    def test_property_passthrough(self):
        """Property passthrough."""
        blob_properties = {"content_type": "text/utf-8"}

        smart_open.gcs.Writer(BUCKET_NAME, BLOB_NAME, blob_properties=blob_properties)

        b = self.client.bucket(BUCKET_NAME).get_blob(BLOB_NAME)

        for k, v in blob_properties.items():
            assert getattr(b, k) == v

    def test_get_blob_kwargs_passthrough(self):
        """Get blob kwargs passthrough."""
        get_blob_kwargs = {"generation": "1111111111111111"}

        with pytest.raises(google.cloud.exceptions.NotFound):
            smart_open.gcs.Reader(BUCKET_NAME, BLOB_NAME, get_blob_kwargs=get_blob_kwargs)

        self.client.bucket(BUCKET_NAME).get_blob.assert_called_once_with(BLOB_NAME, **get_blob_kwargs)

    def test_default_open_kwargs(self):
        """Default open kwargs."""
        smart_open.gcs.Writer(BUCKET_NAME, BLOB_NAME)

        self.client.bucket(BUCKET_NAME).get_blob(BLOB_NAME).open.assert_called_once_with(
            "wb",
            **smart_open.gcs._DEFAULT_WRITE_OPEN_KWARGS,  # test reaches into private state
        )

    def test_open_kwargs_passthrough(self):
        """Open kwargs passthrough."""
        open_kwargs = {"ignore_flush": True, "property": "value", "something": 2}

        smart_open.gcs.Writer(BUCKET_NAME, BLOB_NAME, blob_open_kwargs=open_kwargs)

        self.client.bucket(BUCKET_NAME).get_blob(BLOB_NAME).open.assert_called_once_with("wb", **open_kwargs)

    def test_non_existing_bucket(self):
        """Non existing bucket."""
        with pytest.raises(google.cloud.exceptions.NotFound):
            smart_open.gcs.Writer("unknown_bucket", BLOB_NAME)


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO)
    unittest.main()
