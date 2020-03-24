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
from collections import OrderedDict

from azure.storage.blob import BlobServiceClient
from azure.common import AzureHttpError
import six

import smart_open

CONTAINER_NAME = 'test-smartopen-{}'.format(uuid.uuid4().hex)
BLOB_NAME = 'test-blob'
DISABLE_MOCKS = os.environ.get('SO_DISABLE_GCS_MOCKS') == "1"

logger = logging.getLogger(__name__)


class FakeBlobClient(object):
    # From Azure's BlobClient API
    # https://azuresdkdocs.blob.core.windows.net/$web/python/azure-storage-blob/12.0.0/azure.storage.blob.html#azure.storage.blob.BlobClient
    def __init__(self, container_client, name):
        self._container_client = container_client # type: FakeContainerClient
        self.name = name
        self.metadata = {}
        self.__contents = io.BytesIO()

    def delete_blob(self):
        self._container_client.delete_blob(self)

    def download_blob(self, offset=None, length=None):
        if offset is None:
            return self.__contents
        self.__contents.seek(offset)
        return io.BytesIO(self.__contents.read(length))

    def get_blob_properties(self):
        return self.metadata

    def set_blob_metadata(self, metadata):
        self.metadata = metadata

    def upload_blob(self, data, length=None, metadata=None):
        if metadata is not None:
            self.set_blob_metadata(metadata)
        self.__contents = io.BytesIO(data[:length])
        self._container_client.register_blob(self)


class FakeBlobClientTest(unittest.TestCase):
    def setUp(self):
        self.blob_service_client = FakeBlobServiceClient()
        self.container_client = FakeContainerClient(self.blob_service_client, 'test-container')
        self.blob_client = FakeBlobClient(self.container_client, 'test-blob.txt')

    def test_delete_blob(self):
        data = b'Lorem ipsum'
        self.blob_client.upload_blob(data)
        self.assertEqual(self.container_client.list_blobs(), [self.blob_client.name])
        self.blob_client.delete_blob()
        self.assertEqual(self.container_client.list_blobs(), [])

    def test_upload_blob(self):
        data = b'Lorem ipsum'
        self.blob_client.upload_blob(data)
        actual = self.blob_client.download_blob().read()
        self.assertEqual(actual, data)


class FakeContainerClient(object):
    # From Azure's ContainerClient API
    # https://docs.microsoft.com/fr-fr/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python
    def __init__(self, blob_service_client, name):
        self.blob_service_client = blob_service_client # type: FakeBlobServiceClient
        self.name = name
        self.metadata = {}
        self.__blob_clients = OrderedDict()

    def create_container(self, metadata):
        self.metadata = metadata

    def delete_blob(self, blob):
        del self.__blob_clients[blob.name]

    def delete_blobs(self):
        self.__blob_clients = OrderedDict()

    def download_blob(self, blob):
        if blob.name not in list(self.__blob_clients.keys()):
            raise AzureHttpError('Blob %s not found' % blob.name, status_code=404)
        blob_client = self.__blob_clients[blob.name]
        blob_content = blob_client.download_blob()
        return blob_content

    def get_blob_client(self, blob_name):
        if blob_name not in list(self.__blob_clients.keys()):
            raise AzureHttpError('Blob %s not found' % blob_name, status_code=404)
        blob_client = self.__blob_clients[blob_name]
        return blob_client

    def get_container_properties(self):
        return self.metadata

    def list_blobs(self):
        return list(self.__blob_clients.keys())

    def upload_blob(self, blob_name, data):
        blob_client = FakeBlobClient(self, blob_name)
        blob_client.upload_blob(data)
        self.__blob_clients[blob_name] = blob_client

    def register_blob(self, blob):
        self.__blob_clients[blob.name] = blob


class FakeContainerClientTest(unittest.TestCase):
    def setUp(self):
        self.blob_service_client = FakeBlobServiceClient()
        self.container_client = FakeContainerClient(self.blob_service_client, 'test-container')

    def test_nonexistent_blob(self):
        with self.assertRaises(AzureHttpError):
            self.container_client.get_blob_client('test-blob.txt')
        blob = FakeBlobClient(self.container_client, 'test-blob.txt')
        with self.assertRaises(AzureHttpError):
            self.container_client.download_blob(blob)

    def test_delete_blob(self):
        blob_name = 'test-blob.txt'
        data = b'Lorem ipsum'
        self.container_client.upload_blob(blob_name, data)
        self.assertEqual(self.container_client.list_blobs(), [blob_name])
        blob = FakeBlobClient(self.container_client, 'test-blob.txt')
        self.container_client.delete_blob(blob)
        self.assertEqual(self.container_client.list_blobs(), [])

    def test_delete_blobs(self):
        blob_name_1 = 'test-blob-1.txt'
        blob_name_2 = 'test-blob-2.txt'
        data = b'Lorem ipsum'
        self.container_client.upload_blob(blob_name_1, data)
        self.container_client.upload_blob(blob_name_2, data)
        self.assertEqual(self.container_client.list_blobs(), [blob_name_1, blob_name_2])

    def test_list_blobs(self):
        blob_name_1 = 'test-blob-1.txt'
        blob_name_2 = 'test-blob-2.txt'
        data = b'Lorem ipsum'
        self.container_client.upload_blob(blob_name_1, data)
        self.container_client.upload_blob(blob_name_2, data)
        self.assertEqual(self.container_client.list_blobs(), [blob_name_1, blob_name_2])
        self.container_client.delete_blobs()
        self.assertEqual(self.container_client.list_blobs(), [])

    def test_upload_blob(self):
        blob_name = 'test-blob.txt'
        data = b'Lorem ipsum'
        self.container_client.upload_blob(blob_name, data)
        blob_client = self.container_client.get_blob_client(blob_name)
        actual = blob_client.download_blob().read()
        self.assertEqual(actual, data)


class FakeBlobServiceClient(object):
    # From Azure's BlobServiceClient API
    # https://docs.microsoft.com/fr-fr/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python
    def __init__(self):
        self.__container_clients = OrderedDict()

    def create_container(self, container_name, metadata=None):
        if container_name in self.__container_clients:
            raise AzureHttpError('Container %s already exists' % container_name, status_code=409)
        container_client = FakeContainerClient(self, container_name)
        if metadata is not None:
            container_client.create_container(metadata)
        self.__container_clients[container_name] = container_client
        return container_client

    def delete_container(self, container_name):
        del self.__container_clients[container_name]

    def get_blob_client(self, container, blob):
        container = self.__container_clients[container]
        blob_client = container.get_blob_client(blob)
        return blob_client

    def get_container_client(self, container):
        if container not in self.__container_clients:
            raise AzureHttpError('Container %s not found' % container, status_code=404)
        return self.__container_clients[container]


class FakeBlobServiceClientTest(unittest.TestCase):
    def setUp(self):
        self.blob_service_client = FakeBlobServiceClient()

    def test_nonexistent_container(self):
        with self.assertRaises(AzureHttpError):
            self.blob_service_client.get_container_client('test-container')

    def test_create_container(self):
        container_name = 'test_container'
        expected = self.blob_service_client.create_container(container_name)
        actual = self.blob_service_client.get_container_client(container_name)
        self.assertEqual(actual, expected)

    def test_duplicate_container(self):
        container_name = 'test-container'
        self.blob_service_client.create_container(container_name)
        with self.assertRaises(AzureHttpError):
            self.blob_service_client.create_container(container_name)

    def test_delete_container(self):
        container_name = 'test_container'
        self.blob_service_client.create_container(container_name)
        self.blob_service_client.delete_container(container_name)
        with self.assertRaises(AzureHttpError):
            self.blob_service_client.get_container_client(container_name)


if DISABLE_MOCKS:
    connect_str = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;'
    test_blob_service_client = BlobServiceClient.from_connection_string(connect_str)
else:
    test_blob_service_client = FakeBlobServiceClient()


def get_container_client():
    return test_blob_service_client.create_container(CONTAINER_NAME)


def get_blob_client():
    return test_blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)


def cleanup_container():
    container_client = test_blob_service_client.get_container_client(container=CONTAINER_NAME)
    container_client.delete_blobs()


def put_to_container(contents, num_attempts=12, sleep_time=5):
    logger.debug('%r', locals())

    #
    # In real life, it can take a few seconds for the container to become ready.
    # If we try to write to the key while the container while it isn't ready, we
    # will get a StorageError: NotFound.
    #
    for attempt in range(num_attempts):
        try:
            blob_client = test_blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
            blob_client.upload_blob(contents)
            return
        except AzureHttpError as err:
            logger.error('caught %r, retrying', err)
            time.sleep(sleep_time)

    assert False, 'failed to create container %s after %d attempts' % (CONTAINER_NAME, num_attempts)
