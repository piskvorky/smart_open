#
# Copyright (C) 2020 Radim Rehurek <radim@rare-technologies.com>
# Copyright (C) 2020 Nicolas Mitchell <ncls.mitchell@gmail.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import gzip
import io
import logging
import os
import time
import unittest
import unittest.mock
import uuid
from collections import OrderedDict
from typing import Literal

import azure.common
import azure.core.exceptions
import azure.storage.blob
import pytest

import smart_open
import smart_open.constants

CONTAINER_NAME = f"test-smartopen-{uuid.uuid4().hex}"
BLOB_NAME = "test-blob"
DISABLE_MOCKS = os.environ.get("SO_DISABLE_AZURE_MOCKS") == "1"

"""If mocks are disabled, allow to use the Azurite local Azure Storage API
https://github.com/Azure/Azurite
To use locally:
docker run -p 10000:10000 -p 10001:10001 mcr.microsoft.com/azure-storage/azurite
"""
_AZURITE_DEFAULT_CONNECT_STR = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)
CONNECT_STR = os.environ.get("SO_AZURE_CONNECTION_STRING", _AZURITE_DEFAULT_CONNECT_STR)

logger = logging.getLogger(__name__)


class FakeBlobClient:
    # From Azure's BlobClient API
    # https://azuresdkdocs.blob.core.windows.net/$web/python/azure-storage-blob/12.0.0/azure.storage.blob.html#azure.storage.blob.BlobClient
    def __init__(self, container_client, name):
        self._container_client = container_client  # type: FakeContainerClient
        self.blob_name = name
        self.metadata = {"size": 0}
        self.__contents = io.BytesIO()
        self._staged_contents = {}
        self._blob_type = None
        self._exists = False

    def commit_block_list(self, block_list, metadata=None):
        data = b"".join([self._staged_contents[block_blob["id"]] for block_blob in block_list])
        self.__contents = io.BytesIO(data)
        metadata = metadata or {}
        metadata.update({"size": len(data)})
        self.set_blob_metadata(metadata)
        self._blob_type = azure.storage.blob.BlobType.BLOCKBLOB
        self._exists = True
        self._container_client.register_blob_client(self)
        self._staged_contents = {}

    def delete_blob(self):
        self._container_client.delete_blob(self)

    def download_blob(self, offset=None, length=None, max_concurrency=1):
        if offset is None:
            return self.__contents
        self.__contents.seek(offset)
        return io.BytesIO(self.__contents.read(length))

    def get_blob_properties(self):
        if not self._exists:
            msg = "The specified blob does not exist."
            raise azure.core.exceptions.ResourceNotFoundError(msg)
        return self.metadata

    def get_block_list(self, block_list_type: Literal["all", "uncommitted", "committed"] = "committed"):
        """Returns a tuple of two lists - committed and uncommitted blocks."""
        return [], list(self._staged_contents.keys())

    def set_blob_metadata(self, metadata):
        self.metadata = metadata

    def stage_block(self, block_id, data):
        """Simulates API call to stage a block of data."""
        self._staged_contents[block_id] = data

    def upload_blob(self, data, length=None, metadata=None, **kwargs):
        blob_type = kwargs.get("blob_type")

        if blob_type == azure.storage.blob.BlobType.APPENDBLOB:
            if self._exists and self._blob_type != azure.storage.blob.BlobType.APPENDBLOB:
                msg = "The blob type is invalid for this operation."
                raise azure.core.exceptions.ResourceExistsError(msg)
            self._blob_type = azure.storage.blob.BlobType.APPENDBLOB
            self._exists = True
            # Append to existing contents
            existing = self.__contents.getvalue()
            new_data = data[:length] if length is not None else data
            self.__contents = io.BytesIO(existing + new_data)
            self.set_blob_metadata({"size": len(existing + new_data)})
        else:
            if metadata is not None:
                self.set_blob_metadata(metadata)
            self.__contents = io.BytesIO(data[:length])
            self.set_blob_metadata({"size": len(data[:length])})
            self._blob_type = blob_type
            self._exists = True
        self._container_client.register_blob_client(self)


class FakeBlobClientTest(unittest.TestCase):
    def setUp(self):
        self.blob_service_client = FakeBlobServiceClient.from_connection_string(CONNECT_STR)
        self.container_client = FakeContainerClient(self.blob_service_client, "test-container")
        self.blob_client = FakeBlobClient(self.container_client, "test-blob.txt")

    def test_delete_blob(self):
        data = b"Lorem ipsum"
        self.blob_client.upload_blob(data)
        assert self.container_client.list_blobs() == [self.blob_client.blob_name]
        self.blob_client.delete_blob()
        assert self.container_client.list_blobs() == []

    def test_upload_blob(self):
        data = b"Lorem ipsum"
        self.blob_client.upload_blob(data)
        actual = self.blob_client.download_blob().read()
        assert actual == data


class FakeContainerClient:
    # From Azure's ContainerClient API
    # https://docs.microsoft.com/fr-fr/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python
    def __init__(self, blob_service_client, name):
        self.blob_service_client = blob_service_client  # type: FakeBlobServiceClient
        self.container_name = name
        self.metadata = {}
        self.__blob_clients = OrderedDict()

    def create_container(self, metadata):
        self.metadata = metadata

    def delete_blob(self, blob):
        del self.__blob_clients[blob.blob_name]

    def delete_blobs(self, **kwargs):
        self.__blob_clients = OrderedDict()

    def delete_container(self):
        self.blob_service_client.delete_container(self.container_name)

    def download_blob(self, blob):
        if blob.blob_name not in list(self.__blob_clients.keys()):
            msg = "The specified blob does not exist."
            raise azure.core.exceptions.ResourceNotFoundError(msg)
        blob_client = self.__blob_clients[blob.blob_name]
        return blob_client.download_blob()

    def get_blob_client(self, blob_name):
        return self.__blob_clients.get(blob_name, FakeBlobClient(self, blob_name))

    def get_container_properties(self):
        return self.metadata

    def list_blobs(self):
        return list(self.__blob_clients.keys())

    def upload_blob(self, blob_name, data):
        blob_client = FakeBlobClient(self, blob_name)
        blob_client.upload_blob(data)
        self.__blob_clients[blob_name] = blob_client

    def register_blob_client(self, blob_client):
        self.__blob_clients[blob_client.blob_name] = blob_client


class FakeContainerClientTest(unittest.TestCase):
    def setUp(self):
        self.blob_service_client = FakeBlobServiceClient.from_connection_string(CONNECT_STR)
        self.container_client = FakeContainerClient(self.blob_service_client, "test-container")

    def test_nonexistent_blob(self):
        blob_client = self.container_client.get_blob_client("test-blob.txt")
        with pytest.raises(azure.core.exceptions.ResourceNotFoundError):
            self.container_client.download_blob(blob_client)

    def test_delete_blob(self):
        blob_name = "test-blob.txt"
        data = b"Lorem ipsum"
        self.container_client.upload_blob(blob_name, data)
        assert self.container_client.list_blobs() == [blob_name]
        blob_client = FakeBlobClient(self.container_client, "test-blob.txt")
        self.container_client.delete_blob(blob_client)
        assert self.container_client.list_blobs() == []

    def test_delete_blobs(self):
        blob_name_1 = "test-blob-1.txt"
        blob_name_2 = "test-blob-2.txt"
        data = b"Lorem ipsum"
        self.container_client.upload_blob(blob_name_1, data)
        self.container_client.upload_blob(blob_name_2, data)
        assert self.container_client.list_blobs() == [blob_name_1, blob_name_2]

    def test_delete_container(self):
        container_name = "test-container"
        container_client = self.blob_service_client.create_container(container_name)
        assert self.blob_service_client.get_container_client(container_name).container_name == container_name
        container_client.delete_container()
        with pytest.raises(azure.core.exceptions.ResourceNotFoundError):
            self.blob_service_client.get_container_client(container_name)

    def test_list_blobs(self):
        blob_name_1 = "test-blob-1.txt"
        blob_name_2 = "test-blob-2.txt"
        data = b"Lorem ipsum"
        self.container_client.upload_blob(blob_name_1, data)
        self.container_client.upload_blob(blob_name_2, data)
        assert self.container_client.list_blobs() == [blob_name_1, blob_name_2]
        self.container_client.delete_blobs()
        assert self.container_client.list_blobs() == []

    def test_upload_blob(self):
        blob_name = "test-blob.txt"
        data = b"Lorem ipsum"
        self.container_client.upload_blob(blob_name, data)
        blob_client = self.container_client.get_blob_client(blob_name)
        actual = self.container_client.download_blob(blob_client).read()
        assert actual == data


class FakeBlobServiceClient:
    # From Azure's BlobServiceClient API
    # https://docs.microsoft.com/fr-fr/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python
    def __init__(self, account_url, credential=None, **kwargs):
        self._account_url = account_url
        self._credential = credential

        self.__container_clients = OrderedDict()

    @classmethod
    def from_connection_string(cls, conn_str, credential=None, **kwargs):
        account_url, secondary, credential = azure.storage.blob._shared.base_client.parse_connection_str(
            conn_str, credential, "blob"
        )
        if "secondary_hostname" not in kwargs:
            kwargs["secondary_hostname"] = secondary
        return cls(account_url, credential=credential, **kwargs)

    def create_container(self, container_name, metadata=None):
        if container_name in self.__container_clients:
            msg = "The specified container already exists."
            raise azure.core.exceptions.ResourceExistsError(msg)
        container_client = FakeContainerClient(self, container_name)
        if metadata is not None:
            container_client.create_container(metadata)
        self.__container_clients[container_name] = container_client
        return container_client

    def delete_container(self, container_name):
        del self.__container_clients[container_name]

    def get_blob_client(self, container, blob):
        container = self.__container_clients[container]
        return container.get_blob_client(blob)

    def get_container_client(self, container):
        if container not in self.__container_clients:
            msg = "The specified container does not exist."
            raise azure.core.exceptions.ResourceNotFoundError(msg)
        return self.__container_clients[container]


class FakeBlobServiceClientTest(unittest.TestCase):
    def setUp(self):
        self.blob_service_client = FakeBlobServiceClient.from_connection_string(CONNECT_STR)

    def test_nonexistent_container(self):
        with pytest.raises(azure.core.exceptions.ResourceNotFoundError):
            self.blob_service_client.get_container_client("test-container")

    def test_create_container(self):
        container_name = "test_container"
        expected = self.blob_service_client.create_container(container_name)
        actual = self.blob_service_client.get_container_client(container_name)
        assert actual == expected

    def test_duplicate_container(self):
        container_name = "test-container"
        self.blob_service_client.create_container(container_name)
        with pytest.raises(azure.core.exceptions.ResourceExistsError):
            self.blob_service_client.create_container(container_name)

    def test_delete_container(self):
        container_name = "test_container"
        self.blob_service_client.create_container(container_name)
        self.blob_service_client.delete_container(container_name)
        with pytest.raises(azure.core.exceptions.ResourceNotFoundError):
            self.blob_service_client.get_container_client(container_name)

    def test_get_blob_client(self):
        container_name = "test_container"
        blob_name = "test-blob.txt"
        self.blob_service_client.create_container(container_name)
        blob_client = self.blob_service_client.get_blob_client(container_name, blob_name)
        assert blob_client.blob_name == blob_name


if DISABLE_MOCKS:
    CLIENT = azure.storage.blob.BlobServiceClient.from_connection_string(CONNECT_STR)
else:
    CLIENT = FakeBlobServiceClient.from_connection_string(CONNECT_STR)


def get_container_client():
    return CLIENT.get_container_client(container=CONTAINER_NAME)


def cleanup_container():
    container_client = get_container_client()
    container_client.delete_blobs(delete_snapshots="include")


def put_to_container(blob_name, contents, num_attempts=12, sleep_time=5):
    logger.debug("%r", locals())

    #
    # In real life, it can take a few seconds for the container to become ready.
    # If we try to write to the key while the container while it isn't ready, we
    # will get a StorageError: NotFound.
    #
    for _attempt in range(num_attempts):
        try:
            container_client = get_container_client()
            container_client.upload_blob(blob_name, contents)
            return
        except azure.common.AzureHttpError as err:
            logger.exception("caught %r, retrying", err)
            time.sleep(sleep_time)

    msg = f"failed to create container {CONTAINER_NAME} after {num_attempts} attempts"
    raise AssertionError(msg)


def setUpModule():
    """Set up the test Azure container (called once by unittest)."""
    CLIENT.create_container(CONTAINER_NAME)


def tearDownModule():
    """Empty and remove the test Azure container (called once by unittest)."""
    try:
        container_client = get_container_client()
        container_client.delete_container()
    except azure.common.AzureHttpError:
        pass


class ReaderTest(unittest.TestCase):
    def tearDown(self):
        cleanup_container()

    def test_iter(self):
        """Are Azure Blob Storage files iterated over correctly?"""
        expected = "hello wořld\nhow are you?".encode()
        blob_name = f"test_iter_{BLOB_NAME}"
        put_to_container(blob_name, contents=expected)

        # connect to fake Azure Blob Storage and read from the fake key we filled above
        fin = smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT)
        output = [line.rstrip(b"\n") for line in fin]
        assert output == expected.split(b"\n")

    def test_iter_context_manager(self):
        # same thing but using a context manager
        expected = "hello wořld\nhow are you?".encode()
        blob_name = f"test_iter_context_manager_{BLOB_NAME}"
        put_to_container(blob_name, contents=expected)

        with smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT) as fin:
            output = [line.rstrip(b"\n") for line in fin]
            assert output == expected.split(b"\n")

    def test_read(self):
        """Are Azure Blob Storage files read correctly?"""
        content = "hello wořld\nhow are you?".encode()
        blob_name = f"test_read_{BLOB_NAME}"
        put_to_container(blob_name, contents=content)
        logger.debug("content: %r len: %r", content, len(content))

        fin = smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT)
        assert content[:6] == fin.read(6)
        assert content[6:14] == fin.read(8)  # ř is 2 bytes
        assert content[14:] == fin.read()  # read the rest

    def test_read_max_concurrency(self):
        """Are Azure Blob Storage files read correctly?"""
        content = "hello wořld\nhow are you?".encode()
        blob_name = f"test_read_max_concurrency_{BLOB_NAME}"
        put_to_container(blob_name, contents=content)
        logger.debug("content: %r len: %r", content, len(content))

        fin = smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT, max_concurrency=4)
        assert content[:6] == fin.read(6)
        assert content[6:14] == fin.read(8)  # ř is 2 bytes
        assert content[14:] == fin.read()  # read the rest

    def test_seek_beginning(self):
        """Does seeking to the beginning of Azure Blob Storage files work correctly?"""
        content = "hello wořld\nhow are you?".encode()
        blob_name = f"test_seek_beginning_{BLOB_NAME}"
        put_to_container(blob_name, contents=content)

        fin = smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT)
        assert content[:6] == fin.read(6)
        assert content[6:14] == fin.read(8)  # ř is 2 bytes

        fin.seek(0)
        assert content == fin.read()  # no size given => read whole file

        fin.seek(0)
        assert content == fin.read(-1)  # same thing

    def test_seek_start(self):
        """Does seeking from the start of Azure Blob Storage files work correctly?"""
        content = "hello wořld\nhow are you?".encode()
        blob_name = f"test_seek_start_{BLOB_NAME}"
        put_to_container(blob_name, contents=content)

        fin = smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT)
        seek = fin.seek(6)
        assert seek == 6
        assert fin.tell() == 6
        assert fin.read(6) == "wořld".encode()

    def test_seek_current(self):
        """Does seeking from the middle of Azure Blob Storage files work correctly?"""
        content = "hello wořld\nhow are you?".encode()
        blob_name = f"test_seek_current_{BLOB_NAME}"
        put_to_container(blob_name, contents=content)

        fin = smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT)
        assert fin.read(5) == b"hello"
        seek = fin.seek(1, whence=smart_open.constants.WHENCE_CURRENT)
        assert seek == 6
        assert fin.read(6) == "wořld".encode()

    def test_seek_end(self):
        """Does seeking from the end of Azure Blob Storage files work correctly?"""
        content = "hello wořld\nhow are you?".encode()
        blob_name = f"test_seek_end_{BLOB_NAME}"
        put_to_container(blob_name, contents=content)

        fin = smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT)
        seek = fin.seek(-4, whence=smart_open.constants.WHENCE_END)
        assert seek == len(content) - 4
        assert fin.read() == b"you?"

    def test_seek_forward_within_buffer(self):
        """Does forward seeking within buffered data avoid additional download_blob requests?"""
        content = "hello wořld\nhow are you?".encode()
        blob_name = f"test_seek_forward_within_buffer_{BLOB_NAME}"
        put_to_container(blob_name, contents=content)

        fin = smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT, buffer_size=32)
        assert fin.read(5) == b"hello"

        # Account for the initial download_blob call from the read above.
        with unittest.mock.patch.object(
            fin._blob, "download_blob", wraps=fin._blob.download_blob
        ) as mock_download:
            # Forward seek within buffer using WHENCE_CURRENT - no new download
            seek = fin.seek(1, whence=smart_open.constants.WHENCE_CURRENT)
            assert seek == 6
            assert fin.read(6) == "wořld".encode()

            # Forward seek within buffer using WHENCE_START - no new download
            seek = fin.seek(13, whence=smart_open.constants.WHENCE_START)
            assert seek == 13
            assert fin.read(3) == b"how"

            mock_download.assert_not_called()

    def test_detect_eof(self):
        content = "hello wořld\nhow are you?".encode()
        blob_name = f"test_detect_eof_{BLOB_NAME}"
        put_to_container(blob_name, contents=content)

        fin = smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT)
        fin.read()
        eof = fin.tell()
        assert eof == len(content)
        fin.seek(0, whence=smart_open.constants.WHENCE_END)
        assert eof == fin.tell()

    def test_read_gzip(self):
        expected = "раcцветали яблони и груши, поплыли туманы над рекой...".encode()
        buf = io.BytesIO()
        buf.close = lambda: None  # keep buffer open so that we can .getvalue()
        with gzip.GzipFile(fileobj=buf, mode="w") as zipfile:
            zipfile.write(expected)
        blob_name = f"test_read_gzip_{BLOB_NAME}"
        put_to_container(blob_name, contents=buf.getvalue())

        #
        # Make sure we're reading things correctly.
        #
        with smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT) as fin:
            assert fin.read() == buf.getvalue()

        #
        # Make sure the buffer we wrote is legitimate gzip.
        #
        sanity_buf = io.BytesIO(buf.getvalue())
        with gzip.GzipFile(fileobj=sanity_buf) as zipfile:
            assert zipfile.read() == expected

        logger.debug("starting actual test")
        with smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT) as fin:
            with gzip.GzipFile(fileobj=fin) as zipfile:
                actual = zipfile.read()

        assert expected == actual

    def test_readline(self):
        content = b"englishman\nin\nnew\nyork\n"
        blob_name = f"test_readline_{BLOB_NAME}"
        put_to_container(blob_name, contents=content)

        with smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT) as fin:
            fin.readline()
            assert fin.tell() == content.index(b"\n") + 1

            fin.seek(0)
            actual = list(fin)
            assert fin.tell() == len(content)

        expected = [b"englishman\n", b"in\n", b"new\n", b"york\n"]
        assert expected == actual

    def test_readline_tiny_buffer(self):
        content = b"englishman\nin\nnew\nyork\n"
        blob_name = f"test_readline_tiny_buffer_{BLOB_NAME}"
        put_to_container(blob_name, contents=content)

        with smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT, buffer_size=8) as fin:
            actual = list(fin)

        expected = [b"englishman\n", b"in\n", b"new\n", b"york\n"]
        assert expected == actual

    def test_read0_does_not_return_data(self):
        content = b"englishman\nin\nnew\nyork\n"
        blob_name = f"test_read0_does_not_return_data_{BLOB_NAME}"
        put_to_container(blob_name, contents=content)

        with smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT) as fin:
            data = fin.read(0)

        assert data == b""

    def test_read_past_end(self):
        content = b"englishman\nin\nnew\nyork\n"
        blob_name = f"test_read_past_end_{BLOB_NAME}"
        put_to_container(blob_name, contents=content)

        with smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT) as fin:
            data = fin.read(100)

        assert data == content

    def test_read_container_client(self):
        content = b"spirits in the material world"
        blob_name = f"test_read_container_client_{BLOB_NAME}"
        put_to_container(blob_name, contents=content)

        container_client = CLIENT.get_container_client(CONTAINER_NAME)

        with smart_open.azure.Reader(CONTAINER_NAME, blob_name, container_client) as fin:
            data = fin.read(100)

        assert data == content

    def test_read_blob_client(self):
        content = b"walking on the moon"
        blob_name = f"test_read_blob_client_{BLOB_NAME}"
        put_to_container(blob_name, contents=content)

        container_client = CLIENT.get_container_client(CONTAINER_NAME)
        blob_client = container_client.get_blob_client(blob_name)

        with smart_open.azure.Reader(CONTAINER_NAME, blob_name, blob_client) as fin:
            data = fin.read(100)

        assert data == content

    def test_nonexisting_container(self):
        with (
            pytest.raises(azure.core.exceptions.ResourceNotFoundError),
            smart_open.azure.open("thiscontainerdoesntexist", "mykey", "rb", CLIENT) as fin,
        ):
            fin.read()


class WriterTest(unittest.TestCase):
    """Test writing into Azure Blob files."""

    def tearDown(self):
        cleanup_container()

    def test_write_01(self):
        """Does writing into Azure Blob Storage work correctly?"""
        test_string = "žluťoučký koníček".encode()
        blob_name = f"test_write_01_{BLOB_NAME}"

        with smart_open.azure.Writer(CONTAINER_NAME, blob_name, CLIENT) as fout:
            fout.write(test_string)

        output = list(
            smart_open.open(
                f"azure://{CONTAINER_NAME}/{blob_name}",
                "rb",
                transport_params={"client": CLIENT},
            )
        )
        assert output == [test_string]

    def test_write_container_client(self):
        """Does writing into Azure Blob Storage work correctly?"""
        test_string = "Hiszékeny Öngyilkos Vasárnap".encode()
        blob_name = f"test_write_container_client_{BLOB_NAME}"

        container_client = CLIENT.get_container_client(CONTAINER_NAME)

        with smart_open.azure.Writer(CONTAINER_NAME, blob_name, container_client) as fout:
            fout.write(test_string)

        output = list(
            smart_open.open(
                f"azure://{CONTAINER_NAME}/{blob_name}",
                "rb",
                transport_params={"client": container_client},
            )
        )
        assert output == [test_string]

    def test_write_blob_client(self):
        """Does writing into Azure Blob Storage work correctly?"""
        test_string = "žluťoučký koníček".encode()
        blob_name = f"test_write_blob_client_{BLOB_NAME}"

        container_client = CLIENT.get_container_client(CONTAINER_NAME)
        blob_client = container_client.get_blob_client(blob_name)

        with smart_open.open(
            f"azure://{CONTAINER_NAME}/{blob_name}",
            "wb",
            transport_params={"client": blob_client, "blob_kwargs": {"metadata": {"name": blob_name}}},
        ) as fout:
            fout.write(test_string)

        assert blob_client.get_blob_properties()["name"] == blob_name

        output = list(
            smart_open.open(
                f"azure://{CONTAINER_NAME}/{blob_name}",
                "rb",
                transport_params={"client": CLIENT},
            )
        )
        assert output == [test_string]

    def test_abort_upload(self):
        """Does aborted upload skip commit_block_list?"""
        test_string = b"42" * 42
        blob_name = f"test_abort_upload_{BLOB_NAME}"

        container_client = CLIENT.get_container_client(CONTAINER_NAME)
        blob_client = container_client.get_blob_client(blob_name)

        try:
            with smart_open.open(
                f"azure://{CONTAINER_NAME}/{blob_name}",
                "wb",
                transport_params={"client": blob_client, "min_part_size": 42},
            ) as fout:
                fout.write(test_string)
                raise ValueError
        except ValueError:
            # FakeBlobClient.commit_block_list was not called
            assert len(blob_client.get_block_list("uncommitted")[1]) > 0

    def test_abort_upload_text_mode(self):
        """Does aborted upload skip commit_block_list in text mode?"""
        test_string = "42" * 42
        blob_name = f"test_abort_upload_{BLOB_NAME}"

        container_client = CLIENT.get_container_client(CONTAINER_NAME)
        blob_client = container_client.get_blob_client(blob_name)

        try:
            with smart_open.open(
                f"azure://{CONTAINER_NAME}/{blob_name}",
                "w",
                transport_params={"client": blob_client, "min_part_size": 42},
            ) as fout:
                fout.write(test_string)
                raise ValueError
        except ValueError:
            # FakeBlobClient.commit_block_list was not called
            assert len(blob_client.get_block_list("uncommitted")[1]) > 0

    def test_abort_upload_compressed(self):
        """Does aborted upload skip commit_block_list with compression?"""
        test_string = b"42" * 42
        blob_name = f"test_abort_upload_{BLOB_NAME}.gz"

        container_client = CLIENT.get_container_client(CONTAINER_NAME)
        blob_client = container_client.get_blob_client(blob_name)

        try:
            with smart_open.open(
                f"azure://{CONTAINER_NAME}/{blob_name}",
                "wb",
                transport_params={"client": blob_client, "min_part_size": 42},
            ) as fout:
                fout.write(test_string)
                raise ValueError
        except ValueError:
            # FakeBlobClient.commit_block_list was not called
            assert len(blob_client.get_block_list("uncommitted")[1]) > 0

    def test_incorrect_input(self):
        """Does azure write fail on incorrect input?"""
        blob_name = f"test_incorrect_input_{BLOB_NAME}"
        try:
            with smart_open.azure.Writer(CONTAINER_NAME, blob_name, CLIENT) as fin:
                fin.write(None)
        except TypeError:
            pass
        else:
            self.fail()

    def test_write_02(self):
        """Does Azure Blob Storage write unicode-utf8 conversion work?"""
        blob_name = f"test_write_02_{BLOB_NAME}"
        smart_open_write = smart_open.azure.Writer(CONTAINER_NAME, blob_name, CLIENT)
        smart_open_write.tell()
        logger.info("smart_open_write: %r", smart_open_write)
        with smart_open_write as fout:
            fout.write("testžížáč".encode())
            assert fout.tell() == 14

    def test_write_03(self):
        """Do multiple writes less than the min_part_size work correctly?"""
        # write
        blob_name = f"test_write_03_{BLOB_NAME}"
        min_part_size = 256 * 1024
        smart_open_write = smart_open.azure.Writer(
            CONTAINER_NAME, blob_name, CLIENT, min_part_size=min_part_size
        )
        local_write = io.BytesIO()

        with smart_open_write as fout:
            first_part = b"t" * 262141
            fout.write(first_part)
            local_write.write(first_part)
            assert fout._current_part.tell() == 262141

            second_part = b"t\n"
            fout.write(second_part)
            local_write.write(second_part)
            assert fout._current_part.tell() == 262143
            assert fout._total_parts == 0

            third_part = b"t"
            fout.write(third_part)
            local_write.write(third_part)
            assert fout._current_part.tell() == 0
            assert fout._total_parts == 1

            fourth_part = b"t" * 1
            fout.write(fourth_part)
            local_write.write(fourth_part)
            assert fout._current_part.tell() == 1
            assert fout._total_parts == 1

        # read back the same key and check its content
        uri = f"azure://{CONTAINER_NAME}/{blob_name}"
        output = list(smart_open.open(uri, transport_params={"client": CLIENT}))
        local_write.seek(0)
        actual = [line.decode("utf-8") for line in list(local_write)]
        assert output == actual

    def test_write_03a(self):
        """Do multiple writes greater than or equal to the min_part_size work correctly?"""
        min_part_size = 256 * 1024
        blob_name = f"test_write_03_{BLOB_NAME}"
        smart_open_write = smart_open.azure.Writer(
            CONTAINER_NAME, blob_name, CLIENT, min_part_size=min_part_size
        )
        local_write = io.BytesIO()

        with smart_open_write as fout:
            for i in range(1, 4):
                part = b"t" * min_part_size
                fout.write(part)
                local_write.write(part)
                assert fout._current_part.tell() == 0
                assert fout._total_parts == i

        # read back the same key and check its content
        uri = f"azure://{CONTAINER_NAME}/{blob_name}"
        output = list(smart_open.open(uri, transport_params={"client": CLIENT}))
        local_write.seek(0)
        actual = [line.decode("utf-8") for line in list(local_write)]
        assert output == actual

    def test_write_04(self):
        """Does writing no data cause key with an empty value to be created?"""
        blob_name = f"test_write_04_{BLOB_NAME}"
        smart_open_write = smart_open.azure.Writer(CONTAINER_NAME, blob_name, CLIENT)
        with smart_open_write as fout:  # noqa: F841
            pass

        # read back the same key and check its content
        output = list(
            smart_open.open(f"azure://{CONTAINER_NAME}/{blob_name}", transport_params={"client": CLIENT})
        )
        assert output == []

    def test_gzip(self):
        expected = "а не спеть ли мне песню... о любви".encode()
        blob_name = f"test_gzip_{BLOB_NAME}"
        with smart_open.azure.Writer(CONTAINER_NAME, blob_name, CLIENT) as fout:
            with gzip.GzipFile(fileobj=fout, mode="w") as zipfile:
                zipfile.write(expected)

        with smart_open.azure.Reader(CONTAINER_NAME, blob_name, CLIENT) as fin:
            with gzip.GzipFile(fileobj=fin) as zipfile:
                actual = zipfile.read()

        assert expected == actual

    def test_buffered_writer_wrapper_works(self):
        """Ensure that we can wrap a smart_open azure stream in a `BufferedWriter`."""
        expected = "не думай о секундах свысока"
        blob_name = f"test_buffered_writer_wrapper_works_{BLOB_NAME}"

        with smart_open.azure.Writer(CONTAINER_NAME, blob_name, CLIENT) as fout:
            with io.BufferedWriter(fout) as sub_out:
                sub_out.write(expected.encode("utf-8"))

        with (
            smart_open.open(
                f"azure://{CONTAINER_NAME}/{blob_name}", "rb", transport_params={"client": CLIENT}
            ) as fin,
            io.TextIOWrapper(fin, encoding="utf-8") as text,
        ):
            actual = text.read()

        assert expected == actual

    def test_binary_iterator(self):
        expected = "выйду ночью в поле с конём".encode().split(b" ")
        blob_name = f"test_binary_iterator_{BLOB_NAME}"
        put_to_container(blob_name=blob_name, contents=b"\n".join(expected))
        with smart_open.azure.open(CONTAINER_NAME, blob_name, "rb", CLIENT) as fin:
            actual = [line.rstrip() for line in fin]
        assert expected == actual

    def test_nonexisting_container(self):
        expected = "выйду ночью в поле с конём".encode()
        with (
            pytest.raises(azure.core.exceptions.ResourceNotFoundError),
            smart_open.azure.open("thiscontainerdoesntexist", "mykey", "wb", CLIENT) as fout,
        ):
            fout.write(expected)

    def test_double_close(self):
        text = "там за туманами, вечными, пьяными".encode()
        fout = smart_open.azure.open(CONTAINER_NAME, "key", "wb", CLIENT)
        fout.write(text)
        fout.close()
        fout.close()

    def test_flush_close(self):
        text = "там за туманами, вечными, пьяными".encode()
        fout = smart_open.azure.open(CONTAINER_NAME, "key", "wb", CLIENT)
        fout.write(text)
        fout.flush()
        fout.close()

    def test_close_marks_closed_on_error(self):
        """If close() raises during upload, the writer must still mark itself closed.

        Otherwise __del__ would retry the upload and surface an unraisable exception.
        """
        text = "там за туманами, вечными, пьяными".encode()
        fout = smart_open.azure.Writer(CONTAINER_NAME, "key", CLIENT)
        fout.write(text)
        with unittest.mock.patch.object(fout, "_upload_part", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError):
                fout.close()
        assert fout.closed


class AppendWriterTest(unittest.TestCase):
    """Test appending into Azure Blob files."""

    def tearDown(self):
        cleanup_container()

    def test_append_non_existing_blob(self):
        """Does appending into a non-existing Azure Blob file work correctly?"""
        test_string = "žluťoučký koníček".encode()
        blob_name = f"test_append_non_existing_{BLOB_NAME}"

        with smart_open.azure.AppendWriter(CONTAINER_NAME, blob_name, CLIENT) as fout:
            fout.write(test_string)

        output = list(
            smart_open.open(
                f"azure://{CONTAINER_NAME}/{blob_name}",
                "rb",
                transport_params={"client": CLIENT},
            )
        )
        assert output == [test_string]

    def test_append_existing_blob(self):
        """Does appending into an existing Azure Blob file work correctly?"""
        test_string_1 = "žluťoučký koníček".encode()
        test_string_2 = "příliš žluťoučký kůň".encode()
        blob_name = f"test_append_existing_{BLOB_NAME}"

        with smart_open.azure.AppendWriter(CONTAINER_NAME, blob_name, CLIENT) as fout:
            fout.write(test_string_1)

        with smart_open.azure.AppendWriter(CONTAINER_NAME, blob_name, CLIENT) as fout:
            fout.write(test_string_2)

        output = list(
            smart_open.open(
                f"azure://{CONTAINER_NAME}/{blob_name}",
                "rb",
                transport_params={"client": CLIENT},
            )
        )
        assert output == [test_string_1 + test_string_2]

    def test_append_existing_write_blob(self):
        """Appending to an existing non-AppendBlob blob should fail on close."""
        test_string = "žluťoučký koníček".encode()
        blob_name = f"test_append_existing_write_blob_{BLOB_NAME}"

        # Creating blob of type BlockBlob
        with smart_open.azure.Writer(CONTAINER_NAME, blob_name, CLIENT) as fout:
            fout.write(test_string)

        fout = smart_open.azure.AppendWriter(CONTAINER_NAME, blob_name, CLIENT)
        fout.write(test_string)
        with self.assertRaises(
            azure.core.exceptions.ResourceExistsError, msg="The blob type is invalid for this operation."
        ):
            fout.close()
        # close() raised, but the writer must still mark itself closed so that
        # __del__ does not retry the upload and trigger an unraisable exception.
        assert fout.closed

    def test_append_on_error(self):
        """On error, unflushed AppendWriter buffer is discarded (same as Writer)."""
        test_string = "žluťoučký koníček".encode()
        blob_name = f"test_append_on_error_{BLOB_NAME}"

        try:
            with smart_open.azure.AppendWriter(CONTAINER_NAME, blob_name, CLIENT) as fout:
                fout.write(test_string)
                raise ValueError
        except ValueError:
            pass
        # Small write stays in buffer; terminate() discards it, so the blob
        # is never created.  Opening a non-existent blob for reading raises
        # ResourceNotFoundError.
        with pytest.raises(azure.core.exceptions.ResourceNotFoundError):
            smart_open.open(
                f"azure://{CONTAINER_NAME}/{blob_name}",
                "rb",
                transport_params={"client": CLIENT},
            )

    def test_append_multiple(self):
        """Does appending multiple times into an Azure Blob file work correctly?"""
        test_string_1 = "žluťoučký koníček".encode()
        test_string_2 = "příliš žluťoučký kůň".encode()
        test_string_3 = "škubání skřetů úpělo".encode()
        blob_name = f"test_append_multiple_{BLOB_NAME}"

        with smart_open.azure.AppendWriter(CONTAINER_NAME, blob_name, CLIENT) as fout:
            fout.write(test_string_1)
            fout.write(test_string_2)
            fout.write(test_string_3)

        output = list(
            smart_open.open(
                f"azure://{CONTAINER_NAME}/{blob_name}",
                "rb",
                transport_params={"client": CLIENT},
            )
        )
        assert output == [test_string_1 + test_string_2 + test_string_3]

    def test_append_block_over_max_block_size(self):
        """Appending a payload over the 4MB Azure block limit should still succeed."""
        test_string = b"0" * 4 * 1024 * 1024 + b"1" * 1024  # Create file with size over 4MB
        blob_name = f"test_append_block_over_max_block_size_{BLOB_NAME}"

        with smart_open.azure.AppendWriter(CONTAINER_NAME, blob_name, CLIENT) as fout:
            fout.write(test_string)

        output = list(
            smart_open.open(
                f"azure://{CONTAINER_NAME}/{blob_name}",
                "rb",
                transport_params={"client": CLIENT},
            )
        )
        assert output == [test_string]

    def test_append_compressed_gzip(self):
        """Does appending to a gzip-compressed Azure Blob work via smart_open.open?"""
        expected = "а не спеть ли мне песню... о любви".encode()
        blob_name = f"test_append_gzip_{BLOB_NAME}.gz"
        uri = f"azure://{CONTAINER_NAME}/{blob_name}"
        tp = {"client": CLIENT}

        with smart_open.open(uri, "ab", transport_params=tp) as fp:
            fp.write(expected)

        with smart_open.open(uri, "ab", transport_params=tp) as fp:
            fp.write(expected)

        with smart_open.open(uri, "rb", transport_params=tp) as fp:
            actual = fp.read()

        assert actual == expected * 2

    def test_append_min_part_size_buffering(self):
        """Does the min_part_size buffering mechanic work correctly for AppendWriter?

        A write smaller than min_part_size should stay in the buffer without
        triggering an upload.  Once the buffer reaches min_part_size, the upload
        should be triggered.
        """
        min_part_size = 256 * 1024
        blob_name = f"test_append_min_part_size_{BLOB_NAME}"

        with smart_open.azure.AppendWriter(
            CONTAINER_NAME, blob_name, CLIENT, min_part_size=min_part_size
        ) as fout:
            # First write: min_part_size - 1 bytes, should stay in buffer
            first_part = b"x" * (min_part_size - 1)
            fout.write(first_part)
            assert fout._current_part.tell() == min_part_size - 1

            # Second write: 1 byte reaches min_part_size, triggers upload
            fout.write(b"y")
            assert fout._current_part.tell() == 0

        # Verify the data was written correctly
        output = list(
            smart_open.open(
                f"azure://{CONTAINER_NAME}/{blob_name}",
                "rb",
                transport_params={"client": CLIENT},
            )
        )
        assert output == [first_part + b"y"]
