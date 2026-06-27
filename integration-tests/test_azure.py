import io
import os

import azure.storage.blob
import pytest

import smart_open

_AZURE_CONTAINER = os.environ.get("SO_AZURE_CONTAINER")
_AZURE_STORAGE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
_FILE_PREFIX = f"{smart_open.azure.SCHEME}://{_AZURE_CONTAINER}"

assert _AZURE_CONTAINER is not None, "please set the SO_AZURE_CONTAINER environment variable"
assert _AZURE_STORAGE_CONNECTION_STRING is not None, (
    "please set the AZURE_STORAGE_CONNECTION_STRING environment variable"
)


@pytest.fixture
def client():
    """Provide an Azure BlobServiceClient built from the env connection string."""
    # type: () -> azure.storage.blob.BlobServiceClient
    return azure.storage.blob.BlobServiceClient.from_connection_string(_AZURE_STORAGE_CONNECTION_STRING)


def initialize_bucket(client):
    """Empty the configured Azure container so each test starts clean."""
    container_client = client.get_container_client(_AZURE_CONTAINER)
    blobs = container_client.list_blobs()
    for blob in blobs:
        container_client.delete_blob(blob=blob)


def write_read(key, content, write_mode, read_mode, **kwargs):
    """Write ``content`` to ``key`` then read it back, returning the value read."""
    with smart_open.open(key, write_mode, **kwargs) as fout:
        fout.write(content)
    with smart_open.open(key, read_mode, **kwargs) as fin:
        return fin.read()


def read_length_prefixed_messages(key, read_mode, **kwargs):
    """Read length-prefixed binary messages from ``key`` and concatenate them."""
    result = io.BytesIO()

    with smart_open.open(key, read_mode, **kwargs) as fin:
        length_byte = fin.read(1)
        while len(length_byte):
            result.write(length_byte)
            msg = fin.read(ord(length_byte))
            result.write(msg)
            length_byte = fin.read(1)
    return result.getvalue()


def test_azure_readwrite_text(benchmark, client):
    """Round-trip a text blob via smart_open against Azure."""
    initialize_bucket(client)

    key = _FILE_PREFIX + "/sanity.txt"
    text = "с гранатою в кармане, с чекою в руке"  # noqa: RUF001  # Cyrillic fixture
    actual = benchmark(write_read, key, text, "w", "r", encoding="utf-8", transport_params={"client": client})
    assert actual == text


def test_azure_readwrite_text_gzip(benchmark, client):
    """Round-trip a gzip-compressed text blob via smart_open against Azure."""
    initialize_bucket(client)

    key = _FILE_PREFIX + "/sanity.txt.gz"
    text = "не чайки здесь запели на знакомом языке"
    actual = benchmark(write_read, key, text, "w", "r", encoding="utf-8", transport_params={"client": client})
    assert actual == text


def test_azure_readwrite_binary(benchmark, client):
    """Round-trip a binary blob via smart_open against Azure."""
    initialize_bucket(client)

    key = _FILE_PREFIX + "/sanity.txt"
    binary = b"this is a test"
    actual = benchmark(write_read, key, binary, "wb", "rb", transport_params={"client": client})
    assert actual == binary


def test_azure_readwrite_binary_gzip(benchmark, client):
    """Round-trip a gzip-compressed binary blob via smart_open against Azure."""
    initialize_bucket(client)

    key = _FILE_PREFIX + "/sanity.txt.gz"
    binary = b"this is a test"
    actual = benchmark(write_read, key, binary, "wb", "rb", transport_params={"client": client})
    assert actual == binary


def test_azure_performance(benchmark, client):
    """Benchmark uncompressed binary read/write performance against Azure."""
    initialize_bucket(client)

    one_megabyte = io.BytesIO()
    for _ in range(1024 * 128):
        one_megabyte.write(b"01234567")
    one_megabyte = one_megabyte.getvalue()

    key = _FILE_PREFIX + "/performance.txt"
    actual = benchmark(write_read, key, one_megabyte, "wb", "rb", transport_params={"client": client})
    assert actual == one_megabyte


def test_azure_performance_gz(benchmark, client):
    """Benchmark gzip-compressed binary read/write performance against Azure."""
    initialize_bucket(client)

    one_megabyte = io.BytesIO()
    for _ in range(1024 * 128):
        one_megabyte.write(b"01234567")
    one_megabyte = one_megabyte.getvalue()

    key = _FILE_PREFIX + "/performance.txt.gz"
    actual = benchmark(write_read, key, one_megabyte, "wb", "rb", transport_params={"client": client})
    assert actual == one_megabyte


def test_azure_performance_small_reads(benchmark, client):
    """Benchmark many small reads against Azure with a 1 MiB buffer."""
    initialize_bucket(client)

    ONE_MIB = 1024**2  # noqa: N806  # intentional constant in test scope
    one_megabyte_of_msgs = io.BytesIO()
    msg = b"\x0f" + b"0123456789abcde"  # a length-prefixed "message"
    for _ in range(0, ONE_MIB, len(msg)):
        one_megabyte_of_msgs.write(msg)
    one_megabyte_of_msgs = one_megabyte_of_msgs.getvalue()

    key = _FILE_PREFIX + "/many_reads_performance.bin"

    with smart_open.open(key, "wb", transport_params={"client": client}) as fout:
        fout.write(one_megabyte_of_msgs)

    actual = benchmark(
        read_length_prefixed_messages, key, "rb", buffering=ONE_MIB, transport_params={"client": client}
    )
    assert actual == one_megabyte_of_msgs
