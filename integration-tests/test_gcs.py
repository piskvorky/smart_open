import io
import os
import urllib.parse

import google.cloud.storage

import smart_open

_GCS_URL = os.environ.get("SO_GCS_URL")
assert _GCS_URL is not None, "please set the SO_GCS_URL environment variable"


def initialize_bucket():
    """Empty the configured GCS prefix so each test starts clean."""
    client = google.cloud.storage.Client()
    parsed = urllib.parse.urlparse(_GCS_URL)
    bucket_name = parsed.netloc
    prefix = parsed.path
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        blob.delete()


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


def test_gcs_readwrite_text(benchmark):
    """Round-trip a text object via smart_open against GCS."""
    initialize_bucket()

    key = _GCS_URL + "/sanity.txt"
    text = "с гранатою в кармане, с чекою в руке"  # noqa: RUF001  # Cyrillic fixture
    actual = benchmark(write_read, key, text, "w", "r", encoding="utf-8")
    assert actual == text


def test_gcs_readwrite_text_gzip(benchmark):
    """Round-trip a gzip-compressed text object via smart_open against GCS."""
    initialize_bucket()

    key = _GCS_URL + "/sanity.txt.gz"
    text = "не чайки здесь запели на знакомом языке"
    actual = benchmark(write_read, key, text, "w", "r", encoding="utf-8")
    assert actual == text


def test_gcs_readwrite_binary(benchmark):
    """Round-trip a binary object via smart_open against GCS."""
    initialize_bucket()

    key = _GCS_URL + "/sanity.txt"
    binary = b"this is a test"
    actual = benchmark(write_read, key, binary, "wb", "rb")
    assert actual == binary


def test_gcs_readwrite_binary_gzip(benchmark):
    """Round-trip a gzip-compressed binary object via smart_open against GCS."""
    initialize_bucket()

    key = _GCS_URL + "/sanity.txt.gz"
    binary = b"this is a test"
    actual = benchmark(write_read, key, binary, "wb", "rb")
    assert actual == binary


def test_gcs_performance(benchmark):
    """Benchmark uncompressed binary read/write performance against GCS."""
    initialize_bucket()

    one_megabyte = io.BytesIO()
    for _ in range(1024 * 128):
        one_megabyte.write(b"01234567")
    one_megabyte = one_megabyte.getvalue()

    key = _GCS_URL + "/performance.txt"
    actual = benchmark(write_read, key, one_megabyte, "wb", "rb")
    assert actual == one_megabyte


def test_gcs_performance_gz(benchmark):
    """Benchmark gzip-compressed binary read/write performance against GCS."""
    initialize_bucket()

    one_megabyte = io.BytesIO()
    for _ in range(1024 * 128):
        one_megabyte.write(b"01234567")
    one_megabyte = one_megabyte.getvalue()

    key = _GCS_URL + "/performance.txt.gz"
    actual = benchmark(write_read, key, one_megabyte, "wb", "rb")
    assert actual == one_megabyte


def test_gcs_performance_small_reads(benchmark):
    """Benchmark many small reads against GCS with a 1 MiB buffer."""
    initialize_bucket()

    ONE_MIB = 1024**2  # noqa: N806  # intentional constant in test scope
    one_megabyte_of_msgs = io.BytesIO()
    msg = b"\x0f" + b"0123456789abcde"  # a length-prefixed "message"
    for _ in range(0, ONE_MIB, len(msg)):
        one_megabyte_of_msgs.write(msg)
    one_megabyte_of_msgs = one_megabyte_of_msgs.getvalue()

    key = _GCS_URL + "/many_reads_performance.bin"

    with smart_open.open(key, "wb") as fout:
        fout.write(one_megabyte_of_msgs)

    actual = benchmark(read_length_prefixed_messages, key, "rb", buffering=ONE_MIB)
    assert actual == one_megabyte_of_msgs
