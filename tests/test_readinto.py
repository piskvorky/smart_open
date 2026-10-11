import array
import io

import pytest

from smart_open import azure, hdfs, http, s3, webhdfs


@pytest.fixture(
    params=[azure.Reader, hdfs.CliRawInputBase, http.BufferedInputBase, s3.Reader, webhdfs.BufferedInputBase]
)
def readinto(request):
    """Return a transport's readinto implementation."""
    return request.param.readinto


@pytest.mark.parametrize("destination", [b"1234", memoryview(b"1234"), b""])
def test_readinto_readonly_preserves_position(readinto, destination):
    """Reject read-only destinations before consuming bytes, including at EOF."""
    source = io.BytesIO(b"payload")
    with pytest.raises(TypeError):
        readinto(source, destination)
    assert source.tell() == 0
    assert source.read() == b"payload"
    with pytest.raises(TypeError):
        readinto(source, destination)


@pytest.mark.parametrize("destination", [bytearray(4), array.array("i", [0]), memoryview(bytearray(8))[2:6]])
def test_readinto_writable_buffers(readinto, destination):
    """Fill writable byte, typed, and sliced buffers without losing data."""
    source = io.BytesIO(b"payload")
    view = memoryview(destination).cast("B")
    assert readinto(source, destination) == len(view)
    assert bytes(view) == b"payload"[: len(view)]
    assert source.read() == b"payload"[len(view) :]
