#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements reading and writing to/from HDFS."""

from __future__ import annotations

import io
import logging
import subprocess
import urllib.parse
from typing import TYPE_CHECKING, TypedDict

import smart_open.utils

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer, WriteableBuffer

    from smart_open._typing import TransportParams

logger = logging.getLogger(__name__)

SCHEMES = ("hdfs", "viewfs")

URI_EXAMPLES = (
    "hdfs:///path/file",
    "hdfs://host/path/file",
    "hdfs://host:port/path/file",
    "viewfs:///path/file",
    "viewfs://host/path/file",
)


class _HDFSUri(TypedDict):
    scheme: str
    uri_path: str


def parse_uri(uri_as_string: str) -> _HDFSUri:
    """Parse an ``hdfs://`` or ``viewfs://`` URI into its path component."""
    split_uri = urllib.parse.urlsplit(uri_as_string)
    assert split_uri.scheme in SCHEMES  # noqa: S101  # internal precondition; misuse should crash loudly

    # Preserve the full URI when netloc is set so the hdfs CLI can route to
    # the right cluster; otherwise (e.g. "hdfs:///path/file") pass the
    # absolute path to the CLI.
    uri_path = uri_as_string if split_uri.netloc else split_uri.path
    if not uri_path or uri_path == "/":
        msg = f"invalid HDFS URI: {uri_as_string!r}"
        raise RuntimeError(msg)

    return {"scheme": split_uri.scheme, "uri_path": uri_path}


def open_uri(uri: str, mode: str, transport_params: TransportParams) -> CliRawInputBase | CliRawOutputBase:
    """Open an HDFS URI using the given mode and transport params."""
    smart_open.utils.check_kwargs(open, transport_params)

    parsed_uri = parse_uri(uri)
    fobj = open(parsed_uri["uri_path"], mode)
    fobj.name = parsed_uri["uri_path"].split("/")[-1]
    return fobj


def open(uri: str, mode: str) -> CliRawInputBase | CliRawOutputBase:
    """Open an HDFS `uri` for reading (``"rb"``) or writing (``"wb"``)."""
    if mode == "rb":
        return CliRawInputBase(uri)
    if mode == "wb":
        return CliRawOutputBase(uri)
    msg = f"hdfs support for mode {mode!r} not implemented"
    raise NotImplementedError(msg)


class CliRawInputBase(io.RawIOBase):
    """Reads bytes from HDFS via the "hdfs dfs" command-line interface.

    Implements the io.RawIOBase interface of the standard library.
    """

    name: str
    _sub: subprocess.Popen[bytes] | None = None  # so `closed` works if __init__ fails and __del__ runs

    def __init__(self, uri: str) -> None:
        self._uri = uri
        self._sub = subprocess.Popen(["hdfs", "dfs", "-cat", self._uri], stdout=subprocess.PIPE)  # noqa: S603, S607  # invokes local hdfs CLI

    #
    # Override some methods from io.IOBase.
    #
    def close(self) -> None:
        """Flush and close this stream."""
        logger.debug("close: called")
        sub = self._sub
        if sub is not None:
            sub.terminate()
            self._sub = None

    @property
    def closed(self) -> bool:
        """Return True if the stream is closed."""
        return self._sub is None

    def readable(self) -> bool:
        """Return True if the stream can be read from."""
        return self._sub is not None

    def seekable(self) -> bool:
        """Return False; HDFS streams do not support seeking."""
        return False

    #
    # io.RawIOBase methods.
    #
    def detach(self) -> io.RawIOBase:
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size: int | None = -1) -> bytes:
        """Read up to size bytes from the object and return them."""
        sub = self._sub
        assert sub is not None  # noqa: S101  # subprocess started in __init__
        assert sub.stdout is not None  # noqa: S101  # stdout=PIPE set in __init__
        if size is None:
            size = -1
        return sub.stdout.read(size)

    def read1(self, size: int | None = -1) -> bytes:
        """This is the same as read()."""
        return self.read(size=size)

    def readinto(self, b: WriteableBuffer) -> int:
        """Read up to ``len(b)`` bytes into `b` and return the number of bytes read."""
        mv = memoryview(b).cast("B")
        data = self.read(len(mv))
        if not data:
            return 0
        mv[: len(data)] = data
        return len(data)


class CliRawOutputBase(io.RawIOBase):
    """Writes bytes to HDFS via the "hdfs dfs" command-line interface.

    Implements the io.RawIOBase interface of the standard library.
    """

    name: str
    _sub: subprocess.Popen[bytes] | None = None  # so `closed` works if __init__ fails and __del__ runs

    def __init__(self, uri: str) -> None:
        self._uri = uri
        self._sub = subprocess.Popen(["hdfs", "dfs", "-put", "-f", "-", self._uri], stdin=subprocess.PIPE)  # noqa: S603, S607  # invokes local hdfs CLI

    def close(self) -> None:
        """Flush and close this stream."""
        logger.debug("close: called")
        sub = self._sub
        if sub is not None:
            assert sub.stdin is not None  # noqa: S101  # stdin=PIPE set in __init__
            self.flush()
            sub.stdin.close()
            sub.wait()
            self._sub = None

    @property
    def closed(self) -> bool:
        """Return True if the stream is closed."""
        return self._sub is None

    def flush(self) -> None:
        """Flush the underlying ``hdfs dfs -put`` subprocess stdin."""
        sub = self._sub
        assert sub is not None  # noqa: S101  # subprocess started in __init__
        assert sub.stdin is not None  # noqa: S101  # stdin=PIPE set in __init__
        sub.stdin.flush()

    def writeable(self) -> bool:
        """Return True if this object is writeable."""
        return self._sub is not None

    def seekable(self) -> bool:
        """Return False; HDFS streams do not support seeking."""
        return False

    def write(self, b: ReadableBuffer) -> int:
        """Write the given buffer to the underlying raw stream.

        Returns the number of bytes written, as required by
        :class:`io.RawIOBase`. Without this return value, callers that wrap
        this stream and rely on the documented ``write`` contract (for
        example, ``ray._private.external_storage._write_multiple_objects``,
        which asserts ``written_bytes == payload_len``) fail with an
        ``AssertionError`` because ``write`` would otherwise implicitly
        return ``None``.
        """
        sub = self._sub
        assert sub is not None  # noqa: S101  # subprocess started in __init__
        assert sub.stdin is not None  # noqa: S101  # stdin=PIPE set in __init__
        return sub.stdin.write(b)

    #
    # io.IOBase methods.
    #
    def detach(self) -> io.RawIOBase:
        """Unsupported."""
        msg = "detach() not supported"
        raise io.UnsupportedOperation(msg)
