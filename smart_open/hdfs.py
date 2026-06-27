#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements reading and writing to/from HDFS."""

import io
import logging
import subprocess
import urllib.parse

from smart_open import utils

logger = logging.getLogger(__name__)

SCHEMES = ("hdfs", "viewfs")

URI_EXAMPLES = (
    "hdfs:///path/file",
    "hdfs://host/path/file",
    "hdfs://host:port/path/file",
    "viewfs:///path/file",
    "viewfs://host/path/file",
)


def parse_uri(uri_as_string):
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


def open_uri(uri, mode, transport_params):
    """Open an HDFS URI using the given mode and transport params."""
    utils.check_kwargs(open, transport_params)

    parsed_uri = parse_uri(uri)
    fobj = open(parsed_uri["uri_path"], mode)
    fobj.name = parsed_uri["uri_path"].split("/")[-1]
    return fobj


def open(uri, mode):
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

    _sub = None  # so `closed` property works in case __init__ fails and __del__ is called

    def __init__(self, uri):
        self._uri = uri
        self._sub = subprocess.Popen(["hdfs", "dfs", "-cat", self._uri], stdout=subprocess.PIPE)  # noqa: S603, S607  # invokes local hdfs CLI

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        """Flush and close this stream."""
        logger.debug("close: called")
        if not self.closed:
            self._sub.terminate()
            self._sub = None

    @property
    def closed(self):
        """Return True if the stream is closed."""
        return self._sub is None

    def readable(self):
        """Return True if the stream can be read from."""
        return self._sub is not None

    def seekable(self):
        """Return False; HDFS streams do not support seeking."""
        return False

    #
    # io.RawIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size=-1):
        """Read up to size bytes from the object and return them."""
        return self._sub.stdout.read(size)

    def read1(self, size=-1):
        """This is the same as read()."""
        return self.read(size=size)

    def readinto(self, b):
        """Read up to ``len(b)`` bytes into `b` and return the number of bytes read."""
        data = self.read(len(b))
        if not data:
            return 0
        b[: len(data)] = data
        return len(data)


class CliRawOutputBase(io.RawIOBase):
    """Writes bytes to HDFS via the "hdfs dfs" command-line interface.

    Implements the io.RawIOBase interface of the standard library.
    """

    _sub = None  # so `closed` property works in case __init__ fails and __del__ is called

    def __init__(self, uri):
        self._uri = uri
        self._sub = subprocess.Popen(["hdfs", "dfs", "-put", "-f", "-", self._uri], stdin=subprocess.PIPE)  # noqa: S603, S607  # invokes local hdfs CLI

    def close(self):
        """Flush and close this stream."""
        logger.debug("close: called")
        if not self.closed:
            self.flush()
            self._sub.stdin.close()
            self._sub.wait()
            self._sub = None

    @property
    def closed(self):
        """Return True if the stream is closed."""
        return self._sub is None

    def flush(self):
        """Flush the underlying ``hdfs dfs -put`` subprocess stdin."""
        self._sub.stdin.flush()

    def writeable(self):
        """Return True if this object is writeable."""
        return self._sub is not None

    def seekable(self):
        """Return False; HDFS streams do not support seeking."""
        return False

    def write(self, b):
        """Write the given buffer to the underlying raw stream.

        Returns the number of bytes written, as required by
        :class:`io.RawIOBase`. Without this return value, callers that wrap
        this stream and rely on the documented ``write`` contract (for
        example, ``ray._private.external_storage._write_multiple_objects``,
        which asserts ``written_bytes == payload_len``) fail with an
        ``AssertionError`` because ``write`` would otherwise implicitly
        return ``None``.
        """
        return self._sub.stdin.write(b)

    #
    # io.IOBase methods.
    #
    def detach(self):
        """Unsupported."""
        msg = "detach() not supported"
        raise io.UnsupportedOperation(msg)
