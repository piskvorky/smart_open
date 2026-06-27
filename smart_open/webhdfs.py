#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements reading and writing to/from WebHDFS.

The main entry point is the :func:`~smart_open.webhdfs.open` function.

"""

import io
import logging
import urllib.parse

try:
    import requests
except ImportError:
    MISSING_DEPS = True

import http.client as httplib

from smart_open import constants, utils

logger = logging.getLogger(__name__)

SCHEME = "webhdfs"

URI_EXAMPLES = ("webhdfs://host:port/path/file",)

MIN_PART_SIZE = 50 * 1024**2  # minimum part size for HDFS multipart uploads


def parse_uri(uri_as_str):
    """Return the WebHDFS URI as a dict with `scheme` and `uri` keys."""
    return {"scheme": SCHEME, "uri": uri_as_str}


def open_uri(uri, mode, transport_params):
    """Open a WebHDFS URI using the given mode and transport params."""
    kwargs = utils.check_kwargs(open, transport_params)
    return open(uri, mode, **kwargs)


def open(http_uri, mode, min_part_size=MIN_PART_SIZE):
    """Open a WebHDFS URI for reading or writing.

    Args:
        http_uri: webhdfs url converted to http REST url.
        mode: The mode for opening the object. Must be either "rb" or "wb".
        min_part_size: For writing only.

    Returns:
        A file-like object for reading from or writing to the WebHDFS file.

    Raises:
        NotImplementedError: If the requested mode is not supported.
    """
    if http_uri.startswith(SCHEME):
        http_uri = _convert_to_http_uri(http_uri)

    if mode == constants.READ_BINARY:
        fobj = BufferedInputBase(http_uri)
    elif mode == constants.WRITE_BINARY:
        fobj = BufferedOutputBase(http_uri, min_part_size=min_part_size)
    else:
        msg = f"webhdfs support for mode {mode!r} not implemented"
        raise NotImplementedError(msg)

    fobj.name = http_uri.split("/")[-1]
    return fobj


def _convert_to_http_uri(webhdfs_url):
    """Convert webhdfs uri to http url and return it as text.

    Args:
        webhdfs_url: A URL starting with webhdfs://.

    Returns:
        The converted HTTP URL as a string.
    """
    split_uri = urllib.parse.urlsplit(webhdfs_url)
    netloc = split_uri.hostname
    if split_uri.port:
        netloc += f":{split_uri.port}"
    query = split_uri.query
    if split_uri.username:
        query += ("&" if query else "") + "user.name=" + urllib.parse.quote(split_uri.username)

    return urllib.parse.urlunsplit(("http", netloc, "/webhdfs/v1" + split_uri.path, query, ""))


#
# For old unit tests.
#
def convert_to_http_uri(parsed_uri):
    """Convert a parsed webhdfs URI to its HTTP REST URL (compat wrapper)."""
    return _convert_to_http_uri(parsed_uri.uri)


class BufferedInputBase(io.BufferedIOBase):
    """Buffered WebHDFS reader implementing the `io.BufferedIOBase` interface."""

    _buf = None  # so `closed` property works in case __init__ fails and __del__ is called

    def __init__(self, uri):
        self._uri = uri

        payload = {"op": "OPEN", "offset": 0}
        self._response = requests.get(self._uri, params=payload, stream=True)  # noqa: S113  # WebHDFS server-side timeouts apply
        if self._response.status_code != httplib.OK:
            raise WebHdfsException.from_response(self._response)
        self._buf = b""

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        """Flush and close this stream."""
        logger.debug("close: called")
        if not self.closed:
            self._buf = None

    @property
    def closed(self):
        """Return True if the stream is closed."""
        return self._buf is None

    def readable(self):
        """Return True if the stream can be read from."""
        return True

    def seekable(self):
        """Return False; the WebHDFS reader does not support seeking."""
        return False

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size=None):
        """Read up to `size` bytes (or all remaining bytes if `size` is None)."""
        if size is None:
            self._buf, retval = b"", self._buf + self._response.raw.read()
            return retval
        if size < len(self._buf):
            self._buf, retval = self._buf[size:], self._buf[:size]
            return retval

        try:
            buffers = [self._buf]
            total_read = 0
            while total_read < size:
                raw_data = self._response.raw.read(io.DEFAULT_BUFFER_SIZE)
                # some times read returns 0 length data without throwing a
                # StopIteration exception. We break here if this happens.
                if len(raw_data) == 0:
                    break

                total_read += len(raw_data)
                buffers.append(raw_data)
        except StopIteration:
            pass

        self._buf = b"".join(buffers)
        self._buf, retval = self._buf[size:], self._buf[:size]
        return retval

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

    def readline(self):
        """Read and return one line from the WebHDFS stream."""
        self._buf, retval = b"", self._buf + self._response.raw.readline()
        return retval


class BufferedOutputBase(io.BufferedIOBase):
    """Writes bytes to a WebHDFS file in multipart chunks.

    Args:
        uri: The HTTP WebHDFS REST URL to write to.
        min_part_size: The minimum part size for multipart uploads.
            For writing only.

    Raises:
        WebHdfsException: If the WebHDFS server returns an unexpected status
            code when creating the file.
    """

    def __init__(self, uri, min_part_size=MIN_PART_SIZE):
        self._uri = uri
        self._closed = False
        self.min_part_size = min_part_size
        # creating empty file first
        payload = {"op": "CREATE", "overwrite": True}
        init_response = requests.put(self._uri, params=payload, allow_redirects=False)  # noqa: S113  # WebHDFS server-side timeouts apply
        if not init_response.status_code == httplib.TEMPORARY_REDIRECT:
            raise WebHdfsException.from_response(init_response)
        uri = init_response.headers["location"]
        response = requests.put(uri, data="", headers={"content-type": "application/octet-stream"})  # noqa: S113  # WebHDFS server-side timeouts apply
        if not response.status_code == httplib.CREATED:
            raise WebHdfsException.from_response(response)
        self.lines = []
        self.parts = 0
        self.chunk_bytes = 0
        self.total_size = 0

    #
    # Override some methods from io.IOBase.
    #
    def writable(self):
        """Return True if the stream supports writing."""
        return True

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        msg = "detach() not supported"
        raise io.UnsupportedOperation(msg)

    def _upload(self, data):
        payload = {"op": "APPEND"}
        init_response = requests.post(self._uri, params=payload, allow_redirects=False)  # noqa: S113  # WebHDFS server-side timeouts apply
        if not init_response.status_code == httplib.TEMPORARY_REDIRECT:
            raise WebHdfsException.from_response(init_response)
        uri = init_response.headers["location"]
        response = requests.post(uri, data=data, headers={"content-type": "application/octet-stream"})  # noqa: S113  # WebHDFS server-side timeouts apply
        if not response.status_code == httplib.OK:
            raise WebHdfsException.from_response(response)

    def write(self, b):
        """Write the given bytes (binary string) into the WebHDFS file from constructor."""
        if self._closed:
            msg = "I/O operation on closed file"
            raise ValueError(msg)

        if not isinstance(b, bytes):
            msg = "input must be a binary string"
            raise TypeError(msg)

        self.lines.append(b)
        self.chunk_bytes += len(b)
        self.total_size += len(b)

        if self.chunk_bytes >= self.min_part_size:
            buff = b"".join(self.lines)
            logger.info(
                "uploading part #%i, %i bytes (total %.3fGB)",
                self.parts,
                len(buff),
                self.total_size / 1024.0**3,
            )
            self._upload(buff)
            logger.debug("upload of part #%i finished", self.parts)
            self.parts += 1
            self.lines, self.chunk_bytes = [], 0

    def close(self):
        """Flush any remaining buffered bytes to WebHDFS and close the stream."""
        buff = b"".join(self.lines)
        if buff:
            logger.info(
                "uploading last part #%i, %i bytes (total %.3fGB)",
                self.parts,
                len(buff),
                self.total_size / 1024.0**3,
            )
            self._upload(buff)
            logger.debug("upload of last part #%i finished", self.parts)
        self._closed = True

    @property
    def closed(self):
        """Return True if the stream is closed."""
        return self._closed


class WebHdfsException(Exception):  # noqa: N818  # public name
    """Exception raised when WebHDFS returns an unexpected HTTP status code."""

    def __init__(self, msg="", status_code=None):
        self.msg = msg
        self.status_code = status_code
        super().__init__(repr(self))

    def __repr__(self):
        """Return an unambiguous representation of the exception."""
        return f"{self.__class__.__name__}(status_code={self.status_code}, msg={self.msg!r})"

    @classmethod
    def from_response(cls, response):
        """Build a `WebHdfsException` from a failed `requests.Response`."""
        return cls(msg=response.text, status_code=response.status_code)
