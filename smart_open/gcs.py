# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements file-like objects for reading and writing to/from GCS."""

import io
import logging

try:
    import google.cloud.exceptions
    import google.cloud.storage
    import google.auth.transport.requests
except ImportError:
    MISSING_DEPS = True

import smart_open.bytebuffer
import smart_open.utils

from smart_open import constants

logger = logging.getLogger(__name__)

_BINARY_TYPES = (bytes, bytearray, memoryview)
"""Allowed binary buffer types for writing to the underlying GCS stream"""

SCHEME = "gs"
"""Supported scheme for GCS"""

_MIN_MIN_PART_SIZE = _REQUIRED_CHUNK_MULTIPLE = 256 * 1024
"""Google requires you to upload in multiples of 256 KB, except for the last part."""

_DEFAULT_MIN_PART_SIZE = 50 * 1024**2
"""Default minimum part size for GCS multipart uploads"""

DEFAULT_BUFFER_SIZE = 256 * 1024
"""Default buffer size for working with GCS"""


def parse_uri(uri_as_string):
    sr = smart_open.utils.safe_urlsplit(uri_as_string)
    assert sr.scheme == SCHEME
    bucket_id = sr.netloc
    blob_id = sr.path.lstrip('/')
    return dict(scheme=SCHEME, bucket_id=bucket_id, blob_id=blob_id)


def open_uri(uri, mode, transport_params):
    parsed_uri = parse_uri(uri)
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    return open(parsed_uri['bucket_id'], parsed_uri['blob_id'], mode, **kwargs)


def open(
        bucket_id,
        blob_id,
        mode,
        buffer_size=DEFAULT_BUFFER_SIZE,
        min_part_size=_MIN_MIN_PART_SIZE,
        client=None,  # type: google.cloud.storage.Client
        blob_properties=None
        ):
    """Open an GCS blob for reading or writing.

    Parameters
    ----------
    bucket_id: str
        The name of the bucket this object resides in.
    blob_id: str
        The name of the blob within the bucket.
    mode: str
        The mode for opening the object.  Must be either "rb" or "wb".
    buffer_size: int, optional
        The buffer size to use when performing I/O. For reading only.
    min_part_size: int, optional
        The minimum part size for multipart uploads.  For writing only.
    client: google.cloud.storage.Client, optional
        The GCS client to use when working with google-cloud-storage.
    blob_properties: dict, optional
        Set properties on blob before writing.  For writing only.

    """
    if mode == constants.READ_BINARY:
        fileobj = Reader(
            bucket_id,
            blob_id,
            buffer_size=buffer_size,
            line_terminator=constants.BINARY_NEWLINE,
            client=client,
        )
    elif mode == constants.WRITE_BINARY:
        fileobj = Writer(
            bucket_id,
            blob_id,
            min_part_size=min_part_size,
            client=client,
            blob_properties=blob_properties,
        )
    else:
        raise NotImplementedError('GCS support for mode %r not implemented' % mode)

    fileobj.name = blob_id
    return fileobj

class Reader(io.BufferedIOBase):
    """Reads bytes from GCS.

    Implements the io.BufferedIOBase interface of the standard library.

    :raises google.cloud.exceptions.NotFound: Raised when the blob to read from does not exist.

    """
    def __init__(
            self,
            bucket,
            key,
            buffer_size=DEFAULT_BUFFER_SIZE,
            line_terminator=constants.BINARY_NEWLINE,
            client=None,  # type: google.cloud.storage.Client
    ):
        if client is None:
            client = google.cloud.storage.Client()

        self._blob = client.bucket(bucket).get_blob(key)  # type: google.cloud.storage.Blob

        if self._blob is None:
            raise google.cloud.exceptions.NotFound('blob %s not found in %s' % (key, bucket))

        self._size = self._blob.size if self._blob.size is not None else 0

        self._buffer_size = buffer_size

        self.raw = self._blob.open(
            mode=constants.READ_BINARY,
            chunk_size=self._buffer_size,
        )

    #
    # Override some methods from io.IOBase.
    #

    def readable(self):
        """Return True if the stream can be read from."""
        return True

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError.

        We offer only seek support, and no truncate support."""
        return True

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def seek(self, offset, whence=constants.WHENCE_START):
        """Seek to the specified position.

        :param int offset: The offset in bytes.
        :param int whence: Where the offset is from.

        Returns the position after seeking."""
        return self.raw.seek(offset, whence)

    def tell(self):
        """Return the current position within the file."""
        return self.raw.tell()

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size=-1):
        """Read up to size bytes from the object and return them."""
        return self.raw.read(size)

    def read1(self, size=-1):
        return self.raw.read1(size)

    def readinto(self, b):
        """Read up to len(b) bytes into b, and return the number of bytes
        read."""
        return self.raw.readinto(b)

    def readline(self, limit=-1) -> bytes:
        """Read up to and including the next newline.  Returns the bytes read."""
        return super().readline(limit)

    def __str__(self):
        return "(%s, %r, %r)" % (self.__class__.__name__, self._blob.bucket.name, self._blob.name)

    def __repr__(self):
        return "%s(bucket=%r, blob=%r, buffer_size=%r)" % (
            self.__class__.__name__, self._blob.bucket.name, self._blob.name, self._buffer_size,
        )


class Writer(io.BufferedIOBase):
    """Writes bytes to GCS.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(
            self,
            bucket,
            blob,
            min_part_size=_DEFAULT_MIN_PART_SIZE,
            client=None,  # type: google.cloud.storage.Client
            blob_properties=None,
    ):
        if client is None:
            client = google.cloud.storage.Client()
        self._client = client
        self._blob = self._client.bucket(bucket).blob(blob)  # type: google.cloud.storage.Blob
        assert min_part_size % _REQUIRED_CHUNK_MULTIPLE == 0, 'min part size must be a multiple of 256KB'
        assert min_part_size >= _MIN_MIN_PART_SIZE, 'min part size must be greater than 256KB'
        self._min_part_size = min_part_size

        if blob_properties:
            for k, v in blob_properties.items():
                setattr(self._blob, k, v)

        self.raw = self._blob.open(
            mode=constants.WRITE_BINARY,
            chunk_size=min_part_size,
            ignore_flush=True,
        )

    def flush(self):
        pass

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        logger.debug("closing")
        if not self.closed:
            self.raw.close()
        logger.debug("successfully closed")

    @property
    def closed(self):
        return self.raw.closed

    def writable(self):
        """Return True if the stream supports writing."""
        return True

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError.

        We offer only tell support, and no seek or truncate support."""
        return True

    def seek(self, offset, whence=constants.WHENCE_START):
        """Unsupported."""
        raise io.UnsupportedOperation

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation

    def tell(self):
        """Return the current stream position."""
        return self.raw.tell()

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")

    def write(self, b):
        """Write the given bytes (binary string) to the GCS file.

        There's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away."""

        if not isinstance(b, _BINARY_TYPES):
            raise TypeError("input must be one of %r, got: %r" % (_BINARY_TYPES, type(b)))

        return self.raw.write(b)

    #TODO: Maintaining for api compatibility
    def terminate(self):
        pass

    def __str__(self):
        return "(%s, %r, %r)" % (self.__class__.__name__, self._blob.bucket.name, self._blob.name)

    def __repr__(self):
        return "%s(bucket=%r, blob=%r, min_part_size=%r)" % (
            self.__class__.__name__, self._blob.bucket.name, self._blob.name, self._min_part_size,
        )
