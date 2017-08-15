# -*- coding: utf-8 -*-
"""Implements file-like objects for reading and writing from/to S3."""
import boto3

import io
import logging

import six

_LOGGER = logging.getLogger(__name__)
_LOGGER.addHandler(logging.NullHandler())

START = 0
CURRENT = 1
END = 2
WHENCE_CHOICES = (START, CURRENT, END)

DEFAULT_MIN_PART_SIZE = 50 * 1024**2  # minimum part size for S3 multipart uploads
MIN_MIN_PART_SIZE = 5 * 1024 ** 2
MODES = ("r", "rb", "w", "wb")
"""Allowed I/O modes for working with S3."""


def _range_string(start, stop=None):
    #
    # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    #
    if stop is None:
        return 'bytes=%d-' % start
    return 'bytes=%d-%d' % (start, stop)


def _clamp(value, minval, maxval):
    return max(min(value, maxval), minval)


class RawReader(object):
    """Read an S3 object."""
    def __init__(self, s3_object):
        self.position = 0
        self._object = s3_object
        self._content_length = self._object.content_length

    def read(self, size=-1):
        if self.position == self._content_length:
            return b''
        if size <= 0:
            end = None
        else:
            end = min(self._content_length, self.position + size)
        range_string = _range_string(self.position, stop=end)
        _LOGGER.debug('range_string: %r', range_string)
        body = self._object.get(Range=range_string)['Body'].read()
        self.position += len(body)
        return body


class BufferedInputBase(io.BufferedIOBase):
    """Reads bytes from S3.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(self, bucket, key):
        s3 = boto3.resource('s3')
        self._object = s3.Object(bucket, key)
        self._raw_reader = RawReader(self._object)
        self._content_length = self._object.content_length
        self._current_pos = 0
        self._buffer = b''
        self._eof = False

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        """Flush and close this stream."""
        _LOGGER.debug("close: called")
        self._object = None

    def readable(self):
        """Return True if the stream can be read from."""
        return True

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError.

        We offer only seek support, and no truncate support."""
        return True

    def seek(self, offset, whence=START):
        """Seek to the specified position.

        :param int offset: The offset in bytes.
        :param int whence: Where the offset is from.

        Returns the position after seeking."""
        _LOGGER.debug('seeking to offset: %r whence: %r', offset, whence)
        if whence not in WHENCE_CHOICES:
            raise ValueError('invalid whence, expected one of %r' % WHENCE_CHOICES)

        if whence == START:
            new_position = offset
        elif whence == CURRENT:
            new_position = self._current_pos + offset
        else:
            new_position = self._content_length + offset
        new_position = _clamp(new_position, 0, self._content_length)

        _LOGGER.debug('new_position: %r', new_position)
        self._current_pos = self._raw_reader.position = new_position
        self._buffer = b""
        self._eof = self._current_pos == self._content_length
        return self._current_pos

    def tell(self):
        """Return the current position within the file."""
        return self._current_pos

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size=-1):
        """Read up to size bytes from the object and return them."""
        if size <= 0:
            if len(self._buffer):
                from_buf = self._read_from_buffer(len(self._buffer))
            else:
                from_buf = b''
            self._current_pos = self._content_length
            return from_buf + self._raw_reader.read()

        #
        # Return unused data first
        #
        if len(self._buffer) >= size:
            return self._read_from_buffer(size)

        #
        # If the stream is finished, return what we have.
        #
        if self._eof:
            return self._read_from_buffer(len(self._buffer))

        #
        # Fill our buffer to the required size.
        #
        _LOGGER.debug('filling %r byte-long buffer up to %r bytes', len(self._buffer), size)
        while len(self._buffer) < size and not self._eof:
            raw = self._raw_reader.read(size=io.DEFAULT_BUFFER_SIZE)
            if len(raw):
                self._buffer += raw
            else:
                _LOGGER.debug('reached EOF while filling buffer')
                self._eof = True

        return self._read_from_buffer(size)

    def read1(self, n=-1):
        """Unsupported."""
        raise io.UnsupportedOperation

    def readinto(self, b):
        """Read up to len(b) bytes into b, and return the number of bytes
        read."""
        data = self.read(len(b))
        if not data:
            return 0
        b[:len(data)] = data
        return len(data)

    def terminate(self):
        """Do nothing."""

    #
    # Internal methods.
    #
    def _read_from_buffer(self, size):
        """Remove at most size bytes from our buffer and return them."""
        _LOGGER.debug('reading %r bytes from %r byte-long buffer', size, len(self._buffer))
        assert size >= 0
        part = self._buffer[:size]
        self._buffer = self._buffer[size:]
        self._current_pos += len(part)
        _LOGGER.debug('part: %r', part)
        return part


class BufferedOutputBase(io.BufferedIOBase):
    """Writes bytes to S3.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(self, bucket, key, min_part_size=DEFAULT_MIN_PART_SIZE):
        if min_part_size < MIN_MIN_PART_SIZE:
            _LOGGER.warning("S3 requires minimum part size >= 5MB; \
multipart upload may fail")

        s3 = boto3.resource('s3')
        self._object = s3.Object(bucket, key)
        self._min_part_size = min_part_size
        self._mp = self._object.initiate_multipart_upload()

        self._buf = io.BytesIO()
        self._total_bytes = 0
        self._total_parts = 0
        self._parts = []

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        _LOGGER.debug("closing")
        if self._buf.tell():
            self._upload_next_part()

        if self._total_bytes:
            self._mp.complete(MultipartUpload={'Parts': self._parts})
            _LOGGER.debug("completed multipart upload")
        elif self._mp:
            #
            # AWS complains with "The XML you provided was not well-formed or
            # did not validate against our published schema" when the input is
            # completely empty => abort the upload, no file created.
            #
            # We work around this by creating an empty file explicitly.
            #
            _LOGGER.info("empty input, ignoring multipart upload")
            assert self._mp, "no multipart upload in progress"
            self._mp.abort()

            self._object.put(Body=b'')

        _LOGGER.debug("successfully closed")

    def writable(self):
        """Return True if the stream supports writing."""
        return True

    def tell(self):
        """Return the current stream position."""
        return self._total_bytes

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")

    def write(self, b):
        """Write the given bytes (binary string) to the S3 file.

        There's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away."""
        if not isinstance(b, six.binary_type):
            raise TypeError("input must be a binary string, got: %r", b)

        _LOGGER.debug("writing %r bytes to %r", len(b), self._buf)

        self._buf.write(b)
        self._total_bytes += len(b)

        if self._buf.tell() >= self._min_part_size:
            self._upload_next_part()

        return len(b)

    def terminate(self):
        """Cancel the underlying multipart upload."""
        assert self._mp, "no multipart upload in progress"
        self._mp.abort()
        self._mp = None

    #
    # Internal methods.
    #
    def _upload_next_part(self):
        part_num = self._total_parts + 1
        _LOGGER.info("uploading part #%i, %i bytes (total %.3fGB)",
                     part_num, self._buf.tell(), self._total_bytes / 1024.0 ** 3)
        self._buf.seek(0)
        part = self._mp.Part(part_num)
        upload = part.upload(Body=self._buf)
        self._parts.append({'ETag': upload['ETag'], 'PartNumber': part_num})
        _LOGGER.debug("upload of part #%i finished" % part_num)

        self._total_parts += 1
        self._buf = io.BytesIO()

    def __exit__(self, exc_type, value, traceback):
        if exc_type is not None:
            self.terminate()
        else:
            self.close()
