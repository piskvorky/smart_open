# -*- coding: utf-8 -*-
"""Implements file-like objects for reading and writing to/from GCS."""

import io
import logging
import sys

from google.cloud import storage, exceptions
import google.auth.transport.requests as google_requests
import six

import smart_open.bytebuffer

logger = logging.getLogger(__name__)

READ_BINARY = 'rb'
WRITE_BINARY = 'wb'
MODES = (READ_BINARY, WRITE_BINARY)
"""Allowed I/O modes for working with GCS."""

_BINARY_TYPES = (six.binary_type, bytearray)
"""Allowed binary buffer types for writing to the underlying GCS stream"""
if sys.version_info >= (2, 7):
    _BINARY_TYPES = (six.binary_type, bytearray, memoryview)

_UNKNOWN_FILE_SIZE = '*'

BINARY_NEWLINE = b'\n'

SUPPORTED_SCHEMES = ("gcs", "gs")

DEFAULT_MIN_PART_SIZE = MIN_MIN_PART_SIZE = 256 * 1024
"""The absolute minimum permitted by Google."""
DEFAULT_BUFFER_SIZE = 128 * 1024

START = 0
CURRENT = 1
END = 2
WHENCE_CHOICES = [START, CURRENT, END]


def clamp(value, minval, maxval):
    return max(min(value, maxval), minval)


def make_range_string(start, stop=None, end=_UNKNOWN_FILE_SIZE):
    #
    # https://cloud.google.com/storage/docs/xml-api/resumable-upload#step_3upload_the_file_blocks
    #
    if stop is None:
        return 'bytes %d-/%s' % (start, end)
    return 'bytes %d-%d/%s' % (start, stop, end)


def open(
        bucket_id,
        blob_id,
        mode,
        buffer_size=DEFAULT_BUFFER_SIZE,
        min_part_size=DEFAULT_MIN_PART_SIZE,
        client=None,  # type: storage.Client
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
        The buffer size to use when performing I/O.
    min_part_size: int, optional
        The minimum part size for multipart uploads.  For writing only.
    client: object, optional
        The GCS client to use when working with google-cloud-storage.

    """
    if mode == READ_BINARY:
        return SeekableBufferedInputBase(
            bucket_id,
            blob_id,
            buffer_size=buffer_size,
            line_terminator=BINARY_NEWLINE,
            client=client,
        )
    elif mode == WRITE_BINARY:
        return BufferedOutputBase(
            bucket_id,
            blob_id,
            min_part_size=min_part_size,
            client=client,
        )
    else:
        raise NotImplementedError('GCS support for mode %r not implemented' % mode)


class RawReader(object):
    """Read an GCS blob."""
    def __init__(self,
                 gcs_blob,  # type: storage.Blob
                 ):
        self.position = 0
        self._blob = gcs_blob
        self._body = gcs_blob.download_as_string()

    def read(self, size=-1):
        if size == -1:
            return self._body
        start, end = self.position, self.position + size
        self.position = end
        return self._body[start:end]


class SeekableRawReader(object):
    """Read an GCS object."""

    def __init__(
            self,
            gcs_blob,  # type: storage.Blob
            size,
     ):
        self._blob = gcs_blob
        self._size = size
        self.seek(0)

    def seek(self, position):
        """Seek to the specified position (byte offset) in the GCS key.

        :param int position: The byte offset from the beginning of the key.
        """
        self._position = position

        if position == self._size == 0 or position == self._size:
            #
            # When reading, we can't seek to the first byte of an empty file.
            # Similarly, we can't seek past the last byte.  Do nothing here.
            #
            self._body = io.BytesIO()
        else:
            start, end = position, position + self._size
            self._body = self._blob.download_as_string(start=start, end=end)

    def read(self, size=-1):
        if self._position >= self._size:
            return b''
        if size == -1:
            binary = self._body
        else:
            binary = self._body[:size]
        self._position += len(binary)
        return binary


class BufferedInputBase(io.BufferedIOBase):
    def __init__(
            self,
            bucket,
            key,
            buffer_size=DEFAULT_BUFFER_SIZE,
            line_terminator=BINARY_NEWLINE,
            client=None,  # type: storage.Client
    ):
        if not client:
            client = storage.Client()

        bucket = client.get_bucket(bucket)  # type: storage.Bucket

        self._blob = bucket.get_blob(key)   # type: storage.Blob
        self._size = self._blob.size if self._blob.size else 0

        self._raw_reader = RawReader(self._blob)
        self._current_pos = 0
        self._buffer_size = buffer_size
        self._buffer = smart_open.bytebuffer.ByteBuffer(buffer_size)
        self._eof = False
        self._line_terminator = line_terminator

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        """Flush and close this stream."""
        logger.debug("close: called")
        self._blob = None

    def readable(self):
        """Return True if the stream can be read from."""
        return True

    def seekable(self):
        return False

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size=-1):
        """Read up to size bytes from the object and return them."""
        if size == 0:
            return b''
        elif size < 0:
            from_buf = self._read_from_buffer()
            self._current_pos = self._size
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
            return self._read_from_buffer()

        #
        # Fill our buffer to the required size.
        #
        # logger.debug('filling %r byte-long buffer up to %r bytes', len(self._buffer), size)
        self._fill_buffer(size)
        return self._read_from_buffer(size)

    def read1(self, size=-1):
        """This is the same as read()."""
        return self.read(size=size)

    def readinto(self, b):
        """Read up to len(b) bytes into b, and return the number of bytes
        read."""
        data = self.read(len(b))
        if not data:
            return 0
        b[:len(data)] = data
        return len(data)

    def readline(self, limit=-1):
        """Read up to and including the next newline.  Returns the bytes read."""
        if limit != -1:
            raise NotImplementedError('limits other than -1 not implemented yet')
        the_line = io.BytesIO()
        while not (self._eof and len(self._buffer) == 0):
            #
            # In the worst case, we're reading the unread part of self._buffer
            # twice here, once in the if condition and once when calling index.
            #
            # This is sub-optimal, but better than the alternative: wrapping
            # .index in a try..except, because that is slower.
            #
            remaining_buffer = self._buffer.peek()
            if self._line_terminator in remaining_buffer:
                next_newline = remaining_buffer.index(self._line_terminator)
                the_line.write(self._read_from_buffer(next_newline + 1))
                break
            else:
                the_line.write(self._read_from_buffer())
                self._fill_buffer()
        return the_line.getvalue()

    def terminate(self):
        """Do nothing."""
        pass

    #
    # Internal methods.
    #
    def _read_from_buffer(self, size=-1):
        """Remove at most size bytes from our buffer and return them."""
        # logger.debug('reading %r bytes from %r byte-long buffer', size, len(self._buffer))
        size = size if size >= 0 else len(self._buffer)
        part = self._buffer.read(size)
        self._current_pos += len(part)
        # logger.debug('part: %r', part)
        return part

    def _fill_buffer(self, size=-1):
        size = size if size >= 0 else self._buffer._chunk_size
        while len(self._buffer) < size and not self._eof:
            bytes_read = self._buffer.fill(self._raw_reader)
            if bytes_read == 0:
                logger.debug('reached EOF while filling buffer')
                self._eof = True


class SeekableBufferedInputBase(BufferedInputBase):
    """Reads bytes from GCS.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(
            self,
            bucket,
            key,
            buffer_size=DEFAULT_BUFFER_SIZE,
            line_terminator=BINARY_NEWLINE,
            client=None,  # type: storage.Client
    ):
        if not client:
            client = storage.Client()
        bucket = client.get_bucket(bucket)

        self._blob = bucket.get_blob(key)
        if self._blob is None:
            raise exceptions.NotFound('blob {} not found in {}'.format(key, bucket))
        self._size = self._blob.size if self._blob.size is not None else 0

        self._raw_reader = SeekableRawReader(self._blob, self._size)
        self._current_pos = 0
        self._buffer_size = buffer_size
        self._buffer = smart_open.bytebuffer.ByteBuffer(buffer_size)
        self._eof = False
        self._line_terminator = line_terminator

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError.

        We offer only seek support, and no truncate support."""
        return True

    def seek(self, offset, whence=START):
        """Seek to the specified position.

        :param int offset: The offset in bytes.
        :param int whence: Where the offset is from.

        Returns the position after seeking."""
        logger.debug('seeking to offset: %r whence: %r', offset, whence)
        if whence not in WHENCE_CHOICES:
            raise ValueError('invalid whence, expected one of %r' % WHENCE_CHOICES)

        if whence == START:
            new_position = offset
        elif whence == CURRENT:
            new_position = self._current_pos + offset
        else:
            new_position = self._size + offset
        new_position = clamp(new_position, 0, self._size)
        self._current_pos = new_position
        self._raw_reader.seek(new_position)
        logger.debug('new_position: %r', self._current_pos)

        self._buffer.empty()
        self._eof = self._current_pos == self._size
        return self._current_pos

    def tell(self):
        """Return the current position within the file."""
        return self._current_pos

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation


class BufferedOutputBase(io.BufferedIOBase):
    """Writes bytes to GCS.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(
            self,
            bucket,
            blob,
            min_part_size=DEFAULT_MIN_PART_SIZE,
            client=None,  # type: storage.Client
    ):
        if client is None:
            client = storage.Client()
        self._client = client
        self._credentials = self._client._credentials
        self._bucket = self._client.bucket(bucket)  # type: storage.Bucket
        self._blob = self._bucket.blob(blob)  # type: storage.Blob

        self._min_part_size = min_part_size
        self._total_size = 0
        self._total_parts = 0
        self._buf = io.BytesIO()

        self._session = google_requests.AuthorizedSession(self._credentials)
        # https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload#start-resumable
        self._resumeable_upload_url = self._blob.create_resumable_upload_session()

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    def flush(self):
        pass

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        logger.debug("closing")
        if self._total_size == 0:  # empty files
            self._upload_empty_part()
        if self._buf.tell():
            self._upload_next_part()
        logger.debug("successfully closed")

    def writable(self):
        """Return True if the stream supports writing."""
        return True

    def tell(self):
        """Return the current stream position."""
        return self._total_size

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
            raise TypeError(
                "input must be one of %r, got: %r" % (_BINARY_TYPES, type(b)))

        self._buf.write(b)
        self._total_size += len(b)

        if self._buf.tell() >= self._min_part_size:
            self._upload_next_part()

        return len(b)

    def terminate(self):
        # https://cloud.google.com/storage/docs/xml-api/resumable-upload#example_cancelling_an_upload
        self._session.delete(self._resumeable_upload_url)

    #
    # Internal methods.
    #
    def _upload_next_part(self):
        part_num = self._total_parts + 1
        logger.info("uploading part #%i, %i bytes (total %.3fGB)",
                    part_num, self._buf.tell(), self._total_size / 1024.0 ** 3)
        content_length = self._buf.tell()
        start = self._total_size - content_length
        stop = self._total_size - 1
        if content_length < MIN_MIN_PART_SIZE:
            end = content_length
        else:
            end = _UNKNOWN_FILE_SIZE
        self._buf.seek(0)

        headers = {
            'Content-Length': str(content_length),
            'Content-Range': make_range_string(start, stop, end)
        }
        # TODO: Add error handling / retrying here
        response = self._session.put(self._resumeable_upload_url, data=self._buf, headers=headers)
        assert response.status_code in (200, 201)
        logger.debug("upload of part #%i finished" % part_num)

        self._total_parts += 1
        self._buf = io.BytesIO()

    def _upload_empty_part(self):
        logger.info("creating empty file")
        headers = {
            'Content-Length': '0'
        }
        response = self._session.put(self._resumeable_upload_url, headers=headers)
        assert response.status_code in (200, 201)

        self._total_parts += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.terminate()
        else:
            self.close()