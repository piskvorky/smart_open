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
import sys

import google.cloud.exceptions
import google.cloud.storage
import google.auth.transport.requests as google_requests
import six

import smart_open.bytebuffer
import smart_open.s3

logger = logging.getLogger(__name__)

_READ_BINARY = 'rb'
_WRITE_BINARY = 'wb'

_MODES = (_READ_BINARY, _WRITE_BINARY)
"""Allowed I/O modes for working with GCS."""

_BINARY_TYPES = (six.binary_type, bytearray)
"""Allowed binary buffer types for writing to the underlying GCS stream"""

if sys.version_info >= (2, 7):
    _BINARY_TYPES = (six.binary_type, bytearray, memoryview)

_BINARY_NEWLINE = b'\n'

_UNKNOWN_FILE_SIZE = '*'

SUPPORTED_SCHEME = "gs"
"""Supported scheme for GCS"""

_MIN_MIN_PART_SIZE = _REQUIRED_CHUNK_MULTIPLE = 256 * 1024
"""Google requires you to upload in multiples of 256 KB, except for the last part."""

_DEFAULT_MIN_PART_SIZE = 50 * 1024**2
"""Default minimum part size for GCS multipart uploads"""

DEFAULT_BUFFER_SIZE = 256 * 1024
"""Default buffer size for working with GCS"""

START = 0
"""Seek to the absolute start of a GCS file"""

CURRENT = 1
"""Seek relative to the current positive of a GCS file"""

END = 2
"""Seek relative to the end of a GCS file"""

_WHENCE_CHOICES = (START, CURRENT, END)

_UPLOAD_INCOMPLETE_STATUS_CODE = 308
_UPLOAD_COMPLETE_STATUS_CODES = (200, 201)


def _make_range_string(start, stop=None, end=_UNKNOWN_FILE_SIZE):
    #
    # https://cloud.google.com/storage/docs/xml-api/resumable-upload#step_3upload_the_file_blocks
    #
    if stop is None:
        return 'bytes %d-/%s' % (start, end)
    return 'bytes %d-%d/%s' % (start, stop, end)


class UploadFailedError(Exception):
    def __init__(self, message, status_code, text):
        """Raise when a multi-part upload to GCS returns a failed response status code.

        Parameters
        ----------
        message: str
            The error message to display.
        status_code: int
            The status code returned from the upload response.
        text: str
            The text returned from the upload response.

        """
        super(UploadFailedError, self).__init__(message)
        self.status_code = status_code
        self.text = text

    @classmethod
    def from_response(cls, response, part_num, content_length, total_size, headers):
        status_code = response.status_code
        response_text = response.text
        total_size_gb = total_size / 1024.0 ** 3

        msg = (
            "upload failed (status code: %(status_code)d, response text: %(response_text)s), "
            "part #%(part_num)d, %(total_size)d bytes (total %(total_size_gb).3fGB), headers: %(headers)r"
        ) % locals()
        return cls(msg, response.status_code, response.text)


def open(
        bucket_id,
        blob_id,
        mode,
        buffer_size=DEFAULT_BUFFER_SIZE,
        min_part_size=_MIN_MIN_PART_SIZE,
        client=None,  # type: google.cloud.storage.Client
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

    """
    if mode == _READ_BINARY:
        return SeekableBufferedInputBase(
            bucket_id,
            blob_id,
            buffer_size=buffer_size,
            line_terminator=_BINARY_NEWLINE,
            client=client,
        )
    elif mode == _WRITE_BINARY:
        return BufferedOutputBase(
            bucket_id,
            blob_id,
            min_part_size=min_part_size,
            client=client,
        )
    else:
        raise NotImplementedError('GCS support for mode %r not implemented' % mode)


class _SeekableRawReader(object):
    """Read an GCS object."""

    def __init__(self, gcs_blob, size):
        # type: (google.cloud.storage.Blob, int) -> None
        self._blob = gcs_blob
        self._size = size
        self._position = 0

    def seek(self, position):
        """Seek to the specified position (byte offset) in the GCS key.

        :param int position: The byte offset from the beginning of the key.

        Returns the position after seeking.
        """
        self._position = position
        return self._position

    def read(self, size=-1):
        if self._position >= self._size:
            return b''
        binary = self._download_blob_chunk(size)
        self._position += len(binary)
        return binary

    def _download_blob_chunk(self, size):
        start = position = self._position
        if position == self._size:
            #
            # When reading, we can't seek to the first byte of an empty file.
            # Similarly, we can't seek past the last byte.  Do nothing here.
            #
            binary = b''
        elif size == -1:
            binary = self._blob.download_as_string(start=start)
        else:
            end = position + size
            binary = self._blob.download_as_string(start=start, end=end)
        return binary


class SeekableBufferedInputBase(io.BufferedIOBase):
    """Reads bytes from GCS.

    Implements the io.BufferedIOBase interface of the standard library.

    :raises google.cloud.exceptions.NotFound: Raised when the blob to read from does not exist.

    """
    def __init__(
            self,
            bucket,
            key,
            buffer_size=DEFAULT_BUFFER_SIZE,
            line_terminator=_BINARY_NEWLINE,
            client=None,  # type: google.cloud.storage.Client
    ):
        if client is None:
            client = google.cloud.storage.Client()
        bucket = client.get_bucket(bucket)  # type: google.cloud.storage.Bucket

        self._blob = bucket.get_blob(key)
        if self._blob is None:
            raise google.cloud.exceptions.NotFound('blob {} not found in {}'.format(key, bucket))
        self._size = self._blob.size if self._blob.size is not None else 0

        self._raw_reader = _SeekableRawReader(self._blob, self._size)
        self._current_pos = 0
        self._current_part_size = buffer_size
        self._current_part = smart_open.bytebuffer.ByteBuffer(buffer_size)
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
        self._current_part = None
        self._raw_reader = None

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

    def seek(self, offset, whence=START):
        """Seek to the specified position.

        :param int offset: The offset in bytes.
        :param int whence: Where the offset is from.

        Returns the position after seeking."""
        logger.debug('seeking to offset: %r whence: %r', offset, whence)
        if whence not in _WHENCE_CHOICES:
            raise ValueError('invalid whence, expected one of %r' % _WHENCE_CHOICES)

        if whence == START:
            new_position = offset
        elif whence == CURRENT:
            new_position = self._current_pos + offset
        else:
            new_position = self._size + offset
        new_position = smart_open.s3.clamp(new_position, 0, self._size)
        self._current_pos = new_position
        self._raw_reader.seek(new_position)
        logger.debug('current_pos: %r', self._current_pos)

        self._current_part.empty()
        self._eof = self._current_pos == self._size
        return self._current_pos

    def tell(self):
        """Return the current position within the file."""
        return self._current_pos

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size=-1):
        """Read up to size bytes from the object and return them."""
        if size == 0:
            return b''
        elif size < 0:
            self._current_pos = self._size
            return self._read_from_buffer() + self._raw_reader.read()

        #
        # Return unused data first
        #
        if len(self._current_part) >= size:
            return self._read_from_buffer(size)

        #
        # If the stream is finished, return what we have.
        #
        if self._eof:
            return self._read_from_buffer()

        #
        # Fill our buffer to the required size.
        #
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
        while not (self._eof and len(self._current_part) == 0):
            #
            # In the worst case, we're reading the unread part of self._current_part
            # twice here, once in the if condition and once when calling index.
            #
            # This is sub-optimal, but better than the alternative: wrapping
            # .index in a try..except, because that is slower.
            #
            remaining_buffer = self._current_part.peek()
            if self._line_terminator in remaining_buffer:
                next_newline = remaining_buffer.index(self._line_terminator)
                the_line.write(self._read_from_buffer(next_newline + 1))
                break
            else:
                the_line.write(self._read_from_buffer())
                self._fill_buffer()
        return the_line.getvalue()

    #
    # Internal methods.
    #
    def _read_from_buffer(self, size=-1):
        """Remove at most size bytes from our buffer and return them."""
        # logger.debug('reading %r bytes from %r byte-long buffer', size, len(self._current_part))
        size = size if size >= 0 else len(self._current_part)
        part = self._current_part.read(size)
        self._current_pos += len(part)
        # logger.debug('part: %r', part)
        return part

    def _fill_buffer(self, size=-1):
        size = size if size >= 0 else self._current_part._chunk_size
        while len(self._current_part) < size and not self._eof:
            bytes_read = self._current_part.fill(self._raw_reader)
            if bytes_read == 0:
                logger.debug('reached EOF while filling buffer')
                self._eof = True

    def __str__(self):
        return "(%s, %r, %r)" % (self.__class__.__name__, self._bucket.name, self._blob.name)

    def __repr__(self):
        return "%s(bucket=%r, blob=%r, buffer_size=%r)" % (
            self.__class__.__name__, self._bucket.name, self._blob.name, self._current_part_size,
        )


class BufferedOutputBase(io.BufferedIOBase):
    """Writes bytes to GCS.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(
            self,
            bucket,
            blob,
            min_part_size=_DEFAULT_MIN_PART_SIZE,
            client=None,  # type: google.cloud.storage.Client
    ):
        if client is None:
            client = google.cloud.storage.Client()
        self._client = client
        self._credentials = self._client._credentials  # noqa
        self._bucket = self._client.bucket(bucket)  # type: google.cloud.storage.Bucket
        self._blob = self._bucket.blob(blob)  # type: google.cloud.storage.Blob
        assert min_part_size % _REQUIRED_CHUNK_MULTIPLE == 0, 'min part size must be a multiple of 256KB'
        assert min_part_size >= _MIN_MIN_PART_SIZE, 'min part size must be greater than 256KB'
        self._min_part_size = min_part_size

        self._total_size = 0
        self._total_parts = 0
        self._bytes_uploaded = 0
        self._current_part = io.BytesIO()

        self._session = google_requests.AuthorizedSession(self._credentials)

        #
        # https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload#start-resumable
        #
        self._resumable_upload_url = self._blob.create_resumable_upload_session()

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
        if not self.closed:
            if self._total_size == 0:  # empty files
                self._upload_empty_part()
            else:
                self._upload_final_part()
            self._client = None
        logger.debug("successfully closed")

    @property
    def closed(self):
        return self._client is None

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
            raise TypeError("input must be one of %r, got: %r" % (_BINARY_TYPES, type(b)))

        self._current_part.write(b)
        self._total_size += len(b)

        #
        # If the size of this part is precisely equal to the minimum part size,
        # we don't perform the actual write now, and wait until we see more data.
        # We do this because the very last part of the upload must be handled slightly
        # differently (see comments in the _upload_next_part method).
        #
        if self._current_part.tell() > self._min_part_size:
            self._upload_next_part()

        return len(b)

    def terminate(self):
        """Cancel the underlying resumable upload."""
        #
        # https://cloud.google.com/storage/docs/xml-api/resumable-upload#example_cancelling_an_upload
        #
        self._session.delete(self._resumable_upload_url)

    #
    # Internal methods.
    #
    def _upload_next_part(self):
        part_num = self._total_parts + 1

        # upload the largest amount possible given GCS's restriction
        # of parts being multiples of 256kB, except for the last one
        size_of_leftovers = self._current_part.tell() % self._min_part_size
        content_length = self._current_part.tell() - size_of_leftovers

        # a final upload of 0 bytes does not work, so we need to guard against this edge case
        # this results in occasionally keeping an additional 256kB in the buffer after uploading a part,
        # but until this is fixed on Google's end there is no other option
        # https://stackoverflow.com/questions/60230631/upload-zero-size-final-part-to-google-cloud-storage-resumable-upload
        if size_of_leftovers == 0:
            content_length -= _REQUIRED_CHUNK_MULTIPLE

        total_size = self._bytes_uploaded + content_length

        start = self._bytes_uploaded
        stop = total_size - 1

        self._current_part.seek(0)

        headers = {
            'Content-Length': str(content_length),
            'Content-Range': _make_range_string(start, stop, _UNKNOWN_FILE_SIZE),
        }

        logger.info(
            "uploading part #%i, %i bytes (total %.3fGB) headers %r",
            part_num, content_length, total_size / 1024.0 ** 3, headers,
        )

        response = self._session.put(
            self._resumable_upload_url,
            data=self._current_part.read(content_length),
            headers=headers,
        )

        if response.status_code != _UPLOAD_INCOMPLETE_STATUS_CODE:
            raise UploadFailedError.from_response(
                response,
                part_num,
                content_length,
                self._total_size,
                headers,
            )
        logger.debug("upload of part #%i finished" % part_num)

        self._total_parts += 1
        self._bytes_uploaded += content_length
        # handle the leftovers
        self._current_part = io.BytesIO(self._current_part.read())
        self._current_part.seek(0, io.SEEK_END)

    def _upload_final_part(self):
        part_num = self._total_parts + 1
        content_length = self._current_part.tell()
        stop = self._total_size - 1
        start = self._bytes_uploaded

        headers = {
            'Content-Length': str(content_length),
            'Content-Range': _make_range_string(start, stop, self._total_size),
        }

        logger.info(
            "uploading part #%i, %i bytes (total %.3fGB) headers %r",
            part_num, content_length, self._total_size / 1024.0 ** 3, headers,
        )

        self._current_part.seek(0)

        response = self._session.put(
            self._resumable_upload_url,
            data=self._current_part,
            headers=headers,
        )

        if response.status_code not in _UPLOAD_COMPLETE_STATUS_CODES:
            raise UploadFailedError.from_response(
                response,
                part_num,
                content_length,
                self._total_size,
                headers,
            )
        logger.debug("upload of part #%i finished" % part_num)

        self._total_parts += 1
        self._bytes_uploaded += content_length
        self._current_part = io.BytesIO()

    def _upload_empty_part(self):
        logger.debug("creating empty file")
        headers = {'Content-Length': '0'}
        response = self._session.put(self._resumable_upload_url, headers=headers)
        assert response.status_code in _UPLOAD_COMPLETE_STATUS_CODES

        self._total_parts += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.terminate()
        else:
            self.close()

    def __str__(self):
        return "(%s, %r, %r)" % (self.__class__.__name__, self._bucket.name, self._blob.name)

    def __repr__(self):
        return "%s(bucket=%r, blob=%r, min_part_size=%r)" % (
            self.__class__.__name__, self._bucket.name, self._blob.name, self._min_part_size,
        )
