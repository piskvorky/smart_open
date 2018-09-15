# -*- coding: utf-8 -*-
"""Implements file-like objects for reading and writing from/to S3."""

import io
import contextlib
import functools
import logging

import boto3
import botocore.client
import six


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# Multiprocessing is unavailable in App Engine (and possibly other sandboxes).
# The only method currently relying on it is s3_iter_bucket, which is instructed
# whether to use it by the MULTIPROCESSING flag.
_MULTIPROCESSING = False
try:
    import multiprocessing.pool
    _MULTIPROCESSING = True
except ImportError:
    logger.warning("multiprocessing could not be imported and won't be used")

START = 0
CURRENT = 1
END = 2
WHENCE_CHOICES = (START, CURRENT, END)

DEFAULT_MIN_PART_SIZE = 50 * 1024**2
"""Default minimum part size for S3 multipart uploads"""
MIN_MIN_PART_SIZE = 5 * 1024 ** 2
"""The absolute minimum permitted by Amazon."""
READ_BINARY = 'rb'
WRITE_BINARY = 'wb'
MODES = (READ_BINARY, WRITE_BINARY)
"""Allowed I/O modes for working with S3."""

BINARY_NEWLINE = b'\n'
DEFAULT_BUFFER_SIZE = 128 * 1024

SUPPORTED_SCHEMES = ("s3", "s3n", 's3u', "s3a")


def _range_string(start, stop=None):
    #
    # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    #
    if stop is None:
        return 'bytes=%d-' % start
    return 'bytes=%d-%d' % (start, stop)


def _clamp(value, minval, maxval):
    return max(min(value, maxval), minval)


def open(bucket_id, key_id, mode, **kwargs):
    logger.debug('%r', locals())
    if mode not in MODES:
        raise NotImplementedError('bad mode: %r expected one of %r' % (mode, MODES))

    encoding = kwargs.pop("encoding", "utf-8")
    errors = kwargs.pop("errors", None)
    newline = kwargs.pop("newline", None)
    line_buffering = kwargs.pop("line_buffering", False)
    s3_min_part_size = kwargs.pop("s3_min_part_size", DEFAULT_MIN_PART_SIZE)

    if mode == READ_BINARY:
        fileobj = SeekableBufferedInputBase(bucket_id, key_id, **kwargs)
    elif mode == WRITE_BINARY:
        fileobj = BufferedOutputBase(bucket_id, key_id, min_part_size=s3_min_part_size, **kwargs)
    else:
        assert False, 'unexpected mode: %r' % mode

    return fileobj


class RawReader(object):
    """Read an S3 object."""
    def __init__(self, s3_object):
        self.position = 0
        self._object = s3_object
        self._body = s3_object.get()['Body']

    def read(self, size=-1):
        if size == -1:
            return self._body.read()
        return self._body.read(size)


class SeekableRawReader(object):
    """Read an S3 object."""

    def __init__(self, s3_object):
        self._object = s3_object
        self._content_length = self._object.content_length
        self.seek(0)

    def seek(self, position):
        """Seek to the specified position (byte offset) in the S3 key.

        :param int position: The byte offset from the beginning of the key.
        """
        self._position = position
        range_string = _range_string(self._position)
        logger.debug('content_length: %r range_string: %r', self._content_length, range_string)

        #
        # Close old body explicitly.
        # When first seek(), self._body is not exist. Catch the exception and do nothing.
        #
        try:
            self._body.close()
        except AttributeError as e:
            pass

        if position == self._content_length == 0 or position == self._content_length:
            #
            # When reading, we can't seek to the first byte of an empty file.
            # Similarly, we can't seek past the last byte.  Do nothing here.
            #
            self._body = io.BytesIO()
        else:
            self._body = self._object.get(Range=range_string)['Body']

    def read(self, size=-1):
        if self._position >= self._content_length:
            return b''
        if size == -1:
            binary = self._body.read()
        else:
            binary = self._body.read(size)
        self._position += len(binary)
        return binary


class BufferedInputBase(io.BufferedIOBase):
    def __init__(self, bucket, key, buffer_size=DEFAULT_BUFFER_SIZE,
                 line_terminator=BINARY_NEWLINE, **kwargs):
        session = kwargs.pop(
            's3_session',
            boto3.Session(profile_name=kwargs.pop('profile_name', None))
        )
        s3 = session.resource('s3', **kwargs)
        self._object = s3.Object(bucket, key)
        self._raw_reader = RawReader(self._object)
        self._content_length = self._object.content_length
        self._current_pos = 0
        self._buffer = b''
        self._eof = False
        self._buffer_size = buffer_size
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
        self._object = None

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
            # In the worst case, we're reading self._buffer twice here, once in
            # the if condition, and once when calling index.
            #
            # This is sub-optimal, but better than the alternative: wrapping
            # .index in a try..except, because that is slower.
            #
            if self._line_terminator in self._buffer:
                next_newline = self._buffer.index(self._line_terminator)
                the_line.write(self._read_from_buffer(next_newline + 1))
                break
            else:
                the_line.write(self._read_from_buffer(len(self._buffer)))
                self._fill_buffer(self._buffer_size)
        return the_line.getvalue()

    def terminate(self):
        """Do nothing."""
        pass

    #
    # Internal methods.
    #
    def _read_from_buffer(self, size):
        """Remove at most size bytes from our buffer and return them."""
        # logger.debug('reading %r bytes from %r byte-long buffer', size, len(self._buffer))
        assert size >= 0
        part = self._buffer[:size]
        self._buffer = self._buffer[size:]
        self._current_pos += len(part)
        # logger.debug('part: %r', part)
        return part

    def _fill_buffer(self, size):
        while len(self._buffer) < size and not self._eof:
            raw = self._raw_reader.read(size=self._buffer_size)
            if len(raw):
                self._buffer += raw
            else:
                logger.debug('reached EOF while filling buffer')
                self._eof = True


class SeekableBufferedInputBase(BufferedInputBase):
    """Reads bytes from S3.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(self, bucket, key, buffer_size=DEFAULT_BUFFER_SIZE,
                 line_terminator=BINARY_NEWLINE, **kwargs):
        session = kwargs.pop(
            's3_session',
            boto3.Session(profile_name=kwargs.pop('profile_name', None))
        )
        s3 = session.resource('s3', **kwargs)
        self._object = s3.Object(bucket, key)
        self._raw_reader = SeekableRawReader(self._object)
        self._content_length = self._object.content_length
        self._current_pos = 0
        self._buffer = b''
        self._eof = False
        self._buffer_size = buffer_size
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
            new_position = self._content_length + offset
        new_position = _clamp(new_position, 0, self._content_length)
        self._current_pos = new_position
        self._raw_reader.seek(new_position)
        logger.debug('new_position: %r', self._current_pos)

        self._buffer = b""
        self._eof = self._current_pos == self._content_length
        return self._current_pos

    def tell(self):
        """Return the current position within the file."""
        return self._current_pos

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation


class BufferedOutputBase(io.BufferedIOBase):
    """Writes bytes to S3.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(self, bucket, key, min_part_size=DEFAULT_MIN_PART_SIZE, s3_upload=None, **kwargs):
        if min_part_size < MIN_MIN_PART_SIZE:
            logger.warning("S3 requires minimum part size >= 5MB; \
multipart upload may fail")

        session = kwargs.pop(
            's3_session',
            boto3.Session(profile_name=kwargs.pop('profile_name', None))
        )
        s3 = session.resource('s3', **kwargs)

        #
        # https://stackoverflow.com/questions/26871884/how-can-i-easily-determine-if-a-boto-3-s3-bucket-resource-exists
        #
        try:
            s3.meta.client.head_bucket(Bucket=bucket)
        except botocore.client.ClientError:
            raise ValueError('the bucket %r does not exist, or is forbidden for access' % bucket)
        self._object = s3.Object(bucket, key)
        self._min_part_size = min_part_size
        self._mp = self._object.initiate_multipart_upload(**(s3_upload or {}))

        self._buf = io.BytesIO()
        self._total_bytes = 0
        self._total_parts = 0
        self._parts = []

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
        if self._buf.tell():
            self._upload_next_part()

        if self._total_bytes and self._mp:
            self._mp.complete(MultipartUpload={'Parts': self._parts})
            logger.debug("completed multipart upload")
        elif self._mp:
            #
            # AWS complains with "The XML you provided was not well-formed or
            # did not validate against our published schema" when the input is
            # completely empty => abort the upload, no file created.
            #
            # We work around this by creating an empty file explicitly.
            #
            logger.info("empty input, ignoring multipart upload")
            assert self._mp, "no multipart upload in progress"
            self._mp.abort()

            self._object.put(Body=b'')
        self._mp = None
        logger.debug("successfully closed")

    @property
    def closed(self):
        return self._mp is None

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
        logger.info("uploading part #%i, %i bytes (total %.3fGB)",
                    part_num, self._buf.tell(), self._total_bytes / 1024.0 ** 3)
        self._buf.seek(0)
        part = self._mp.Part(part_num)
        upload = part.upload(Body=self._buf)
        self._parts.append({'ETag': upload['ETag'], 'PartNumber': part_num})
        logger.debug("upload of part #%i finished" % part_num)

        self._total_parts += 1
        self._buf = io.BytesIO()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.terminate()
        else:
            self.close()


def iter_bucket(bucket_name, prefix='', accept_key=lambda key: True,
                key_limit=None, workers=16, retries=3):
    """
    Iterate and download all S3 files under `bucket/prefix`, yielding out
    `(key, key content)` 2-tuples (generator).

    `accept_key` is a function that accepts a key name (unicode string) and
    returns True/False, signalling whether the given key should be downloaded out or
    not (default: accept all keys).

    If `key_limit` is given, stop after yielding out that many results.

    The keys are processed in parallel, using `workers` processes (default: 16),
    to speed up downloads greatly. If multiprocessing is not available, thus
    _MULTIPROCESSING is False, this parameter will be ignored.

    Example::

      >>> # get all JSON files under "mybucket/foo/"
      >>> for key, content in iter_bucket(bucket_name, prefix='foo/', accept_key=lambda key: key.endswith('.json')):
      ...     print key, len(content)

      >>> # limit to 10k files, using 32 parallel workers (default is 16)
      >>> for key, content in iter_bucket(bucket_name, key_limit=10000, workers=32):
      ...     print key, len(content)
    """
    #
    # If people insist on giving us bucket instances, silently extract the name
    # before moving on.  Works for boto3 as well as boto.
    #
    try:
        bucket_name = bucket_name.name
    except AttributeError:
        pass

    total_size, key_no = 0, -1
    key_iterator = _list_bucket(bucket_name, prefix=prefix, accept_key=accept_key)
    download_key = functools.partial(_download_key, bucket_name=bucket_name, retries=retries)

    with _create_process_pool(processes=workers) as pool:
        result_iterator = pool.imap_unordered(download_key, key_iterator)
        for key_no, (key, content) in enumerate(result_iterator):
            if True or key_no % 1000 == 0:
                logger.info(
                    "yielding key #%i: %s, size %i (total %.1fMB)",
                    key_no, key, len(content), total_size / 1024.0 ** 2
                )
            yield key, content
            total_size += len(content)

            if key_limit is not None and key_no + 1 >= key_limit:
                # we were asked to output only a limited number of keys => we're done
                break
    logger.info("processed %i keys, total size %i" % (key_no + 1, total_size))


def _list_bucket(bucket_name, prefix='', accept_key=lambda k: True):
    client = boto3.client('s3')
    ctoken = None

    while True:
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        try:
            content = response['Contents']
        except KeyError:
            pass
        else:
            for c in content:
                key = c['Key']
                if accept_key(key):
                    yield key
        ctoken = response.get('NextContinuationToken', None)
        if not ctoken:
            break


def _download_key(key_name, bucket_name=None, retries=3):
    if bucket_name is None:
        raise ValueError('bucket_name may not be None')

    #
    # https://geekpete.com/blog/multithreading-boto3/
    #
    session = boto3.session.Session()
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)

    # Sometimes, https://github.com/boto/boto/issues/2409 can happen because of network issues on either side.
    # Retry up to 3 times to ensure its not a transient issue.
    for x in range(retries + 1):
        try:
            content_bytes = _download_fileobj(bucket, key_name)
        except botocore.client.ClientError:
            # Actually fail on last pass through the loop
            if x == retries:
                raise
            # Otherwise, try again, as this might be a transient timeout
            pass
        else:
            return key_name, content_bytes


def _download_fileobj(bucket, key_name):
    #
    # This is a separate function only because it makes it easier to inject
    # exceptions during tests.
    #
    buf = io.BytesIO()
    bucket.download_fileobj(key_name, buf)
    return buf.getvalue()


class DummyPool(object):
    """A class that mimics multiprocessing.pool.Pool for our purposes."""
    def imap_unordered(self, function, items):
        return six.moves.map(function, items)

    def terminate(self):
        pass


@contextlib.contextmanager
def _create_process_pool(processes=1):
    if _MULTIPROCESSING and processes:
        logger.info("creating pool with %i workers", processes)
        pool = multiprocessing.pool.Pool(processes=processes)
    else:
        logger.info("creating dummy pool")
        pool = DummyPool()
    yield pool
    pool.terminate()
