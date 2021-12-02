# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements file-like objects for reading and writing from/to AWS S3."""

import io
import functools
import logging
import time
import warnings

try:
    import boto3
    import botocore.client
    import botocore.exceptions
    import urllib3.exceptions
except ImportError:
    MISSING_DEPS = True

import smart_open.bytebuffer
import smart_open.concurrency
import smart_open.utils

from smart_open import constants

logger = logging.getLogger(__name__)

DEFAULT_MIN_PART_SIZE = 50 * 1024**2
"""Default minimum part size for S3 multipart uploads"""
MIN_MIN_PART_SIZE = 5 * 1024 ** 2
"""The absolute minimum permitted by Amazon."""

SCHEMES = ("s3", "s3n", 's3u', "s3a")
DEFAULT_PORT = 443
DEFAULT_HOST = 's3.amazonaws.com'

DEFAULT_BUFFER_SIZE = 128 * 1024

URI_EXAMPLES = (
    's3://my_bucket/my_key',
    's3://my_key:my_secret@my_bucket/my_key',
    's3://my_key:my_secret@my_server:my_port@my_bucket/my_key',
)

_UPLOAD_ATTEMPTS = 6
_SLEEP_SECONDS = 10

# Returned by AWS when we try to seek beyond EOF.
_OUT_OF_RANGE = 'InvalidRange'


class _ClientWrapper:
    """Wraps a client to inject the appropriate keyword args into each method call.

    The keyword args are a dictionary keyed by the fully qualified method name.
    For example, S3.Client.create_multipart_upload.

    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client

    This wrapper behaves identically to the client otherwise.
    """
    def __init__(self, client, kwargs):
        self.client = client
        self.kwargs = kwargs

    def __getattr__(self, method_name):
        method = getattr(self.client, method_name)
        kwargs = self.kwargs.get('S3.Client.%s' % method_name, {})
        return functools.partial(method, **kwargs)


def parse_uri(uri_as_string):
    #
    # Restrictions on bucket names and labels:
    #
    # - Bucket names must be at least 3 and no more than 63 characters long.
    # - Bucket names must be a series of one or more labels.
    # - Adjacent labels are separated by a single period (.).
    # - Bucket names can contain lowercase letters, numbers, and hyphens.
    # - Each label must start and end with a lowercase letter or a number.
    #
    # We use the above as a guide only, and do not perform any validation.  We
    # let boto3 take care of that for us.
    #
    split_uri = smart_open.utils.safe_urlsplit(uri_as_string)
    assert split_uri.scheme in SCHEMES

    port = DEFAULT_PORT
    host = DEFAULT_HOST
    ordinary_calling_format = False
    #
    # These defaults tell boto3 to look for credentials elsewhere
    #
    access_id, access_secret = None, None

    #
    # Common URI template [secret:key@][host[:port]@]bucket/object
    #
    # The urlparse function doesn't handle the above schema, so we have to do
    # it ourselves.
    #
    uri = split_uri.netloc + split_uri.path

    if '@' in uri and ':' in uri.split('@')[0]:
        auth, uri = uri.split('@', 1)
        access_id, access_secret = auth.split(':')

    head, key_id = uri.split('/', 1)
    if '@' in head and ':' in head:
        ordinary_calling_format = True
        host_port, bucket_id = head.split('@')
        host, port = host_port.split(':', 1)
        port = int(port)
    elif '@' in head:
        ordinary_calling_format = True
        host, bucket_id = head.split('@')
    else:
        bucket_id = head

    return dict(
        scheme=split_uri.scheme,
        bucket_id=bucket_id,
        key_id=key_id,
        port=port,
        host=host,
        ordinary_calling_format=ordinary_calling_format,
        access_id=access_id,
        access_secret=access_secret,
    )


def _consolidate_params(uri, transport_params):
    """Consolidates the parsed Uri with the additional parameters.

    This is necessary because the user can pass some of the parameters can in
    two different ways:

    1) Via the URI itself
    2) Via the transport parameters

    These are not mutually exclusive, but we have to pick one over the other
    in a sensible way in order to proceed.

    """
    transport_params = dict(transport_params)

    def inject(**kwargs):
        try:
            client_kwargs = transport_params['client_kwargs']
        except KeyError:
            client_kwargs = transport_params['client_kwargs'] = {}

        try:
            init_kwargs = client_kwargs['S3.Client']
        except KeyError:
            init_kwargs = client_kwargs['S3.Client'] = {}

        init_kwargs.update(**kwargs)

    client = transport_params.get('client')
    if client is not None and (uri['access_id'] or uri['access_secret']):
        logger.warning(
            'ignoring credentials parsed from URL because they conflict with '
            'transport_params["client"]. Set transport_params["client"] to None '
            'to suppress this warning.'
        )
        uri.update(access_id=None, access_secret=None)
    elif (uri['access_id'] and uri['access_secret']):
        inject(
            aws_access_key_id=uri['access_id'],
            aws_secret_access_key=uri['access_secret'],
        )
        uri.update(access_id=None, access_secret=None)

    if client is not None and uri['host'] != DEFAULT_HOST:
        logger.warning(
            'ignoring endpoint_url parsed from URL because they conflict with '
            'transport_params["client"]. Set transport_params["client"] to None '
            'to suppress this warning.'
        )
        uri.update(host=None)
    elif uri['host'] != DEFAULT_HOST:
        inject(endpoint_url='https://%(host)s:%(port)d' % uri)
        uri.update(host=None)

    return uri, transport_params


def open_uri(uri, mode, transport_params):
    deprecated = (
        'multipart_upload_kwargs',
        'object_kwargs',
        'resource',
        'resource_kwargs',
        'session',
        'singlepart_upload_kwargs',
    )
    detected = [k for k in deprecated if k in transport_params]
    if detected:
        doc_url = (
            'https://github.com/RaRe-Technologies/smart_open/blob/develop/'
            'MIGRATING_FROM_OLDER_VERSIONS.rst'
        )
        #
        # We use warnings.warn /w UserWarning instead of logger.warn here because
        #
        # 1) Not everyone has logging enabled; and
        # 2) check_kwargs (below) already uses logger.warn with a similar message
        #
        # https://github.com/RaRe-Technologies/smart_open/issues/614
        #
        message = (
            'ignoring the following deprecated transport parameters: %r. '
            'See <%s> for details' % (detected, doc_url)
        )
        warnings.warn(message, UserWarning)
    parsed_uri = parse_uri(uri)
    parsed_uri, transport_params = _consolidate_params(parsed_uri, transport_params)
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    return open(parsed_uri['bucket_id'], parsed_uri['key_id'], mode, **kwargs)


def open(
    bucket_id,
    key_id,
    mode,
    version_id=None,
    buffer_size=DEFAULT_BUFFER_SIZE,
    min_part_size=DEFAULT_MIN_PART_SIZE,
    multipart_upload=True,
    defer_seek=False,
    client=None,
    client_kwargs=None,
    writebuffer=None,
):
    """Open an S3 object for reading or writing.

    Parameters
    ----------
    bucket_id: str
        The name of the bucket this object resides in.
    key_id: str
        The name of the key within the bucket.
    mode: str
        The mode for opening the object.  Must be either "rb" or "wb".
    buffer_size: int, optional
        The buffer size to use when performing I/O.
    min_part_size: int, optional
        The minimum part size for multipart uploads.  For writing only.
    multipart_upload: bool, optional
        Default: `True`
        If set to `True`, will use multipart upload for writing to S3. If set
        to `False`, S3 upload will use the S3 Single-Part Upload API, which
        is more ideal for small file sizes.
        For writing only.
    version_id: str, optional
        Version of the object, used when reading object.
        If None, will fetch the most recent version.
    defer_seek: boolean, optional
        Default: `False`
        If set to `True` on a file opened for reading, GetObject will not be
        called until the first seek() or read().
        Avoids redundant API queries when seeking before reading.
    client: object, optional
        The S3 client to use when working with boto3.
        If you don't specify this, then smart_open will create a new client for you.
    client_kwargs: dict, optional
        Additional parameters to pass to the relevant functions of the client.
        The keys are fully qualified method names, e.g. `S3.Client.create_multipart_upload`.
        The values are kwargs to pass to that method each time it is called.
    writebuffer: IO[bytes], optional
        By default, this module will buffer data in memory using io.BytesIO
        when writing. Pass another binary IO instance here to use it instead.
        For example, you may pass a file object to buffer to local disk instead
        of in RAM. Use this to keep RAM usage low at the expense of additional
        disk IO. If you pass in an open file, then you are responsible for
        cleaning it up after writing completes.
    """
    logger.debug('%r', locals())
    if mode not in constants.BINARY_MODES:
        raise NotImplementedError('bad mode: %r expected one of %r' % (mode, constants.BINARY_MODES))

    if (mode == constants.WRITE_BINARY) and (version_id is not None):
        raise ValueError("version_id must be None when writing")

    if mode == constants.READ_BINARY:
        fileobj = Reader(
            bucket_id,
            key_id,
            version_id=version_id,
            buffer_size=buffer_size,
            defer_seek=defer_seek,
            client=client,
            client_kwargs=client_kwargs,
        )
    elif mode == constants.WRITE_BINARY:
        if multipart_upload:
            fileobj = MultipartWriter(
                bucket_id,
                key_id,
                min_part_size=min_part_size,
                client=client,
                client_kwargs=client_kwargs,
                writebuffer=writebuffer,
            )
        else:
            fileobj = SinglepartWriter(
                bucket_id,
                key_id,
                client=client,
                client_kwargs=client_kwargs,
                writebuffer=writebuffer,
            )
    else:
        assert False, 'unexpected mode: %r' % mode

    fileobj.name = key_id
    return fileobj


def _get(client, bucket, key, version, range_string):
    try:
        if version:
            return client.get_object(Bucket=bucket, Key=key, VersionId=version, Range=range_string)
        else:
            return client.get_object(Bucket=bucket, Key=key, Range=range_string)
    except botocore.client.ClientError as error:
        wrapped_error = IOError(
            'unable to access bucket: %r key: %r version: %r error: %s' % (
                bucket, key, version, error
            )
        )
        wrapped_error.backend_error = error
        raise wrapped_error from error


def _unwrap_ioerror(ioe):
    """Given an IOError from _get, return the 'Error' dictionary from boto."""
    try:
        return ioe.backend_error.response['Error']
    except (AttributeError, KeyError):
        return None


class _SeekableRawReader(object):
    """Read an S3 object.

    This class is internal to the S3 submodule.
    """

    def __init__(
        self,
        client,
        bucket,
        key,
        version_id=None,
    ):
        self._client = client
        self._bucket = bucket
        self._key = key
        self._version_id = version_id

        self._content_length = None
        self._position = 0
        self._body = None

    def seek(self, offset, whence=constants.WHENCE_START):
        """Seek to the specified position.

        :param int offset: The offset in bytes.
        :param int whence: Where the offset is from.

        :returns: the position after seeking.
        :rtype: int
        """
        if whence not in constants.WHENCE_CHOICES:
            raise ValueError('invalid whence, expected one of %r' % constants.WHENCE_CHOICES)

        #
        # Close old body explicitly.
        # When first seek() after __init__(), self._body is not exist.
        #
        if self._body is not None:
            self._body.close()
        self._body = None

        start = None
        stop = None
        if whence == constants.WHENCE_START:
            start = max(0, offset)
        elif whence == constants.WHENCE_CURRENT:
            start = max(0, offset + self._position)
        else:
            stop = max(0, -offset)

        #
        # If we can figure out that we've read past the EOF, then we can save
        # an extra API call.
        #
        if self._content_length is None:
            reached_eof = False
        elif start is not None and start >= self._content_length:
            reached_eof = True
        elif stop == 0:
            reached_eof = True
        else:
            reached_eof = False

        if reached_eof:
            self._body = io.BytesIO()
            self._position = self._content_length
        else:
            self._open_body(start, stop)

        return self._position

    def _open_body(self, start=None, stop=None):
        """Open a connection to download the specified range of bytes. Store
        the open file handle in self._body.

        If no range is specified, start defaults to self._position.
        start and stop follow the semantics of the http range header,
        so a stop without a start will read bytes beginning at stop.

        As a side effect, set self._content_length. Set self._position
        to self._content_length if start is past end of file.
        """
        if start is None and stop is None:
            start = self._position
        range_string = smart_open.utils.make_range_string(start, stop)

        try:
            # Optimistically try to fetch the requested content range.
            response = _get(
                self._client,
                self._bucket,
                self._key,
                self._version_id,
                range_string,
            )
        except IOError as ioe:
            # Handle requested content range exceeding content size.
            error_response = _unwrap_ioerror(ioe)
            if error_response is None or error_response.get('Code') != _OUT_OF_RANGE:
                raise
            self._position = self._content_length = int(error_response['ActualObjectSize'])
            self._body = io.BytesIO()
        else:
            #
            # Keep track of how many times boto3's built-in retry mechanism
            # activated.
            #
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html#checking-retry-attempts-in-an-aws-service-response
            #
            logger.debug(
                '%s: RetryAttempts: %d',
                self,
                response['ResponseMetadata']['RetryAttempts'],
            )
            units, start, stop, length = smart_open.utils.parse_content_range(response['ContentRange'])
            self._content_length = length
            self._position = start
            self._body = response['Body']

    def read(self, size=-1):
        """Read from the continuous connection with the remote peer."""
        if self._body is None:
            # This is necessary for the very first read() after __init__().
            self._open_body()
        if self._position >= self._content_length:
            return b''

        #
        # Boto3 has built-in error handling and retry mechanisms:
        #
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html
        #
        # Unfortunately, it isn't always enough. There is still a non-zero
        # possibility that an exception will slip past these mechanisms and
        # terminate the read prematurely.  Luckily, at this stage, it's very
        # simple to recover from the problem: wait a little bit, reopen the
        # HTTP connection and try again.  Usually, a single retry attempt is
        # enough to recover, but we try multiple times "just in case".
        #
        for attempt, seconds in enumerate([1, 2, 4, 8, 16], 1):
            try:
                if size == -1:
                    binary = self._body.read()
                else:
                    binary = self._body.read(size)
            except (
                ConnectionResetError,
                botocore.exceptions.BotoCoreError,
                urllib3.exceptions.HTTPError,
            ) as err:
                logger.warning(
                    '%s: caught %r while reading %d bytes, sleeping %ds before retry',
                    self,
                    err,
                    size,
                    seconds,
                )
                time.sleep(seconds)
                self._open_body()
            else:
                self._position += len(binary)
                return binary

        raise IOError('%s: failed to read %d bytes after %d attempts' % (self, size, attempt))

    def __str__(self):
        return 'smart_open.s3._SeekableReader(%r, %r)' % (self._bucket, self._key)


def _initialize_boto3(rw, client, client_kwargs, bucket, key):
    """Created the required objects for accessing S3.  Ideally, they have
    been already created for us and we can just reuse them."""
    if client_kwargs is None:
        client_kwargs = {}

    if client is None:
        init_kwargs = client_kwargs.get('S3.Client', {})
        client = boto3.client('s3', **init_kwargs)
    assert client

    rw._client = _ClientWrapper(client, client_kwargs)
    rw._bucket = bucket
    rw._key = key


class Reader(io.BufferedIOBase):
    """Reads bytes from S3.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(
        self,
        bucket,
        key,
        version_id=None,
        buffer_size=DEFAULT_BUFFER_SIZE,
        line_terminator=constants.BINARY_NEWLINE,
        defer_seek=False,
        client=None,
        client_kwargs=None,
    ):
        self._version_id = version_id
        self._buffer_size = buffer_size

        _initialize_boto3(self, client, client_kwargs, bucket, key)

        self._raw_reader = _SeekableRawReader(
            self._client,
            bucket,
            key,
            self._version_id,
        )
        self._current_pos = 0
        self._buffer = smart_open.bytebuffer.ByteBuffer(buffer_size)
        self._eof = False
        self._line_terminator = line_terminator

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

        if not defer_seek:
            self.seek(0)

    #
    # io.BufferedIOBase methods.
    #

    def close(self):
        """Flush and close this stream."""
        pass

    def readable(self):
        """Return True if the stream can be read from."""
        return True

    def read(self, size=-1):
        """Read up to size bytes from the object and return them."""
        if size == 0:
            return b''
        elif size < 0:
            # call read() before setting _current_pos to make sure _content_length is set
            out = self._read_from_buffer() + self._raw_reader.read()
            self._current_pos = self._raw_reader._content_length
            return out

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

        #
        # A single line may span multiple buffers.
        #
        line = io.BytesIO()
        while not (self._eof and len(self._buffer) == 0):
            line_part = self._buffer.readline(self._line_terminator)
            line.write(line_part)
            self._current_pos += len(line_part)

            if line_part.endswith(self._line_terminator):
                break
            else:
                self._fill_buffer()

        return line.getvalue()

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError.

        We offer only seek support, and no truncate support."""
        return True

    def seek(self, offset, whence=constants.WHENCE_START):
        """Seek to the specified position.

        :param int offset: The offset in bytes.
        :param int whence: Where the offset is from.

        Returns the position after seeking."""
        # Convert relative offset to absolute, since self._raw_reader
        # doesn't know our current position.
        if whence == constants.WHENCE_CURRENT:
            whence = constants.WHENCE_START
            offset += self._current_pos

        self._current_pos = self._raw_reader.seek(offset, whence)

        self._buffer.empty()
        self._eof = self._current_pos == self._raw_reader._content_length
        return self._current_pos

    def tell(self):
        """Return the current position within the file."""
        return self._current_pos

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation

    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def terminate(self):
        """Do nothing."""
        pass

    def to_boto3(self, resource):
        """Create an **independent** `boto3.s3.Object` instance that points to
        the same S3 object as this instance.
        Changes to the returned object will not affect the current instance.
        """
        assert resource, 'resource must be a boto3.resource instance'
        obj = resource.Object(self._bucket, self._key)
        if self._version_id is not None:
            return obj.Version(self._version_id)
        else:
            return obj

    #
    # Internal methods.
    #
    def _read_from_buffer(self, size=-1):
        """Remove at most size bytes from our buffer and return them."""
        size = size if size >= 0 else len(self._buffer)
        part = self._buffer.read(size)
        self._current_pos += len(part)
        return part

    def _fill_buffer(self, size=-1):
        size = max(size, self._buffer._chunk_size)
        while len(self._buffer) < size and not self._eof:
            bytes_read = self._buffer.fill(self._raw_reader)
            if bytes_read == 0:
                logger.debug('%s: reached EOF while filling buffer', self)
                self._eof = True

    def __str__(self):
        return "smart_open.s3.Reader(%r, %r)" % (self._bucket, self._key)

    def __repr__(self):
        return (
            "smart_open.s3.Reader("
            "bucket=%r, "
            "key=%r, "
            "version_id=%r, "
            "buffer_size=%r, "
            "line_terminator=%r)"
        ) % (
            self._bucket,
            self._key,
            self._version_id,
            self._buffer_size,
            self._line_terminator,
        )


class MultipartWriter(io.BufferedIOBase):
    """Writes bytes to S3 using the multi part API.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(
        self,
        bucket,
        key,
        min_part_size=DEFAULT_MIN_PART_SIZE,
        client=None,
        client_kwargs=None,
        writebuffer=None,
    ):
        if min_part_size < MIN_MIN_PART_SIZE:
            logger.warning("S3 requires minimum part size >= 5MB; \
multipart upload may fail")
        self._min_part_size = min_part_size

        _initialize_boto3(self, client, client_kwargs, bucket, key)

        try:
            partial = functools.partial(
                self._client.create_multipart_upload,
                Bucket=bucket,
                Key=key,
            )
            self._upload_id = _retry_if_failed(partial)['UploadId']
        except botocore.client.ClientError as error:
            raise ValueError(
                'the bucket %r does not exist, or is forbidden for access (%r)' % (
                    bucket, error
                )
            ) from error

        if writebuffer is None:
            self._buf = io.BytesIO()
        else:
            self._buf = writebuffer

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
        if self._buf.tell():
            self._upload_next_part()

        if self._total_bytes and self._upload_id:
            partial = functools.partial(
                self._client.complete_multipart_upload,
                Bucket=self._bucket,
                Key=self._key,
                UploadId=self._upload_id,
                MultipartUpload={'Parts': self._parts},
            )
            _retry_if_failed(partial)
            logger.debug('%s: completed multipart upload', self)
        elif self._upload_id:
            #
            # AWS complains with "The XML you provided was not well-formed or
            # did not validate against our published schema" when the input is
            # completely empty => abort the upload, no file created.
            #
            # We work around this by creating an empty file explicitly.
            #
            assert self._upload_id, "no multipart upload in progress"
            self._client.abort_multipart_upload(
                Bucket=self._bucket,
                Key=self._key,
                UploadId=self._upload_id,
            )
            self._client.put_object(
                Bucket=self._bucket,
                Key=self._key,
                Body=b'',
            )
            logger.debug('%s: wrote 0 bytes to imitate multipart upload', self)
        self._upload_id = None

    @property
    def closed(self):
        return self._upload_id is None

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
        return self._total_bytes

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")

    def write(self, b):
        """Write the given buffer (bytes, bytearray, memoryview or any buffer
        interface implementation) to the S3 file.

        For more information about buffers, see https://docs.python.org/3/c-api/buffer.html

        There's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away."""

        length = self._buf.write(b)
        self._total_bytes += length

        if self._buf.tell() >= self._min_part_size:
            self._upload_next_part()

        return length

    def terminate(self):
        """Cancel the underlying multipart upload."""
        assert self._upload_id, "no multipart upload in progress"
        self._client.abort_multipart_upload(
            Bucket=self._bucket,
            Key=self._key,
            UploadId=self._upload_id,
        )
        self._upload_id = None

    def to_boto3(self, resource):
        """Create an **independent** `boto3.s3.Object` instance that points to
        the same S3 object as this instance.
        Changes to the returned object will not affect the current instance.
        """
        assert resource, 'resource must be a boto3.resource instance'
        return resource.Object(self._bucket, self._key)

    #
    # Internal methods.
    #
    def _upload_next_part(self):
        part_num = self._total_parts + 1
        logger.info(
            "%s: uploading part_num: %i, %i bytes (total %.3fGB)",
            self,
            part_num,
            self._buf.tell(),
            self._total_bytes / 1024.0 ** 3,
        )
        self._buf.seek(0)

        #
        # Network problems in the middle of an upload are particularly
        # troublesome.  We don't want to abort the entire upload just because
        # of a temporary connection problem, so this part needs to be
        # especially robust.
        #
        upload = _retry_if_failed(
            functools.partial(
                self._client.upload_part,
                Bucket=self._bucket,
                Key=self._key,
                UploadId=self._upload_id,
                PartNumber=part_num,
                Body=self._buf,
            )
        )

        self._parts.append({'ETag': upload['ETag'], 'PartNumber': part_num})
        logger.debug("%s: upload of part_num #%i finished", self, part_num)

        self._total_parts += 1

        self._buf.seek(0)
        self._buf.truncate(0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.terminate()
        else:
            self.close()

    def __str__(self):
        return "smart_open.s3.MultipartWriter(%r, %r)" % (self._bucket, self._key)

    def __repr__(self):
        return "smart_open.s3.MultipartWriter(bucket=%r, key=%r, min_part_size=%r)" % (
            self._bucket,
            self._key,
            self._min_part_size,
        )


class SinglepartWriter(io.BufferedIOBase):
    """Writes bytes to S3 using the single part API.

    Implements the io.BufferedIOBase interface of the standard library.

    This class buffers all of its input in memory until its `close` method is called. Only then will
    the data be written to S3 and the buffer is released."""

    def __init__(
        self,
        bucket,
        key,
        client=None,
        client_kwargs=None,
        writebuffer=None,
    ):
        _initialize_boto3(self, client, client_kwargs, bucket, key)

        try:
            self._client.head_bucket(Bucket=bucket)
        except botocore.client.ClientError as e:
            raise ValueError('the bucket %r does not exist, or is forbidden for access' % bucket) from e

        if writebuffer is None:
            self._buf = io.BytesIO()
        else:
            self._buf = writebuffer

        self._total_bytes = 0

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
        if self._buf is None:
            return

        self._buf.seek(0)

        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=self._key,
                Body=self._buf,
            )
        except botocore.client.ClientError as e:
            raise ValueError(
                'the bucket %r does not exist, or is forbidden for access' % self._bucket) from e

        logger.debug("%s: direct upload finished", self)
        self._buf = None

    @property
    def closed(self):
        return self._buf is None

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
        return self._total_bytes

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")

    def write(self, b):
        """Write the given buffer (bytes, bytearray, memoryview or any buffer
        interface implementation) into the buffer. Content of the buffer will be
        written to S3 on close as a single-part upload.

        For more information about buffers, see https://docs.python.org/3/c-api/buffer.html"""

        length = self._buf.write(b)
        self._total_bytes += length
        return length

    def terminate(self):
        """Nothing to cancel in single-part uploads."""
        return

    #
    # Internal methods.
    #
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.terminate()
        else:
            self.close()

    def __str__(self):
        return "smart_open.s3.SinglepartWriter(%r, %r)" % (self._object.bucket_name, self._object.key)

    def __repr__(self):
        return "smart_open.s3.SinglepartWriter(bucket=%r, key=%r)" % (self._bucket, self._key)


def _retry_if_failed(
        partial,
        attempts=_UPLOAD_ATTEMPTS,
        sleep_seconds=_SLEEP_SECONDS,
        exceptions=None):
    if exceptions is None:
        exceptions = (botocore.exceptions.EndpointConnectionError, )
    for attempt in range(attempts):
        try:
            return partial()
        except exceptions:
            logger.critical(
                'Unable to connect to the endpoint. Check your network connection. '
                'Sleeping and retrying %d more times '
                'before giving up.' % (attempts - attempt - 1)
            )
            time.sleep(sleep_seconds)
    else:
        logger.critical('Unable to connect to the endpoint. Giving up.')
        raise IOError('Unable to connect to the endpoint after %d attempts' % attempts)


def _accept_all(key):
    return True


def iter_bucket(
        bucket_name,
        prefix='',
        accept_key=None,
        key_limit=None,
        workers=16,
        retries=3,
        **session_kwargs):
    """
    Iterate and download all S3 objects under `s3://bucket_name/prefix`.

    Parameters
    ----------
    bucket_name: str
        The name of the bucket.
    prefix: str, optional
        Limits the iteration to keys starting with the prefix.
    accept_key: callable, optional
        This is a function that accepts a key name (unicode string) and
        returns True/False, signalling whether the given key should be downloaded.
        The default behavior is to accept all keys.
    key_limit: int, optional
        If specified, the iterator will stop after yielding this many results.
    workers: int, optional
        The number of subprocesses to use.
    retries: int, optional
        The number of time to retry a failed download.
    session_kwargs: dict, optional
        Keyword arguments to pass when creating a new session.
        For a list of available names and values, see:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session


    Yields
    ------
    str
        The full key name (does not include the bucket name).
    bytes
        The full contents of the key.

    Notes
    -----
    The keys are processed in parallel, using `workers` processes (default: 16),
    to speed up downloads greatly. If multiprocessing is not available, thus
    _MULTIPROCESSING is False, this parameter will be ignored.

    Examples
    --------

      >>> # get all JSON files under "mybucket/foo/"
      >>> for key, content in iter_bucket(
      ...         bucket_name, prefix='foo/',
      ...         accept_key=lambda key: key.endswith('.json')):
      ...     print key, len(content)

      >>> # limit to 10k files, using 32 parallel workers (default is 16)
      >>> for key, content in iter_bucket(bucket_name, key_limit=10000, workers=32):
      ...     print key, len(content)
    """
    if accept_key is None:
        accept_key = _accept_all

    #
    # If people insist on giving us bucket instances, silently extract the name
    # before moving on.  Works for boto3 as well as boto.
    #
    try:
        bucket_name = bucket_name.name
    except AttributeError:
        pass

    total_size, key_no = 0, -1
    key_iterator = _list_bucket(
        bucket_name,
        prefix=prefix,
        accept_key=accept_key,
        **session_kwargs)
    download_key = functools.partial(
        _download_key,
        bucket_name=bucket_name,
        retries=retries,
        **session_kwargs)

    with smart_open.concurrency.create_pool(processes=workers) as pool:
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


def _list_bucket(
        bucket_name,
        prefix='',
        accept_key=lambda k: True,
        **session_kwargs):
    session = boto3.session.Session(**session_kwargs)
    client = session.client('s3')
    ctoken = None

    while True:
        # list_objects_v2 doesn't like a None value for ContinuationToken
        # so we don't set it if we don't have one.
        if ctoken:
            kwargs = dict(Bucket=bucket_name, Prefix=prefix, ContinuationToken=ctoken)
        else:
            kwargs = dict(Bucket=bucket_name, Prefix=prefix)
        response = client.list_objects_v2(**kwargs)
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


def _download_key(key_name, bucket_name=None, retries=3, **session_kwargs):
    if bucket_name is None:
        raise ValueError('bucket_name may not be None')

    #
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html#multithreading-or-multiprocessing-with-resources
    #
    session = boto3.session.Session(**session_kwargs)
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)

    # Sometimes, https://github.com/boto/boto/issues/2409 can happen
    # because of network issues on either side.
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
