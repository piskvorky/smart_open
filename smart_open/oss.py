import io
import logging
import os
import re

import smart_open.bytebuffer
import smart_open.concurrency
import smart_open.utils
from smart_open import constants
from oss2.exceptions import OssError

try:
    import oss2
except ImportError:
    MISSING_DEPS = True

logger = logging.getLogger(__name__)

DEFAULT_MIN_PART_SIZE = 50 * 1024 ** 2  # 50MB

# for Oss multipart uploads minimum part size is 100KB, maximum part size is 5 GB
# There is no size limit for the last part
MIN_PART_SIZE = 100 * 1024
# maximum parts count is 10000
MAX_PART_SIZE = 10000

DEFAULT_BUFFER_SIZE = 128 * 1024  # 128KB

SCHEME = 'oss'

URI_EXAMPLES = (
    'oss://oss_bucket/object_key',
    'oss://oss_key:oss_secret@oss_bucket/object_key',
    'oss://oss_key:oss_secret@oss_ednpoint@oss_bucket/object_key',
)

DEFAULT_OSS_ENDPOINT = 'https://oss-cn-shanghai.aliyuncs.com'


def parse_uri(uri_as_string):
    #
    # The maximum number of buckets that can be created by
    # using an Alibaba Cloud account within a region is 100.
    # After a bucket is created, its name cannot be modified.
    # OSS supports the following bucket naming conventions:
    #
    # - The name of a bucket must be unique in OSS in an Alibaba Cloud account.
    # - The name can contain only lowercase letters, digits, and hyphens (-).
    # - The name must start and end with a lowercase letter or a digit.
    # - The name must be 3 to 63 characters in length.
    #
    # The name of an object must comply with the following conventions:
    #
    # - The name can contain only UTF-8 characters.
    # - The name must be 1 to 1,023 bytes in length.
    # - The name cannot start with a forward slash (/) or a backslash (\).
    #
    # We use the above as a guide only, and do not perform any validation.  We
    # let alicloud oss take care of that for us.
    split_uri = smart_open.utils.safe_urlsplit(uri_as_string)
    assert split_uri.scheme == SCHEME

    endpoint = DEFAULT_OSS_ENDPOINT
    ordinary_calling_format = False
    #
    # These defaults tell oss to look for credentials elsewhere
    #
    access_id, access_secret = None, None

    #
    # Common URI template [access_id:access_secret@][endpoint@]bucket/object
    #
    # The urlparse function doesn't handle the above schema, so we have to do
    # it ourselves.
    #
    uri = split_uri.netloc + split_uri.path

    if '@' in uri and ':' in uri.split('@')[0]:
        auth, uri = uri.split('@', 1)
        access_id, access_secret = auth.split(':')

    head, key_id = uri.split('/', 1)
    if '@' in head:
        ordinary_calling_format = True
        endpoint, bucket_id = head.split('@')
    else:
        bucket_id = head

    return dict(
        scheme=split_uri.scheme,
        bucket_id=bucket_id,
        key_id=key_id,
        endpoint=endpoint,
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
            init_kwargs = client_kwargs['oss.Client']
        except KeyError:
            init_kwargs = client_kwargs['oss.Client'] = {}

        init_kwargs.update(**kwargs)

    client = transport_params.get('client')
    if client is not None and (uri['access_id'] or uri['access_secret']):
        logger.warning(
            'ignoring credentials parsed from URL because they conflict with '
            'transport_params["client"]. Set transport_params["client"] to None '
            'to suppress this warning.'
        )
        uri.update(access_id=None, access_secret=None)
    elif uri['access_id'] and uri['access_secret']:
        inject(
            access_key_id=uri['access_id'],
            secret_access_key=uri['access_secret'],
        )
        uri.update(access_id=None, access_secret=None)

    if client is not None and uri['endpoint'] != DEFAULT_OSS_ENDPOINT:
        logger.warning(
            'ignoring endpoint_url parsed from URL because they conflict with '
            'transport_params["client"]. Set transport_params["client"] to None '
            'to suppress this warning.'
        )
        uri.update(endpoint=None)
    elif uri['endpoint'] != DEFAULT_OSS_ENDPOINT:
        inject(endpoint=uri['endpoint'])
        uri.update(endpoint=None)

    return uri, transport_params


def open_uri(uri, mode, transport_params):
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
        line_terminator=None
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
    line_terminator: str
        The line terminator to use to split the line, by default linux using '/n', windows using '/r/n'
    """
    logger.debug('%r', locals())
    if not client:
        default_access_key_id = os.getenv('OSS_ACCESS_KEY_ID', '')
        default_access_key_secret = os.getenv('OSS_ACCESS_KEY_SECRET', '')
        default_endpoint = os.getenv('OSS_ENDPOINT', '')

        client_kwargs = client_kwargs or {}
        oss_client_args = client_kwargs.get('oss.Client', {})
        access_key_id = oss_client_args.get('access_key_id', default_access_key_id)
        access_key_secret = oss_client_args.get('access_key_secret', default_access_key_secret)
        endpoint = oss_client_args.get('endpoint', default_endpoint)

        client = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_id)

    if mode not in constants.BINARY_MODES:
        raise NotImplementedError('bad mode: %r expected one of %r' % (mode, constants.BINARY_MODES))

    if (mode == constants.WRITE_BINARY) and (version_id is not None):
        raise ValueError("version_id must be None when writing")

    fileobj = None
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


class Reader(io.BufferedIOBase):
    """Reads bytes from ALICLOUD OSS.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(
            self,
            bucket_id,
            key_id,
            version_id=None,
            buffer_size=DEFAULT_BUFFER_SIZE,
            line_terminator=constants.BINARY_NEWLINE,
            defer_seek=False,
            client=None,
            client_kwargs=None,
    ):
        self._bucket = bucket_id
        self._key = key_id
        self._version_id = version_id
        self._buffer_size = buffer_size


        self._raw_reader = _RawReader(
            ali_bucket=client,
            bucket=bucket_id,
            key=key_id,
            version_id=None,
        )
        self._current_pos = 0
        buffer_size = max(DEFAULT_BUFFER_SIZE, buffer_size)
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

    def to_oss(self):
        pass

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
        return "smart_open.oss.Reader(%r, %r)" % (self._bucket, self._key)

    def __repr__(self):
        return (
                   "smart_open.oss.Reader("
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

