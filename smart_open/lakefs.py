import typing
import io
import re
import logging
import functools

try:
    from lakefs_client import client, apis, models
except ImportError:
    MISSING_DEPS = True

from smart_open import bytebuffer, constants
import smart_open.utils

SCHEME = "lakefs"

URI_EXAMPLES = (
    "lakefs://REPO/REF/file",
    "lakefs:///REPO/main/file.bz2",
)

"""Default buffer size is 256MB."""
DEFAULT_BUFFER_SIZE = 4 * 1024**2

logger = logging.getLogger(__name__)


def parse_uri(uri_as_string):
    """lakefs protocol URIs.

    lakeFS uses a specific format for path URIs. The URI lakefs://<REPO>/<REF>/<KEY>
    is a path to objects in the given repo and ref expression under key. This is used
    both for path prefixes and for full paths. In similar fashion, lakefs://<REPO>/<REF>
    identifies the repository at a ref expression, and lakefs://<REPO> identifes a repo.
    """
    sr = smart_open.utils.safe_urlsplit(uri_as_string)
    assert sr.scheme == SCHEME
    repo = sr.netloc
    _pattern = r"^/(?P<ref>[^/]+)/(?P<key>.+)"
    _match = re.fullmatch(_pattern, sr.path)
    if _match:
        ref = _match.group("ref")
        key = _match.group("key")
    else:
        ref = None
        key = None
    return dict(scheme=SCHEME, repo=repo, ref=ref, key=key)


def open_uri(uri: str, mode: str, transport_params: dict) -> typing.IO:
    """Return a file-like object pointing to the URI.

    :param str uri: The URI to open
    :param str mode: Either "rb" or "wb".
    :param dict transport_params:  Any additional parameters to pass to the `open` function (see below).

    :returns: file-like object.
    :rtype: file-like
    """
    parsed_uri = parse_uri(uri)
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    return open(
        parsed_uri["repo"], parsed_uri["ref"], parsed_uri["key"], mode, **kwargs
    )


def open(
    repo,
    ref,
    key,
    mode,
    client=None,
    buffer_size=DEFAULT_BUFFER_SIZE,
    client_kwargs=None,
    writebuffer=None,
):
    pass


class _RawReader(io.RawIOBase):
    """Read a lakeFS object."""

    def __init__(
        self,
        client: client.LakeFSClient,
        repo: str,
        ref: str,
        path: str,
    ):
        self._client = client
        self._repo = repo
        self._ref = ref
        self._path = path
        self._position = 0

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    @functools.cached_property
    def content_length(self) -> int:
        objects: apis.ObjectsApi = self._client.objects
        obj_stats: models.ObjectStats = objects.stat_object(
            self._repo, self._ref, self._path
        )
        return obj_stats.size_bytes

    @property
    def eof(self) -> bool:
        return self._position == self.content_length

    def seek(self, offset: int, whence: int = constants.WHENCE_START) -> int:
        """Seek to the specified position.

        :param int offset: The byte offset.
        :param int whence: Where the offset is from.

        :returns: The position after seeking.
        :rtype: int
        """
        if whence not in constants.WHENCE_CHOICES:
            raise ValueError(
                "invalid whence, expected one of %r" % constants.WHENCE_CHOICES
            )

        if whence == constants.WHENCE_START:
            start = max(0, offset)
        elif whence == constants.WHENCE_CURRENT:
            start = max(0, self._position + offset)
        elif whence == constants.WHENCE_END:
            start = max(0, self.content_length + offset)

        self._position = min(start, self.content_length)

        return self._position

    def readinto(self, __buffer: bytes) -> int | None:
        """Read bytes into a pre-allocated bytes-like object __buffer.

        :param int size: number of bytes to read.

        :returns: the number of bytes read from lakefs
        :rtype: int
        """
        if self._position >= self.content_length:
            return 0
        size = len(__buffer)
        start_range = self._position
        end_range = max(self.content_length, (start_range + size))
        range = f"bytes={start_range}-{end_range}"
        objects: apis.ObjectsApi = self._client.objects
        data = objects.get_object(
            self._repo, self._ref, self._path, range=range
        ).read()
        if not data:
            return 0
        self._position += len(data)
        __buffer[: len(data)] = data
        return len(data)


class Reader(io.BufferedIOBase):
    """Reads bytes from a lakefs object.

    Implements the io.BufferedIOBase interface of the standard library.
    """
    def __init__(
        self,
        client: client.LakeFSClient,
        repo: str,
        ref: str,
        path: str,
        buffer_size=DEFAULT_BUFFER_SIZE,
        line_terminator=smart_open.constants.BINARY_NEWLINE,
    ):
        self._repo = repo
        self._ref = ref
        self._path = path
        self.raw = _RawReader(client, repo, ref, path)
        self._position = 0
        self._buffer = bytebuffer.ByteBuffer(buffer_size)
        self._line_terminator = line_terminator

    @property
    def bytes_buffered(self) -> int:
        return len(self._buffer)

    def close(self) -> None:
        """Flush and close this stream."""
        self._buffer.empty()

    def readable(self) -> bool:
        """Return True if the stream can be read from."""
        return True

    def read(self, size: int = -1) -> bytes:
        """Read and return up to size bytes.

        :param int size:

        :returns: read bytes
        :rtype: bytes
        """
        if size == 0:
            return b""
        elif size < 0:
            out = self._read_from_buffer() + self.raw.read()
            self._position = self.raw.content_length
            return out
        elif size <= self.bytes_buffered:
            # Fast path: the data to read is fully buffered.
            return self._read_from_buffer(size)
        if not self.raw.eof:
            self._fill_buffer(size)
        return self._read_from_buffer(size)

    def read1(self, size: int = -1):
        """Read and return up to size bytes.

        with at most one call to the underlying raw stream readinto().
        This can be useful if you are implementing your own buffering
        on top of a BufferedIOBase object.
        """
        if size == 0:
            return b""
        elif size < 0:
            out = self._read_from_buffer() + self.raw.read()
            self._position = self.raw.content_length
            return out
        elif size <= self.bytes_buffered:
            # Fast path: the data to read is fully buffered.
            return self._read_from_buffer(size)
        else:
            out = self._read_from_buffer()
            out += self.raw.read(size-len(out))
            self._position += len(out)
            return out

    def readline(self, limit=-1) -> bytes:
        """Read up to and including the next newline.

        :param int limit:

        :returns: bytes read
        :rtype: bytes
        """
        if limit != -1:
            raise NotImplementedError("limits other than -1 not implemented yet")

        line = io.BytesIO()
        while not (self.raw.eof and self.bytes_buffered == 0):
            # while we are not in eof or buffer is not empty
            line_part = self._buffer.readline(self._line_terminator)
            line.write(line_part)
            self._position += len(line_part)
            if line_part.endswith(self._line_terminator):
                break
            else:
                self._fill_buffer()
        return line.getvalue()

    def seekable(self):
        """If the stream supports random access or not."""
        return True

    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation

    def seek(self, offset: int, whence: int = smart_open.constants.WHENCE_START):
        """Seek to the specified position.

        :param int offset: The offset in bytes.
        :param int whence: Where the offset is from.

        :returns: the position after seeking.
        :r
        """
        logger.debug('seeking to offset: %r whence: %r', offset, whence)
        if whence not in smart_open.constants.WHENCE_CHOICES:
            raise ValueError('invalid whence %i, expected one of %r' % (whence,
                                                                       smart_open.constants.WHENCE_CHOICES))

        # Convert relative offset to absolute, since self.raw
        # doesn't know our current position.
        if whence == constants.WHENCE_CURRENT:
            whence = constants.WHENCE_START
            offset += self._position

        self._position = self.raw.seek(offset, whence)
        self._buffer.empty()
        logger.debug('current_pos: %r', self._position)
        return self._position

    def tell(self):
        """Return the current stream position."""
        return self._position

    def _read_from_buffer(self, size: int = -1) -> bytes:
        """Reads from buffer and updates position."""
        part = self._buffer.read(size)
        self._position += len(part)
        return part

    def _fill_buffer(self, size: int = -1) -> None:
        """Fills the buffer with either the default buffer size or size."""
        size = max(size, self._buffer._chunk_size)
        while self.bytes_buffered < size and not self.raw.eof:
            bytes_read = self._buffer.fill(self.raw)
            if bytes_read == 0:
                logger.debug("%s: reached EOF while filling buffer", self)

    def __str__(self):
        return "smart_open.lakefs.Reader(%r, %r, %r)" % (
            self._repo,
            self._ref,
            self._path,
        )

    def __repr__(self):
        return (
            "smart_open.lakefs.Reader("
            "repo=%r, "
            "ref=%r, "
            "path=%r, "
            "buffer_size=%r"
        ) % (self._repo, self._ref, self._path, self._buffer._chunk_size)
