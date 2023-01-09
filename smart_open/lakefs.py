import io
import re
import logging
import urllib.parse

try:
    from lakefs_client import client
    from lakefs_client import apis
    from lakefs_client import models
except ImportError:
    MISSING_DEPS = True

from smart_open import bytebuffer, constants
import smart_open.utils

SCHEME = "lakefs"

URI_EXAMPLES = (
    "lakefs://REPO/REF/file",
    "lakefs:///REPO/main/file.bz2",
)

DEFAULT_BUFFER_SIZE = 4 * 1024**2
"""Default buffer size is 256MB."""

DEFAULT_MAX_CONCURRENCY = 1
"""Default number of parallel connections with which to download."""


logger = logging.getLogger(__name__)


def parse_uri(uri_as_string):
    """lakefs protocol URIs.

    lakeFS uses a specific format for path URIs. The URI lakefs://<REPO>/<REF>/<KEY>
    is a path to objects in the given repo and ref expression under key. This is used
    both for path prefixes and for full paths. In similar fashion, lakefs://<REPO>/<REF>
    identifies the repository at a ref expression, and lakefs://<REPO> identifes a repo.
    """
    sr = urllib.parse.urlsplit(uri_as_string, allow_fragments=False)
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


def open_uri(uri, mode, transport_params):
    parsed_uri = parse_uri(uri)
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    return open(
        parsed_uri["repo"], parsed_uri["ref"], parsed_uri["key"], mode, **kwargs
    )


def open(
    repo,
    ref,
    key,
    client=None,
    buffer_size=DEFAULT_BUFFER_SIZE,
    max_concurrency=DEFAULT_MAX_CONCURRENCY,
    client_kwargs=None,
    writebuffer=None,
):
    pass


class _RawReader(object):
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

        self._content_length = self._get_content_length()
        self._position = 0

    def _get_content_length(self):
        objects: apis.ObjectsApi = self._client.objects
        obj_stats: models.ObjectStats = objects.stat_object(
            self._repo, self._ref, self._path
        )
        return obj_stats.size_bytes

    def seek(self, offset, whence=constants.WHENCE_START):
        """Seek to the specified position.

        :param int offset: The offset in bytes.
        :param int whence: Where the offset is from.

        :returns: the position after seeking.
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
            start = max(0, self._content_length + offset)

        self._position = min(start, self._content_length)

        return self._position

    def read(self, size=-1):
        if self._position >= self._content_length:
            return b""
        _size = max(-1, size)
        objects: apis.ObjectsApi = self._client.objects
        start_range = self._position
        end_range = self._content_length if _size == -1 else (start_range + _size)
        range = f"bytes={start_range}-{end_range}"
        binary = objects.get_object(
            self._repo, self._ref, self._path, range=range
        ).read()
        self._position += len(binary)
        return binary


class Reader(io.BufferedIOBase):
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
        self._raw_reader = _RawReader(client, repo, ref, path)
        self._position = 0
        self._eof = False
        self._buffer_size = buffer_size
        self._buffer = bytebuffer.ByteBuffer(buffer_size)
        self._line_terminator = line_terminator
        self.raw = None

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
            return b""
        elif size < 0:
            out = self._read_from_buffer() + self._raw_reader.read()
            self._position = self._raw_reader._content_length
            return out

        if len(self._buffer) >= size:
            return self._read_from_buffer(size)

        if self._eof:
            return self._read_from_buffer()

        self._fill_buffer(size)
        return self._read_from_buffer(size)

    def read1(self, size=-1):
        """This is the same as read()."""
        return self.read(size=size)

    def readinto(self, b):
        """Read up to len(b) bytes into b, and return the number of bytes read."""
        data = self.read(len(b))
        if not data:
            return 0
        b[: len(data)] = data
        return len(data)

    def readline(self, limit=-1):
        """Read up to and including the next newline. Returns the bytes read."""
        if limit != -1:
            raise NotImplementedError("limits other than -1 not implemented yet")

        line = io.BytesIO()
        while not (self._eof and len(self._buffer) == 0):
            line_part = self._buffer.readline(self._line_terminator)
            line.write(line_part)
            self._position += len(line_part)

            if line_part.endswith(self._line_terminator):
                break
            else:
                self._fill_buffer()

        return line.getvalue()

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError.

        We offer only seek support, and no truncate support."""
        return True

    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def tell(self):
        """Return the current position within the file."""
        return self._position

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation

    #
    # Internal methods.
    #
    def _read_from_buffer(self, size=-1):
        size = size if size >= 0 else len(self._buffer)
        part = self._buffer.read(size)
        self._position += len(part)
        return part

    def _fill_buffer(self, size=-1):
        size = max(size, self._buffer._chunk_size)
        while len(self._buffer) < size and not self._eof:
            bytes_read = self._buffer.fill(self._raw_reader)
            if bytes_read == 0:
                logger.debug("%s: reached EOF while filling buffer", self)
                self._eof = True

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
        ) % (self._repo, self._ref, self._path, self._buffer_size)
