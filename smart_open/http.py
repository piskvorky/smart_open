#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements file-like objects for reading from http."""

from __future__ import annotations

import io
import logging
import posixpath
import urllib.parse
from typing import TYPE_CHECKING, TypedDict

try:
    import requests
except ImportError:
    MISSING_DEPS = True

import smart_open.utils
from smart_open import bytebuffer, constants

if TYPE_CHECKING:
    from _typeshed import WriteableBuffer

    from smart_open._typing import TransportParams

DEFAULT_BUFFER_SIZE = 128 * 1024
SCHEMES = ("http", "https")

logger = logging.getLogger(__name__)


class _HTTPUri(TypedDict):
    scheme: str
    uri_path: str


_HEADERS = {"Accept-Encoding": "identity"}
"""The headers we send to the server with every HTTP request.

For now, we ask the server to send us the files as they are.
Sometimes, servers compress the file for more efficient transfer, in which case
the client (us) has to decompress them with the appropriate algorithm.
"""


def parse_uri(uri_as_string: str) -> _HTTPUri:
    """Parse an ``http://`` or ``https://`` URI into its path component."""
    split_uri = urllib.parse.urlsplit(uri_as_string)
    assert split_uri.scheme in SCHEMES  # noqa: S101  # internal precondition; misuse should crash loudly

    uri_path = split_uri.netloc + split_uri.path
    uri_path = "/" + uri_path.lstrip("/")
    return {"scheme": split_uri.scheme, "uri_path": uri_path}


def open_uri(uri: str, mode: str, transport_params: TransportParams) -> BufferedInputBase:
    """Open an HTTP/HTTPS URI using the given mode and transport params."""
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    return open(uri, mode, **kwargs)


def open(  # noqa: PLR0913  # legacy public API; refactor in a dedicated PR
    uri: str,
    mode: str,
    kerberos: bool = False,  # noqa: FBT001, FBT002  # public API
    user: str | None = None,
    password: str | None = None,
    cert: str | tuple[str, str] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float | None = None,
    session: requests.Session | None = None,
    buffer_size: int = DEFAULT_BUFFER_SIZE,
) -> BufferedInputBase:
    """Implement streamed reader from a web site.

    Supports Kerberos and Basic HTTP authentication.

    Args:
        uri: The URL to open.
        mode: The mode to open using.
        kerberos: If True, will attempt to use the local Kerberos credentials.
        user: The username for authenticating over HTTP.
        password: The password for authenticating over HTTP.
        cert: If a string, path to ssl client cert file (``.pem``).
            If a tuple, ``('cert', 'key')``.
        headers: Any headers to send in the request. If ``None``, the default headers
            are sent: ``{'Accept-Encoding': 'identity'}``. To use no headers at all,
            set this variable to an empty dict, ``{}``.
        timeout: Request timeout in seconds.
        session: The ``requests.Session`` object to use with HTTP GET requests.
            Can be used for OAuth2 clients.
        buffer_size: The buffer size to use when performing I/O.

    Returns:
        A file-like object opened for reading.

    Raises:
        NotImplementedError: If ``mode`` is anything other than ``"rb"``.

    Note:
        If neither ``kerberos`` nor ``(user, password)`` are set, will connect
        unauthenticated, unless set separately in headers.
    """
    if mode == constants.READ_BINARY:
        fobj = SeekableBufferedInputBase(
            uri,
            mode,
            buffer_size=buffer_size,
            kerberos=kerberos,
            user=user,
            password=password,
            cert=cert,
            headers=headers,
            session=session,
            timeout=timeout,
        )
        fobj.name = posixpath.basename(urllib.parse.urlparse(uri).path)
        return fobj
    msg = f"http support for mode {mode!r} not implemented"
    raise NotImplementedError(msg)


class BufferedInputBase(io.BufferedIOBase):
    """Buffered HTTP reader implementing the `io.BufferedIOBase` interface."""

    name: str
    response: requests.Response | None = None  # so `closed` works if __init__ fails and __del__ runs

    def __init__(  # noqa: PLR0913  # legacy public API; refactor in a dedicated PR
        self,
        url: str,
        mode: str = "r",
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        kerberos: bool = False,  # noqa: FBT001, FBT002  # public API
        user: str | None = None,
        password: str | None = None,
        cert: str | tuple[str, str] | None = None,
        headers: dict[str, str] | None = None,
        session: requests.Session | None = None,
        timeout: float | None = None,
    ) -> None:

        self.url = url
        self.cert = cert
        self.session = session or requests

        if kerberos:
            import requests_kerberos  # ty: ignore[unresolved-import]  # optional, install separately for kerberos=True

            self.auth = requests_kerberos.HTTPKerberosAuth()
        elif user is not None and password is not None:
            self.auth = (user, password)
        else:
            self.auth = None

        self.buffer_size = buffer_size
        self.mode = mode

        if headers is None:
            self.headers = _HEADERS.copy()
        else:
            self.headers = headers

        self.timeout = timeout

        self.response = self.session.get(
            self.url,
            auth=self.auth,
            cert=self.cert,
            stream=True,
            headers=self.headers,
            timeout=self.timeout,
        )

        if not self.response.ok:
            self.response.raise_for_status()

        self._read_buffer: bytebuffer.ByteBuffer | None = bytebuffer.ByteBuffer(buffer_size)
        self._current_pos = 0

    #
    # Override some methods from io.IOBase.
    #
    def close(self) -> None:
        """Flush and close this stream."""
        logger.debug("close: called")
        if not self.closed:
            self.response = None
            self._read_buffer = None

    @property
    def closed(self) -> bool:
        """Return True if the stream is closed."""
        return self.response is None

    def readable(self) -> bool:
        """Return True if the stream can be read from."""
        return True

    def seekable(self) -> bool:
        """Return False; the base HTTP reader does not support seeking."""
        return False

    #
    # io.BufferedIOBase methods.
    #
    def detach(self) -> io.RawIOBase:
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size: int | None = -1) -> bytes:
        """Mimic the read call to a filehandle object."""
        if size is None:
            size = -1
        if size < -1:
            msg = f"size must be >= -1, got {size}"
            raise ValueError(msg)

        logger.debug("reading with size: %d", size)
        buf, response = self._read_buffer, self.response
        if buf is None or response is None or size == 0:
            return b""

        if size == -1:
            if len(buf):  # noqa: SIM108  # avoid the unnecessary + when the buffer is empty
                retval = buf.read() + response.raw.read()
            else:
                retval = response.raw.read()
        else:
            # Fill _read_buffer until it contains enough bytes
            while len(buf) < size:
                if buf.fill(response.raw) == 0:
                    break  # EOF reached
            retval = buf.read(size)

        self._current_pos += len(retval)
        return retval

    def read1(self, size: int | None = -1) -> bytes:
        """This is the same as read()."""
        return self.read(size=size)

    def readinto(self, b: WriteableBuffer) -> int:
        """Read up to ``len(b)`` bytes into ``b``, and return the number of bytes read."""
        mv = memoryview(b).cast("B")
        data = self.read(len(mv))
        if not data:
            return 0
        mv[: len(data)] = data
        return len(data)


class SeekableBufferedInputBase(BufferedInputBase):
    """Seekable streamed reader from a web site.

    Supports Kerberos, client certificate and Basic HTTP authentication.
    If ``kerberos`` is True, will attempt to use the local Kerberos credentials.
    If ``cert`` is set, will try to use a client certificate. Otherwise, will try
    to use "basic" HTTP authentication via username/password. If none of those are
    set, will connect unauthenticated.
    """

    def __init__(  # noqa: PLR0913  # legacy public API; refactor in a dedicated PR
        self,
        url: str,
        mode: str = "r",
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        kerberos: bool = False,  # noqa: FBT001, FBT002  # public API
        user: str | None = None,
        password: str | None = None,
        cert: str | tuple[str, str] | None = None,
        headers: dict[str, str] | None = None,
        session: requests.Session | None = None,
        timeout: float | None = None,
    ) -> None:
        super().__init__(url, mode, buffer_size, kerberos, user, password, cert, headers, session, timeout)
        assert self.response is not None  # noqa: S101  # set by super().__init__
        self.content_length = int(self.response.headers.get("Content-Length", -1))
        #
        # We assume the HTTP stream is seekable unless the server explicitly
        # tells us it isn't.  It's better to err on the side of "seekable"
        # because we don't want to prevent users from seeking a stream that
        # does not appear to be seekable but really is.
        #
        self._seekable = self.response.headers.get("Accept-Ranges", "").lower() != "none"

    def seek(self, offset: int, whence: int = 0) -> int:  # noqa: C901, PLR0912  # legacy public API; refactor in a dedicated PR
        """Seek to the specified position.

        Args:
            offset: The offset in bytes.
            whence: Where the offset is from.

        Returns:
            The position after seeking.

        Raises:
            ValueError: If ``whence`` is not one of ``WHENCE_CHOICES``.
            OSError: If the stream is not seekable.
        """
        logger.debug("seeking to offset: %r whence: %r", offset, whence)
        if whence not in constants.WHENCE_CHOICES:
            msg = f"invalid whence, expected one of {constants.WHENCE_CHOICES!r}"
            raise ValueError(msg)

        if not self.seekable():
            msg = "stream is not seekable"
            raise OSError(msg)

        buf = self._read_buffer
        if buf is None:
            msg = "seek on closed stream"
            raise OSError(msg)

        if whence == constants.WHENCE_START:
            new_pos = offset
        elif whence == constants.WHENCE_CURRENT:
            new_pos = self._current_pos + offset
        else:  # constants.WHENCE_END
            new_pos = self.content_length + offset

        if self.content_length == -1:
            new_pos = smart_open.utils.clamp(new_pos, maxval=None)
        else:
            new_pos = smart_open.utils.clamp(new_pos, maxval=self.content_length)

        if self._current_pos == new_pos:
            return self._current_pos

        # Check if we can satisfy the seek from buffer (forward seek within buffered data)
        if new_pos > self._current_pos and new_pos - self._current_pos <= len(buf):
            buf.read(new_pos - self._current_pos)
            self._current_pos = new_pos
            return self._current_pos

        logger.debug("http seeking from current_pos: %d to new_pos: %d", self._current_pos, new_pos)

        self._current_pos = new_pos

        if new_pos == self.content_length:
            self.response = None
            buf.empty()
        else:
            response = self._partial_request(new_pos)
            if response.ok:
                self.response = response
                buf.empty()
            else:
                self.response = None

        return self._current_pos

    def tell(self) -> int:
        """Return the current stream position."""
        return self._current_pos

    def seekable(self, *args: object, **kwargs: object) -> bool:
        """Return True if the server reports it accepts byte-range requests."""
        return self._seekable

    def truncate(self, size: int | None = None) -> int:
        """Unsupported."""
        raise io.UnsupportedOperation

    def _partial_request(self, start_pos: int | None = None) -> requests.Response:
        headers = self.headers.copy()
        if start_pos is not None:
            headers["range"] = smart_open.utils.make_range_string(start_pos)

        return self.session.get(
            self.url,
            auth=self.auth,
            stream=True,
            cert=self.cert,
            headers=headers,
            timeout=self.timeout,
        )
