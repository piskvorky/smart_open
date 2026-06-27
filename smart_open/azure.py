#
# Copyright (C) 2020 Radim Rehurek <radim@rare-technologies.com>
# Copyright (C) 2020 Nicolas Mitchell <ncls.mitchell@gmail.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements file-like objects for reading and writing to/from Azure Blob Storage."""

from __future__ import annotations

import base64
import io
import logging
from typing import TYPE_CHECKING, Any, TypedDict, cast

import smart_open.bytebuffer
import smart_open.constants
import smart_open.utils

try:
    import azure.core.exceptions
    import azure.storage.blob
except ImportError:
    MISSING_DEPS = True

if TYPE_CHECKING:
    from types import TracebackType
    from typing import IO

    from _typeshed import ReadableBuffer, WriteableBuffer
    from typing_extensions import Self

    from smart_open._typing import TransportParams

    _AzureClient = (
        azure.storage.blob.BlobServiceClient
        | azure.storage.blob.ContainerClient
        | azure.storage.blob.BlobClient
    )

logger = logging.getLogger(__name__)

_BINARY_TYPES = (bytes, bytearray, memoryview)
"""Allowed binary buffer types for writing to the underlying Azure Blob Storage stream"""

SCHEME = "azure"
"""Supported scheme for Azure Blob Storage in smart_open endpoint URL"""

_DEFAULT_MIN_PART_SIZE = 64 * 1024**2
"""Default minimum part size for Azure Cloud Storage multipart uploads is 64MB"""

DEFAULT_BUFFER_SIZE = 4 * 1024**2
"""Default buffer size for working with Azure Blob Storage is 256MB
https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
"""

DEFAULT_MAX_CONCURRENCY = 1
"""Default number of parallel connections with which to download."""


class _AzureUri(TypedDict):
    scheme: str
    container_id: str
    blob_id: str


def parse_uri(uri_as_string: str) -> _AzureUri:
    """Parse an ``azure://`` URI into its container and blob components."""
    sr = smart_open.utils.safe_urlsplit(uri_as_string)
    assert sr.scheme == SCHEME  # noqa: S101  # internal precondition; misuse should crash loudly
    first = sr.netloc
    second = sr.path.lstrip("/")

    # https://docs.microsoft.com/en-us/rest/api/storageservices/working-with-the-root-container
    if not second:
        container_id = "$root"
        blob_id = first
    else:
        container_id = first
        blob_id = second

    return {"scheme": SCHEME, "container_id": container_id, "blob_id": blob_id}


def open_uri(uri: str, mode: str, transport_params: TransportParams) -> io.BufferedIOBase:
    """Open an Azure Blob Storage URI using the given mode and transport params."""
    parsed_uri = parse_uri(uri)
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    return open(parsed_uri["container_id"], parsed_uri["blob_id"], mode, **kwargs)


def open(  # noqa: PLR0913  # legacy public API; refactor in a dedicated PR
    container_id: str,
    blob_id: str,
    mode: str,
    client: _AzureClient | None = None,
    blob_kwargs: dict[str, Any] | None = None,
    buffer_size: int = DEFAULT_BUFFER_SIZE,
    min_part_size: int = _DEFAULT_MIN_PART_SIZE,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
) -> io.BufferedIOBase:
    """Open an Azure Blob Storage blob for reading or writing.

    Args:
        container_id: The name of the container this object resides in.
        blob_id: The name of the blob within the bucket.
        mode: The mode for opening the object.  Must be either "rb", "wb", or "ab".
        client: The Azure Blob Storage client to use when working with
            azure-storage-blob. May be a BlobServiceClient, ContainerClient, or
            BlobClient.
        blob_kwargs: Additional parameters to pass to
            ``BlobClient.commit_block_list`` (for "wb") or
            ``BlobClient.upload_blob`` (for "ab"). For writing only.
        buffer_size: The buffer size to use when performing I/O. For reading only.
        min_part_size: The minimum part size for multipart uploads. For writing
            only.
        max_concurrency: The number of parallel connections with which to
            download. For reading only.

    Returns:
        A file-like object for reading from or writing to the blob.

    Raises:
        ValueError: If no client is provided.
        NotImplementedError: If the requested mode is not supported.
    """
    if not client:
        msg = "you must specify the client to connect to Azure"
        raise ValueError(msg)

    if mode == smart_open.constants.READ_BINARY:
        return Reader(
            container_id,
            blob_id,
            client,
            buffer_size=buffer_size,
            line_terminator=smart_open.constants.BINARY_NEWLINE,
            max_concurrency=max_concurrency,
        )
    if mode == smart_open.constants.WRITE_BINARY:
        return Writer(container_id, blob_id, client, blob_kwargs=blob_kwargs, min_part_size=min_part_size)
    if mode == smart_open.constants.APPEND_BINARY:
        return AppendWriter(
            container_id, blob_id, client, blob_kwargs=blob_kwargs, min_part_size=min_part_size
        )
    msg = f"Azure Blob Storage support for mode {mode!r} not implemented"
    raise NotImplementedError(msg)


def _get_blob_client(
    client: _AzureClient,
    container: str,
    blob: str,
) -> azure.storage.blob.BlobClient:
    """Return an Azure BlobClient for the given container and blob."""
    obj: Any = client
    if hasattr(obj, "get_container_client"):
        obj = obj.get_container_client(container)

    if hasattr(obj, "container_name") and obj.container_name != container:
        msg = f"Client for {obj.container_name!r} doesn't match container {container!r}"
        raise ValueError(msg)

    if hasattr(obj, "get_blob_client"):
        obj = obj.get_blob_client(blob)

    return cast("azure.storage.blob.BlobClient", obj)


class _RawReader:
    """Read an Azure Blob Storage file."""

    def __init__(self, blob: azure.storage.blob.BlobClient, size: int, concurrency: int) -> None:
        self._blob = blob
        self._size = size
        self._position = 0
        self._concurrency = concurrency

    def seek(self, position: int) -> int:
        """Seek to the specified position (byte offset) in the Azure Blob Storage blob.

        Args:
            position: The byte offset from the beginning of the blob.

        Returns:
            The position after seeking.
        """
        self._position = position
        return self._position

    def read(self, size: int = -1) -> bytes:
        if self._position >= self._size:
            return b""
        binary = self._download_blob_chunk(size)
        self._position += len(binary)
        return binary

    def _download_blob_chunk(self, size: int) -> bytes:
        if self._size == self._position:
            #
            # When reading, we can't seek to the first byte of an empty file.
            # Similarly, we can't seek past the last byte.  Do nothing here.
            #
            return b""
        if size == -1:
            stream = self._blob.download_blob(offset=self._position, max_concurrency=self._concurrency)
        else:
            stream = self._blob.download_blob(
                offset=self._position, max_concurrency=self._concurrency, length=size
            )
        logger.debug("reading with a max concurrency of %d", self._concurrency)
        if isinstance(stream, azure.storage.blob.StorageStreamDownloader):
            binary = stream.readall()
        else:
            binary = stream.read()
        return cast("bytes", binary)


class Reader(io.BufferedIOBase):
    """Reads bytes from Azure Blob Storage.

    Implements the io.BufferedIOBase interface of the standard library.

    Args:
        container: The name of the container the blob resides in.
        blob: The name of the blob within the container.
        client: The Azure Blob Storage client. May be a BlobServiceClient,
            ContainerClient, or BlobClient.
        buffer_size: The buffer size to use when performing I/O.
        line_terminator: The line terminator to use when reading lines.
        max_concurrency: The number of parallel connections with which to
            download.

    Raises:
        azure.core.exceptions.ResourceNotFoundError: Raised when the blob to read
            from does not exist.
    """

    name: str
    _blob: azure.storage.blob.BlobClient | None = None  # so `closed` works if __init__ fails and __del__ runs

    def __init__(  # noqa: PLR0913  # legacy public API; refactor in a dedicated PR
        self,
        container: str,
        blob: str,
        client: _AzureClient,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        line_terminator: bytes = smart_open.constants.BINARY_NEWLINE,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    ) -> None:
        self._container_name = container
        self._blob_name = blob

        self._blob = _get_blob_client(client, container, blob)

        if self._blob is None:
            msg = f"blob {blob} not found in {container}"
            raise azure.core.exceptions.ResourceNotFoundError(msg)
        try:
            self._size = self._blob.get_blob_properties()["size"]
        except KeyError:
            self._size = 0

        self._raw_reader: _RawReader | None = _RawReader(self._blob, self._size, max_concurrency)
        self._position = 0
        self._current_part = smart_open.bytebuffer.ByteBuffer(buffer_size)
        self._line_terminator = line_terminator

    #
    # Override some methods from io.IOBase.
    #
    def close(self) -> None:
        """Flush and close this stream."""
        logger.debug("close: called")
        if not self.closed:
            self._blob = None
            self._raw_reader = None

    @property
    def closed(self) -> bool:
        """Return True if the stream is closed."""
        return self._blob is None

    def readable(self) -> bool:
        """Return True if the stream can be read from."""
        return True

    def seekable(self) -> bool:
        """Return True; we support `seek` but not `truncate`."""
        return True

    #
    # io.BufferedIOBase methods.
    #
    def detach(self) -> io.RawIOBase:
        """Unsupported."""
        raise io.UnsupportedOperation

    def seek(self, offset: int, whence: int = smart_open.constants.WHENCE_START) -> int:
        """Seek to the specified position.

        Args:
            offset: The offset in bytes.
            whence: Where the offset is from.

        Returns:
            The position after seeking.

        Raises:
            ValueError: If ``whence`` is not one of the accepted values.
        """
        logger.debug("seeking to offset: %r whence: %r", offset, whence)
        if whence not in smart_open.constants.WHENCE_CHOICES:
            msg = f"invalid whence {whence}, expected one of {smart_open.constants.WHENCE_CHOICES!r}"
            raise ValueError(msg)

        if whence == smart_open.constants.WHENCE_START:
            new_position = offset
        elif whence == smart_open.constants.WHENCE_CURRENT:
            new_position = self._position + offset
        else:
            new_position = self._size + offset

        # Check if we can satisfy the seek from buffer (forward seek within buffered data)
        if new_position > self._position and new_position - self._position <= len(self._current_part):
            self._current_part.read(new_position - self._position)
            self._position = new_position
            return self._position

        raw_reader = self._raw_reader
        assert raw_reader is not None  # noqa: S101  # set in __init__, cleared only on close

        self._position = new_position
        raw_reader.seek(new_position)
        logger.debug("current_pos: %r", self._position)

        self._current_part.empty()
        return self._position

    def tell(self) -> int:
        """Return the current position within the file."""
        return self._position

    def truncate(self, size: int | None = None) -> int:
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size: int | None = -1) -> bytes:
        """Read up to size bytes from the object and return them."""
        if size is None:
            size = -1
        raw_reader = self._raw_reader
        assert raw_reader is not None  # noqa: S101  # set in __init__, cleared only on close
        if size == 0:
            return b""
        if size < 0:
            self._position = self._size
            return self._read_from_buffer() + raw_reader.read()

        #
        # Return unused data first
        #
        if len(self._current_part) >= size:
            return self._read_from_buffer(size)

        if self._position == self._size:
            return self._read_from_buffer()

        self._fill_buffer(size)
        return self._read_from_buffer(size)

    def read1(self, size: int | None = -1) -> bytes:
        """This is the same as read()."""
        return self.read(size=size)

    def readinto(self, b: WriteableBuffer) -> int:
        """Read up to len(b) bytes into b, and return the number of bytes read."""
        mv = memoryview(b).cast("B")
        data = self.read(len(mv))
        if not data:
            return 0
        mv[: len(data)] = data
        return len(data)

    def readline(self, limit: int | None = -1) -> bytes:
        """Read up to and including the next newline.  Returns the bytes read."""
        if limit is None:
            limit = -1
        if limit != -1:
            msg = "limits other than -1 not implemented yet"
            raise NotImplementedError(msg)

        #
        # A single line may span multiple buffers.
        #
        line = io.BytesIO()
        while not (self._position == self._size and len(self._current_part) == 0):
            line_part = self._current_part.readline(self._line_terminator)
            line.write(line_part)
            self._position += len(line_part)

            if line_part.endswith(self._line_terminator):
                break
            self._fill_buffer()

        return line.getvalue()

    #
    # Internal methods.
    #
    def _read_from_buffer(self, size: int = -1) -> bytes:
        """Remove at most size bytes from our buffer and return them."""
        size = size if size >= 0 else len(self._current_part)
        part = self._current_part.read(size)
        self._position += len(part)
        return part

    def _fill_buffer(self, size: int = -1) -> bool | None:
        raw_reader = self._raw_reader
        assert raw_reader is not None  # noqa: S101  # set in __init__, cleared only on close
        size = max(size, self._current_part._chunk_size)  # noqa: SLF001  # intra-package coupling
        while len(self._current_part) < size and self._position != self._size:
            # _RawReader has a compatible ``read`` method but is not nominally IO[bytes].
            bytes_read = self._current_part.fill(cast("IO[bytes]", raw_reader))
            if bytes_read == 0:
                logger.debug("reached EOF while filling buffer")
                return True
        return None

    def __enter__(self) -> Self:
        """Enter the reader context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the reader on context exit."""
        self.close()

    def __str__(self) -> str:
        """Return a short human-readable description of the reader."""
        return f"({self.__class__.__name__}, {self._container_name!r}, {self._blob_name!r})"

    def __repr__(self) -> str:
        """Return an unambiguous representation of the reader."""
        return f"{self.__class__.__name__}(container={self._container_name!r}, blob={self._blob_name!r})"


class Writer(io.BufferedIOBase):
    """Writes bytes to Azure Blob Storage.

    Implements the io.BufferedIOBase interface of the standard library.
    """

    name: str
    _blob: azure.storage.blob.BlobClient | None = None  # so `closed` works if __init__ fails and __del__ runs

    def __init__(
        self,
        container: str,
        blob: str,
        client: _AzureClient,
        blob_kwargs: dict[str, Any] | None = None,
        min_part_size: int = _DEFAULT_MIN_PART_SIZE,
    ) -> None:
        self._container_name = container
        self._blob_name = blob
        self._blob_kwargs = blob_kwargs or {}
        self._min_part_size = min_part_size
        self._total_size = 0
        self._total_parts = 0
        self._bytes_uploaded = 0
        self._current_part = io.BytesIO()
        self._block_list: list[azure.storage.blob.BlobBlock] = []

        self._blob = _get_blob_client(client, container, blob)

    def flush(self) -> None:
        """No-op flush; data is buffered until `close` or `_upload_part`."""

    def terminate(self) -> None:
        """Do not commit block list on abort.

        Uploaded (uncommitted) blocks will be garbage collected after 7 days.

        See also https://stackoverflow.com/a/69673084/5511061.
        """
        logger.debug("%s: terminating multipart upload", self)
        if not self.closed:
            self._block_list = []
            self._blob = None
        logger.debug("%s: terminated multipart upload", self)

    #
    # Override some methods from io.IOBase.
    #
    def close(self) -> None:
        """Commit the buffered block list and close the stream."""
        logger.debug("close: called")
        if not self.closed:
            blob = self._blob
            assert blob is not None  # noqa: S101  # not closed implies blob is set
            logger.debug("%s: completing multipart upload", self)
            try:
                if self._current_part.tell() > 0:
                    self._upload_part()
                blob.commit_block_list(self._block_list, **self._blob_kwargs)
            finally:
                self._block_list = []
                self._blob = None
            logger.debug("%s: completed multipart upload", self)

    @property
    def closed(self) -> bool:
        """Return True if the stream is closed."""
        return self._blob is None

    def writable(self) -> bool:
        """Return True if the stream supports writing."""
        return True

    def seekable(self) -> bool:
        """Return True; we support `tell` but not `seek` or `truncate`."""
        return True

    def seek(self, offset: int, whence: int = smart_open.constants.WHENCE_START) -> int:
        """Unsupported."""
        raise io.UnsupportedOperation

    def truncate(self, size: int | None = None) -> int:
        """Unsupported."""
        raise io.UnsupportedOperation

    def tell(self) -> int:
        """Return the current stream position."""
        return self._total_size

    #
    # io.BufferedIOBase methods.
    #
    def detach(self) -> io.RawIOBase:
        """Unsupported."""
        msg = "detach() not supported"
        raise io.UnsupportedOperation(msg)

    def write(self, b: ReadableBuffer) -> int:
        """Write the given bytes (binary string) to the Azure Blob Storage file.

        There's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away.
        """
        if not isinstance(b, _BINARY_TYPES):
            msg = f"input must be one of {_BINARY_TYPES!r}, got: {type(b)!r}"
            raise TypeError(msg)

        length = len(memoryview(b))
        self._current_part.write(b)
        self._total_size += length

        if self._current_part.tell() >= self._min_part_size:
            self._upload_part()

        return length

    def _upload_part(self) -> None:
        blob = self._blob
        assert blob is not None  # noqa: S101  # _upload_part is only called while the writer is open
        part_num = self._total_parts + 1
        content_length = self._current_part.tell()
        range_stop = self._bytes_uploaded + content_length - 1

        # block_id's must be base64 encoded, all the same length, and less than or equal to
        # 64 bytes in size prior to encoding.
        # https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python#stage-block-block-id--data--length-none----kwargs-
        zero_padded_part_num = str(part_num).zfill(64 // 2)
        block_id = base64.b64encode(zero_padded_part_num.encode())
        self._current_part.seek(0)
        # the SDK accepts bytes block IDs at runtime even though its stubs say str
        blob.stage_block(block_id, self._current_part.read(content_length))  # ty: ignore[invalid-argument-type]
        self._block_list.append(
            azure.storage.blob.BlobBlock(block_id=block_id),  # ty: ignore[invalid-argument-type]
        )

        logger.info(
            "uploading part #%i, %i bytes (total %.3fGB)",
            part_num,
            content_length,
            range_stop / 1024.0**3,
        )

        self._total_parts += 1
        self._bytes_uploaded += content_length
        self._current_part = io.BytesIO(self._current_part.read())
        self._current_part.seek(0, io.SEEK_END)

    def __enter__(self) -> Self:
        """Enter the writer context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close or terminate the writer on context exit."""
        if exc_type is not None:
            self.terminate()
        else:
            self.close()

    def __str__(self) -> str:
        """Return a short human-readable description of the writer."""
        return f"({self.__class__.__name__}, {self._container_name!r}, {self._blob_name!r})"

    def __repr__(self) -> str:
        """Return an unambiguous representation of the writer."""
        return f"{self.__class__.__name__}(container={self._container_name!r}, blob={self._blob_name!r}, min_part_size={self._min_part_size!r})"


class AppendWriter(io.BufferedIOBase):
    """Append bytes to Azure Blob Storage.

    Implements the io.BufferedIOBase interface of the standard library.
    """

    name: str
    _blob: azure.storage.blob.BlobClient | None = None  # so `closed` works if __init__ fails and __del__ runs

    def __init__(
        self,
        container: str,
        blob: str,
        client: _AzureClient,
        blob_kwargs: dict[str, Any] | None = None,
        min_part_size: int = _DEFAULT_MIN_PART_SIZE,
    ) -> None:
        self._container_name = container
        self._blob_name = blob
        self._blob_kwargs = blob_kwargs or {}
        self._min_part_size = min_part_size
        self._total_size = 0
        self._current_part = io.BytesIO()

        self._blob = _get_blob_client(client, container, blob)

    def flush(self) -> None:
        """No-op flush; data is buffered until `close` or `_upload_part`."""

    def terminate(self) -> None:
        """AppendBlob cannot be aborted, so we do nothing here."""
        if not self.closed:
            self._current_part = io.BytesIO()
            self._blob = None

    def close(self) -> None:
        """No action needed here, as the AppendBlob is automatically committed."""
        if not self.closed:
            try:
                if self._current_part.tell() > 0:
                    self._upload_part()
            finally:
                self._blob = None

    @property
    def closed(self) -> bool:
        """Return True if the stream is closed."""
        return self._blob is None

    def writable(self) -> bool:
        """Return True if the stream supports writing."""
        return True

    def seekable(self) -> bool:
        """Return True; we support `tell` but not `seek` or `truncate`."""
        return True

    def seek(self, offset: int, whence: int = smart_open.constants.WHENCE_START) -> int:
        """Unsupported."""
        raise io.UnsupportedOperation

    def truncate(self, size: int | None = None) -> int:
        """Unsupported."""
        raise io.UnsupportedOperation

    def tell(self) -> int:
        """Return the current stream position."""
        return self._total_size

    def detach(self) -> io.RawIOBase:
        """Unsupported."""
        msg = "detach() not supported"
        raise io.UnsupportedOperation(msg)

    def write(self, b: ReadableBuffer) -> int:
        """Append `b` to the AppendBlob, buffering until ``min_part_size``."""
        if not isinstance(b, _BINARY_TYPES):
            msg = f"input must be one of {_BINARY_TYPES!r}, got: {type(b)!r}"
            raise TypeError(msg)
        length = len(memoryview(b))
        self._current_part.write(b)
        self._total_size += length
        if self._current_part.tell() >= self._min_part_size:
            self._upload_part()
        return length

    def _upload_part(self) -> None:
        blob = self._blob
        assert blob is not None  # noqa: S101  # _upload_part is only called while the writer is open
        data = self._current_part.getvalue()
        blob.upload_blob(
            data=data,
            blob_type=azure.storage.blob.BlobType.APPENDBLOB,
            overwrite=False,
            **self._blob_kwargs,
        )
        self._current_part = io.BytesIO()

    def __enter__(self) -> Self:
        """Enter the append writer context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close or terminate the append writer on context exit."""
        if exc_type is not None:
            self.terminate()
        else:
            self.close()

    def __str__(self) -> str:
        """Return a short human-readable description of the append writer."""
        return f"({self.__class__.__name__}, {self._container_name!r}, {self._blob_name!r})"

    def __repr__(self) -> str:
        """Return an unambiguous representation of the append writer."""
        return f"{self.__class__.__name__}(container={self._container_name!r}, blob={self._blob_name!r})"
