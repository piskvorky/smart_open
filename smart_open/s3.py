#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements file-like objects for reading and writing from/to AWS S3."""

from __future__ import annotations

import contextlib
import functools
import http
import io
import itertools
import logging
import re
import time
from collections.abc import Callable, Iterator
from math import inf
from typing import (
    TYPE_CHECKING,
    Any,
    TypedDict,
    cast,
)

try:
    import boto3
    import boto3.s3.transfer
    import boto3.session
    import botocore.client
    import botocore.exceptions
    import urllib3.exceptions
except ImportError:
    MISSING_DEPS = True

import smart_open.bytebuffer
import smart_open.concurrency
import smart_open.utils
from smart_open import constants

if TYPE_CHECKING:
    from types import TracebackType

    from _typeshed import WriteableBuffer
    from botocore.response import StreamingBody
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_s3.type_defs import CompletedMultipartUploadTypeDef
    from typing_extensions import Buffer, Self

    from smart_open._typing import TransportParams

logger = logging.getLogger(__name__)

#
# AWS puts restrictions on the part size for multipart uploads.
# Each part must be more than 5MB, and less than 5GB.
#
# On top of that, our MultipartWriter has a min_part_size option.
# In retrospect, it's an unfortunate name, because it conflicts with the
# minimum allowable part size (5MB), but it's too late to change it, because
# people are using that parameter (unlike the MIN, DEFAULT, MAX constants).
# It really just means "part size": as soon as you have this many bytes,
# write a part to S3 (see the MultipartWriter.write method).
#

MIN_PART_SIZE = 5 * 1024**2
"""The absolute minimum permitted by Amazon."""

DEFAULT_PART_SIZE = 50 * 1024**2
"""The default part size for S3 multipart uploads, chosen carefully by smart_open"""

MAX_PART_SIZE = 5 * 1024**3
"""The absolute maximum permitted by Amazon."""

SCHEMES = ("s3", "s3n", "s3a")

DEFAULT_BUFFER_SIZE = 128 * 1024

URI_EXAMPLES = (
    "s3://my_bucket/my_key",
    "s3://my_key:my_secret@my_bucket/my_key",
)

# Returned by AWS when we try to seek beyond EOF.
_OUT_OF_RANGE = "InvalidRange"

# Matches the ``versionId`` query parameter in an S3 URI (issue #595).
# The leading group preserves whether it was the first (``?``) or a later (``&``)
# query parameter so that we can rebuild the remaining query correctly.
_VERSION_ID_RE = re.compile(r"(?P<sep>[?&])versionId=(?P<value>[^&]*)")


class _S3Uri(TypedDict):
    scheme: str
    bucket_id: str
    key_id: str
    access_id: str | None
    access_secret: str | None
    version_id: str | None


class Retry:
    """Retry policy used by the S3 transport for transient client/network errors."""

    def __init__(self) -> None:
        self.attempts: int = 6
        self.sleep_seconds: int = 10
        self.exceptions: list[type[BaseException]] = [botocore.exceptions.EndpointConnectionError]
        self.client_error_codes: list[str] = ["NoSuchUpload"]

    def _do(self, fn: functools.partial[Any]) -> Any:
        for attempt in range(self.attempts):
            try:
                return fn()
            except tuple(self.exceptions) as err:  # noqa: PERF203  # retry semantics require per-attempt try/except
                logger.critical(
                    "Caught non-fatal %s, retrying %d more times",
                    err,
                    self.attempts - attempt - 1,
                )
                logger.exception("retryable error")
                time.sleep(self.sleep_seconds)
            except botocore.exceptions.ClientError as err:
                error_code = err.response["Error"].get("Code")
                if error_code not in self.client_error_codes:
                    raise
                logger.critical(
                    "Caught non-fatal ClientError (%s), retrying %d more times",
                    error_code,
                    self.attempts - attempt - 1,
                )
                logger.exception("retryable error")
                time.sleep(self.sleep_seconds)
        logger.critical("encountered too many non-fatal errors, giving up")
        msg = f"{fn.func} failed after {self.attempts} attempts"
        raise OSError(msg)


#
# The retry mechanism for this submodule.  Client code may modify it, e.g. by
# updating RETRY.sleep_seconds and friends.
#
if "MISSING_DEPS" not in locals():
    RETRY = Retry()


class _ClientWrapper:
    """Wraps a client to inject the appropriate keyword args into each method call.

    The keyword args are a dictionary keyed by the fully qualified method name.
    For example, S3.Client.create_multipart_upload.

    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client

    This wrapper behaves identically to the client otherwise.
    """

    def __init__(self, client: S3Client, kwargs: dict[str, Any]) -> None:
        self.client = client
        self.kwargs = kwargs

    def __getattr__(self, method_name: str) -> Callable[..., Any]:
        method = getattr(self.client, method_name)
        kwargs = self.kwargs.get(f"S3.Client.{method_name}", {})
        return functools.partial(method, **kwargs)


def parse_uri(uri_as_string: str) -> _S3Uri:
    """Parse an ``s3://`` URI into bucket, key, credential, and version components."""
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
    assert split_uri.scheme in SCHEMES  # noqa: S101  # internal precondition; misuse should crash loudly

    #
    # These defaults tell boto3 to look for credentials elsewhere
    #
    access_id, access_secret = None, None

    #
    # Common URI template [secret:key@]bucket/object
    #
    # The urlparse function doesn't handle the above schema, so we have to do
    # it ourselves.
    #
    uri = split_uri.netloc + split_uri.path

    #
    # Attempt to extract edge-case authentication details from the URL.
    #
    # See:
    #   1. https://summitroute.com/blog/2018/06/20/aws_security_credential_formats/
    #   2. test_s3_uri_with_credentials* in test_smart_open.py for example edge cases
    #
    if "@" in uri:
        maybe_auth, rest = uri.split("@", 1)
        if ":" in maybe_auth:
            maybe_id, maybe_secret = maybe_auth.split(":", 1)
            if "/" not in maybe_id:
                access_id, access_secret = maybe_id, maybe_secret
                uri = rest

    bucket_id, key_id = uri.split("/", 1)

    #
    # Extract ``?versionId=...`` from the key (issue #595).  ``safe_urlsplit``
    # preserves any ``?`` in the URI as part of the key, so we surgically
    # remove just the ``versionId`` parameter and leave any other ``?...``
    # segments alone.  Users with a literal ``?versionId=`` in their S3 key
    # can keep working by calling ``smart_open.s3.open(bucket, key, ...)``
    # directly with the raw key.
    #
    version_id = None
    match = _VERSION_ID_RE.search(key_id)
    if match:
        version_id = match.group("value")
        before = key_id[: match.start()]
        after = key_id[match.end() :]
        if match.group("sep") == "?" and after.startswith("&"):
            after = "?" + after[1:]
        key_id = before + after

    return {
        "scheme": split_uri.scheme,
        "bucket_id": bucket_id,
        "key_id": key_id,
        "access_id": access_id,
        "access_secret": access_secret,
        "version_id": version_id,
    }


def _consolidate_params(uri: _S3Uri, transport_params: TransportParams) -> tuple[_S3Uri, TransportParams]:
    """Consolidates the parsed Uri with the additional parameters.

    This is necessary because the user can pass some of the parameters can in
    two different ways:

    1) Via the URI itself
    2) Via the transport parameters

    These are not mutually exclusive, but we have to pick one over the other
    in a sensible way in order to proceed.

    """
    transport_params = dict(transport_params)

    def inject(**kwargs: Any) -> None:
        try:
            client_kwargs = transport_params["client_kwargs"]
        except KeyError:
            client_kwargs = transport_params["client_kwargs"] = {}

        try:
            init_kwargs = client_kwargs["S3.Client"]
        except KeyError:
            init_kwargs = client_kwargs["S3.Client"] = {}

        init_kwargs.update(**kwargs)

    client = transport_params.get("client")
    if client is not None and (uri["access_id"] or uri["access_secret"]):
        logger.warning(
            "ignoring credentials parsed from URL because they conflict with "
            'transport_params["client"]. Set transport_params["client"] to None '
            "to suppress this warning."
        )
        uri.update(access_id=None, access_secret=None)
    elif uri["access_id"] and uri["access_secret"]:
        inject(
            aws_access_key_id=uri["access_id"],
            aws_secret_access_key=uri["access_secret"],
        )
        uri.update(access_id=None, access_secret=None)

    if uri["version_id"] is not None:
        if transport_params.get("version_id") is not None:
            logger.warning(
                "ignoring versionId parsed from URL because it conflicts with "
                'transport_params["version_id"]. Drop the URL versionId to '
                "suppress this warning."
            )
        else:
            transport_params["version_id"] = uri["version_id"]
        uri.update(version_id=None)

    return uri, transport_params


def open_uri(uri: str, mode: str, transport_params: TransportParams) -> io.BufferedIOBase:
    """Open an S3 URI using the given mode and transport params."""
    parsed_uri = parse_uri(uri)
    parsed_uri, transport_params = _consolidate_params(parsed_uri, transport_params)
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    return open(parsed_uri["bucket_id"], parsed_uri["key_id"], mode, **kwargs)


def open(  # noqa: PLR0913  # legacy public API; refactor in a dedicated PR
    bucket_id: str,
    key_id: str,
    mode: str,
    version_id: str | None = None,
    buffer_size: int = DEFAULT_BUFFER_SIZE,
    min_part_size: int = DEFAULT_PART_SIZE,
    multipart_upload: bool = True,  # noqa: FBT001, FBT002  # public API
    defer_seek: bool = False,  # noqa: FBT001, FBT002  # public API
    client: S3Client | None = None,
    client_kwargs: dict[str, Any] | None = None,
    writebuffer: io.BytesIO | None = None,
    range_chunk_size: int | None = None,
) -> io.BufferedIOBase:
    """Open an S3 object for reading or writing.

    Args:
        bucket_id: The name of the bucket this object resides in.
        key_id: The name of the key within the bucket.
        mode: The mode for opening the object.  Must be either "rb" or "wb".
        version_id: Version of the object, used when reading object.
            If None, will fetch the most recent version.
        buffer_size: Default: 128KB.
            The buffer size in bytes for reading. Controls memory usage. Data is
            streamed from a S3 network stream in buffer_size chunks. Forward seeks
            within the current buffer are satisfied without additional GET requests.
            Backward seeks always open a new GET request. For forward seek-intensive
            workloads, increase buffer_size to reduce GET requests at the cost of
            higher memory usage.
        min_part_size: The minimum part size for multipart uploads, in bytes.
            When the writebuffer contains this many bytes, smart_open will upload
            the bytes to S3 as a single part of a multi-part upload, freeing the
            buffer either partially or entirely.  When you close the writer, it
            will assemble the parts together.
            The value determines the upper limit for the writebuffer.  If buffer
            space is short (e.g. you are buffering to memory), then use a smaller
            value for min_part_size, or consider buffering to disk instead (see
            the writebuffer option).
            The value must be between 5MB and 5GB.  If you specify a value outside
            of this range, smart_open will adjust it for you, because otherwise the
            upload _will_ fail.
            For writing only.  Does not apply if you set multipart_upload=False.
        multipart_upload: Default: `True`.
            If set to `True`, will use multipart upload for writing to S3. If set
            to `False`, S3 upload will use the S3 Single-Part Upload API, which
            is more ideal for small file sizes.
            For writing only.
        defer_seek: Default: `False`.
            If set to `True` on a file opened for reading, GetObject will not be
            called until the first seek() or read().
            Avoids redundant API queries when seeking before reading.
        client: The S3 client to use when working with boto3.
            If you don't specify this, then smart_open will create a new client for
            you.
        client_kwargs: Additional parameters to pass to the relevant functions of
            the client. The keys are fully qualified method names,
            e.g. `S3.Client.create_multipart_upload`.
            The values are kwargs to pass to that method each time it is called.
        writebuffer: By default, this module will buffer data in memory using
            io.BytesIO when writing. Pass another binary IO instance here to use it
            instead. For example, you may pass a file object to buffer to local disk
            instead of in RAM. Use this to keep RAM usage low at the expense of
            additional disk IO. If you pass in an open file, then you are
            responsible for cleaning it up after writing completes.
        range_chunk_size: Default: `None`.
            Maximum byte range per S3 GET request when reading.
            When None (default), a single GET request is made for the entire file,
            and data is streamed from that single botocore.response.StreamingBody
            in buffer_size chunks.
            When set to a positive integer, multiple GET requests are made, each
            limited to at most this many bytes via HTTP Range headers. Each GET
            returns a new StreamingBody that is streamed in buffer_size chunks.
            Useful for reading small portions of large files without forcing
            S3-compatible systems like SeaweedFS/Ceph to load the entire file.
            Larger values mean fewer billable GET requests but higher load on S3
            servers. Smaller values mean more GET requests but less server load per
            request. Values larger than the file size result in a single GET for the
            whole file. Affects reading only. Does not affect memory usage
            (controlled by buffer_size).

    Returns:
        A file-like object for reading or writing.

    Raises:
        AssertionError: If the mode is somehow neither read nor write binary.
        NotImplementedError: If the mode is not "rb" or "wb".
        ValueError: If version_id is specified for write mode.
    """
    logger.debug("%r", locals())
    if mode not in constants.BINARY_MODES:
        msg = f"bad mode: {mode!r} expected one of {constants.BINARY_MODES!r}"
        raise NotImplementedError(msg)

    if (mode == constants.WRITE_BINARY) and (version_id is not None):
        msg = "version_id must be None when writing"
        raise ValueError(msg)

    if mode == constants.READ_BINARY:
        fileobj = Reader(
            bucket_id,
            key_id,
            version_id=version_id,
            buffer_size=buffer_size,
            defer_seek=defer_seek,
            client=client,
            client_kwargs=client_kwargs,
            range_chunk_size=range_chunk_size,
        )
    elif mode == constants.WRITE_BINARY:
        if multipart_upload:
            fileobj = MultipartWriter(
                bucket_id,
                key_id,
                client=client,
                client_kwargs=client_kwargs,
                writebuffer=writebuffer,
                part_size=min_part_size,
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
        msg = f"unexpected mode: {mode!r}"
        raise AssertionError(msg)

    fileobj.name = key_id
    return fileobj


def _get(
    client: S3Client,
    bucket: str,
    key: str,
    version: str | None,
    range_string: str | None,
) -> dict[str, Any]:
    try:
        params: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if version:
            params["VersionId"] = version
        if range_string:
            params["Range"] = range_string

        return cast("dict[str, Any]", client.get_object(**params))
    except botocore.client.ClientError as error:
        wrapped_error = OSError(
            f"unable to access bucket: {bucket!r} key: {key!r} version: {version!r} error: {error}"
        )
        wrapped_error.backend_error = error  # ty: ignore[unresolved-attribute]  # smuggle the botocore error through
        raise wrapped_error from error


def _unwrap_ioerror(ioe: OSError) -> dict[str, Any] | None:
    """Given an IOError from _get, return the 'Error' dictionary from botocore."""
    try:
        return ioe.backend_error.response["Error"]  # ty: ignore[unresolved-attribute]  # set by _get
    except (AttributeError, KeyError):
        return None


class _SeekableRawReader:
    """Read an S3 object.

    This class is internal to the S3 submodule.
    """

    def __init__(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        version_id: str | None = None,
        range_chunk_size: int | None = None,
    ) -> None:
        self._client = client
        self._bucket = bucket
        self._key = key
        self._version_id = version_id
        self._range_chunk_size = range_chunk_size

        self._content_length: int | None = None
        self._position = 0
        self._body: StreamingBody | io.BytesIO | None = None

    @property
    def closed(self) -> bool:
        return self._body is None

    def close(self) -> None:
        body = self._body
        if body is not None:
            body.close()
            self._body = None

    def seek(self, offset: int, whence: int = constants.WHENCE_START) -> int:
        """Seek to the specified position.

        Args:
            offset: The offset in bytes.
            whence: Where the offset is from.

        Returns:
            The position after seeking.

        Raises:
            ValueError: If whence is not one of the supported values.
        """
        if whence not in constants.WHENCE_CHOICES:
            msg = f"invalid whence, expected one of {constants.WHENCE_CHOICES!r}"
            raise ValueError(msg)

        #
        # Close old body explicitly.
        #
        self.close()

        start = None
        stop = None
        if whence == constants.WHENCE_START:
            start = max(0, offset)
        elif whence == constants.WHENCE_CURRENT:
            start = max(0, offset + self._position)
        elif whence == constants.WHENCE_END:
            stop = max(0, -offset)

        #
        # If we can figure out that we've read past the EOF, then we can save
        # an extra API call.
        #
        if self._content_length is None:  # _open_body has not been called yet
            if start is None and stop == 0:
                # seek(0, WHENCE_END) seeks straight to EOF:
                # make a minimal request to populate _content_length
                self._open_body(start=0, stop=0)
                self.close()
                reached_eof = True
            else:
                reached_eof = False
        elif (start is not None and start >= self._content_length) or stop == 0:
            reached_eof = True
        else:
            reached_eof = False

        if reached_eof:
            self._body = io.BytesIO()
            assert self._content_length is not None  # noqa: S101  # reached_eof implies _content_length is set
            self._position = self._content_length
        else:
            self._open_body(start, stop)

        return self._position

    def _open_body(self, start: int | None = None, stop: int | None = None) -> None:  # noqa: C901, PLR0912  # legacy internal helper; refactor in a dedicated PR
        """Open a connection to download the specified range of bytes.

        Store the open file handle in self._body.

        If no range is specified, start defaults to self._position.
        start and stop follow the semantics of the http range header,
        so a stop without a start will read bytes beginning at stop.

        If self._range_chunk_size is set, the S3 server is protected from open range
        headers and stop will be set such that at most self._range_chunk_size bytes
        are returned in a single GET request.

        As a side effect, set self._content_length. Set self._position
        to self._content_length if start is past end of file.
        """
        if start is None and stop is None:
            start = self._position

        # Apply chunking: limit the stop position if range_chunk_size is set
        if stop is None and self._range_chunk_size is not None:
            assert start is not None  # noqa: S101  # stop is None implies start was set above
            stop = start + self._range_chunk_size - 1
            # Don't request beyond known content length
            if self._content_length is not None:
                stop = min(stop, self._content_length - 1)

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
        except OSError as ioe:
            # Handle requested content range exceeding content size.
            error_response = _unwrap_ioerror(ioe)
            if error_response is None or error_response.get("Code") != _OUT_OF_RANGE:
                raise

            actual_object_size = int(error_response.get("ActualObjectSize", 0))
            if (
                # empty file (==) or start is past end of file (>)
                (start is not None and start >= actual_object_size)
                # negative seek requested more bytes than file has
                or (start is None and stop is not None and stop >= actual_object_size)
            ):
                self._position = self._content_length = actual_object_size
                self._body = io.BytesIO()
            else:  # stop is past end of file: request the correct remainder instead
                self._open_body(start=start, stop=actual_object_size - 1)
            return

        #
        # Keep track of how many times boto3's built-in retry mechanism
        # activated.
        #
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html#checking-retry-attempts-in-an-aws-service-response
        #
        logger.debug(
            "%s: RetryAttempts: %d",
            self,
            response["ResponseMetadata"]["RetryAttempts"],
        )
        #
        # range request may not always return partial content, see:
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests#partial_request_responses
        #
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == http.HTTPStatus.PARTIAL_CONTENT:
            # 206 guarantees that the response body only contains the requested byte range
            _, resp_start, _, length = smart_open.utils.parse_content_range(response["ContentRange"])
            self._position = resp_start
            self._content_length = length
            self._body = response["Body"]
        elif status_code == http.HTTPStatus.OK:
            # 200 guarantees the response body contains the full file (server ignored range header)
            content_length = response["ContentLength"]
            body = response["Body"]
            self._position = 0
            self._content_length = content_length
            self._body = body
            #
            # If we got a full request when we were actually expecting a range, we need to
            # read some data to ensure that the body starts in the place that the caller expects
            #
            if start is not None:
                expected_position = min(content_length, start)
            elif start is None and stop is not None:
                expected_position = max(0, content_length - stop)
            else:
                expected_position = 0
            if expected_position > 0:
                logger.debug(
                    "%s: discarding %d bytes to reach expected position",
                    self,
                    expected_position,
                )
                self._position = len(body.read(expected_position))
        else:
            msg = f"Unexpected status code {status_code!r}"
            raise ValueError(msg)

    def read(self, size: int = -1) -> bytes:
        """Read from the continuous connection with the remote peer."""
        if size < -1:
            msg = f"size must be >= -1, got {size}"
            raise ValueError(msg)

        size_limit: int | float = inf if size == -1 else size  # makes for a simple while-condition below

        binary_collected = io.BytesIO()

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
        def retry_read(attempts: tuple[int, ...] = (1, 2, 4, 8, 16)) -> bytes:
            for seconds in attempts:
                if self.closed:
                    self._open_body()
                body = self._body
                assert body is not None  # noqa: S101  # _open_body (re)opens the body when closed
                try:
                    if size_limit == inf:
                        return body.read()
                    return body.read(size - binary_collected.tell())
                except (
                    ConnectionResetError,
                    botocore.exceptions.BotoCoreError,
                    urllib3.exceptions.HTTPError,
                ) as err:
                    logger.warning(
                        "%s: caught %r while reading %d bytes, sleeping %ds before retry",
                        self,
                        err,
                        -1 if size_limit == inf else size,
                        seconds,
                    )
                    self.close()
                    time.sleep(seconds)
            msg = f"{self}: failed to read {-1 if size_limit == inf else size} bytes after {len(attempts)} attempts"
            raise OSError(
                msg,
            )

        while (
            self._content_length is None  # very first read call
            or (
                self._position < self._content_length  # not yet end of file
                and binary_collected.tell() < size_limit  # not yet read enough
            )
        ):
            binary = retry_read()
            self._position += len(binary)
            binary_collected.write(binary)
            if not binary:  # end of stream
                self.close()

        return binary_collected.getvalue()

    def __str__(self) -> str:
        """Return a short human-readable description of the seekable reader."""
        return f"smart_open.s3._SeekableReader({self._bucket!r}, {self._key!r})"


def _initialize_boto3(
    rw: Reader | MultipartWriter | SinglepartWriter,
    client: S3Client | None,
    client_kwargs: dict[str, Any] | None,
    bucket: str,
    key: str,
) -> None:
    """Create the boto3 client/bucket/key wrappers required for accessing S3."""
    if client_kwargs is None:
        client_kwargs = {}

    if client is None:
        init_kwargs = client_kwargs.get("S3.Client", {})
        if "config" not in init_kwargs:
            init_kwargs["config"] = botocore.client.Config(
                max_pool_connections=64, tcp_keepalive=True, retries={"max_attempts": 6, "mode": "adaptive"}
            )
        # boto3.client re-uses the default session which is not thread-safe when this is called
        # from within a thread. when using smart_open with multithreading, create a thread-safe
        # client with the config above and share it between threads using transport_params
        # https://github.com/boto/boto3/blob/1.38.41/docs/source/guide/clients.rst?plain=1#L111
        client = boto3.client("s3", **init_kwargs)
    assert client  # noqa: S101  # internal precondition; misuse should crash loudly

    # _ClientWrapper delegates to a real S3Client via __getattr__; present it as
    # S3Client so callers type-check against the genuine boto3 method signatures.
    rw._client = cast("S3Client", _ClientWrapper(client, client_kwargs))  # noqa: SLF001  # intra-package coupling
    rw._bucket = bucket  # noqa: SLF001  # intra-package coupling
    rw._key = key  # noqa: SLF001  # intra-package coupling


class Reader(io.BufferedIOBase):
    """Reads bytes from S3.

    Implements the io.BufferedIOBase interface of the standard library.
    """

    name: str
    _client: S3Client
    _bucket: str
    _key: str

    def __init__(  # noqa: PLR0913  # legacy public API; refactor in a dedicated PR
        self,
        bucket: str,
        key: str,
        version_id: str | None = None,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        line_terminator: bytes = constants.BINARY_NEWLINE,
        defer_seek: bool = False,  # noqa: FBT001, FBT002  # public API
        client: S3Client | None = None,
        client_kwargs: dict[str, Any] | None = None,
        range_chunk_size: int | None = None,
    ) -> None:
        self._version_id = version_id
        self._buffer_size = buffer_size

        _initialize_boto3(self, client, client_kwargs, bucket, key)

        self._raw_reader = _SeekableRawReader(
            self._client,
            bucket,
            key,
            self._version_id,
            range_chunk_size=range_chunk_size,
        )
        self._current_pos = 0
        self._buffer = smart_open.bytebuffer.ByteBuffer(buffer_size)
        self._eof = False
        self._line_terminator = line_terminator
        self._seek_initialized = False

        if not defer_seek:
            self.seek(0)

    #
    # io.BufferedIOBase methods.
    #

    def close(self) -> None:
        """Flush and close this stream."""
        logger.debug("close: called")

    def readable(self) -> bool:
        """Return True if the stream can be read from."""
        return True

    def read(self, size: int | None = -1) -> bytes:
        """Read up to size bytes from the object and return them."""
        if size is None:
            size = -1
        if size == 0:
            return b""
        if size < 0:
            # call read() before setting _current_pos to make sure _content_length is set
            out = self._read_from_buffer() + self._raw_reader.read()
            content_length = self._raw_reader._content_length  # noqa: SLF001  # intra-package coupling
            assert content_length is not None  # noqa: S101  # read() populates _content_length
            self._current_pos = content_length
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

    def read1(self, size: int | None = -1) -> bytes:
        """This is the same as read()."""
        return self.read(size=size)

    def readinto(self, b: WriteableBuffer) -> int:
        """Read up to len(b) bytes into b, and return the number of bytes read.

        Args:
            b: The buffer to read into.

        Returns:
            The number of bytes read.
        """
        mv = memoryview(b).cast("B")
        data = self.read(len(mv))
        if not data:
            return 0
        mv[: len(data)] = data
        return len(data)

    def readline(self, limit: int | None = -1) -> bytes:
        """Read up to and including the next newline.  Returns the bytes read."""
        if limit != -1:
            msg = "limits other than -1 not implemented yet"
            raise NotImplementedError(msg)

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
            self._fill_buffer()

        return line.getvalue()

    def seekable(self) -> bool:
        """If False, seek(), tell() and truncate() will raise IOError.

        We offer only seek support, and no truncate support.
        """
        return True

    def seek(self, offset: int, whence: int = constants.WHENCE_START) -> int:
        """Seek to the specified position.

        Args:
            offset: The offset in bytes.
            whence: Where the offset is from.

        Returns:
            The position after seeking.
        """
        # Convert relative offset to absolute, since self._raw_reader
        # doesn't know our current position.
        if whence == constants.WHENCE_CURRENT:
            whence = constants.WHENCE_START
            offset += self._current_pos

        # Check if we can satisfy seek from buffer
        if whence == constants.WHENCE_START and offset > self._current_pos:
            buffer_end = self._current_pos + len(self._buffer)
            if offset <= buffer_end:
                # Forward seek within buffered data - avoid S3 request
                self._buffer.read(offset - self._current_pos)
                self._current_pos = offset
                return self._current_pos

        if not self._seek_initialized or not (
            whence == constants.WHENCE_START and offset == self._current_pos
        ):
            self._current_pos = self._raw_reader.seek(offset, whence)
            self._buffer.empty()

        self._eof = self._current_pos == self._raw_reader._content_length  # noqa: SLF001  # intra-package coupling

        self._seek_initialized = True
        return self._current_pos

    def tell(self) -> int:
        """Return the current position within the file."""
        return self._current_pos

    def truncate(self, size: int | None = None) -> int:
        """Unsupported."""
        raise io.UnsupportedOperation

    def detach(self) -> io.RawIOBase:
        """Unsupported."""
        raise io.UnsupportedOperation

    def terminate(self) -> None:
        """Do nothing."""

    def to_boto3(self, resource: Any) -> Any:
        """Create an **independent** `boto3.s3.Object` instance.

        The returned object points to the same S3 object as this instance.
        Changes to the returned object will not affect the current instance.

        Args:
            resource: A ``boto3.resource`` instance.

        Returns:
            A ``boto3.s3.Object`` (or ``ObjectVersion`` when ``version_id`` is set).
        """
        assert resource, "resource must be a boto3.resource instance"  # noqa: S101  # internal precondition; misuse should crash loudly
        obj = resource.Object(self._bucket, self._key)
        if self._version_id is not None:
            return obj.Version(self._version_id)
        return obj

    #
    # Internal methods.
    #
    def _read_from_buffer(self, size: int = -1) -> bytes:
        """Remove at most size bytes from our buffer and return them."""
        size = size if size >= 0 else len(self._buffer)
        part = self._buffer.read(size)
        self._current_pos += len(part)
        return part

    def _fill_buffer(self, size: int = -1) -> None:
        size = max(size, self._buffer._chunk_size)  # noqa: SLF001  # intra-package coupling
        while len(self._buffer) < size and not self._eof:
            # _SeekableRawReader is duck-typed to ByteBuffer.fill's read-based protocol
            bytes_read = self._buffer.fill(cast("io.IOBase", self._raw_reader))
            if bytes_read == 0:
                logger.debug("%s: reached EOF while filling buffer", self)
                self._eof = True

    def __str__(self) -> str:
        """Return a short human-readable description of the reader."""
        return f"smart_open.s3.Reader({self._bucket!r}, {self._key!r})"

    def __repr__(self) -> str:
        """Return an unambiguous representation of the reader."""
        return f"smart_open.s3.Reader(bucket={self._bucket!r}, key={self._key!r}, version_id={self._version_id!r}, buffer_size={self._buffer_size!r}, line_terminator={self._line_terminator!r})"


class MultipartWriter(io.BufferedIOBase):
    """Writes bytes to S3 using the multi part API.

    Implements the io.BufferedIOBase interface of the standard library.
    """

    name: str
    _client: S3Client
    _bucket: str
    _key: str
    _upload_id: str | None = None  # so `closed` property works in case __init__ fails and __del__ is called

    def __init__(  # noqa: PLR0913  # legacy public API; refactor in a dedicated PR
        self,
        bucket: str,
        key: str,
        part_size: int = DEFAULT_PART_SIZE,
        client: S3Client | None = None,
        client_kwargs: dict[str, Any] | None = None,
        writebuffer: io.BytesIO | None = None,
    ) -> None:
        adjusted_ps = smart_open.utils.clamp(part_size, MIN_PART_SIZE, MAX_PART_SIZE)
        if part_size != adjusted_ps:
            logger.warning("adjusting part_size from %s to %s", part_size, adjusted_ps)
            part_size = adjusted_ps
        self._part_size = part_size

        _initialize_boto3(self, client, client_kwargs, bucket, key)

        try:
            partial = functools.partial(
                self._client.create_multipart_upload,
                Bucket=bucket,
                Key=key,
            )
            self._upload_id = RETRY._do(partial)["UploadId"]  # noqa: SLF001  # intra-package coupling
        except botocore.client.ClientError as error:
            msg = f"the bucket {bucket!r} does not exist, or is forbidden for access ({error!r})"
            raise ValueError(msg) from error

        if writebuffer is None:
            self._buf = io.BytesIO()
        else:
            self._buf = writebuffer

        self._total_bytes = 0
        self._total_parts = 0
        self._parts: list[dict[str, Any]] = []

    def flush(self) -> None:
        """No-op flush; data is buffered until ``close`` or ``_upload_next_part``."""

    #
    # Override some methods from io.IOBase.
    #
    def close(self) -> None:
        """Complete the multipart upload (or abort) and close the stream."""
        logger.debug("close: called")
        if self.closed:
            return

        if self._buf.tell():
            self._upload_next_part()

        logger.debug("%s: completing multipart upload", self)
        if self._total_bytes and self._upload_id:
            partial = functools.partial(
                self._client.complete_multipart_upload,
                Bucket=self._bucket,
                Key=self._key,
                UploadId=self._upload_id,
                MultipartUpload=cast("CompletedMultipartUploadTypeDef", {"Parts": self._parts}),
            )
            RETRY._do(partial)  # noqa: SLF001  # intra-package coupling
            logger.debug("%s: completed multipart upload", self)
        elif self._upload_id:
            #
            # AWS complains with "The XML you provided was not well-formed or
            # did not validate against our published schema" when the input is
            # completely empty => abort the upload, no file created.
            #
            # We work around this by creating an empty file explicitly.
            #
            self._client.abort_multipart_upload(
                Bucket=self._bucket,
                Key=self._key,
                UploadId=self._upload_id,
            )
            self._client.put_object(
                Bucket=self._bucket,
                Key=self._key,
                Body=b"",
            )
            logger.debug("%s: wrote 0 bytes to imitate multipart upload", self)
        self._upload_id = None

    @property
    def closed(self) -> bool:
        """Return True if the stream is closed."""
        return self._upload_id is None

    def writable(self) -> bool:
        """Return True if the stream supports writing."""
        return True

    def seekable(self) -> bool:
        """Return True; we support `tell` but not `seek` or `truncate`."""
        return True

    def seek(self, offset: int, whence: int = constants.WHENCE_START) -> int:
        """Unsupported."""
        raise io.UnsupportedOperation

    def truncate(self, size: int | None = None) -> int:
        """Unsupported."""
        raise io.UnsupportedOperation

    def tell(self) -> int:
        """Return the current stream position."""
        return self._total_bytes

    #
    # io.BufferedIOBase methods.
    #
    def detach(self) -> io.RawIOBase:
        """Unsupported."""
        msg = "detach() not supported"
        raise io.UnsupportedOperation(msg)

    def write(self, b: Buffer) -> int:
        """Write the given buffer to the S3 file.

        The buffer can be bytes, bytearray, memoryview or any buffer interface
        implementation. For more information about buffers, see
        https://docs.python.org/3/c-api/buffer.html.

        There's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away.
        """
        offset = 0
        mv = memoryview(b)
        self._total_bytes += len(mv)

        #
        # botocore does not accept memoryview, otherwise we could've gotten
        # away with not needing to write a copy to the buffer aside from cases
        # where b is smaller than part_size
        #
        while offset < len(mv):
            start = offset
            end = offset + self._part_size - self._buf.tell()
            self._buf.write(mv[start:end])
            if self._buf.tell() < self._part_size:
                #
                # Not enough data to write a new part just yet. The assert
                # ensures that we've consumed all of the input buffer.
                #
                assert end >= len(mv)  # noqa: S101  # internal precondition; misuse should crash loudly
                return len(mv)

            self._upload_next_part()
            offset = end
        return len(mv)

    def terminate(self) -> None:
        """Cancel the underlying multipart upload."""
        upload_id = self._upload_id
        if upload_id is None:
            return
        logger.debug("%s: terminating multipart upload", self)
        self._client.abort_multipart_upload(
            Bucket=self._bucket,
            Key=self._key,
            UploadId=upload_id,
        )
        self._upload_id = None
        logger.debug("%s: terminated multipart upload", self)

    def to_boto3(self, resource: Any) -> Any:
        """Create an **independent** `boto3.s3.Object` instance.

        The returned object points to the same S3 object as this instance.
        Changes to the returned object will not affect the current instance.

        Args:
            resource: A ``boto3.resource`` instance.

        Returns:
            A ``boto3.s3.Object`` for the same bucket and key.
        """
        assert resource, "resource must be a boto3.resource instance"  # noqa: S101  # internal precondition; misuse should crash loudly
        return resource.Object(self._bucket, self._key)

    #
    # Internal methods.
    #
    def _upload_next_part(self) -> None:
        assert self._upload_id is not None  # noqa: S101  # only called during an active upload
        part_num = self._total_parts + 1
        logger.info(
            "%s: uploading part_num: %i, %i bytes (total %.3fGB)",
            self,
            part_num,
            self._buf.tell(),
            self._total_bytes / 1024.0**3,
        )
        self._buf.seek(0)

        #
        # Network problems in the middle of an upload are particularly
        # troublesome.  We don't want to abort the entire upload just because
        # of a temporary connection problem, so this part needs to be
        # especially robust.
        #
        upload = RETRY._do(  # noqa: SLF001  # intra-package coupling
            functools.partial(
                self._client.upload_part,
                Bucket=self._bucket,
                Key=self._key,
                UploadId=self._upload_id,
                PartNumber=part_num,
                Body=self._buf,
            )
        )

        self._parts.append({"ETag": upload["ETag"], "PartNumber": part_num})
        logger.debug("%s: upload of part_num #%i finished", self, part_num)

        self._total_parts += 1

        self._buf.seek(0)
        self._buf.truncate(0)

    def __enter__(self) -> Self:
        """Enter the multipart writer context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close or terminate the multipart writer on context exit."""
        if exc_type is not None:
            self.terminate()
        else:
            self.close()

    def __str__(self) -> str:
        """Return a short human-readable description of the writer."""
        return f"smart_open.s3.MultipartWriter({self._bucket!r}, {self._key!r})"

    def __repr__(self) -> str:
        """Return an unambiguous representation of the writer."""
        return f"smart_open.s3.MultipartWriter(bucket={self._bucket!r}, key={self._key!r}, part_size={self._part_size!r})"


class SinglepartWriter(io.BufferedIOBase):
    """Writes bytes to S3 using the single part API.

    Implements the io.BufferedIOBase interface of the standard library.

    This class buffers all of its input in memory until its `close` method is called. Only then will
    the data be written to S3 and the buffer is released.
    """

    name: str
    _client: S3Client
    _bucket: str
    _key: str
    _buf: io.BytesIO | None = None  # so `closed` property works in case __init__ fails and __del__ is called

    def __init__(
        self,
        bucket: str,
        key: str,
        client: S3Client | None = None,
        client_kwargs: dict[str, Any] | None = None,
        writebuffer: io.BytesIO | None = None,
    ) -> None:
        _initialize_boto3(self, client, client_kwargs, bucket, key)

        if writebuffer is None:
            self._buf = io.BytesIO()
        elif not writebuffer.seekable():
            msg = "writebuffer needs to be seekable"
            raise ValueError(msg)
        else:
            self._buf = writebuffer

    @property
    def _writebuffer(self) -> io.BytesIO:
        """Return the active write buffer, asserting the stream is still open."""
        buf = self._buf
        assert buf is not None  # noqa: S101  # buffer is set in __init__ and only cleared on close
        return buf

    def flush(self) -> None:
        """No-op flush; data is buffered until `close`."""

    #
    # Override some methods from io.IOBase.
    #
    def close(self) -> None:
        """Upload the buffered bytes as a single-part PUT and close the stream."""
        logger.debug("close: called")
        if self.closed:
            return

        self.seek(0)

        buf = self._writebuffer
        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=self._key,
                Body=buf,
            )
        except botocore.client.ClientError as e:
            msg = f"the bucket {self._bucket!r} does not exist, or is forbidden for access"
            raise ValueError(msg) from e
        else:
            logger.debug("%s: direct upload finished", self)
        finally:
            buf.close()

    @property
    def closed(self) -> bool:
        """Return True if the stream is closed."""
        return self._buf is None or self._buf.closed

    def readable(self) -> bool:
        """Propagate."""
        return self._writebuffer.readable()

    def writable(self) -> bool:
        """Propagate."""
        return self._writebuffer.writable()

    def seekable(self) -> bool:
        """Propagate."""
        return self._writebuffer.seekable()

    def seek(self, offset: int, whence: int = constants.WHENCE_START) -> int:
        """Propagate."""
        return self._writebuffer.seek(offset, whence)

    def truncate(self, size: int | None = None) -> int:
        """Propagate."""
        return self._writebuffer.truncate(size)

    def tell(self) -> int:
        """Propagate."""
        return self._writebuffer.tell()

    def write(self, b: Buffer) -> int:
        """Write the given buffer into the in-memory buffer.

        The buffer can be bytes, bytearray, memoryview or any buffer interface
        implementation. Content of the buffer will be written to S3 on close as a
        single-part upload. For more information about buffers, see
        https://docs.python.org/3/c-api/buffer.html.

        Args:
            b: The buffer to write.

        Returns:
            The number of bytes written.
        """
        return self._writebuffer.write(b)

    def read(self, size: int | None = -1) -> bytes:
        """Propagate."""
        return self._writebuffer.read(size)

    def read1(self, size: int | None = -1) -> bytes:
        """Propagate."""
        return self._writebuffer.read1(size)

    def terminate(self) -> None:
        """Close buffer and skip upload."""
        self._writebuffer.close()
        logger.debug("%s: terminated singlepart upload", self)

    #
    # Internal methods.
    #
    def __enter__(self) -> Self:
        """Enter the singlepart writer context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close or terminate the singlepart writer on context exit."""
        if exc_type is not None:
            self.terminate()
        else:
            self.close()

    def __str__(self) -> str:
        """Return a short human-readable description of the writer."""
        return f"smart_open.s3.SinglepartWriter({self._bucket!r}, {self._key!r})"

    def __repr__(self) -> str:
        """Return an unambiguous representation of the writer."""
        return f"smart_open.s3.SinglepartWriter(bucket={self._bucket!r}, key={self._key!r})"


def _accept_all(key: str) -> bool:  # noqa: ARG001  # interface conformance
    return True


def iter_bucket(  # noqa: PLR0913  # legacy public API; refactor in a dedicated PR
    bucket_name: str,
    prefix: str = "",
    accept_key: Callable[[str], bool] | None = None,
    key_limit: int | None = None,
    workers: int = 16,
    retries: int = 3,
    max_threads_per_fileobj: int = 4,
    client_kwargs: dict[str, Any] | None = None,
    session_kwargs: dict[str, Any] | None = None,
) -> Iterator[tuple[str, bytes]]:
    """Iterate and download all S3 objects under `s3://bucket_name/prefix`.

    Args:
        bucket_name: The name of the bucket.
        prefix: Limits the iteration to keys starting with the prefix.
        accept_key: A function that accepts a key name (unicode string) and
            returns True/False, signalling whether the given key should be
            downloaded. The default behavior is to accept all keys.
        key_limit: If specified, the iterator will stop after yielding this many
            results.
        workers: The number of objects to download concurrently. The entire
            operation uses a single ThreadPoolExecutor and shared thread-safe boto3
            S3.Client. Default: 16.
        retries: The number of time to retry a failed download. Default: 3.
        max_threads_per_fileobj: The maximum number of download threads per worker.
            The maximum size of the connection pool will be
            `workers * max_threads_per_fileobj + 1`. Default: 4.
        client_kwargs: Keyword arguments to pass when creating a new session.
            For a list of available names and values, see:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
        session_kwargs: Keyword arguments to pass when creating a new session.
            For a list of available names and values, see:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session

    Yields:
        Tuples of ``(key, content)``, where ``key`` is the full key name (does not
        include the bucket name) and ``content`` is the full contents of the key.

    Raises:
        ValueError: If bucket_name is None.

    Note:
        The keys are processed in parallel, using `workers` threads (default: 16),
        to speed up downloads greatly.

    Example:
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
    bucket: Any = bucket_name
    with contextlib.suppress(AttributeError):
        bucket = bucket.name

    if bucket is None:
        msg = "bucket_name may not be None"
        raise ValueError(msg)
    bucket_name = bucket

    total_size, key_no = 0, 0

    # thread-safe client to share across _list_bucket and _download_key calls
    # https://github.com/boto/boto3/blob/1.38.41/docs/source/guide/clients.rst?plain=1#L111
    if session_kwargs is None:
        session_kwargs = {}
    session = boto3.session.Session(**session_kwargs)
    if client_kwargs is None:
        client_kwargs = {}
    if "config" not in client_kwargs:
        client_kwargs["config"] = botocore.client.Config(
            max_pool_connections=workers * max_threads_per_fileobj + 1,  # 1 thread for _list_bucket
            tcp_keepalive=True,
            retries={"max_attempts": retries * 2, "mode": "adaptive"},
        )
    client = session.client("s3", **client_kwargs)

    transfer_config = boto3.s3.transfer.TransferConfig(max_concurrency=max_threads_per_fileobj)

    key_iterator = _list_bucket(
        bucket_name=bucket_name,
        prefix=prefix,
        accept_key=accept_key,
        client=client,
    )
    download_key = functools.partial(
        _download_key,
        bucket_name=bucket_name,
        retries=retries,
        client=client,
        transfer_config=transfer_config,
    )

    # Limit the iterator ('infinite' iterators are supported, key_limit=None is supported)
    key_iterator = itertools.islice(key_iterator, key_limit)

    with smart_open.concurrency.ThreadPoolExecutor(workers) as executor:
        result_iterator = executor.imap(download_key, key_iterator)
        for key_no, (key, content) in enumerate(result_iterator, start=1):
            # Skip deleted objects (404 responses)
            if key is None:
                continue

            if key_no % 1000 == 0:
                logger.info(
                    "yielding key #%i: %s, size %i (total %.1f MB)",
                    key_no,
                    key,
                    len(content),
                    total_size / 1024.0**2,
                )

            yield key, content
            total_size += len(content)
    logger.info(
        "processed %i keys, total size %.1f MB",
        key_no,
        total_size / 1024.0**2,
    )


def _list_bucket(
    *,
    bucket_name: str,
    client: S3Client,
    prefix: str = "",
    accept_key: Callable[[str], bool] = lambda k: True,  # noqa: ARG005  # interface conformance
) -> Iterator[str]:
    ctoken: str | None = None

    while True:
        # list_objects_v2 doesn't like a None value for ContinuationToken
        # so we don't set it if we don't have one.
        kwargs: dict[str, Any]
        if ctoken:
            kwargs = {"Bucket": bucket_name, "Prefix": prefix, "ContinuationToken": ctoken}
        else:
            kwargs = {"Bucket": bucket_name, "Prefix": prefix}
        response = client.list_objects_v2(**kwargs)
        for c in response.get("Contents", []):
            key = c["Key"]
            if accept_key(key):
                yield key
        ctoken = response.get("NextContinuationToken", None)
        if not ctoken:
            break


def _download_key(
    key_name: str,
    *,
    client: S3Client,
    bucket_name: str,
    retries: int,
    transfer_config: Any,
) -> tuple[str, bytes] | tuple[None, None] | None:
    # Sometimes, https://github.com/boto/boto/issues/2409 can happen
    # because of network issues on either side.
    # Retry up to 3 times to ensure its not a transient issue.
    for x in range(retries + 1):
        try:
            content_bytes = _download_fileobj(
                client=client,
                bucket_name=bucket_name,
                key_name=key_name,
                transfer_config=transfer_config,
            )
        except botocore.exceptions.ClientError as err:
            #
            # ignore 404 not found errors: they mean the object was deleted
            # after we listed the contents of the bucket, but before we
            # downloaded the object.
            #
            if "Error" in err.response and err.response["Error"].get("Code") == "404":
                return None, None
            # Actually fail on last pass through the loop
            if x == retries:
                raise
            # Otherwise, try again, as this might be a transient timeout
            continue
        return key_name, content_bytes
    return None


def _download_fileobj(
    *,
    client: S3Client,
    bucket_name: str,
    key_name: str,
    transfer_config: Any,
) -> bytes:
    #
    # This is a separate function only because it makes it easier to inject
    # exceptions during tests.
    #
    buf = io.BytesIO()
    client.download_fileobj(
        Bucket=bucket_name,
        Key=key_name,
        Fileobj=buf,
        Config=transfer_config,
    )
    return buf.getvalue()
