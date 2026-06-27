#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements file-like objects for reading and writing to/from GCS."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, TypedDict

try:
    import google.auth.transport.requests
    import google.cloud.exceptions
    import google.cloud.storage
except ImportError:
    MISSING_DEPS = True

import smart_open.bytebuffer
import smart_open.utils
from smart_open import constants

if TYPE_CHECKING:
    import io

    from smart_open._typing import TransportParams

logger = logging.getLogger(__name__)

SCHEMES = ("gcs", "gs")
"""Supported schemes for GCS.  ``gcs`` is canonical; ``gs`` is kept as a backwards-compatible alias."""

_DEFAULT_MIN_PART_SIZE = 50 * 1024**2
"""Default minimum part size for GCS multipart uploads"""

_DEFAULT_WRITE_OPEN_KWARGS = {"ignore_flush": True}


class _GCSUri(TypedDict):
    scheme: str
    bucket_id: str
    blob_id: str


def parse_uri(uri_as_string: str) -> _GCSUri:
    """Parse a ``gcs://`` or ``gs://`` URI into its bucket and blob components."""
    sr = smart_open.utils.safe_urlsplit(uri_as_string)
    assert sr.scheme in SCHEMES  # noqa: S101  # internal precondition; misuse should crash loudly
    bucket_id = sr.netloc
    blob_id = sr.path.lstrip("/")
    return {"scheme": sr.scheme, "bucket_id": bucket_id, "blob_id": blob_id}


def open_uri(uri: str, mode: str, transport_params: TransportParams) -> io.IOBase:
    """Open a GCS URI using the given mode and transport params."""
    parsed_uri = parse_uri(uri)
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    return open(parsed_uri["bucket_id"], parsed_uri["blob_id"], mode, **kwargs)


def open(  # noqa: PLR0913  # legacy public API; refactor in a dedicated PR
    bucket_id: str,
    blob_id: str,
    mode: str,
    min_part_size: int = _DEFAULT_MIN_PART_SIZE,
    client: google.cloud.storage.Client | None = None,
    get_blob_kwargs: dict[str, Any] | None = None,
    blob_properties: dict[str, Any] | None = None,
    blob_open_kwargs: dict[str, Any] | None = None,
) -> io.IOBase:
    """Open an GCS blob for reading or writing.

    Args:
        bucket_id: The name of the bucket this object resides in.
        blob_id: The name of the blob within the bucket.
        mode: The mode for opening the object. Must be either "rb" or "wb".
        min_part_size: The minimum part size for multipart uploads. For writing only.
        client: The GCS client to use when working with google-cloud-storage.
        get_blob_kwargs: Additional keyword arguments to propagate to the bucket.get_blob
            method of the google-cloud-storage library. For reading only.
        blob_properties: Set properties on blob before writing. For writing only.
        blob_open_kwargs: Additional keyword arguments to propagate to the blob.open method
            of the google-cloud-storage library.

    Returns:
        A file-like object for the GCS blob.

    Raises:
        NotImplementedError: If `mode` is not one of the supported modes.
    """
    if blob_open_kwargs is None:
        blob_open_kwargs = {}

    if mode in (constants.READ_BINARY, "r", "rt"):
        _blob = Reader(
            bucket=bucket_id,
            key=blob_id,
            client=client,
            get_blob_kwargs=get_blob_kwargs,
            blob_open_kwargs=blob_open_kwargs,
        )

    elif mode in (constants.WRITE_BINARY, "w", "wt"):
        _blob = Writer(
            bucket=bucket_id,
            blob=blob_id,
            min_part_size=min_part_size,
            client=client,
            blob_properties=blob_properties,
            blob_open_kwargs=blob_open_kwargs,
        )

    else:
        msg = f"GCS support for mode {mode} not implemented"
        raise NotImplementedError(msg)

    return _blob


def Reader(  # noqa: N802  # factory function named after returned class
    bucket: str,
    key: str,
    client: google.cloud.storage.Client | None = None,
    get_blob_kwargs: dict[str, Any] | None = None,
    blob_open_kwargs: dict[str, Any] | None = None,
) -> io.IOBase:
    """Return a file-like object for reading the GCS blob `key` from `bucket`."""
    if get_blob_kwargs is None:
        get_blob_kwargs = {}
    if blob_open_kwargs is None:
        blob_open_kwargs = {}
    if client is None:
        client = google.cloud.storage.Client()

    bkt = client.bucket(bucket)
    blob = bkt.get_blob(key, **get_blob_kwargs)

    if blob is None:
        msg = f"blob {key} not found in {bucket}"
        raise google.cloud.exceptions.NotFound(msg)

    return blob.open("rb", **blob_open_kwargs)


def Writer(  # noqa: N802, PLR0913  # factory function named after returned class; legacy public API
    bucket: str,
    blob: str,
    min_part_size: int | None = None,
    client: google.cloud.storage.Client | None = None,
    blob_properties: dict[str, Any] | None = None,
    blob_open_kwargs: dict[str, Any] | None = None,
) -> io.IOBase:
    """Return a file-like object for writing to GCS blob `blob` in `bucket`."""
    if blob_open_kwargs is None:
        blob_open_kwargs = {}
    if blob_properties is None:
        blob_properties = {}
    if client is None:
        client = google.cloud.storage.Client()

    blob_open_kwargs = {**_DEFAULT_WRITE_OPEN_KWARGS, **blob_open_kwargs}

    g_blob = client.bucket(bucket).blob(
        blob,
        chunk_size=min_part_size,
    )

    for k, v in blob_properties.items():
        setattr(g_blob, k, v)

    return g_blob.open("wb", **blob_open_kwargs)
