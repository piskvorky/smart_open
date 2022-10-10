# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements file-like objects for reading and writing to/from GCS."""

import logging

try:
    import google.cloud.exceptions
    import google.cloud.storage
    import google.auth.transport.requests
except ImportError:
    MISSING_DEPS = True

import smart_open.bytebuffer
import smart_open.utils

from smart_open import constants

logger = logging.getLogger(__name__)

SCHEME = "gs"
"""Supported scheme for GCS"""

_DEFAULT_MIN_PART_SIZE = 50 * 1024**2
"""Default minimum part size for GCS multipart uploads"""

_DEFAULT_WRITE_OPEN_KWARGS = {'ignore_flush': True}


def parse_uri(uri_as_string):
    sr = smart_open.utils.safe_urlsplit(uri_as_string)
    assert sr.scheme == SCHEME
    bucket_id = sr.netloc
    blob_id = sr.path.lstrip('/')
    return dict(scheme=SCHEME, bucket_id=bucket_id, blob_id=blob_id)


def open_uri(uri, mode, transport_params):
    parsed_uri = parse_uri(uri)
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    return open(parsed_uri['bucket_id'], parsed_uri['blob_id'], mode, **kwargs)


def open(
    bucket_id,
    blob_id,
    mode,
    min_part_size=_DEFAULT_MIN_PART_SIZE,
    client=None,  # type: google.cloud.storage.Client
    blob_properties=None,
    blob_open_kwargs=None,
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
    min_part_size: int, optional
        The minimum part size for multipart uploads.  For writing only.
    client: google.cloud.storage.Client, optional
        The GCS client to use when working with google-cloud-storage.
    blob_properties: dict, optional
        Set properties on blob before writing.  For writing only.
    blob_open_kwargs: dict, optional
        Set properties on the blob 
    """
    if blob_open_kwargs is None:
        blob_open_kwargs = {}
    if blob_properties is None:
        blob_properties = {}

    if client is None:
        client = google.cloud.storage.Client()

    bucket = client.bucket(bucket_id)
    if not bucket.exists():
        raise google.cloud.exceptions.NotFound(f'bucket {bucket_id} not found')

    if mode in (constants.READ_BINARY, 'r', 'rt'):
        blob = bucket.get_blob(blob_id)
        if blob is None:
            raise google.cloud.exceptions.NotFound(f'blob {blob_id} not found in {bucket_id}')

    elif mode in (constants.WRITE_BINARY, 'w', 'wt'):
        blob_open_kwargs = {**_DEFAULT_WRITE_OPEN_KWARGS, **blob_open_kwargs}
        blob = bucket.blob(
            blob_id,
            chunk_size=min_part_size,
        )

        for k, v in blob_properties.items():
            try:
                setattr(blob, k, v)
            except AttributeError:
                logger.warn(f'Unable to set property {k} on blob')

    else:
        raise NotImplementedError(f'GCS support for mode {mode} not implemented')

    return blob.open(mode, **blob_open_kwargs)
