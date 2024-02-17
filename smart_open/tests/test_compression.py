# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import io
import gzip
import pytest

import smart_open.compression

import zstandard as zstd



plain = 'доброе утро планета!'.encode()


def label(thing, name):
    setattr(thing, 'name', name)
    return thing


@pytest.mark.parametrize(
    'fileobj,compression,filename',
    [
        (io.BytesIO(plain), 'disable', None),
        (io.BytesIO(plain), 'disable', ''),
        (io.BytesIO(plain), 'infer_from_extension', 'file.txt'),
        (io.BytesIO(plain), 'infer_from_extension', 'file.TXT'),
        (io.BytesIO(plain), '.unknown', ''),
        (io.BytesIO(gzip.compress(plain)), 'infer_from_extension', 'file.gz'),
        (io.BytesIO(gzip.compress(plain)), 'infer_from_extension', 'file.GZ'),
        (label(io.BytesIO(gzip.compress(plain)), 'file.gz'), 'infer_from_extension', ''),
        (io.BytesIO(gzip.compress(plain)), '.gz', 'file.gz'),
        (io.BytesIO(zstd.ZstdCompressor().compress(plain)), 'infer_from_extension', 'file.zst'),
        (io.BytesIO(zstd.ZstdCompressor().compress(plain)), 'infer_from_extension', 'file.ZST'),
        (label(io.BytesIO(zstd.ZstdCompressor().compress(plain)), 'file.zst'), 'infer_from_extension', ''),
        (io.BytesIO(zstd.ZstdCompressor().compress(plain)), '.zst', 'file.zst'),
    ]
)
def test_compression_wrapper_read(fileobj, compression, filename):
    wrapped = smart_open.compression.compression_wrapper(fileobj, 'rb', compression, filename)
    assert wrapped.read() == plain
