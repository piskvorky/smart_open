# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import bz2
import gzip
import io
import lzma

import pytest
import zstandard as zstd

import smart_open.compression

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
        (io.BytesIO(lzma.compress(plain)), 'infer_from_extension', 'file.xz'),
        (io.BytesIO(lzma.compress(plain)), 'infer_from_extension', 'file.XZ'),
        (label(io.BytesIO(lzma.compress(plain)), 'file.xz'), 'infer_from_extension', ''),
        (io.BytesIO(lzma.compress(plain)), '.xz', 'file.xz'),
        (io.BytesIO(bz2.compress(plain)), 'infer_from_extension', 'file.bz2'),
        (io.BytesIO(bz2.compress(plain)), 'infer_from_extension', 'file.BZ2'),
        (label(io.BytesIO(bz2.compress(plain)), 'file.bz2'), 'infer_from_extension', ''),
        (io.BytesIO(bz2.compress(plain)), '.bz2', 'file.bz2'),
    ]
)
def test_compression_wrapper_read(fileobj, compression, filename):
    wrapped = smart_open.compression.compression_wrapper(fileobj, 'rb', compression, filename)
    assert wrapped.read() == plain
