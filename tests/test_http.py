# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import functools
import gzip
import unittest

import pytest
import responses

import smart_open.http
import smart_open.s3
import smart_open.constants
import requests

BYTES = b'i tried so hard and got so far but in the end it doesn\'t even matter'
GZIPPED_BYTES = gzip.compress(BYTES)
URL = 'http://localhost'
HTTPS_URL = 'https://localhost'
HEADERS = {
    'Accept-Ranges': 'bytes',
}


def request_callback(request, headers=HEADERS, data=BYTES):
    headers = headers.copy()
    range_string = request.headers.get('range', 'bytes=0-')

    start, end = range_string.replace('bytes=', '', 1).split('-', 1)
    start = int(start)
    end = int(end) if end else len(data)

    data = data[start:end]
    headers['Content-Length'] = str(len(data))

    return (200, headers, data)


class HttpTest(unittest.TestCase):

    @responses.activate
    def test_read_all(self):
        responses.add(responses.GET, URL, body=BYTES)
        reader = smart_open.http.SeekableBufferedInputBase(URL)
        read_bytes = reader.read()
        self.assertEqual(BYTES, read_bytes)

    @responses.activate
    def test_seek_from_start(self):
        responses.add_callback(responses.GET, URL, callback=request_callback)
        reader = smart_open.http.SeekableBufferedInputBase(URL)

        reader.seek(10)
        self.assertEqual(reader.tell(), 10)
        read_bytes = reader.read(size=10)
        self.assertEqual(reader.tell(), 20)
        self.assertEqual(BYTES[10:20], read_bytes)

        reader.seek(20)
        read_bytes = reader.read(size=10)
        self.assertEqual(BYTES[20:30], read_bytes)

        reader.seek(0)
        read_bytes = reader.read(size=10)
        self.assertEqual(BYTES[:10], read_bytes)

    @responses.activate
    def test_seek_from_current(self):
        responses.add_callback(responses.GET, URL, callback=request_callback)
        reader = smart_open.http.SeekableBufferedInputBase(URL)

        reader.seek(10)
        read_bytes = reader.read(size=10)
        self.assertEqual(BYTES[10:20], read_bytes)

        self.assertEqual(reader.tell(), 20)
        reader.seek(10, whence=smart_open.constants.WHENCE_CURRENT)
        self.assertEqual(reader.tell(), 30)
        read_bytes = reader.read(size=10)
        self.assertEqual(reader.tell(), 40)
        self.assertEqual(BYTES[30:40], read_bytes)

    @responses.activate
    def test_seek_from_end(self):
        responses.add_callback(responses.GET, URL, callback=request_callback)
        reader = smart_open.http.SeekableBufferedInputBase(URL)

        reader.seek(-10, whence=smart_open.constants.WHENCE_END)
        self.assertEqual(reader.tell(), len(BYTES) - 10)
        read_bytes = reader.read(size=10)
        self.assertEqual(reader.tell(), len(BYTES))
        self.assertEqual(BYTES[-10:], read_bytes)

    @responses.activate
    def test_headers_are_as_assigned(self):
        responses.add_callback(responses.GET, URL, callback=request_callback)

        # use default _HEADERS
        x = smart_open.http.BufferedInputBase(URL)
        # set different ones
        x.headers['Accept-Encoding'] = 'compress, gzip'
        x.headers['Other-Header'] = 'value'

        # use default again, global shoudn't overwritten from x
        y = smart_open.http.BufferedInputBase(URL)
        # should be default headers
        self.assertEqual(y.headers, {'Accept-Encoding': 'identity'})
        # should be assigned headers
        self.assertEqual(x.headers, {'Accept-Encoding': 'compress, gzip', 'Other-Header': 'value'})

    @responses.activate
    def test_headers(self):
        """Does the top-level http.open function handle headers correctly?"""
        responses.add_callback(responses.GET, URL, callback=request_callback)
        reader = smart_open.http.open(URL, 'rb', headers={'Foo': 'bar'})
        self.assertEqual(reader.headers['Foo'], 'bar')

    @responses.activate
    def test_https_seek_start(self):
        """Did the seek start over HTTPS work?"""
        responses.add_callback(responses.GET, HTTPS_URL, callback=request_callback)

        with smart_open.open(HTTPS_URL, "rb") as fin:
            read_bytes_1 = fin.read(size=10)
            fin.seek(0)
            read_bytes_2 = fin.read(size=10)
            self.assertEqual(read_bytes_1, read_bytes_2)

    @responses.activate
    def test_https_seek_forward(self):
        """Did the seek forward over HTTPS work?"""
        responses.add_callback(responses.GET, HTTPS_URL, callback=request_callback)

        with smart_open.open(HTTPS_URL, "rb") as fin:
            fin.seek(10)
            read_bytes = fin.read(size=10)
            self.assertEqual(BYTES[10:20], read_bytes)

    @responses.activate
    def test_https_seek_reverse(self):
        """Did the seek in reverse over HTTPS work?"""
        responses.add_callback(responses.GET, HTTPS_URL, callback=request_callback)

        with smart_open.open(HTTPS_URL, "rb") as fin:
            read_bytes_1 = fin.read(size=10)
            fin.seek(-10, whence=smart_open.constants.WHENCE_CURRENT)
            read_bytes_2 = fin.read(size=10)
            self.assertEqual(read_bytes_1, read_bytes_2)

    @responses.activate
    def test_timeout_attribute(self):
        timeout = 1
        responses.add_callback(responses.GET, URL, callback=request_callback)
        reader = smart_open.open(URL, "rb", transport_params={'timeout': timeout})
        assert hasattr(reader, 'timeout')
        assert reader.timeout == timeout

    @responses.activate
    def test_session_attribute(self):
        session = requests.Session()
        responses.add_callback(responses.GET, URL, callback=request_callback)
        reader = smart_open.open(URL, "rb", transport_params={'session': session})
        assert hasattr(reader, 'session')
        assert reader.session == session
        assert reader.read() == BYTES


@responses.activate
def test_seek_implicitly_enabled(numbytes=10):
    """Can we seek even if the server hasn't explicitly allowed it?"""
    callback = functools.partial(request_callback, headers={})
    responses.add_callback(responses.GET, HTTPS_URL, callback=callback)
    with smart_open.open(HTTPS_URL, 'rb') as fin:
        assert fin.seekable()
        first = fin.read(size=numbytes)
        fin.seek(-numbytes, whence=smart_open.constants.WHENCE_CURRENT)
        second = fin.read(size=numbytes)
        assert first == second


@responses.activate
def test_seek_implicitly_disabled():
    """Does seeking fail when the server has explicitly disabled it?"""
    callback = functools.partial(request_callback, headers={'Accept-Ranges': 'none'})
    responses.add_callback(responses.GET, HTTPS_URL, callback=callback)
    with smart_open.open(HTTPS_URL, 'rb') as fin:
        assert not fin.seekable()
        fin.read()
        with pytest.raises(OSError):
            fin.seek(0)


@responses.activate
def test_gzip_encoding_default_headers():
    """Does Accept-Encoding: identity prevent gzip compression?"""
    def callback(request):
        # Server respects Accept-Encoding: identity and sends uncompressed
        headers = HEADERS.copy()
        headers['Content-Length'] = str(len(BYTES))
        return (200, headers, BYTES)

    responses.add_callback(responses.GET, URL, callback=callback)
    reader = smart_open.http.SeekableBufferedInputBase(URL)
    read_bytes = reader.read()
    assert read_bytes == BYTES


@responses.activate
def test_gzip_encoding_explicit_request():
    """Does Accept-Encoding: gzip properly decompress via response.raw?"""
    def callback(request):
        # Server sees gzip in Accept-Encoding and returns compressed data
        if 'gzip' in request.headers.get('Accept-Encoding', ''):
            headers = HEADERS.copy()
            headers['Content-Encoding'] = 'gzip'
            headers['Content-Length'] = str(len(GZIPPED_BYTES))
            return (200, headers, GZIPPED_BYTES)
        else:
            headers = HEADERS.copy()
            headers['Content-Length'] = str(len(BYTES))
            return (200, headers, BYTES)

    responses.add_callback(responses.GET, URL, callback=callback)
    # Explicitly request gzip encoding
    reader = smart_open.http.SeekableBufferedInputBase(
        URL, headers={'Accept-Encoding': 'gzip'}
    )
    read_bytes = reader.read()
    assert read_bytes == BYTES  # Should be decompressed by requests/urllib3


@responses.activate
def test_gzip_consistency_between_read_methods():
    """Regression test: read() and read(size) should return same decompressed data."""
    def callback(request):
        # Server returns gzipped data when client accepts it
        if 'gzip' in request.headers.get('Accept-Encoding', ''):
            headers = HEADERS.copy()
            headers['Content-Encoding'] = 'gzip'
            headers['Content-Length'] = str(len(GZIPPED_BYTES))
            return (200, headers, GZIPPED_BYTES)
        else:
            headers = HEADERS.copy()
            headers['Content-Length'] = str(len(BYTES))
            return (200, headers, BYTES)

    responses.add_callback(responses.GET, URL, callback=callback)
    reader = smart_open.http.SeekableBufferedInputBase(URL, headers={'Accept-Encoding': 'gzip'})
    partial = reader.read(2) + reader.read()
    assert len(partial) == len(BYTES), f"Expected {len(BYTES)} bytes, got {len(partial)}"
    assert partial == BYTES
