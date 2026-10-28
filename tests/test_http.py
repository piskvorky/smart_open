#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import functools
import gzip
import io
import unittest

import pytest
import requests
import responses

import smart_open.constants
import smart_open.http
import smart_open.s3

BYTES = b"i tried so hard and got so far but in the end it doesn't even matter"
GZIPPED_BYTES = gzip.compress(BYTES)
URL = "http://localhost"
HTTPS_URL = "https://localhost"
HEADERS = {
    "Accept-Ranges": "bytes",
}


@responses.activate
def test_invalid_content_length_releases_response():
    """Release the streamed response before a header-parsing error propagates."""
    responses.add(responses.GET, URL, body=BYTES, headers={"Content-Length": "invalid"})
    try:
        with pytest.raises(ValueError, match="invalid literal for int") as exc_info:
            smart_open.open(URL, "rb")
        assert exc_info.type is ValueError
        assert responses.calls[-1].response.raw.closed
    finally:
        for call in responses.calls:
            call.response.close()


@pytest.mark.parametrize(
    "content_range",
    ["invalid", "bytes invalid-66/67", "bytes 10-invalid/67", "bytes 10-66/invalid"],
)
@responses.activate
def test_invalid_content_range_releases_response(content_range):
    """Release a failed range response and retain the explicit retry path."""
    responses.add(responses.GET, URL, body=BYTES, headers={"Content-Length": str(len(BYTES))})
    responses.add(responses.GET, URL, body=BYTES[10:], status=206, headers={"Content-Range": content_range})
    reader = smart_open.http.SeekableBufferedInputBase(URL, buffer_size=1)
    try:
        assert reader.read(2) == BYTES[:2]
        with pytest.raises(ValueError, match="invalid literal for int") as exc_info:
            reader.seek(10)
        assert exc_info.type is ValueError
        assert responses.calls[-1].response.raw.closed
        assert reader.tell() == 2
        with pytest.raises(OSError, match="an earlier range request"):
            reader.read(1)
        responses.add(
            responses.GET,
            URL,
            body=BYTES[10:],
            status=206,
            headers={"Content-Range": f"bytes 10-{len(BYTES) - 1}/{len(BYTES)}"},
        )
        assert reader.seek(10) == 10
        assert reader.read() == BYTES[10:]
    finally:
        reader.close()
        for call in responses.calls:
            call.response.close()


def request_callback(request, headers=HEADERS, data=BYTES):
    """Request callback."""
    headers = headers.copy()
    range_string = request.headers.get("range", "bytes=0-")

    start, end = range_string.replace("bytes=", "", 1).split("-", 1)
    start = int(start)
    end = int(end) if end else len(data)

    data = data[start:end]
    headers["Content-Length"] = str(len(data))

    return (206 if "range" in request.headers else 200, headers, data)


class HttpTest(unittest.TestCase):
    """Tests for Http."""

    @responses.activate
    def test_read_all(self):
        """Read all."""
        responses.add(responses.GET, URL, body=BYTES)
        reader = smart_open.http.SeekableBufferedInputBase(URL)
        read_bytes = reader.read()
        assert read_bytes == BYTES

    @responses.activate
    def test_seek_from_start(self):
        """Seek from start."""
        responses.add_callback(responses.GET, URL, callback=request_callback)
        reader = smart_open.http.SeekableBufferedInputBase(URL)

        reader.seek(10)
        assert reader.tell() == 10  # test uses inline magic value
        read_bytes = reader.read(size=10)
        assert reader.tell() == 20  # test uses inline magic value
        assert BYTES[10:20] == read_bytes

        reader.seek(20)
        read_bytes = reader.read(size=10)
        assert BYTES[20:30] == read_bytes

        reader.seek(0)
        read_bytes = reader.read(size=10)
        assert BYTES[:10] == read_bytes

    @responses.activate
    def test_seek_from_current(self):
        """Seek from current."""
        responses.add_callback(responses.GET, URL, callback=request_callback)
        reader = smart_open.http.SeekableBufferedInputBase(URL)

        reader.seek(10)
        read_bytes = reader.read(size=10)
        assert BYTES[10:20] == read_bytes

        assert reader.tell() == 20  # test uses inline magic value
        reader.seek(10, whence=smart_open.constants.WHENCE_CURRENT)
        assert reader.tell() == 30  # test uses inline magic value
        read_bytes = reader.read(size=10)
        assert reader.tell() == 40  # test uses inline magic value
        assert BYTES[30:40] == read_bytes

    @responses.activate
    def test_seek_forward_within_buffer(self):
        """Does forward seeking within buffered data avoid additional GET requests?"""
        responses.add_callback(responses.GET, URL, callback=request_callback)
        reader = smart_open.http.SeekableBufferedInputBase(URL, buffer_size=32)

        assert reader.read(5) == BYTES[:5]
        initial_calls = len(responses.calls)

        # Forward seek within buffer using WHENCE_CURRENT - no new GET request
        reader.seek(1, whence=smart_open.constants.WHENCE_CURRENT)
        assert reader.tell() == 6  # test uses inline magic value
        assert reader.read(5) == BYTES[6:11]

        # Forward seek within buffer using WHENCE_START - no new GET request
        reader.seek(13, whence=smart_open.constants.WHENCE_START)
        assert reader.tell() == 13  # test uses inline magic value
        assert reader.read(3) == BYTES[13:16]

        assert len(responses.calls) == initial_calls

    @responses.activate
    def test_seek_from_end(self):
        """Seek from end."""
        responses.add_callback(responses.GET, URL, callback=request_callback)
        reader = smart_open.http.SeekableBufferedInputBase(URL)

        reader.seek(-10, whence=smart_open.constants.WHENCE_END)
        assert reader.tell() == len(BYTES) - 10
        read_bytes = reader.read(size=10)
        assert reader.tell() == len(BYTES)
        assert BYTES[-10:] == read_bytes

    @responses.activate
    def test_headers_are_as_assigned(self):
        """Headers are as assigned."""
        responses.add_callback(responses.GET, URL, callback=request_callback)

        # use default _HEADERS
        x = smart_open.http.BufferedInputBase(URL)
        # set different ones
        x.headers["Accept-Encoding"] = "compress, gzip"
        x.headers["Other-Header"] = "value"

        # use default again, global shoudn't overwritten from x
        y = smart_open.http.BufferedInputBase(URL)
        # should be default headers
        assert y.headers == {"Accept-Encoding": "identity"}
        # should be assigned headers
        assert x.headers == {"Accept-Encoding": "compress, gzip", "Other-Header": "value"}

    @responses.activate
    def test_headers(self):
        """Does the top-level http.open function handle headers correctly?"""
        responses.add_callback(responses.GET, URL, callback=request_callback)
        reader = smart_open.http.open(URL, "rb", headers={"Foo": "bar"})
        assert reader.headers["Foo"] == "bar"

    @responses.activate
    def test_https_seek_start(self):
        """Did the seek start over HTTPS work?"""
        responses.add_callback(responses.GET, HTTPS_URL, callback=request_callback)

        with smart_open.open(HTTPS_URL, "rb") as fin:
            read_bytes_1 = fin.read(size=10)
            fin.seek(0)
            read_bytes_2 = fin.read(size=10)
            assert read_bytes_1 == read_bytes_2

    @responses.activate
    def test_https_seek_forward(self):
        """Did the seek forward over HTTPS work?"""
        responses.add_callback(responses.GET, HTTPS_URL, callback=request_callback)

        with smart_open.open(HTTPS_URL, "rb") as fin:
            fin.seek(10)
            read_bytes = fin.read(size=10)
            assert BYTES[10:20] == read_bytes

    @responses.activate
    def test_https_seek_reverse(self):
        """Did the seek in reverse over HTTPS work?"""
        responses.add_callback(responses.GET, HTTPS_URL, callback=request_callback)

        with smart_open.open(HTTPS_URL, "rb") as fin:
            read_bytes_1 = fin.read(size=10)
            fin.seek(-10, whence=smart_open.constants.WHENCE_CURRENT)
            read_bytes_2 = fin.read(size=10)
            assert read_bytes_1 == read_bytes_2

    @responses.activate
    def test_timeout_attribute(self):
        """Timeout attribute."""
        timeout = 1
        responses.add_callback(responses.GET, URL, callback=request_callback)
        reader = smart_open.open(URL, "rb", transport_params={"timeout": timeout})
        assert hasattr(reader, "timeout")
        assert reader.timeout == timeout

    @responses.activate
    def test_session_attribute(self):
        """Session attribute."""
        session = requests.Session()
        responses.add_callback(responses.GET, URL, callback=request_callback)
        reader = smart_open.open(URL, "rb", transport_params={"session": session})
        assert hasattr(reader, "session")
        assert reader.session == session
        assert reader.read() == BYTES


@responses.activate
def test_seek_implicitly_enabled():
    """Can we seek even if the server hasn't explicitly allowed it?"""
    numbytes = 10
    callback = functools.partial(request_callback, headers={})
    responses.add_callback(responses.GET, HTTPS_URL, callback=callback)
    with smart_open.open(HTTPS_URL, "rb") as fin:
        assert fin.seekable()
        first = fin.read(size=numbytes)
        fin.seek(-numbytes, whence=smart_open.constants.WHENCE_CURRENT)
        second = fin.read(size=numbytes)
        assert first == second


@responses.activate
def test_seek_implicitly_disabled():
    """Does seeking fail when the server has explicitly disabled it?"""
    callback = functools.partial(request_callback, headers={"Accept-Ranges": "none"})
    responses.add_callback(responses.GET, HTTPS_URL, callback=callback)
    with smart_open.open(HTTPS_URL, "rb") as fin:
        assert not fin.seekable()
        fin.read()
        with pytest.raises(OSError, match="stream is not seekable"):
            fin.seek(0)


@responses.activate
def test_gzip_encoding_default_headers():
    """Does Accept-Encoding: identity prevent gzip compression?"""

    def callback(request):  # noqa: ARG001  # unused fn arg in fixture
        # Server respects Accept-Encoding: identity and sends uncompressed
        headers = HEADERS.copy()
        headers["Content-Length"] = str(len(BYTES))
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
        if "gzip" in request.headers.get("Accept-Encoding", ""):
            headers = HEADERS.copy()
            headers["Content-Encoding"] = "gzip"
            headers["Content-Length"] = str(len(GZIPPED_BYTES))
            return (200, headers, GZIPPED_BYTES)
        headers = HEADERS.copy()
        headers["Content-Length"] = str(len(BYTES))
        return (200, headers, BYTES)

    responses.add_callback(responses.GET, URL, callback=callback)
    # Explicitly request gzip encoding
    reader = smart_open.http.SeekableBufferedInputBase(URL, headers={"Accept-Encoding": "gzip"})
    read_bytes = reader.read()
    assert read_bytes == BYTES  # Should be decompressed by requests/urllib3

    # Combining multiple read calls also works
    reader.seek(0)
    partial = reader.read(2) + reader.read(1000)
    assert partial == BYTES


@responses.activate
def test_read_after_read_to_eof():
    """Reading after reading to EOF should return empty bytes."""
    responses.add_callback(responses.GET, URL, callback=request_callback)
    reader = smart_open.http.SeekableBufferedInputBase(URL)

    # Read to EOF
    result = reader.read(-1)
    assert len(result) == len(BYTES)
    assert reader.tell() == len(BYTES)

    # Read should return empty bytes
    result = reader.read()
    assert result == b""

    # Read with size should also return empty bytes
    result = reader.read(10)
    assert result == b""


@responses.activate
def test_read_after_seek_to_eof():
    """Reading after seeking to EOF should return empty bytes."""
    responses.add_callback(responses.GET, URL, callback=request_callback)
    reader = smart_open.http.SeekableBufferedInputBase(URL)

    # Seek to EOF
    reader.seek(0, whence=smart_open.constants.WHENCE_END)
    assert reader.tell() == len(BYTES)

    # Read should return empty bytes
    result = reader.read()
    assert result == b""


@responses.activate
def test_readline_after_seek_to_eof():
    """Readline after seeking to EOF should return empty bytes, like read()."""
    responses.add_callback(responses.GET, URL, callback=request_callback)
    reader = smart_open.http.SeekableBufferedInputBase(URL)
    reader.seek(0, whence=smart_open.constants.WHENCE_END)
    assert reader.readline() == b""
    assert reader.readline(10) == b""

    # Read with size should also return empty bytes
    result = reader.read(10)
    assert result == b""


@responses.activate
def test_read_with_invalid_size():
    """Read with size < -1 should raise ValueError."""
    responses.add_callback(responses.GET, URL, callback=request_callback)
    reader = smart_open.http.SeekableBufferedInputBase(URL)

    with pytest.raises(ValueError, match="size must be >= -1"):
        reader.read(-2)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/a/b.gz", "b.gz"),
        ("/foo/", ""),  # trailing slash -> empty, not "foo" (regression: PurePosixPath.name diverges)
        ("/", ""),
        ("", ""),
    ],
)
@responses.activate
def test_open_sets_fobj_name(path, expected):
    """`http.open` derives `fobj.name` from the URL path, posix-basename style."""
    url = HTTPS_URL + path
    responses.add_callback(responses.GET, url, callback=request_callback)
    fobj = smart_open.http.open(url, "rb")
    assert fobj.name == expected


@pytest.mark.parametrize(
    "reader_class", [smart_open.http.BufferedInputBase, smart_open.http.SeekableBufferedInputBase]
)
@responses.activate
def test_close_releases_response(reader_class):
    """Release a partially read response when closing its reader."""
    responses.add(responses.GET, URL, body=BYTES)
    reader = reader_class(URL, buffer_size=1)
    response = reader.response
    assert reader.read(1) == BYTES[:1]
    assert not response.raw.closed
    reader.close()
    assert response.raw.closed
    assert reader.closed
    reader.close()


@responses.activate
def test_failed_open_releases_response():
    """Raise on and release a response that cannot be used for reading."""
    responses.add(responses.GET, URL, status=404, body=b"not found")
    with pytest.raises(requests.HTTPError, match="404"):
        smart_open.http.SeekableBufferedInputBase(URL)
    assert responses.calls[-1].response.raw.closed


@pytest.mark.parametrize("offset", [10, len(BYTES)])
@responses.activate
def test_seek_releases_response(offset):
    """Release the previous response before a range request or a seek to EOF."""
    responses.add_callback(responses.GET, URL, callback=request_callback)
    reader = smart_open.http.SeekableBufferedInputBase(URL, buffer_size=1)
    response = reader.response
    assert reader.read(1) == BYTES[:1]
    assert not response.raw.closed
    reader.seek(offset)
    assert response.raw.closed
    assert reader.read(1) == BYTES[offset : offset + 1]
    reader.close()


@responses.activate
def test_buffered_seek_keeps_response_open():
    """A seek within buffered data keeps the current response usable."""
    responses.add_callback(responses.GET, URL, callback=request_callback)
    with smart_open.http.SeekableBufferedInputBase(URL, buffer_size=32) as reader:
        response = reader.response
        assert reader.read(1) == BYTES[:1]
        reader.seek(1)
        reader.seek(5)
        assert not response.raw.closed
        assert reader.read() == BYTES[5:]
        assert len(responses.calls) == 1


@responses.activate
def test_seek_releases_failed_response():
    """Raise on and release a range response that cannot be used for reading."""
    responses.add(responses.GET, URL, body=BYTES)
    responses.add(responses.GET, URL, status=503, body=b"unavailable")
    responses.add(responses.GET, URL, status=416, body=b"not satisfiable")
    with smart_open.http.SeekableBufferedInputBase(URL, buffer_size=1) as reader:
        with pytest.raises(requests.HTTPError, match="503"):
            reader.seek(10)
        assert responses.calls[-1].response.raw.closed
        assert reader.tell() == 0
        assert reader.seek(10) == 10  # 416 means past EOF of a stream of unknown length
        assert responses.calls[-1].response.raw.closed
        assert reader.read() == b""


@responses.activate
def test_failed_seek_keeps_position_and_drops_stale_data():
    """Keep the position and drop stale buffered data when a range request fails."""
    responses.add(responses.GET, URL, body=BYTES)
    responses.add(responses.GET, URL, body=requests.ConnectionError("range failed"))
    responses.add_callback(responses.GET, URL, callback=request_callback)
    with smart_open.http.SeekableBufferedInputBase(URL, buffer_size=8) as reader:
        assert reader.read(1) == BYTES[:1]
        with pytest.raises(requests.ConnectionError):
            reader.seek(20)
        assert reader.tell() == 1
        assert reader.seek(1) == 1
        assert reader.read(3) == BYTES[1:4]
        assert reader.seek(20) == 20
        assert reader.read(3) == BYTES[20:23]


@responses.activate
def test_seek_rejects_ignored_range():
    """Raise when a nonzero range request gets the full body, and still allow rewinding."""
    responses.add(responses.GET, URL, body=BYTES)
    with smart_open.http.SeekableBufferedInputBase(URL) as reader:
        with pytest.raises(io.UnsupportedOperation, match="ignored the range request"):
            reader.seek(10)
        assert responses.calls[-1].response.raw.closed
        assert reader.tell() == 0
        assert reader.seek(0) == 0
        assert reader.read() == BYTES


@responses.activate
def test_seek_from_end_needs_length():
    """Refuse to seek from the end of a response of unknown length."""
    responses.add(responses.GET, URL, body=BYTES)
    with (
        smart_open.http.SeekableBufferedInputBase(URL) as reader,
        pytest.raises(io.UnsupportedOperation, match="unknown length"),
    ):
        reader.seek(-3, smart_open.constants.WHENCE_END)


@pytest.mark.parametrize(
    ("first", "last"),
    [(8, len(BYTES) - 1), (10, 9), (10, len(BYTES))],
    ids=["wrong_start", "end_before_start", "end_past_total"],
)
@responses.activate
def test_seek_rejects_other_range(first, last):
    """Raise when a partial response starts elsewhere than the requested offset, or is invalid."""
    content_range = f"bytes {first}-{last}/{len(BYTES)}"
    partial = (206, {"Content-Range": content_range}, BYTES[first : last + 1])
    responses.add_callback(
        responses.GET, URL, callback=lambda r: partial if "range" in r.headers else (200, {}, BYTES)
    )
    with (
        smart_open.http.SeekableBufferedInputBase(URL) as reader,
        pytest.raises(OSError, match=content_range),
    ):
        reader.seek(10)


@pytest.mark.parametrize(
    ("read", "total", "at_end", "calls"),
    [
        (lambda reader: reader.read(), len(BYTES), None, 8),
        (lambda reader: reader.read(len(BYTES)), len(BYTES), None, 8),
        (lambda reader: b"".join(iter(reader.readline, b"")), len(BYTES), None, 8),
        (lambda reader: reader.read(), "*", (416, {}, b""), 9),
        (
            lambda reader: reader.read(),
            "*",
            (200, {"Content-Length": str(len(BYTES))}, BYTES),
            9,
        ),  # Range ignored
    ],
    ids=["read", "read_size", "readline", "unknown_total_416", "unknown_total_200"],
)
@responses.activate
def test_read_continues_after_capped_range(read, total, at_end, calls):
    """Request the rest from where a server cut a partial response short."""

    def callback(request):
        first = int(request.headers.get("range", "bytes=0-").removeprefix("bytes=").partition("-")[0])
        if not first:  # also a rewind, answered without Content-Range
            return 200, {"ETag": '"v1"'}, BYTES
        assert request.headers["If-Match"] == '"v1"'  # every range pins the first version
        if first >= len(BYTES):
            return at_end
        last = min(first + 10, len(BYTES)) - 1  # the server caps every range at 10 bytes
        return 206, {"Content-Range": f"bytes {first}-{last}/{total}"}, BYTES[first : last + 1]

    responses.add_callback(responses.GET, URL, callback=callback)
    with smart_open.http.SeekableBufferedInputBase(URL) as reader:
        reader.seek(5)
        assert read(reader) == BYTES[5:]
        assert len(responses.calls) == calls  # no request past a known total
        reader.seek(0)
        assert reader.read() == BYTES


@pytest.mark.parametrize(
    ("version", "precondition"),
    [
        ({"ETag": '"v1"'}, {"If-Match": '"v1"'}),
        (
            {"ETag": 'W/"v1"', "Last-Modified": "Tue, 27 Oct 2026 10:00:00 GMT"},
            {"If-Unmodified-Since": "Tue, 27 Oct 2026 10:00:00 GMT"},
        ),
    ],
)
@responses.activate
def test_seek_requires_same_version(version, precondition):
    """Fail a seek with 412 when the resource changed since the first response."""

    def callback(request):
        if "range" not in request.headers:
            return 200, version, BYTES
        name, stale = next(iter(precondition.items()))
        if request.headers.get(name) == stale:  # the resource was replaced in between
            return 412, {}, b""
        return request_callback(request)

    responses.add_callback(responses.GET, URL, callback=callback)
    with (
        smart_open.http.SeekableBufferedInputBase(URL) as reader,
        pytest.raises(requests.HTTPError, match=r"412.*If-Match: \*.*Accept-Encoding: identity"),
    ):
        reader.seek(10)


@pytest.mark.parametrize(
    ("body", "match"),
    [(BYTES[5:8], "has 3 bytes"), (BYTES[5:], "has 63 bytes"), (BYTES[5:15], "416")],
    ids=["short", "long", "416_before_total"],
)
@responses.activate
def test_read_rejects_inconsistent_partial_response(body, match):
    """Raise instead of skipping, repeating or truncating bytes when a capped response does not add up."""

    def callback(request):
        if "range" not in request.headers:
            return request_callback(request)
        if request.headers["range"] == "bytes=5-":  # declares 10 bytes, no Content-Length to catch it
            return 206, {"Content-Range": f"bytes 5-14/{len(BYTES)}"}, body
        return 416, {}, b""  # the rest is gone before the declared total

    responses.add_callback(responses.GET, URL, callback=callback)
    with smart_open.http.SeekableBufferedInputBase(URL) as reader:
        reader.seek(5)
        with pytest.raises(OSError, match=match):
            reader.read()


@pytest.mark.parametrize("read", [lambda r: r.read(), lambda r: r.readline()], ids=["read", "readline"])
@responses.activate
def test_read_after_failed_seek_raises(read):
    """Raise on reads after a failed range request instead of reading as EOF, until a seek succeeds."""
    responses.add(responses.GET, URL, body=BYTES)
    responses.add(responses.GET, URL, status=503, body=b"unavailable")
    responses.add_callback(responses.GET, URL, callback=request_callback)
    with smart_open.http.SeekableBufferedInputBase(URL) as reader:
        with pytest.raises(requests.HTTPError, match="503"):
            reader.seek(10)
        with pytest.raises(OSError, match="seek to retry"):
            read(reader)
        assert reader.seek(reader.tell()) == 0
        assert read(reader) == BYTES


@responses.activate
def test_read_after_failed_continuation_raises():
    """Keep raising after a failed request for the rest of a capped range, until a seek succeeds."""
    failures = [503]  # the first request for the rest fails, a retry succeeds

    def callback(request):
        first = int(request.headers.get("range", "bytes=0-").removeprefix("bytes=").partition("-")[0])
        if not first:
            return 200, {}, BYTES
        if first == 15 and failures:
            return failures.pop(), {}, b"unavailable"
        last = min(first + 10, len(BYTES)) - 1  # the server caps every range at 10 bytes
        return 206, {"Content-Range": f"bytes {first}-{last}/{len(BYTES)}"}, BYTES[first : last + 1]

    responses.add_callback(responses.GET, URL, callback=callback)
    with smart_open.http.SeekableBufferedInputBase(URL) as reader:
        reader.seek(5)
        with pytest.raises(requests.HTTPError, match="503"):
            reader.read(len(BYTES))
        with pytest.raises(OSError, match="seek to retry"):
            reader.read(len(BYTES))
        assert reader.seek(reader.tell()) == 5
        assert reader.read() == BYTES[5:]
