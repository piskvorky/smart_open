import unittest

import responses

import smart_open.http
import smart_open.s3


BYTES = b'i tried so hard and got so far but in the end it doesn\'t even matter'
URL = 'http://localhost'
HEADERS = {
    'Content-Length': str(len(BYTES)),
    'Accept-Ranges': 'bytes',
}


def request_callback(request):
    try:
        range_string = request.headers['range']
    except KeyError:
        return (200, HEADERS, BYTES)

    start, end = range_string.replace('bytes=', '').split('-', 1)
    start = int(start)
    if end:
        end = int(end)
    else:
        end = len(BYTES)
    return (200, HEADERS, BYTES[start:end])


class HttpTest(unittest.TestCase):

    @responses.activate
    def test_read_all(self):
        responses.add(responses.GET, URL, body=BYTES, stream=True)
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
        reader.seek(10, whence=smart_open.s3.CURRENT)
        self.assertEqual(reader.tell(), 30)
        read_bytes = reader.read(size=10)
        self.assertEqual(reader.tell(), 40)
        self.assertEqual(BYTES[30:40], read_bytes)

    @responses.activate
    def test_seek_from_end(self):
        responses.add_callback(responses.GET, URL, callback=request_callback)
        reader = smart_open.http.SeekableBufferedInputBase(URL)

        reader.seek(-10, whence=smart_open.s3.END)
        self.assertEqual(reader.tell(), len(BYTES) - 10)
        read_bytes = reader.read(size=10)
        self.assertEqual(reader.tell(), len(BYTES))
        self.assertEqual(BYTES[-10:], read_bytes)
