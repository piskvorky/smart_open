import io
import logging

import requests
import six

if six.PY2:
    import httplib
else:
    import http.client as httplib

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

WEBHDFS_MIN_PART_SIZE = 50 * 1024**2  # minimum part size for HDFS multipart uploads


class BufferedInputBase(io.BufferedIOBase):
    def __init__(self, uri):
        self._uri = uri

        payload = {"op": "OPEN", "offset": 0}
        self._response = requests.get("http://" + self._uri, params=payload, stream=True)
        self._buf = b''

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        """Flush and close this stream."""
        logger.debug("close: called")

    def readable(self):
        """Return True if the stream can be read from."""
        return True

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError.

        We offer only seek support, and no truncate support."""
        return False

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size=None):
        if size is None:
            self._buf, retval = b'', self._buf + self._response.raw.read()
            return retval
        elif size < len(self._buf):
            self._buf, retval = self._buf[size:], self._buf[:size]
            return retval

        try:
            while len(self._buf) < size:
                self._buf += self._response.raw.readline()
        except StopIteration:
            pass

        self._buf, retval = self._buf[size:], retval[:size]
        return retval

    def read1(self, size=-1):
        """This is the same as read()."""
        return self.read(size=size)

    def readinto(self, b):
        """Read up to len(b) bytes into b, and return the number of bytes
        read."""
        data = self.read(len(b))
        if not data:
            return 0
        b[:len(data)] = data
        return len(data)

    def readline(self):
        self._buf, retval = b'', self._buf + self._response.raw.readline()
        return retval


class BufferedOutputBase(io.BufferedIOBase):
    def __init__(self, uri_path, min_part_size=WEBHDFS_MIN_PART_SIZE):
        self.uri_path = uri_path
        self._closed = False
        self.min_part_size = min_part_size
        # creating empty file first
        payload = {"op": "CREATE", "overwrite": True}
        init_response = requests.put("http://" + self.uri_path,
                                     params=payload, allow_redirects=False)
        if not init_response.status_code == httplib.TEMPORARY_REDIRECT:
            raise WebHdfsException(str(init_response.status_code) + "\n" + init_response.content)
        uri = init_response.headers['location']
        response = requests.put(uri, data="", headers={'content-type': 'application/octet-stream'})
        if not response.status_code == httplib.CREATED:
            raise WebHdfsException(str(response.status_code) + "\n" + response.content)
        self.lines = []
        self.parts = 0
        self.chunk_bytes = 0
        self.total_size = 0

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    #
    # Override some methods from io.IOBase.
    #
    def writable(self):
        """Return True if the stream supports writing."""
        return True

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")

    def _upload(self, data):
        payload = {"op": "APPEND"}
        init_response = requests.post("http://" + self.uri_path,
                                      params=payload, allow_redirects=False)
        if not init_response.status_code == httplib.TEMPORARY_REDIRECT:
            raise WebHdfsException(str(init_response.status_code) + "\n" + init_response.content)
        uri = init_response.headers['location']
        response = requests.post(uri, data=data,
                                 headers={'content-type': 'application/octet-stream'})
        if not response.status_code == httplib.OK:
            raise WebHdfsException(str(response.status_code) + "\n" + repr(response.content))

    def write(self, b):
        """
        Write the given bytes (binary string) into the WebHDFS file from constructor.

        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if not isinstance(b, six.binary_type):
            raise TypeError("input must be a binary string")

        self.lines.append(b)
        self.chunk_bytes += len(b)
        self.total_size += len(b)

        if self.chunk_bytes >= self.min_part_size:
            buff = b"".join(self.lines)
            logger.info(
                "uploading part #%i, %i bytes (total %.3fGB)",
                self.parts, len(buff), self.total_size / 1024.0 ** 3
            )
            self._upload(buff)
            logger.debug("upload of part #%i finished", self.parts)
            self.parts += 1
            self.lines, self.chunk_bytes = [], 0

    def close(self):
        buff = b"".join(self.lines)
        if buff:
            logger.info(
                "uploading last part #%i, %i bytes (total %.3fGB)",
                self.parts, len(buff), self.total_size / 1024.0 ** 3
            )
            self._upload(buff)
            logger.debug("upload of last part #%i finished", self.parts)
        self._closed = True

    @property
    def closed(self):
        return self._closed


class WebHdfsException(Exception):
    def __init__(self, msg=str()):
        self.msg = msg
        super(WebHdfsException, self).__init__(self.msg)
