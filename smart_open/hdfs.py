import io
import logging
import subprocess

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class CliRawInputBase(io.RawIOBase):
    """Reads bytes from HDFS via the "hdfs dfs" command-line interface.

    Implements the io.RawIOBase interface of the standard library.
    """

    def __init__(self, uri):
        self._uri = uri
        self._sub = subprocess.Popen(["hdfs", "dfs", '-cat', self._uri], stdout=subprocess.PIPE)

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        """Flush and close this stream."""
        logger.debug("close: called")
        self._sub.terminate()
        self._sub = None

    def readable(self):
        """Return True if the stream can be read from."""
        return self._sub is not None

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError."""
        return False

    #
    # io.RawIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size=-1):
        """Read up to size bytes from the object and return them."""
        return self._sub.stdout.read(size)

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


class CliRawOutputBase(io.RawIOBase):
    """Writes bytes to HDFS via the "hdfs dfs" command-line interface.

    Implements the io.RawIOBase interface of the standard library.
    """
    def __init__(self, uri):
        self._uri = uri
        self._sub = subprocess.Popen(["hdfs", "dfs", '-put', '-f', '-', self._uri],
                                     stdin=subprocess.PIPE)

        #
        # This member is part of the io.RawIOBase interface.
        #
        self.raw = None

    def close(self):
        self.flush()
        self._sub.stdin.close()
        self._sub.wait()

    def flush(self):
        self._sub.stdin.flush()

    def writeable(self):
        """Return True if this object is writeable."""
        return self._sub is not None

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError."""
        return False

    def write(self, b):
        self._sub.stdin.write(b)

    #
    # io.IOBase methods.
    #
    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")
