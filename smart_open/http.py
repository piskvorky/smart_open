import io
import logging

import requests

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

_HEADERS = {'Accept-Encoding': 'identity'}
"""The headers we send to the server with every HTTP request.

For now, we ask the server to send us the files as they are.
Sometimes, servers compress the file for more efficient transfer, in which case
the client (us) has to decompress them with the appropriate algorithm.
"""


class BufferedInputBase(io.BufferedIOBase):
    """
    Implement streamed reader from a web site.
    Supports Kerberos and Basic HTTP authentication.
    """

    def __init__(self, url, mode='r', kerberos=False, user=None, password=None):
        """
        If Kerberos is True, will attempt to use the local Kerberos credentials.
        Otherwise, will try to use "basic" HTTP authentication via username/password.

        If none of those are set, will connect unauthenticated.
        """
        if kerberos:
            import requests_kerberos
            auth = requests_kerberos.HTTPKerberosAuth()
        elif user is not None and password is not None:
            auth = (user, password)
        else:
            auth = None

        self.response = requests.get(url, auth=auth, stream=True, headers=_HEADERS)

        if not self.response.ok:
            self.response.raise_for_status()

        logger.debug('self.response: %r, raw: %r', self.response, self.response.raw)

        self.mode = mode
        self._read_buffer = None
        self._read_iter = None
        self._readline_iter = None

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
        self.response = None

    def readable(self):
        """Return True if the stream can be read from."""
        return True

    def seekable(self):
        return False

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size=None):
        """
        Mimics the read call to a filehandle object.
        """
        logger.debug('read: %r', locals())
        if size is None:
            return self.response.raw.read()
        else:
            if self._read_iter is None:
                self._read_iter = self.response.iter_content(size)
                self._read_buffer = next(self._read_iter)

            while len(self._read_buffer) < size:
                try:
                    self._read_buffer += next(self._read_iter)
                except StopIteration:
                    # Oops, ran out of data early.
                    retval = self._read_buffer
                    self._read_buffer = b''
                    return retval

            # If we got here, it means we have enough data in the buffer
            # to return to the caller.
            retval = self._read_buffer[:size]
            self._read_buffer = self._read_buffer[size:]
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
