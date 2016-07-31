#
# Adapted from Stephen Merity's gzipstream
# (https://github.com/Smerity/gzipstream)
#
# Used under the MIT license
# (https://github.com/Smerity/gzipstream/blob/master/LICENSE)
#
import io
import zlib


class GzipStreamFileInner(object):
    def __init__(self, stream):
        self.stream = stream
        self.decoder = None
        self.restart_decoder()
        ###
        self.unused_buffer = b''
        self.closed = False
        self.finished = False

    def restart_decoder(self):
        unused_raw = self.decoder.unused_data if self.decoder else None
        self.decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
        if unused_raw:
            self.unused_buffer += self.decoder.decompress(unused_raw)

    def read_from_buffer(self, size):
        """Read at most size bytes from buffer."""
        part = self.unused_buffer[:size]
        self.unused_buffer = self.unused_buffer[size:]
        return part

    def read_until_eof(self):
        #
        # This method is here because boto.s3.Key.read() reads the entire
        # file, which isn't expected behavior.
        #
        # https://github.com/boto/boto/issues/3311
        #
        while not self.finished:
            while self.decoder and self.decoder.unused_data:
                self.restart_decoder()

            raw = self.stream.read(io.DEFAULT_BUFFER_SIZE)
            if len(raw) > 0:
                self.unused_buffer += self.decoder.decompress(raw)
            else:
                self.finished = True
        return self.unused_buffer

    def read(self, size=None):
        if not size or size < 0:
            return self.read_from_buffer(
                len(self.unused_buffer)) + self.read_until_eof()

        # Use unused data first
        if len(self.unused_buffer) >= size:
            return self.read_from_buffer(size)

        # If the stream is finished and no unused raw data, return what we have
        if self.stream.closed or self.finished:
            self.finished = True
            return self.read_from_buffer(size)

        # Otherwise consume new data
        while len(self.unused_buffer) < size:
            # TODO: Update this to use unconsumed_tail and a StringIO buffer
            # http://docs.python.org/2/library/zlib.html#zlib.Decompress.unconsumed_tail
            # Check if we need to start a new decoder
            while self.decoder and self.decoder.unused_data:
                self.restart_decoder()

            raw = self.stream.read(io.DEFAULT_BUFFER_SIZE)
            if len(raw):
                self.unused_buffer += self.decoder.decompress(raw)
            else:
                self.finished = True
                break

        return self.read_from_buffer(size)

    def readinto(self, b):
        # Read up to len(b) bytes into bytearray b
        # Sadly not as efficient as lower level
        data = self.read(len(b))
        if not data:
            return None
        b[:len(data)] = data
        return len(data)

    def readable(self):
        # io.BufferedReader needs us to appear readable
        return True

    def _checkReadable(self, msg=None):
        # This is required to satisfy io.BufferedReader on Python 2.6.
        # Another way to achieve this is to inherit from io.IOBase, but that
        # leads to other problems.
        return True


class GzipStreamFile(io.BufferedReader):
    def __init__(self, stream):
        self._gzipstream = GzipStreamFileInner(stream)
        super(GzipStreamFile, self).__init__(self._gzipstream)

    def read(self, *args, **kwargs):
        # Patch read to return '' instead of raise Value Error
        # TODO: what actually raises ValueError in the following code?
        try:
            result = super(GzipStreamFile, self).read(*args, **kwargs)
            #
            # io.BufferedReader behaves differently to a built-in file object.
            # If the object is in non-blocking mode and no bytes are available,
            # the former will return None. The latter returns an empty string.
            # We want to behave like a built-in file object here.
            #
            if result is None:
                result = ""
            return result
        except ValueError:
            return ''

    def readline(self, *args, **kwargs):
        # Patch readline to return '' instead of raise Value Error
        # TODO: what actually raises ValueError in the following code?
        try:
            result = super(GzipStreamFile, self).readline(*args, **kwargs)
            return result
        except ValueError:
            return ''
