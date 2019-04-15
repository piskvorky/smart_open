# -*- coding: utf-8 -*-
"""Implements ByteBuffer class for amortizing network transfer overhead."""

import io


class ByteBuffer(object):
    """Implements a byte buffer that allows callers to read data with minimal
    copying, and has a fast __len__ method. The buffer is parametrized by its
    chunk_size, which is the number of bytes that it will read in from the
    supplied reader or iterable when the buffer is being filled. As primary use
    case for this buffer is to amortize the overhead costs of transferring data
    over the network (rather than capping memory consumption), it leads to more
    predictable performance to always read the same amount of bytes each time
    the buffer is filled, hence the chunk_size parameter instead of some fixed
    capacity.

    The bytes are stored in a bytestring, and previously-read bytes are freed
    when the buffer is next filled (by slicing the bytestring into a smaller
    copy)."""

    def __init__(self, chunk_size=io.DEFAULT_BUFFER_SIZE):
        """Create a ByteBuffer instance that reads chunk_size bytes when filled.
        Note that the buffer has no maximum size.

        Parameters
        -----------
        chunk_size: int, optional
            The the number of bytes that will be read from the supplied reader
            or iterable when filling the buffer.
        """
        self._chunk_size = chunk_size
        self.empty()

    def __len__(self):
        """Return the number of unread bytes in the buffer as an int"""
        return len(self._bytes) - self._pos

    def read(self, size = -1):
        """Read bytes from the buffer and advance the read position. Returns
        the bytes in a bytestring.

        Parameters
        ----------
        size: int, optional
            Maximum number of bytes to read. If negative or not supplied, read
            all unread bytes in the buffer.

        Returns
        -------
        bytes
        """
        part = self.peek(size)
        self._pos += len(part)
        return part

    def peek(self, size = -1):
        """Get bytes from the buffer without advancing the read position.
        Returns the bytes in a bytestring.

        Parameters
        ----------
        size: int, optional
            Maximum number of bytes to return. If negative or not supplied,
            return all unread bytes in the buffer.

        Returns
        -------
        bytes
        """
        if size < 0 or size > len(self):
            size = len(self)

        part = self._bytes[self._pos : self._pos + size]
        return part

    def empty(self):
        """Remove all bytes from the buffer"""
        self._bytes = b''
        self._pos = 0

    def fill(self, reader_or_iterable, size = -1):
        """Fill the buffer with bytes from reader_or_iterable until one of these
        conditions is met:
            * size bytes have been read from reader_or_iterable (if size >= 0);
            * chunk_size bytes have been read from reader_or_iterable;
            * no more bytes can be read from reader_or_iterable;
        Returns the number of new bytes added to the buffer.
        Note: all previously-read bytes in the buffer are removed.

        Parameters
        ----------
        reader_or_iterable: a file-like object, or iterable that contains bytes
            The source of bytes to fill the buffer with. If this argument has
            the `read` attribute, it's assumed to be a file-like object and
            `read` is called to get the bytes; otherwise it's assumed to be an
            iterable that contains bytes, and `next` is used to get them.
        size: int, optional
            The number of bytes to try to read from reader_or_iterable. If not
            supplied, negative, or larger than the buffer's chunk_size, then
            chunk_size bytes are read. Note that if reader_or_iterable is an
            iterable, then it's possible that more bytes will be read if the
            iterable produces more than one byte on each `next` call.

        Returns
        -------
        int, the number of new bytes added to the buffer.
        """
        size = size if size >= 0 else self._chunk_size
        size = min(size, self._chunk_size)

        if self._pos != 0:
            self._bytes = self._bytes[self._pos:]
            self._pos = 0

        if hasattr(reader_or_iterable, 'read'):
            new_bytes = reader_or_iterable.read(size)
        else:
            new_bytes = b''
            while len(new_bytes) < size:
                try:
                    new_bytes += next(reader_or_iterable)
                except StopIteration:
                    break

        self._bytes += new_bytes
        return len(new_bytes)
