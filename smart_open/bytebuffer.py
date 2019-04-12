# -*- coding: utf-8 -*-
"""Implements a byte buffer that allows callers to read data with minimal
copying, and has a fast __len__ method. The buffer is parametrized by its
chunk_size, which is the number of bytes that it will read in from the supplied
reader or iterable when the buffer is being filled. As primary use case for this
buffer is to amortize the overhead costs of transferring data over the network
(rather than capping memory consumption), it leads to more predictable
performance to always read the same amount of bytes each time the buffer is
filled, hence the chunk_size parameter instead of some fixed capacity.

The bytes are stored in a bytestring, and previously-read bytes are freed when
the buffer is next filled (by slicing the bytestring into a smaller copy)."""

class ByteBuffer(object):
    def __init__(self, chunk_size):
        """The chunk_size indicates the number of bytes that will be read from
        the supplied reader when filling the buffer. The buffer has no maximum
        size."""
        self._chunk_size = chunk_size
        self.empty()

    def __len__(self):
        """Return the number of unread bytes in the buffer"""
        return len(self._bytes) - self._pos

    def read(self, size = -1):
        """Read up to size bytes from the buffer while advancing the read
        position, and then return the bytes. If size is negative, return all
        unread bytes."""
        part = self.peek(size)
        self._pos += len(part)
        return part

    def peek(self, size = -1):
        """Return up to the next size bytes from the buffer, without advancing
        the read position. If size is negative, return all unread bytes."""
        if size < 0 or size > len(self):
            size = len(self)

        part = self._bytes[self._pos : self._pos + size]
        return part

    def empty(self):
        """Remove all bytes from the buffer"""
        self._bytes = b''
        self._pos = 0

    def fill(self, reader_or_iterable, size = -1):
        """Fill the buffer with bytes from `reader_or_iterable` until one of
        these conditions is met:
            * size bytes have been read from reader_or_iterable (if size >= 0);
            * chunk_size bytes have been read from reader_or_iterable;
            * no more bytes can be read from reader_or_iterable;
        Return number of bytes added to the buffer.
        Note: all previously-read bytes in the buffer are removed."""
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
