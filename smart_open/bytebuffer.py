#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements ByteBuffer class for amortizing network transfer overhead."""

from __future__ import annotations

import io
from typing import IO, TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


class _TerminatorMatcher:
    """Remember a partial terminator while the byte buffer is drained and refilled."""

    def __init__(self, terminator: bytes) -> None:
        self._terminator = terminator
        self._matched = 0
        self._prefix = [0] * len(terminator)
        for index in range(1, len(terminator)):
            matched = self._prefix[index - 1]
            while matched and terminator[index] != terminator[matched]:
                matched = self._prefix[matched - 1]
            if terminator[index] == terminator[matched]:
                matched += 1
            self._prefix[index] = matched

    def _advance(self, value: int) -> bool:
        while self._matched and value != self._terminator[self._matched]:
            self._matched = self._prefix[self._matched - 1]
        if value == self._terminator[self._matched]:
            self._matched += 1
        if self._matched == len(self._terminator):
            self._matched = self._prefix[-1]
            return True
        return False

    def find(self, data: bytearray, start: int, end: int) -> int:
        """Return the terminator's end offset, or remember its prefix for a refill."""
        # Finish a possible match carried over from the previous buffer first.
        while start < end and self._matched:
            if self._advance(data[start]):
                return start + 1
            start += 1
        index = data.find(self._terminator, start, end)
        if index != -1:
            return index + len(self._terminator)
        # There is no complete match.  Only the suffix can start a terminator
        # that continues into the next buffer; scan it with prefix fallbacks.
        for value in data[max(start, end - len(self._terminator) + 1) : end]:
            self._advance(value)
        return -1


class ByteBuffer:
    """Byte buffer that allows callers to read data with minimal copying, and has a fast ``__len__`` method.

    The buffer is parametrized by its ``chunk_size``, which is the number of
    bytes that it will read in from the supplied reader or iterable when the
    buffer is being filled. As the primary use case for this buffer is to
    amortize the overhead costs of transferring data over the network (rather
    than capping memory consumption), it leads to more predictable performance
    to always read the same amount of bytes each time the buffer is filled,
    hence the ``chunk_size`` parameter instead of some fixed capacity.

    The bytes are stored in a bytestring, and previously-read bytes are freed
    when the buffer is next filled (by slicing the bytestring into a smaller
    copy).

    Args:
        chunk_size: The number of bytes that will be read from the supplied reader
            or iterable when filling the buffer.

    Example:
        >>> buf = ByteBuffer(chunk_size=8)
        >>> message_bytes = iter([b"Hello, W", b"orld!"])
        >>> buf.fill(message_bytes)
        8
        >>> len(buf)  # only chunk_size bytes are filled
        8
        >>> buf.peek()
        b'Hello, W'
        >>> len(buf)  # peek() does not change read position
        8
        >>> buf.read(6)
        b'Hello,'
        >>> len(buf)  # read() does change read position
        2
        >>> buf.fill(message_bytes)
        5
        >>> buf.read()
        b' World!'
        >>> len(buf)
        0
    """

    def __init__(self, chunk_size: int = io.DEFAULT_BUFFER_SIZE) -> None:
        self._chunk_size = chunk_size
        self.empty()

    def __len__(self) -> int:
        """Return the number of unread bytes in the buffer as an int."""
        return len(self._bytes) - self._pos

    def read(self, size: int = -1) -> bytes:
        """Read bytes from the buffer and advance the read position.

        Args:
            size: Maximum number of bytes to read. If negative or not supplied, read
                all unread bytes in the buffer.

        Returns:
            The bytes read from the buffer.
        """
        part = self.peek(size)
        self._pos += len(part)
        return part

    def peek(self, size: int = -1) -> bytes:
        """Get bytes from the buffer without advancing the read position.

        Args:
            size: Maximum number of bytes to return. If negative or not supplied,
                return all unread bytes in the buffer.

        Returns:
            The peeked bytes from the buffer.
        """
        if size < 0 or size > len(self):
            size = len(self)

        return bytes(self._bytes[self._pos : self._pos + size])

    def empty(self) -> None:
        """Remove all bytes from the buffer."""
        self._bytes = bytearray()
        self._pos = 0

    def fill(self, source: IO[bytes] | Iterable[bytes], size: int = -1) -> int:
        """Fill the buffer with bytes from source.

        Reads from ``source`` until one of these conditions is met:

        * ``size`` bytes have been read from source (if ``size >= 0``);
        * ``chunk_size`` bytes have been read from source;
        * no more bytes can be read from source.

        Note:
            All previously-read bytes in the buffer are removed.

        Args:
            source: The source of bytes to fill the buffer with, either a file-like
                object or an iterable/list of bytes. If this argument has the ``read``
                attribute, it's assumed to be a file-like object and ``read`` is called
                to get the bytes; otherwise it's assumed to be an iterable or list that
                contains bytes, and a for loop is used to get the bytes.
            size: The number of bytes to try to read from source. If not supplied,
                negative, or larger than the buffer's ``chunk_size``, then ``chunk_size``
                bytes are read. Note that if source is an iterable or list, then
                it's possible that more than size bytes will be read if iterating
                over source produces more than one byte at a time.

        Returns:
            The number of new bytes added to the buffer.
        """
        size = size if size >= 0 else self._chunk_size
        size = min(size, self._chunk_size)

        if self._pos != 0:
            self._bytes = self._bytes[self._pos :]
            self._pos = 0

        if size == 0:
            return 0

        if hasattr(source, "read"):
            new_bytes = cast("IO[bytes]", source).read(size)
        else:
            new_bytes = bytearray()
            for more_bytes in source:
                new_bytes += more_bytes
                if len(new_bytes) >= size:
                    break

        self._bytes += new_bytes
        return len(new_bytes)

    def readline(
        self,
        terminator: bytes,
        limit: int | None = -1,
        fill: Callable[[], object] | None = None,
    ) -> bytes:
        """Read a line from this buffer efficiently.

        A line is a contiguous sequence of bytes that ends with either:

        1. The complete ``terminator`` sequence
        2. ``limit`` bytes, if ``limit`` is not negative
        3. The end of the buffer itself, or with ``fill``, the end of the source

        Args:
            terminator: The line terminator byte sequence.
            limit: Maximum number of bytes to return. If negative or None, no limit.
            fill: Called when the buffer runs out mid line, to add more bytes.
                A buffer still empty afterwards means the end of the source.

        Returns:
            The line bytes (including the terminator if present).
        """
        if limit is None:
            limit = -1
        matcher = _TerminatorMatcher(terminator) if len(terminator) > 1 else None
        parts: list[bytes] = []
        while True:
            if matcher is None:
                index = self._bytes.find(terminator, self._pos)
                end = -1 if index == -1 else index + 1
            else:
                stop = len(self._bytes)
                if limit >= 0:
                    stop = min(stop, self._pos + limit)
                end = matcher.find(self._bytes, self._pos, stop)
            size = len(self) if end == -1 else end - self._pos
            if limit >= 0:
                size = min(size, limit)
                limit -= size
            parts.append(self.read(size))
            if end != -1 or limit == 0 or fill is None:
                break
            fill()
            if not len(self):
                break
        return b"".join(parts)
