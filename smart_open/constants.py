#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Some universal constants that are common to I/O operations."""

from __future__ import annotations

from typing import Final

READ_BINARY: Final = "rb"

WRITE_BINARY: Final = "wb"

APPEND_BINARY: Final = "ab"

# APPEND_BINARY intentionally excluded: only Azure supports it, other transports should error.
BINARY_MODES: Final = (READ_BINARY, WRITE_BINARY)

BINARY_NEWLINE: Final = b"\n"

WHENCE_START: Final = 0

WHENCE_CURRENT: Final = 1

WHENCE_END: Final = 2

WHENCE_CHOICES: Final = (WHENCE_START, WHENCE_CURRENT, WHENCE_END)
