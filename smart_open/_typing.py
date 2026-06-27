#
# Copyright (C) 2026 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Shared type aliases for ``smart_open``'s public and internal APIs.

For internal use only.  These aliases keep the type annotations consistent
across the transport, compression and top-level modules.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import IO, Any, TypeAlias

FileObj: TypeAlias = IO[Any]
"""A binary or text file-like object, as returned by :func:`smart_open.open`."""

Uri: TypeAlias = str | os.PathLike[str] | int | IO[bytes]
"""Anything :func:`smart_open.open` accepts as its first argument."""

TransportParams: TypeAlias = dict[str, Any]
"""Per-transport keyword arguments forwarded by :func:`smart_open.open`."""

CompressionKwargs: TypeAlias = dict[str, Any]
"""Keyword arguments forwarded to a registered compressor callback."""

Compressor: TypeAlias = Callable[..., IO[Any]]
"""A compressor callback registered via :func:`smart_open.register_compressor`."""
