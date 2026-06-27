#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements the compression layer of the `smart_open` library."""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any

if TYPE_CHECKING:
    from smart_open._typing import CompressionKwargs, Compressor

logger = logging.getLogger(__name__)

_COMPRESSOR_REGISTRY: dict[str, Compressor] = {}

NO_COMPRESSION = "disable"
"""Use no compression. Read/write the data as-is."""
INFER_FROM_EXTENSION = "infer_from_extension"
"""Determine the compression to use from the file extension.

See get_supported_extensions().
"""


def get_supported_compression_types() -> list[str]:
    """Return the list of supported compression types available to open.

    See compression paratemeter to smart_open.open().
    """
    return [NO_COMPRESSION, INFER_FROM_EXTENSION, *get_supported_extensions()]


def get_supported_extensions() -> list[str]:
    """Return the list of file extensions for which we have registered compressors."""
    return sorted(_COMPRESSOR_REGISTRY.keys())


def register_compressor(ext: str, callback: Compressor) -> None:
    """Register a callback for transparently decompressing files with a specific extension.

    Args:
        ext: The extension.  Must include the leading period, e.g. `.gz`.
        callback: The callback.  It must accept two positional arguments, file_obj and mode,
            and is recommended to also accept **kwargs so that whatever the caller passes
            via smart_open.open(..., compression_kwargs={...}) reaches the underlying
            library unchanged.  Callbacks with the legacy (file_obj, mode) signature still
            work, but will raise TypeError if the caller supplies compression_kwargs
            that the callback doesn't declare.

    Raises:
        ValueError: If `ext` does not start with a period.

    Example:
        Instruct smart_open to use the `lzma` module whenever opening a file
        with a .xz extension (see README.md for the complete example showing I/O):

        >>> def _handle_xz(file_obj, mode, **kwargs):
        ...     import lzma
        ...
        ...     return lzma.open(filename=file_obj, mode=mode, **kwargs)
        >>>
        >>> register_compressor(".xz", _handle_xz)

        This is just an example: `lzma` is in the standard library and is registered by default.
    """
    if not (ext and ext[0] == "."):
        msg = f"ext must be a string starting with ., not {ext!r}"
        raise ValueError(msg)
    ext = ext.lower()
    if ext in _COMPRESSOR_REGISTRY:
        logger.warning("overriding existing compression handler for %r", ext)
    _COMPRESSOR_REGISTRY[ext] = callback


def _maybe_wrap_buffered(file_obj: Any, mode: str) -> IO[bytes]:
    # https://github.com/piskvorky/smart_open/issues/760#issuecomment-1553971657
    result = file_obj
    if "b" in mode and "w" in mode:
        result = io.BufferedWriter(result)
    elif "b" in mode and "r" in mode:
        result = io.BufferedReader(result)
    return result


def _handle_bz2(file_obj: IO[bytes], mode: str, **kwargs: Any) -> IO[Any]:
    import bz2

    result = bz2.open(filename=file_obj, mode=mode, **kwargs)  # noqa: SIM115  # returns the file object to caller
    return _maybe_wrap_buffered(result, mode)


def _handle_gzip(file_obj: IO[bytes], mode: str, **kwargs: Any) -> IO[Any]:
    import gzip

    result = gzip.open(filename=file_obj, mode=mode, **kwargs)  # noqa: SIM115  # returns the file object to caller
    return _maybe_wrap_buffered(result, mode)


def _handle_zstd(file_obj: IO[bytes], mode: str, **kwargs: Any) -> IO[Any]:
    import sys

    if sys.version_info >= (3, 14):
        from compression import zstd
    else:
        from backports import zstd
    # dynamic **kwargs cannot be matched against zstd.open()'s overloads, so go through Any
    zstd_open: Any = zstd.open
    result = zstd_open(file_obj, mode=mode, **kwargs)
    return _maybe_wrap_buffered(result, mode)


def _handle_xz(file_obj: IO[bytes], mode: str, **kwargs: Any) -> IO[Any]:
    import lzma

    result = lzma.open(filename=file_obj, mode=mode, **kwargs)  # noqa: SIM115  # returns the file object to caller
    return _maybe_wrap_buffered(result, mode)


def _handle_lz4(file_obj: IO[bytes], mode: str, **kwargs: Any) -> IO[Any]:
    import lz4.frame

    result = lz4.frame.open(file_obj, mode=mode, **kwargs)
    return _maybe_wrap_buffered(result, mode)


def compression_wrapper(
    file_obj: IO[Any],
    mode: str,
    compression: str = INFER_FROM_EXTENSION,
    filename: str | None = None,
    compression_kwargs: CompressionKwargs | None = None,
) -> IO[Any]:
    """Wrap `file_obj` with an appropriate [de]compression mechanism based on its file extension.

    If the filename extension isn't recognized, simply return the original `file_obj` unchanged.

    `file_obj` must either be a filehandle object, or a class which behaves like one.

    If `filename` is specified, it will be used to extract the extension.
    If not, the `file_obj.name` attribute is used as the filename.

    If `compression_kwargs` is specified, its contents are forwarded as keyword
    arguments to the registered compressor callback.
    """
    if compression == NO_COMPRESSION:
        return file_obj
    if compression == INFER_FROM_EXTENSION:
        try:
            inferred_name = (filename or file_obj.name).lower()
        except (AttributeError, TypeError):
            logger.warning(
                "unable to transparently decompress %r because it seems to lack a string-like .name", file_obj
            )
            return file_obj
        compression = Path(inferred_name).suffix

    if compression in _COMPRESSOR_REGISTRY and mode.endswith("+"):
        msg = f"transparent (de)compression unsupported for mode {mode!r}"
        raise ValueError(msg)

    try:
        callback = _COMPRESSOR_REGISTRY[compression]
    except KeyError:
        return file_obj

    return callback(file_obj, mode, **(compression_kwargs or {}))


#
# NB. avoid using lambda here to make stack traces more readable.
#
register_compressor(".bz2", _handle_bz2)
register_compressor(".gz", _handle_gzip)
register_compressor(".zst", _handle_zstd)
register_compressor(".xz", _handle_xz)
register_compressor(".lz4", _handle_lz4)
