# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements the compression layer of the ``smart_open`` library."""
import logging
import os.path

logger = logging.getLogger(__name__)

_COMPRESSOR_REGISTRY = {}

NO_COMPRESSION = 'disable'
"""Use no compression. Read/write the data as-is."""
INFER_FROM_EXTENSION = 'infer_from_extension'
"""Determine the compression to use from the file extension.

See get_supported_extensions().
"""


def get_supported_compression_types():
    """Return the list of supported compression types available to open.

    See compression paratemeter to smart_open.open().
    """
    return [NO_COMPRESSION, INFER_FROM_EXTENSION] + get_supported_extensions()


def get_supported_extensions():
    """Return the list of file extensions for which we have registered compressors."""
    return sorted(_COMPRESSOR_REGISTRY.keys())


def register_compressor(ext, callback):
    """Register a callback for transparently decompressing files with a specific extension.

    Parameters
    ----------
    ext: str
        The extension.  Must include the leading period, e.g. ``.gz``.
    callback: callable
        The callback.  It must accept two position arguments, file_obj and mode.
        This function will be called when ``smart_open`` is opening a file with
        the specified extension.

    Examples
    --------

    Instruct smart_open to use the `lzma` module whenever opening a file
    with a .xz extension (see README.rst for the complete example showing I/O):

    >>> def _handle_xz(file_obj, mode):
    ...     import lzma
    ...     return lzma.LZMAFile(filename=file_obj, mode=mode, format=lzma.FORMAT_XZ)
    >>>
    >>> register_compressor('.xz', _handle_xz)

    """
    if not (ext and ext[0] == '.'):
        raise ValueError('ext must be a string starting with ., not %r' % ext)
    if ext in _COMPRESSOR_REGISTRY:
        logger.warning('overriding existing compression handler for %r', ext)
    _COMPRESSOR_REGISTRY[ext] = callback


def tweak_close(outer, inner):
    """Ensure that closing the `outer` stream closes the `inner` stream as well.

    Use this when your compression library's `close` method does not
    automatically close the underlying filestream.  See
    https://github.com/RaRe-Technologies/smart_open/issues/630 for an
    explanation why that is a problem for smart_open.
    """
    outer_close = outer.close

    def close_both(*args):
        nonlocal inner
        try:
            outer_close()
        finally:
            if inner:
                inner, fp = None, inner
                fp.close()

    outer.close = close_both


def _handle_bz2(file_obj, mode):
    from bz2 import BZ2File
    result = BZ2File(file_obj, mode)
    tweak_close(result, file_obj)
    return result


def _handle_gzip(file_obj, mode):
    import gzip
    result = gzip.GzipFile(fileobj=file_obj, mode=mode)
    tweak_close(result, file_obj)
    return result


def compression_wrapper(file_obj, mode, compression):
    """
    This function will wrap the file_obj with an appropriate
    [de]compression mechanism based on the specified extension.

    file_obj must either be a filehandle object, or a class which behaves
    like one. It must have a .name attribute.

    If the filename extension isn't recognized, will simply return the original
    file_obj.

    """
    if compression == NO_COMPRESSION:
        return file_obj
    elif compression == INFER_FROM_EXTENSION:
        try:
            filename = file_obj.name
            filename.upper()  # make sure this thing is a string
        except (AttributeError, TypeError):
            logger.warning(
                'unable to transparently decompress %r because it '
                'seems to lack a string-like .name', file_obj
            )
            return file_obj
        _, compression = os.path.splitext(filename)

    if compression in _COMPRESSOR_REGISTRY and mode.endswith('+'):
        raise ValueError('transparent (de)compression unsupported for mode %r' % mode)

    try:
        callback = _COMPRESSOR_REGISTRY[compression]
    except KeyError:
        return file_obj
    else:
        return callback(file_obj, mode)


#
# NB. avoid using lambda here to make stack traces more readable.
#
register_compressor('.bz2', _handle_bz2)
register_compressor('.gz', _handle_gzip)
