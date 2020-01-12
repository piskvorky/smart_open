# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements the compression layer of the ``smart_open`` library."""
import io
import logging
import os.path
import warnings

import six

logger = logging.getLogger(__name__)


_COMPRESSOR_REGISTRY = {}
_ISSUE_189_URL = 'https://github.com/RaRe-Technologies/smart_open/issues/189'


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


def _handle_bz2(file_obj, mode):
    if six.PY2:
        from bz2file import BZ2File
    else:
        from bz2 import BZ2File
    return BZ2File(file_obj, mode)


def _handle_gzip(file_obj, mode):
    import gzip
    return gzip.GzipFile(fileobj=file_obj, mode=mode)


def compression_wrapper(file_obj, filename, mode):
    """
    This function will wrap the file_obj with an appropriate
    [de]compression mechanism based on the extension of the filename.

    file_obj must either be a filehandle object, or a class which behaves
        like one.

    If the filename extension isn't recognized, will simply return the original
    file_obj.
    """
    _, ext = os.path.splitext(filename)

    if _need_to_buffer(file_obj, mode, ext):
        warnings.warn('streaming gzip support unavailable, see %s' % _ISSUE_189_URL)
        file_obj = io.BytesIO(file_obj.read())
    if ext in _COMPRESSOR_REGISTRY and mode.endswith('+'):
        raise ValueError('transparent (de)compression unsupported for mode %r' % mode)

    try:
        callback = _COMPRESSOR_REGISTRY[ext]
    except KeyError:
        return file_obj
    else:
        return callback(file_obj, mode)


def _need_to_buffer(file_obj, mode, ext):
    """Returns True if we need to buffer the whole file in memory in order to proceed."""
    try:
        is_seekable = file_obj.seekable()
    except AttributeError:
        #
        # Under Py2, built-in file objects returned by open do not have
        # .seekable, but have a .seek method instead.
        #
        is_seekable = hasattr(file_obj, 'seek')
    is_compressed = ext in _COMPRESSOR_REGISTRY
    return six.PY2 and mode.startswith('r') and is_compressed and not is_seekable


#
# NB. avoid using lambda here to make stack traces more readable.
#
register_compressor('.bz2', _handle_bz2)
register_compressor('.gz', _handle_gzip)
