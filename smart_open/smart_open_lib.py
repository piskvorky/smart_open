#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements the majority of smart_open's top-level API."""

from __future__ import annotations

import collections
import contextlib
import locale
import logging
import os
import os.path
import pathlib
import urllib.parse
from typing import IO, TYPE_CHECKING, Any, BinaryIO, Literal, TextIO, cast, overload

import smart_open.compression as so_compression

#
# This module defines a function called smart_open so we cannot use
# smart_open.submodule to reference to the submodules.
#
import smart_open.local_file as so_file
import smart_open.utils as so_utils
from smart_open import doctools, transport

if TYPE_CHECKING:
    from collections.abc import Callable

    from typing_extensions import Self

    from smart_open._typing import CompressionKwargs, TransportParams, Uri

logger = logging.getLogger(__name__)

DEFAULT_ENCODING = locale.getpreferredencoding(do_setlocale=False)


def _sniff_scheme(uri_as_string: str) -> str:
    """Returns the scheme of the URL only, as a string."""
    #
    # urlsplit doesn't work on Windows -- it parses the drive as the scheme...
    # no protocol given => assume a local file
    #
    if os.name == "nt" and "://" not in uri_as_string:
        uri_as_string = "file://" + uri_as_string

    return urllib.parse.urlsplit(uri_as_string).scheme


def parse_uri(uri_as_string: str) -> tuple[Any, ...]:
    """Parse the given URI from a string.

    Args:
        uri_as_string: The URI to parse.

    Returns:
        The parsed URI as a ``collections.namedtuple``.

    smart_open/doctools.py magic goes here
    """
    scheme = _sniff_scheme(uri_as_string)
    submodule = transport.get_transport(scheme)
    as_dict = submodule.parse_uri(uri_as_string)

    #
    # The conversion to a namedtuple is just to keep the old tests happy while
    # I'm still refactoring.
    #
    Uri = collections.namedtuple("Uri", sorted(as_dict.keys()))  # noqa: PYI024  # legacy public type
    return Uri(**as_dict)


#
# To keep old unit tests happy while I'm refactoring.
#
_parse_uri = parse_uri

_builtin_open = open


@overload
def open(
    uri: Uri,
    mode: Literal["r", "w", "a", "x", "r+", "w+", "a+", "rt", "wt", "at", "xt"] = ...,
    buffering: int = ...,
    encoding: str | None = ...,
    errors: str | None = ...,
    newline: str | None = ...,
    closefd: bool = ...,  # noqa: FBT001  # public API
    opener: Callable[[str, int], int] | None = ...,
    compression: str = ...,
    compression_kwargs: CompressionKwargs | None = ...,
    transport_params: TransportParams | None = ...,
) -> TextIO: ...


@overload
def open(
    uri: Uri,
    mode: Literal["rb", "wb", "ab", "xb", "rb+", "wb+", "ab+", "br", "bw", "ba"],
    buffering: int = ...,
    *,
    encoding: None = ...,
    errors: str | None = ...,
    newline: str | None = ...,
    closefd: bool = ...,
    opener: Callable[[str, int], int] | None = ...,
    compression: str = ...,
    compression_kwargs: CompressionKwargs | None = ...,
    transport_params: TransportParams | None = ...,
) -> BinaryIO: ...


@overload
def open(
    uri: Uri,
    mode: str = ...,
    buffering: int = ...,
    encoding: str | None = ...,
    errors: str | None = ...,
    newline: str | None = ...,
    closefd: bool = ...,  # noqa: FBT001  # public API
    opener: Callable[[str, int], int] | None = ...,
    compression: str = ...,
    compression_kwargs: CompressionKwargs | None = ...,
    transport_params: TransportParams | None = ...,
) -> IO[Any]: ...


def open(  # noqa: C901, PLR0913  # legacy public API; refactor in a dedicated PR
    uri: Uri,
    mode: str = "r",
    buffering: int = -1,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    closefd: bool = True,  # noqa: FBT001, FBT002  # public API
    opener: Callable[[str, int], int] | None = None,
    compression: str = so_compression.INFER_FROM_EXTENSION,
    compression_kwargs: CompressionKwargs | None = None,
    transport_params: TransportParams | None = None,
) -> IO[Any]:
    r"""Open the URI object, returning a file-like object.

    The URI is usually a string in a variety of formats.
    For a full list of examples, see the :func:`parse_uri` function.

    The URI may also be one of:

    - an instance of the pathlib.Path class
    - a stream (anything that implements io.IOBase-like functionality)

    Args:
        uri: The object to open.
        mode: Mimicks built-in open parameter of the same name.
        buffering: Mimicks built-in open parameter of the same name.
        encoding: Mimicks built-in open parameter of the same name.
        errors: Mimicks built-in open parameter of the same name.
        newline: Mimicks built-in open parameter of the same name.
        closefd: Mimicks built-in open parameter of the same name.  Ignored.
        opener: Mimicks built-in open parameter of the same name.  Ignored.
        compression: Explicitly specify the compression/decompression behavior.
            See ``smart_open.compression.get_supported_compression_types``.
        compression_kwargs: Keyword arguments forwarded to the registered
            compressor callback. Examples of each library's max-compression
            option: ``{'compresslevel': 9}`` for .gz/.bz2, ``{'preset': 9}`` for
            .xz, ``{'level': 22}`` for .zst, ``{'compression_level': 12}`` for
            .lz4. Ignored when compression is 'disable' or the URI's extension
            doesn't match a registered compressor.
        transport_params: Additional parameters for the transport layer (see
            notes below).

    Returns:
        A file-like object.

    Raises:
        TypeError: If ``mode`` is not a string or if the URI type is not
            recognized.
        ValueError: If ``compression`` is not a supported value.
        NotImplementedError: If ``mode`` cannot be parsed into a valid binary
            mode.

    Note:
        smart_open has several implementations for its transport layer
        (e.g. S3, HTTP). Each transport layer has a different set of keyword
        arguments for overriding default behavior. If you specify a keyword
        argument that is *not* supported by the transport layer being used,
        smart_open will ignore that argument and log a warning message.

    smart_open/doctools.py magic goes here

    See Also:
        - `Standard library reference <https://docs.python.org/3.14/library/functions.html#open>`__
        - `smart_open README.md
          <https://github.com/piskvorky/smart_open/blob/master/README.md>`__
    """
    logger.debug("%r", locals())

    if not isinstance(mode, str):
        msg = "mode should be a string"
        raise TypeError(msg)

    if compression not in so_compression.get_supported_compression_types():
        msg = f"invalid compression type: {compression}"
        raise ValueError(msg)

    if transport_params is None:
        transport_params = {}

    fobj = _shortcut_open(
        uri,
        mode,
        compression=compression,
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
    )
    if fobj is not None:
        return fobj

    #
    # This is a work-around for the problem described in Issue #144.
    # If the user has explicitly specified an encoding, then assume they want
    # us to open the destination in text mode, instead of the default binary.
    #
    # If we change the default mode to be text, and match the normal behavior
    # of Py2 and 3, then the above assumption will be unnecessary.
    #
    if encoding is not None and "b" in mode:
        mode = mode.replace("b", "")

    if isinstance(uri, pathlib.Path):
        uri = str(uri)

    explicit_encoding = encoding
    encoding = explicit_encoding or DEFAULT_ENCODING

    #
    # This is how we get from the filename to the end result.  Decompression is
    # optional, but it always accepts bytes and returns bytes.
    #
    # Decoding is also optional, accepts bytes and returns text.  The diagram
    # below is for reading, for writing, the flow is from right to left, but
    # the code is identical.
    #
    #           open as binary         decompress?          decode?
    # filename ---------------> bytes -------------> bytes ---------> text
    #                          binary             decompressed       decode
    #

    try:
        binary_mode = _get_binary_mode(mode)
    except ValueError as ve:
        raise NotImplementedError(ve.args[0]) from ve

    binary = _open_binary_stream(uri, binary_mode, transport_params)
    name = getattr(binary, "name", None)
    # prefer the stream's own name; if it's not string-like (e.g. ftp socket fileno), fall back to uri
    filename = name if isinstance(name, str) else uri if isinstance(uri, str) else None
    decompressed = so_compression.compression_wrapper(
        binary,
        binary_mode,
        compression,
        filename=filename,
        compression_kwargs=compression_kwargs,
    )

    if "b" not in mode or explicit_encoding is not None:
        decoded = _encoding_wrapper(
            decompressed,
            mode,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )
    else:
        decoded = decompressed

    #
    # There are some useful methods in the binary readers, e.g. to_boto3, that get
    # hidden by the multiple layers of wrapping we just performed.  Promote
    # them so they are visible to the user.
    #
    if decoded != binary:
        promoted_attrs = ["to_boto3"]
        for attr in promoted_attrs:
            with contextlib.suppress(AttributeError):
                setattr(decoded, attr, getattr(binary, attr))

    return cast("IO[Any]", so_utils.FileLikeProxy(decoded, binary))


def _get_binary_mode(mode_str: str) -> str:  # noqa: C901  # legacy internal helper; refactor in a dedicated PR
    #
    # https://docs.python.org/3/library/functions.html#open
    #
    # The order of characters in the mode parameter appears to be unspecified.
    # The implementation follows the examples, just to be safe.
    #
    mode = list(mode_str)
    binmode = []

    if "t" in mode and "b" in mode:
        msg = "can't have text and binary mode at once"
        raise ValueError(msg)

    counts = [mode.count(x) for x in "rwa"]
    if sum(counts) > 1:
        msg = "must have exactly one of create/read/write/append mode"
        raise ValueError(msg)

    def transfer(char: str) -> None:
        binmode.append(mode.pop(mode.index(char)))

    if "a" in mode:
        transfer("a")
    elif "w" in mode:
        transfer("w")
    elif "r" in mode:
        transfer("r")
    else:
        msg = "Must have exactly one of create/read/write/append mode and at most one plus"
        raise ValueError(msg)

    if "b" in mode:
        transfer("b")
    elif "t" in mode:
        mode.pop(mode.index("t"))
        binmode.append("b")
    else:
        binmode.append("b")

    if "+" in mode:
        transfer("+")

    #
    # There shouldn't be anything left in the mode list at this stage.
    # If there is, then either we've missed something and the implementation
    # of this function is broken, or the original input mode is invalid.
    #
    if mode:
        msg = f"invalid mode: {mode_str!r}"
        raise ValueError(msg)

    return "".join(binmode)


def _shortcut_open(  # noqa: PLR0913  # legacy internal helper; refactor in a dedicated PR
    uri: Uri,
    mode: str,
    compression: str,
    buffering: int = -1,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
) -> IO[Any] | None:
    """Try to open the URI using the standard library io.open function.

    This can be much faster than the alternative of opening in binary mode and
    then decoding.

    This is only possible under the following conditions:

        1. Opening a local file; and
        2. Compression is disabled

    If it is not possible to use the built-in open for the specified URI,
    returns None.

    Args:
        uri: A string indicating what to open.
        mode: The mode to pass to the open function.
        compression: The compression type selected.
        buffering: Mimicks built-in open parameter of the same name.
        encoding: Mimicks built-in open parameter of the same name.
        errors: Mimicks built-in open parameter of the same name.
        newline: Mimicks built-in open parameter of the same name.

    Returns:
        The opened file, or None if no shortcut is possible.
    """
    if not isinstance(uri, str):
        return None

    scheme = _sniff_scheme(uri)
    if scheme not in (transport.NO_SCHEME, so_file.SCHEME):
        return None

    local_path = so_file.extract_local_path(uri)
    if compression == so_compression.INFER_FROM_EXTENSION:
        extension = pathlib.Path(local_path).suffix
        if extension in so_compression.get_supported_extensions():
            return None
    elif compression != so_compression.NO_COMPRESSION:
        return None

    open_kwargs: dict[str, Any] = {}
    if encoding is not None:
        open_kwargs["encoding"] = encoding
        mode = mode.replace("b", "")
    if newline is not None:
        open_kwargs["newline"] = newline

    #
    # binary mode of the builtin/stdlib open function doesn't take an errors argument
    #
    if errors and "b" not in mode:
        open_kwargs["errors"] = errors

    return _builtin_open(local_path, mode, buffering=buffering, **open_kwargs)


def _open_binary_stream(uri: Uri, mode: str, transport_params: TransportParams) -> IO[bytes]:
    """Open an arbitrary URI in the specified binary mode.

    Not all modes are supported for all protocols.

    Args:
        uri: The URI to open.  May be a string, or something else.
        mode: The mode to open with.  Must be rb, wb or ab.
        transport_params: Keyword arguments for the transport layer.

    Returns:
        A file-like object with a ``.name`` attribute.

    Raises:
        NotImplementedError: If ``mode`` is not a supported binary mode.
        TypeError: If ``uri`` is not a string or integer file descriptor.
    """
    if mode not in ("rb", "rb+", "wb", "wb+", "ab", "ab+"):
        #
        # This should really be a ValueError, but for the sake of compatibility
        # with older versions, which raise NotImplementedError, we do the same.
        #
        msg = f"unsupported mode: {mode!r}"
        raise NotImplementedError(msg)

    if isinstance(uri, int):
        #
        # We're working with a file descriptor.  If we open it, its name is
        # just the integer value, which isn't helpful.  Unfortunately, there's
        # no easy cross-platform way to go from a file descriptor to the filename,
        # so we just give up here.  The user will have to handle their own
        # compression, etc. explicitly.
        #
        return _builtin_open(uri, mode, closefd=False)

    if not isinstance(uri, str):
        msg = f"don't know how to handle uri {uri!r}"
        raise TypeError(msg)

    scheme = _sniff_scheme(uri)
    submodule = transport.get_transport(scheme)
    fobj = submodule.open_uri(uri, mode, transport_params)
    if not hasattr(fobj, "name"):
        fobj.name = uri

    return fobj


def _encoding_wrapper(
    fileobj: IO[Any],
    mode: str,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
) -> IO[Any]:
    """Decode bytes into text, if necessary.

    If mode specifies binary access, does nothing, unless the encoding is
    specified.  A non-null encoding implies text mode.

    Args:
        fileobj: Must quack like a filehandle object.
        mode: The mode which was originally requested by the user.
        encoding: The text encoding to use.  If mode is binary, overrides mode.
        errors: The method to use when handling encoding/decoding errors.
        newline: Forwarded to the text wrapper.

    Returns:
        A file object.
    """
    logger.debug("encoding_wrapper: %r", locals())

    #
    # If the mode is binary, but the user specified an encoding, assume they
    # want text.  If we don't make this assumption, ignore the encoding and
    # return bytes, smart_open behavior will diverge from the built-in open:
    #
    #   open(filename, encoding='utf-8') returns a text stream in Py3
    #   smart_open(filename, encoding='utf-8') would return a byte stream
    #       without our assumption, because the default mode is rb.
    #
    if "b" in mode and encoding is None:
        return fileobj

    if encoding is None:
        encoding = DEFAULT_ENCODING

    return so_utils.TextIOWrapper(
        fileobj,
        encoding=encoding,
        errors=errors,
        newline=newline,
        write_through=True,
    )


class patch_pathlib:  # noqa: N801  # function-shaped name in public API
    """Replace `Path.open` with `smart_open.open`."""

    def __init__(self) -> None:
        self.old_impl = _patch_pathlib(open)

    def __enter__(self) -> Self:  # noqa: D105
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:  # noqa: D105
        _patch_pathlib(self.old_impl)


def _patch_pathlib(func: Callable[..., Any]) -> Callable[..., Any]:
    """Replace `Path.open` with `func`."""
    old_impl = pathlib.Path.open
    pathlib.Path.open = func  # ty: ignore[invalid-assignment]  # intentional monkeypatch
    return old_impl


#
# Prevent failures with doctools from messing up the entire library.  We don't
# expect such failures, but contributed modules (e.g. new transport mechanisms)
# may not be as polished.
#
try:
    doctools.tweak_open_docstring(open)
    doctools.tweak_parse_uri_docstring(parse_uri)
except Exception:
    logger.exception(
        "Encountered a non-fatal error while building docstrings (see below). "
        "help(smart_open) will provide incomplete information as a result. "
        "For full help text, see "
        "<https://github.com/piskvorky/smart_open/blob/master/help.txt>."
    )
