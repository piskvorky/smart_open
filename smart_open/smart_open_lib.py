# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements the majority of smart_open's top-level API.

The main functions are:

  * ``parse_uri()``
  * ``open()``

"""

import codecs
import collections
import logging
import io
import importlib
import os
import os.path as P
import warnings
import sys

import boto3
import six

from six.moves.urllib import parse as urlparse

#
# This module defines a function called smart_open so we cannot use
# smart_open.submodule to reference to the submodules.
#
import smart_open.local_file as so_file

from smart_open import compression
from smart_open import doctools
from smart_open import utils

#
# For backwards compatibility and keeping old unit tests happy.
#
from smart_open.compression import register_compressor  # noqa: F401
from smart_open.utils import check_kwargs as _check_kwargs  # noqa: F401
from smart_open.utils import inspect_kwargs as _inspect_kwargs  # noqa: F401

# Import ``pathlib`` if the builtin ``pathlib`` or the backport ``pathlib2`` are
# available. The builtin ``pathlib`` will be imported with higher precedence.
for pathlib_module in ('pathlib', 'pathlib2'):
    try:
        pathlib = importlib.import_module(pathlib_module)
        PATHLIB_SUPPORT = True
        break
    except ImportError:
        PATHLIB_SUPPORT = False

logger = logging.getLogger(__name__)

SYSTEM_ENCODING = sys.getdefaultencoding()

NO_SCHEME = ''

_TO_BINARY_LUT = {
    'r': 'rb', 'r+': 'rb+', 'rt': 'rb', 'rt+': 'rb+',
    'w': 'wb', 'w+': 'wb+', 'wt': 'wb', "wt+": 'wb+',
    'a': 'ab', 'a+': 'ab+', 'at': 'ab', 'at+': 'ab+',
}

_TRANSPORT = {NO_SCHEME: so_file}


def _register_transport(submodule):
    global _TRANSPORT
    if isinstance(submodule, str):
        try:
            submodule = importlib.import_module(submodule)
        except ImportError:
            _LOGGER.warning('unable to import %r, disabling that module', submodule)
            return

    if hasattr(submodule, 'SCHEME'):
        schemes = [submodule.SCHEME]
    elif hasattr(submodule, 'SCHEMES'):
        schemes = submodule.SCHEMES
    else:
        raise ValueError('%r does not have a .SCHEME or .SCHEMES attribute' % submodule)

    assert hasattr(submodule, 'open_uri'), '%r is missing open_uri' % submodule
    assert hasattr(submodule, 'parse_uri'), '%r is missing parse_uri' % submodule

    for scheme in schemes:
        assert scheme not in _TRANSPORT
        _TRANSPORT[scheme] = submodule


_register_transport(so_file)
_register_transport('smart_open.gcs')
_register_transport('smart_open.hdfs')
_register_transport('smart_open.http')
_register_transport('smart_open.s3')
_register_transport('smart_open.ssh')
_register_transport('smart_open.webhdfs')

SUPPORTED_SCHEMES = tuple(sorted(_TRANSPORT.keys()))
"""The transport schemes that ``smart_open`` supports."""


def _sniff_scheme(uri_as_string):
    """Returns the scheme of the URL only, as a string."""
    #
    # urlsplit doesn't work on Windows -- it parses the drive as the scheme...
    # no protocol given => assume a local file
    #
    if os.name == 'nt' and '://' not in uri_as_string:
        uri_as_string = 'file://' + uri_as_string

    return urlparse.urlsplit(uri_as_string).scheme


def parse_uri(uri_as_string):
    """
    Parse the given URI from a string.

    Parameters
    ----------
    uri_as_string: str
        The URI to parse.

    Returns
    -------
    collections.namedtuple
        The parsed URI.

    Notes
    -----

<<<<<<< HEAD
    Supported URI schemes are:

%(schemes)s
    s3, s3a and s3n are treated the same way.  s3u is s3 but without SSL.

    Valid URI examples::

%(uri_examples)s


    """
    scheme = _sniff_scheme(uri_as_string)

    try:
        transport = _TRANSPORT[scheme]
    except KeyError:
        raise NotImplementedError("unknown URI scheme %r in %r" % (scheme, uri_as_string))

    try:
        parse_uri = getattr(transport, 'parse_uri')
    except AttributeError:
        raise NotImplementedError('%r transport does not implement parse_uri', scheme)

    as_dict = parse_uri(uri_as_string)

    #
    # The conversion to a namedtuple is just to keep the old tests happy while
    # I'm still refactoring.
    #
    Uri = collections.namedtuple('Uri', sorted(as_dict.keys()))
    return Uri(**as_dict)


#
# To keep old unit tests happy while I'm refactoring.
#
_parse_uri = parse_uri

_builtin_open = open


def open(
        uri,
        mode='r',
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        closefd=True,
        opener=None,
        ignore_ext=False,
        transport_params=None,
        ):
    r"""Open the URI object, returning a file-like object.

    The URI is usually a string in a variety of formats.
    For a full list of examples, see the :func:`parse_uri` function.

    The URI may also be one of:

    - an instance of the pathlib.Path class
    - a stream (anything that implements io.IOBase-like functionality)

    This function supports transparent compression and decompression using the
    following codecs:

%(codecs)s

    The function depends on the file extension to determine the appropriate codec.

    Parameters
    ----------
    uri: str or object
        The object to open.
    mode: str, optional
        Mimicks built-in open parameter of the same name.
    buffering: int, optional
        Mimicks built-in open parameter of the same name.
    encoding: str, optional
        Mimicks built-in open parameter of the same name.
    errors: str, optional
        Mimicks built-in open parameter of the same name.
    newline: str, optional
        Mimicks built-in open parameter of the same name.
    closefd: boolean, optional
        Mimicks built-in open parameter of the same name.  Ignored.
    opener: object, optional
        Mimicks built-in open parameter of the same name.  Ignored.
    ignore_ext: boolean, optional
        Disable transparent compression/decompression based on the file extension.
    transport_params: dict, optional
        Additional parameters for the transport layer (see notes below).

    Returns
    -------
    A file-like object.

    Notes
    -----
    smart_open has several implementations for its transport layer (e.g. S3, HTTP).
    Each transport layer has a different set of keyword arguments for overriding
    default behavior.  If you specify a keyword argument that is *not* supported
    by the transport layer being used, smart_open will ignore that argument and
    log a warning message.

    S3 (for details, see :mod:`smart_open.s3` and :func:`smart_open.s3.open`):

%(s3)s
    HTTP (for details, see :mod:`smart_open.http` and :func:`smart_open.http.open`):

%(http)s
    WebHDFS (for details, see :mod:`smart_open.webhdfs` and :func:`smart_open.webhdfs.open`):

%(webhdfs)s
    SSH (for details, see :mod:`smart_open.ssh` and :func:`smart_open.ssh.open`):

%(ssh)s

    Examples
    --------
%(examples)s

    See Also
    --------
    - `Standard library reference <https://docs.python.org/3.7/library/functions.html#open>`__
    - `smart_open README.rst
      <https://github.com/RaRe-Technologies/smart_open/blob/master/README.rst>`__

    """
    logger.debug('%r', locals())

    if not isinstance(mode, six.string_types):
        raise TypeError('mode should be a string')

    if transport_params is None:
        transport_params = {}

    fobj = _shortcut_open(
        uri,
        mode,
        ignore_ext=ignore_ext,
        buffering=buffering,
        encoding=encoding,
        errors=errors,
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
    if encoding is not None and 'b' in mode:
        mode = mode.replace('b', '')

    # Support opening ``pathlib.Path`` objects by casting them to strings.
    if PATHLIB_SUPPORT and isinstance(uri, pathlib.Path):
        uri = str(uri)

    explicit_encoding = encoding
    encoding = explicit_encoding if explicit_encoding else SYSTEM_ENCODING

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
    binary_mode = _TO_BINARY_LUT.get(mode, mode)
    binary = _open_binary_stream(uri, binary_mode, transport_params)
    if ignore_ext:
        decompressed = binary
    else:
        decompressed = compression.compression_wrapper(binary, mode)

    if 'b' not in mode or explicit_encoding is not None:
        decoded = _encoding_wrapper(decompressed, mode, encoding=encoding, errors=errors)
    else:
        decoded = decompressed

    return decoded


_MIGRATION_NOTES_URL = (
    'https://github.com/RaRe-Technologies/smart_open/blob/master/README.rst'
    '#migrating-to-the-new-open-function'
)


def smart_open(uri, mode="rb", **kw):
    """Deprecated, use smart_open.open instead.

    See the migration instructions: %s

    """ % _MIGRATION_NOTES_URL

    warnings.warn(
        'This function is deprecated, use smart_open.open instead. '
        'See the migration notes for details: %s' % _MIGRATION_NOTES_URL
    )

    #
    # The new function uses a shorter name for this parameter, handle it separately.
    #
    ignore_extension = kw.pop('ignore_extension', False)

    expected_kwargs = utils.inspect_kwargs(open)
    scrubbed_kwargs = {}
    transport_params = {}

    #
    # Handle renamed keyword arguments.  This is required to maintain backward
    # compatibility.  See test_smart_open_old.py for tests.
    #
    if 'host' in kw or 's3_upload' in kw:
        transport_params['multipart_upload_kwargs'] = {}
        transport_params['resource_kwargs'] = {}

    if 'host' in kw:
        url = kw.pop('host')
        if not url.startswith('http'):
            url = 'http://' + url
        transport_params['resource_kwargs'].update(endpoint_url=url)

    if 's3_upload' in kw and kw['s3_upload']:
        transport_params['multipart_upload_kwargs'].update(**kw.pop('s3_upload'))

    #
    # Providing the entire Session object as opposed to just the profile name
    # is more flexible and powerful, and thus preferable in the case of
    # conflict.
    #
    if 'profile_name' in kw and 's3_session' in kw:
        logger.error('profile_name and s3_session are mutually exclusive, ignoring the former')

    if 'profile_name' in kw:
        transport_params['session'] = boto3.Session(profile_name=kw.pop('profile_name'))

    if 's3_session' in kw:
        transport_params['session'] = kw.pop('s3_session')

    for key, value in kw.items():
        if key in expected_kwargs:
            scrubbed_kwargs[key] = value
        else:
            #
            # Assume that anything not explicitly supported by the new function
            # is a transport layer keyword argument.  This is safe, because if
            # the argument ends up being unsupported in the transport layer,
            # it will only cause a logging warning, not a crash.
            #
            transport_params[key] = value

    return open(uri, mode, ignore_ext=ignore_extension,
                transport_params=transport_params, **scrubbed_kwargs)


def _shortcut_open(
        uri,
        mode,
        ignore_ext=False,
        buffering=-1,
        encoding=None,
        errors=None,
        ):
    """Try to open the URI using the standard library io.open function.

    This can be much faster than the alternative of opening in binary mode and
    then decoding.

    This is only possible under the following conditions:

        1. Opening a local file
        2. Ignore extension is set to True

    If it is not possible to use the built-in open for the specified URI, returns None.

    :param str uri: A string indicating what to open.
    :param str mode: The mode to pass to the open function.
    :param dict kw:
    :returns: The opened file
    :rtype: file
    """
    if not isinstance(uri, six.string_types):
        return None

    scheme = _sniff_scheme(uri)
    if scheme not in (NO_SCHEME, so_file.SCHEME):
        return None

    local_path = so_file.extract_local_path(uri)
    _, extension = P.splitext(local_path)
    if extension in compression.get_supported_extensions() and not ignore_ext:
        return None

    open_kwargs = {}

    if encoding is not None:
        open_kwargs['encoding'] = encoding
        mode = mode.replace('b', '')

    #
    # binary mode of the builtin/stdlib open function doesn't take an errors argument
    #
    if errors and 'b' not in mode:
        open_kwargs['errors'] = errors

    #
    # Under Py3, the built-in open accepts kwargs, and it's OK to use that.
    # Under Py2, the built-in open _doesn't_ accept kwargs, but we still use it
    # whenever possible (see issue #207).  If we're under Py2 and have to use
    # kwargs, then we have no option other to use io.open.
    #
    if six.PY3:
        return _builtin_open(local_path, mode, buffering=buffering, **open_kwargs)
    elif not open_kwargs:
        return _builtin_open(local_path, mode, buffering=buffering)
    return io.open(local_path, mode, buffering=buffering, **open_kwargs)


def _open_binary_stream(uri, mode, transport_params):
    """Open an arbitrary URI in the specified binary mode.

    Not all modes are supported for all protocols.

    :arg uri: The URI to open.  May be a string, or something else.
    :arg str mode: The mode to open with.  Must be rb, wb or ab.
    :arg transport_params: Keyword argumens for the transport layer.
    :returns: A named file object
    :rtype: file-like object with a .name attribute
    """
    if mode not in ('rb', 'rb+', 'wb', 'wb+', 'ab', 'ab+'):
        #
        # This should really be a ValueError, but for the sake of compatibility
        # with older versions, which raise NotImplementedError, we do the same.
        #
        raise NotImplementedError('unsupported mode: %r' % mode)

    if hasattr(uri, 'read'):
        # simply pass-through if already a file-like
        # we need to return something as the file name, but we don't know what
        # so we probe for uri.name (e.g., this works with open() or tempfile.NamedTemporaryFile)
        # if the value ends with COMPRESSED_EXT, we will note it in compression_wrapper()
        # if there is no such an attribute, we return "unknown" - this
        # effectively disables any compression
        if not hasattr(uri, 'name'):
            uri.name = getattr(uri, 'name', 'unknown')
        return uri

    if not isinstance(uri, six.string_types):
        raise TypeError("don't know how to handle uri %r" % uri)

    scheme = _sniff_scheme(uri)

    bad_scheme = NotImplementedError(
        "scheme %r is not supported, expected one of %r" % (
            scheme, SUPPORTED_SCHEMES,
        )
    )

    try:
        transport = _TRANSPORT[scheme]
    except KeyError:
        raise bad_scheme

    try:
        open_uri = getattr(transport, 'open_uri')
    except AttributeError:
        raise bad_scheme

    fobj = open_uri(uri, mode, transport_params)
    if not hasattr(fobj, 'name'):
        logger.critical('TODO')
        fobj.name = 'unknown'

    return fobj


def _encoding_wrapper(fileobj, mode, encoding=None, errors=None):
    """Decode bytes into text, if necessary.

    If mode specifies binary access, does nothing, unless the encoding is
    specified.  A non-null encoding implies text mode.

    :arg fileobj: must quack like a filehandle object.
    :arg str mode: is the mode which was originally requested by the user.
    :arg str encoding: The text encoding to use.  If mode is binary, overrides mode.
    :arg str errors: The method to use when handling encoding/decoding errors.
    :returns: a file object
    """
    logger.debug('encoding_wrapper: %r', locals())

    #
    # If the mode is binary, but the user specified an encoding, assume they
    # want text.  If we don't make this assumption, ignore the encoding and
    # return bytes, smart_open behavior will diverge from the built-in open:
    #
    #   open(filename, encoding='utf-8') returns a text stream in Py3
    #   smart_open(filename, encoding='utf-8') would return a byte stream
    #       without our assumption, because the default mode is rb.
    #
    if 'b' in mode and encoding is None:
        return fileobj

    if encoding is None:
        encoding = SYSTEM_ENCODING

    kw = {'errors': errors} if errors else {}
    if mode[0] == 'r' or mode.endswith('+'):
        fileobj = codecs.getreader(encoding)(fileobj, **kw)
    if mode[0] in ('w', 'a') or mode.endswith('+'):
        fileobj = codecs.getwriter(encoding)(fileobj, **kw)
    return fileobj


def _tweak_docstrings():
    substrings = {}
    schemes = io.StringIO()
    seen_examples = set()
    uri_examples = io.StringIO()

    for scheme, transport in sorted(_TRANSPORT.items()):
        if scheme == NO_SCHEME:
            continue

        schemes.write('    * %s\n' % scheme)

        try:
            fn = transport.open
        except AttributeError:
            substrings[scheme] = ''
        else:
            kwargs = doctools.extract_kwargs(fn.__doc__)
            substrings[scheme] = doctools.to_docstring(kwargs, lpad=u'    ')

        try:
            examples = transport.URI_EXAMPLES
        except AttributeError:
            continue
        else:
            for e in examples:
                if e not in seen_examples:
                    uri_examples.write('    * %s\n' % e)
                seen_examples.add(e)

    substrings['codecs'] = '\n'.join(
        ['    * %s' % e for e in compression.get_supported_extensions()]
    )
    substrings['examples'] = doctools.extract_examples_from_readme_rst()

    #
    # The docstring can be None if -OO was passed to the interpreter.
    #
    if open.__doc__:
        open.__doc__ = open.__doc__ % substrings

    if parse_uri.__doc__:
        parse_uri.__doc__ = parse_uri.__doc__ % dict(
            schemes=schemes.getvalue(),
            uri_examples=uri_examples.getvalue(),
        )


#
# The code below doesn't work on Py2.  We _could_ make it work, but given that
# it's 2020 and Py2 is on it's way out, I'm just going to disable it.
#
if not six.PY2:
    _tweak_docstrings()
