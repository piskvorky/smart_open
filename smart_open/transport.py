# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Maintains a registry of transport mechanisms.

The main entrypoint is :func:`get_transport`.  See also :file:`extending.md`.

"""
import importlib
import logging

import smart_open.local_file

logger = logging.getLogger(__name__)

NO_SCHEME = ''

_REGISTRY = {NO_SCHEME: smart_open.local_file}
_ERRORS = {}


def register_transport(submodule, blind_schemes):
    """Register a submodule as a transport mechanism for ``smart_open``.

    This module **must** have:

        - `SCHEME` attribute (or `SCHEMES`, if the submodule supports multiple schemes)
        - `open` function
        - `open_uri` function
        - `parse_uri' function

    Once registered, you can get the submodule by calling :func:`get_transport`.

    """
    global _REGISTRY, _ERRORS
    blind_schemes = blind_schemes if isinstance(blind_schemes, list) else [blind_schemes]
    if isinstance(submodule, str):
        try:
            submodule = importlib.import_module(submodule)
        except ImportError as error:
            for scheme in blind_schemes:
                _ERRORS[scheme] = str(error)
            return

    if hasattr(submodule, 'SCHEME'):
        schemes = [submodule.SCHEME]
    elif hasattr(submodule, 'SCHEMES'):
        schemes = submodule.SCHEMES
    else:
        raise ValueError('%r does not have a .SCHEME or .SCHEMES attribute' % submodule)

    # It's not ideal to repeat the schemes, but if we can't load a module because
    # of an ImportError we can't know the schemes that are defined inside of it.
    if "".join(sorted(blind_schemes)) != "".join(sorted(schemes[:])):
        raise ValueError('%r schemes do not match the blind schemes listed in transport.py')

    for f in ('open', 'open_uri', 'parse_uri'):
        assert hasattr(submodule, f), '%r is missing %r' % (submodule, f)

    for scheme in schemes:
        assert scheme not in _REGISTRY
        _REGISTRY[scheme] = submodule


def get_transport(scheme):
    """Get the submodule that handles transport for the specified scheme.

    This submodule must have been previously registered via :func:`register_transport`.

    """
    global _ERRORS
    expected = SUPPORTED_SCHEMES
    readme_url = 'https://github.com/RaRe-Technologies/smart_open/blob/master/README.rst'
    message = (
        "Unable to handle scheme %(scheme)r, expected one of %(expected)r. "
        "Extra dependencies required by %(scheme)r may be missing. "
        "See <%(readme_url)s> for details." % locals()
    )
    if scheme in _REGISTRY:
        return _REGISTRY[scheme]
    if scheme in _ERRORS:
        raise ImportError(_ERRORS[scheme])
    raise NotImplementedError(message)


register_transport(smart_open.local_file, ["file"])
register_transport('smart_open.azure', ["azure"])
register_transport('smart_open.gcs', ["gs"])
register_transport('smart_open.hdfs', ["hdfs"])
register_transport('smart_open.http', ['http', 'https'])
register_transport('smart_open.s3', ["s3", "s3n", 's3u', "s3a"])
register_transport('smart_open.ssh', ["ssh", "scp", "sftp"])
register_transport('smart_open.webhdfs', ["webhdfs"])

SUPPORTED_SCHEMES = tuple(sorted(_REGISTRY.keys()))
"""The transport schemes that the local installation of ``smart_open`` supports."""
