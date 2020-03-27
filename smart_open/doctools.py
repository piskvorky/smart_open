# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Common functions for working with docstrings.

For internal use only.
"""

import inspect
import io
import os.path
import re

import six

from . import compression
from . import transport


def extract_kwargs(docstring):
    """Extract keyword argument documentation from a function's docstring.

    Parameters
    ----------
    docstring: str
        The docstring to extract keyword arguments from.

    Returns
    -------
    list of (str, str, list str)

    str
        The name of the keyword argument.
    str
        Its type.
    str
        Its documentation as a list of lines.

    Notes
    -----
    The implementation is rather fragile.  It expects the following:

    1. The parameters are under an underlined Parameters section
    2. Keyword parameters have the literal ", optional" after the type
    3. Names and types are not indented
    4. Descriptions are indented with 4 spaces
    5. The Parameters section ends with an empty line.

    Examples
    --------

    >>> docstring = '''The foo function.
    ... Parameters
    ... ----------
    ... bar: str, optional
    ...     This parameter is the bar.
    ... baz: int, optional
    ...     This parameter is the baz.
    ...
    ... '''
    >>> kwargs = extract_kwargs(docstring)
    >>> kwargs[0]
    ('bar', 'str, optional', ['This parameter is the bar.'])

    """
    if not docstring:
        return []

    lines = inspect.cleandoc(docstring).split('\n')
    retval = []

    #
    # 1. Find the underlined 'Parameters' section
    # 2. Once there, continue parsing parameters until we hit an empty line
    #
    while lines and lines[0] != 'Parameters':
        lines.pop(0)

    if not lines:
        return []

    lines.pop(0)
    lines.pop(0)

    while lines and lines[0]:
        name, type_ = lines.pop(0).split(':', 1)
        description = []
        while lines and lines[0].startswith('    '):
            description.append(lines.pop(0).strip())
        if 'optional' in type_:
            retval.append((name.strip(), type_.strip(), description))

    return retval


def to_docstring(kwargs, lpad=''):
    """Reconstruct a docstring from keyword argument info.

    Basically reverses :func:`extract_kwargs`.

    Parameters
    ----------
    kwargs: list
        Output from the extract_kwargs function
    lpad: str, optional
        Padding string (from the left).

    Returns
    -------
    str
        The docstring snippet documenting the keyword arguments.

    Examples
    --------

    >>> kwargs = [
    ...     ('bar', 'str, optional', ['This parameter is the bar.']),
    ...     ('baz', 'int, optional', ['This parameter is the baz.']),
    ... ]
    >>> print(to_docstring(kwargs), end='')
    bar: str, optional
        This parameter is the bar.
    baz: int, optional
        This parameter is the baz.

    """
    buf = io.StringIO()
    for name, type_, description in kwargs:
        buf.write('%s%s: %s\n' % (lpad, name, type_))
        for line in description:
            buf.write('%s    %s\n' % (lpad, line))
    return buf.getvalue()


def extract_examples_from_readme_rst(indent='    '):
    """Extract examples from this project's README.rst file.

    Parameters
    ----------
    indent: str
        Prepend each line with this string.  Should contain some number of spaces.

    Returns
    -------
    str
        The examples.

    Notes
    -----
    Quite fragile, depends on named labels inside the README.rst file.
    """
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    readme_path = os.path.join(curr_dir, '..', 'README.rst')
    try:
        with open(readme_path) as fin:
            lines = list(fin)
        start = lines.index('.. _doctools_before_examples:\n')
        end = lines.index(".. _doctools_after_examples:\n")
        lines = lines[start+4:end-2]
        return ''.join([indent + re.sub('^  ', '', l) for l in lines])
    except Exception:
        return indent + 'See README.rst'


def tweak_docstrings(open_function, parse_uri_function):
    #
    # The code below doesn't work on Py2.  We _could_ make it work, but given
    # that it's 2020 and Py2 is on it's way out, I'm just going to disable it.
    #
    if six.PY2:
        return

    substrings = {}
    schemes = io.StringIO()
    seen_examples = set()
    uri_examples = io.StringIO()

    for scheme, submodule in sorted(transport._REGISTRY.items()):
        if scheme == transport.NO_SCHEME:
            continue

        schemes.write('    * %s\n' % scheme)

        try:
            fn = submodule.open
        except AttributeError:
            substrings[scheme] = ''
        else:
            kwargs = extract_kwargs(fn.__doc__)
            substrings[scheme] = to_docstring(kwargs, lpad=u'    ')

        try:
            examples = submodule.URI_EXAMPLES
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
    substrings['examples'] = extract_examples_from_readme_rst()

    #
    # The docstring can be None if -OO was passed to the interpreter.
    #
    if open_function.__doc__:
        open_function.__doc__ = open_function.__doc__ % substrings

    if parse_uri_function.__doc__:
        parse_uri_function.__doc__ = parse_uri_function.__doc__ % dict(
            schemes=schemes.getvalue(),
            uri_examples=uri_examples.getvalue(),
        )
