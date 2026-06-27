#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Common functions for working with docstrings.

For internal use only.
"""

import contextlib
import inspect
import io
import os.path
import re
import sys

from . import compression, transport

#
# Python 3.13+ automatically trims docstrings (like inspect.cleandoc),
# so we need to adjust the placeholder and indentation accordingly.
#
if sys.version_info >= (3, 13):
    PLACEHOLDER = "smart_open/doctools.py magic goes here"
    LPAD = ""
else:
    PLACEHOLDER = "    smart_open/doctools.py magic goes here"
    LPAD = "    "


def extract_kwargs(docstring):
    """Extract keyword argument documentation from a function's docstring.

    Supports both NumPy-style (underlined ``Parameters``) and Google-style
    (``Args:``) sections, so transport submodules can migrate independently.

    Args:
        docstring: The docstring to extract keyword arguments from.

    Returns:
        A list of ``[name, type, description_lines]`` triples. ``name`` is the
        name of the keyword argument, ``type`` is its type (may be an empty
        string when missing) and ``description_lines`` is its documentation as
        a list of lines.

    Note:
        The implementation is rather fragile. For NumPy style it expects:

        1. The parameters are under an underlined Parameters section
        2. Keyword parameters have the literal ", optional" after the type
        3. Names and types are not indented
        4. Descriptions are indented with 4 spaces
        5. The Parameters section ends with an empty line.

        For Google style it expects:

        1. The parameters are under an ``Args:`` header
        2. Argument lines start with 4 spaces of indent (``    name: desc``)
        3. Continuation lines for a description are indented with 8 spaces
        4. The ``Args:`` section ends with an empty line or another section header.

    Example:
        >>> docstring = '''The foo function.
        ... Args:
        ...     bar: This parameter is the bar.
        ...     baz: This parameter is the baz.
        ...
        ... '''
        >>> kwargs = extract_kwargs(docstring)
        >>> kwargs[0]
        ['bar', '', ['This parameter is the bar.']]
    """
    if not docstring:
        return []

    lines = inspect.cleandoc(docstring).split("\n")

    #
    # Detect the section style by scanning for a header.
    #
    for idx, line in enumerate(lines):
        if line == "Parameters" and idx + 1 < len(lines) and lines[idx + 1].startswith("---"):
            return _extract_kwargs_numpy(lines[idx:])
        if line.rstrip() == "Args:":
            return _extract_kwargs_google(lines[idx:])

    return []


def _extract_kwargs_numpy(lines):
    """Parse a NumPy-style ``Parameters`` section into kwargs triples.

    Args:
        lines: Cleaned docstring lines beginning with the ``Parameters`` header.

    Returns:
        A list of ``[name, type, description_lines]`` triples.
    """
    kwargs = []
    # Drop the 'Parameters' header and the '----------' underline.
    lines = lines[2:]

    for line in lines:
        if not line.strip():  # stop at the first empty line encountered
            break
        is_arg_line = not line.startswith(" ")
        if is_arg_line:
            name, type_ = line.split(":", 1)
            name, type_, description = name.strip(), type_.strip(), []
            kwargs.append([name, type_, description])
            continue
        is_description_line = line.startswith("    ")
        if is_description_line:
            kwargs[-1][-1].append(line.strip())

    return kwargs


def _extract_kwargs_google(lines):
    """Parse a Google-style ``Args:`` section into kwargs triples.

    Type information is not extracted (returned as an empty string) since
    Google-style docstrings in this codebase don't include argument types in
    the docstring.

    Args:
        lines: Cleaned docstring lines beginning with the ``Args:`` header.

    Returns:
        A list of ``[name, type, description_lines]`` triples.
    """
    kwargs = []
    # Drop the 'Args:' header.
    lines = lines[1:]

    for line in lines:
        if not line.strip():  # stop at the first empty line encountered
            break
        # Argument line: 4-space indent, then ``name: description``.
        if line.startswith("    ") and not line.startswith("        "):
            stripped = line[4:]
            if ":" not in stripped:
                continue
            name, desc = stripped.split(":", 1)
            kwargs.append([name.strip(), "", [desc.strip()] if desc.strip() else []])
            continue
        # Continuation line for the previous arg: 8-space (or deeper) indent.
        if line.startswith("        ") and kwargs:
            kwargs[-1][-1].append(line.strip())

    return kwargs


def to_docstring(kwargs, lpad=""):
    """Reconstruct a docstring from keyword argument info.

    Basically reverses :func:`extract_kwargs`.

    Args:
        kwargs: Output from the :func:`extract_kwargs` function.
        lpad: Padding string (from the left).

    Returns:
        The docstring snippet documenting the keyword arguments.

    Example:
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
        if type_:
            buf.write(f"{lpad}{name}: {type_}\n")
        else:
            buf.write(f"{lpad}{name}:\n")
        for line in description:
            buf.write(f"{lpad}    {line}\n")
    return buf.getvalue()


def extract_examples_from_readme_rst(indent=None):
    """Extract examples from this project's README.rst file.

    Args:
        indent: Prepend each line with this string.  Should contain some number
            of spaces.

    Returns:
        The examples as a single string.

    Note:
        Quite fragile, depends on named labels inside the README.rst file.
    """
    if indent is None:
        indent = LPAD
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    readme_path = os.path.join(curr_dir, "..", "README.rst")
    try:
        with open(readme_path) as fin:
            lines = list(fin)
        start = lines.index(".. _doctools_before_examples:\n")
        end = lines.index(".. _doctools_after_examples:\n")
        lines = lines[start + 4 : end - 2]
        return "".join([indent + re.sub("^  ", "", line) for line in lines])
    except Exception:  # noqa: BLE001
        return indent + "See README.rst"


def tweak_open_docstring(f):
    """Inject transport, compression and example sections into ``f``'s docstring."""
    buf = io.StringIO()
    seen = set()

    root_path = os.path.dirname(os.path.dirname(__file__))

    with contextlib.redirect_stdout(buf):
        print(f"{LPAD}smart_open supports the following transport mechanisms:")
        print()
        for scheme, submodule in sorted(transport._REGISTRY.items()):
            if scheme == transport.NO_SCHEME or submodule in seen:
                continue
            seen.add(submodule)

            try:
                schemes = submodule.SCHEMES
            except AttributeError:
                schemes = [scheme]

            relpath = os.path.relpath(submodule.__file__, start=root_path)
            heading = "{} ({})".format("/".join(schemes), relpath)
            print(f"{LPAD}{heading}")
            print(f"{LPAD}{'~' * len(heading)}")
            print(f"{LPAD}{submodule.__doc__.split(chr(10))[0]}")
            print()

            kwargs = extract_kwargs(submodule.open.__doc__)
            if kwargs:
                print(to_docstring(kwargs, lpad=LPAD))

        print(f"{LPAD}Examples:")
        print()
        print(extract_examples_from_readme_rst(indent=LPAD))

        print(f"{LPAD}This function also supports transparent compression and decompression ")
        print(f"{LPAD}using the following codecs:")
        print()
        for extension in compression.get_supported_extensions():
            print(f"{LPAD}* {extension}")
        print()
        print(f"{LPAD}The function depends on the file extension to determine the appropriate codec.")

    #
    # The docstring can be None if -OO was passed to the interpreter.
    #
    if f.__doc__:
        f.__doc__ = f.__doc__.replace(PLACEHOLDER, buf.getvalue())


def tweak_parse_uri_docstring(f):
    """Inject supported schemes and example URIs into ``f``'s docstring."""
    buf = io.StringIO()
    seen = set()
    schemes = []
    examples = []

    for scheme, submodule in sorted(transport._REGISTRY.items()):
        if scheme == transport.NO_SCHEME or submodule in seen:
            continue

        seen.add(submodule)

        with contextlib.suppress(AttributeError):
            examples.extend(submodule.URI_EXAMPLES)

        try:
            schemes.extend(submodule.SCHEMES)
        except AttributeError:
            schemes.append(scheme)

    with contextlib.redirect_stdout(buf):
        print(f"{LPAD}Supported URI schemes are:")
        print()
        for scheme in schemes:
            print(f"{LPAD}* {scheme}")
        print()
        print(f"{LPAD}Valid URI examples::")
        print()
        for example in examples:
            print(f"{LPAD}* {example}")

    if f.__doc__:
        f.__doc__ = f.__doc__.replace(PLACEHOLDER, buf.getvalue())
