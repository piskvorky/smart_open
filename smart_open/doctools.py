#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Common functions for working with docstrings.

For internal use only.
"""

# ruff: noqa: T201  # this module builds the open()/parse_uri() docstrings by writing to sys.stdout

from __future__ import annotations

import contextlib
import inspect
import io
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from . import compression, transport

if TYPE_CHECKING:
    from collections.abc import Callable

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


def extract_kwargs(docstring: str | None) -> list[list[Any]]:
    """Extract keyword argument documentation from a Google-style ``Args:`` section.

    Args:
        docstring: The docstring to extract keyword arguments from.

    Returns:
        A list of ``[name, type, description_lines]`` triples. ``type`` is
        always an empty string since Google-style docstrings in this codebase
        don't carry argument types, and ``description_lines`` is a list of
        lines.

    Note:
        The implementation expects:

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

    for idx, line in enumerate(lines):
        if line.rstrip() == "Args:":
            lines = lines[idx + 1 :]
            break
    else:
        return []

    kwargs: list[list[Any]] = []
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


def to_docstring(kwargs: list[Any], lpad: str = "") -> str:
    """Reconstruct a docstring from keyword argument info.

    Basically reverses :func:`extract_kwargs`.

    Args:
        kwargs: Output from the :func:`extract_kwargs` function.
        lpad: Padding string (from the left).

    Returns:
        The docstring snippet documenting the keyword arguments.

    Example:
        >>> kwargs = [
        ...     ("bar", "str, optional", ["This parameter is the bar."]),
        ...     ("baz", "int, optional", ["This parameter is the baz."]),
        ... ]
        >>> print(to_docstring(kwargs), end="")
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


def extract_examples_from_readme(indent: str | None = None) -> str:
    """Extract examples from this project's README.md file.

    Args:
        indent: Prepend each line with this string.  Should contain some number
            of spaces.

    Returns:
        The examples as a single string.

    Note:
        Quite fragile, depends on the example markers and the fenced code block
        inside the README.md file.
    """
    if indent is None:
        indent = LPAD
    readme_path = Path(__file__).resolve().parent.parent / "README.md"
    try:
        text = readme_path.read_text(encoding="utf-8")
        body = text.split("<!-- doctools_before_examples -->", 1)[1]
        body = body.split("<!-- doctools_after_examples -->", 1)[0]
        # keep only the contents of the ```python ... ``` fenced code block
        body = body.split("```python", 1)[1].rsplit("```", 1)[0]
        lines = body.strip("\n").split("\n")
        return "".join(indent + line + "\n" for line in lines)
    except Exception:  # noqa: BLE001  # README parsing is best-effort; any failure falls back gracefully
        return indent + "See README.md"


def tweak_open_docstring(f: Callable[..., Any]) -> None:
    """Inject transport, compression and example sections into ``f``'s docstring."""
    buf = io.StringIO()
    seen = set()

    root_path = Path(__file__).parent.parent
    body_pad = LPAD + "    "

    with contextlib.redirect_stdout(buf):
        print(f"{LPAD}Transports:")  # builds docstring via redirect_stdout
        print()
        for scheme, submodule in sorted(transport._REGISTRY.items()):  # noqa: SLF001  # intra-package coupling
            if scheme == transport.NO_SCHEME or submodule in seen:
                continue
            seen.add(submodule)

            try:
                schemes = submodule.SCHEMES
            except AttributeError:
                schemes = [scheme]

            relpath = Path(cast("str", submodule.__file__)).relative_to(root_path)
            heading = "{} ({})".format("/".join(schemes), relpath)
            print(f"{body_pad}{heading}")
            print(f"{body_pad}{'~' * len(heading)}")
            print(f"{body_pad}{(submodule.__doc__ or '').split(chr(10))[0]}")
            print()

            kwargs = extract_kwargs(submodule.open.__doc__)
            if kwargs:
                print(to_docstring(kwargs, lpad=body_pad))

        print(f"{LPAD}Examples:")
        print()
        print(extract_examples_from_readme(indent=body_pad))

        print(f"{LPAD}Codecs:")
        print()
        print(f"{body_pad}smart_open supports transparent compression and decompression for files")
        print(f"{body_pad}with the following extensions:")
        print()
        for extension in compression.get_supported_extensions():
            print(f"{body_pad}* {extension}")
        print()
        print(f"{body_pad}The codec is selected based on the file extension.")

    #
    # The docstring can be None if -OO was passed to the interpreter.
    #
    if f.__doc__:
        f.__doc__ = f.__doc__.replace(PLACEHOLDER, buf.getvalue())


def tweak_parse_uri_docstring(f: Callable[..., Any]) -> None:
    """Inject supported schemes and example URIs into ``f``'s docstring."""
    buf = io.StringIO()
    seen = set()
    schemes = []
    examples = []

    for scheme, submodule in sorted(transport._REGISTRY.items()):  # noqa: SLF001  # intra-package coupling
        if scheme == transport.NO_SCHEME or submodule in seen:
            continue

        seen.add(submodule)

        with contextlib.suppress(AttributeError):
            examples.extend(submodule.URI_EXAMPLES)

        try:
            schemes.extend(submodule.SCHEMES)
        except AttributeError:
            schemes.append(scheme)

    body_pad = LPAD + "    "

    with contextlib.redirect_stdout(buf):
        print(f"{LPAD}Schemes:")
        print()
        for scheme in schemes:
            print(f"{body_pad}* {scheme}")
        print()
        print(f"{LPAD}Examples:")
        print()
        for example in examples:
            print(f"{body_pad}* {example}")

    if f.__doc__:
        f.__doc__ = f.__doc__.replace(PLACEHOLDER, buf.getvalue())
