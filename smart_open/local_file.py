#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements the transport for the file:// schema."""

import builtins
import io
import os.path

SCHEME = "file"

URI_EXAMPLES = (
    "./local/path/file",
    "~/local/path/file",
    "local/path/file",
    "./local/path/file.gz",
    "file:///home/user/file",
    "file:///home/user/file.bz2",
)


open = io.open


def parse_uri(uri_as_string):
    """Parse a ``file://`` URI (or bare local path) into its path component."""
    local_path = extract_local_path(uri_as_string)
    return {"scheme": SCHEME, "uri_path": local_path}


def open_uri(uri_as_string, mode, transport_params):  # noqa: ARG001  # interface conformance
    """Open a local file URI using the given mode."""
    parsed_uri = parse_uri(uri_as_string)
    return builtins.open(parsed_uri["uri_path"], mode)  # noqa: PTH123  # mirrors builtins.open signature exactly


def extract_local_path(uri_as_string):
    """Return the user-expanded local filesystem path from `uri_as_string`."""
    if uri_as_string.startswith("file://"):
        local_path = uri_as_string.replace("file://", "", 1)
    else:
        local_path = uri_as_string
    return os.path.expanduser(local_path)  # noqa: PTH111  # pathlib collapses leading double slashes; preserve os.path semantics
