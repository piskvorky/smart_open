"""A no-op transport that registers scheme 'foo'."""

import io

SCHEME = "foo"
open = io.open


def parse_uri(uri_as_string):  # pragma: no cover
    """No-op stub for test fixture."""


def open_uri(uri_as_string, mode, transport_params):  # pragma: no cover
    """No-op stub for test fixture."""
