"""A transport that is missing the required SCHEME/SCHEMAS attributes."""

import io

open = io.open


def parse_uri(uri_as_string):  # pragma: no cover
    """No-op stub for test fixture."""


def open_uri(uri_as_string, mode, transport_params):  # pragma: no cover
    """No-op stub for test fixture."""
