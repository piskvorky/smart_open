"""Transport that has missing deps."""

import io

try:
    import this_module_does_not_exist_but_we_need_it  # noqa: F401  # fixture intentionally imports a missing module
except ImportError:
    MISSING_DEPS = True

SCHEME = "missing"
open = io.open


def parse_uri(uri_as_string):  # pragma: no cover
    """No-op stub for test fixture."""


def open_uri(uri_as_string, mode, transport_params):  # pragma: no cover
    """No-op stub for test fixture."""
