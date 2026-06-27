import unittest

import pytest

from smart_open.transport import get_transport, register_transport


class TransportTest(unittest.TestCase):
    """Tests for Transport."""

    def test_registry_requires_declared_schemes(self):
        """Registry requires declared schemes."""
        with pytest.raises(ValueError, match=r"does not have a \.SCHEME or \.SCHEMES attribute"):
            register_transport("tests.fixtures.no_schemes_transport")

    def test_registry_errors_on_double_register_scheme(self):
        """Registry errors on double register scheme."""
        register_transport("tests.fixtures.good_transport")
        with pytest.raises(AssertionError):
            register_transport("tests.fixtures.good_transport")

    def test_registry_errors_get_transport_for_module_with_missing_deps(self):
        """Registry errors get transport for module with missing deps."""
        register_transport("tests.fixtures.missing_deps_transport")
        with pytest.raises(ImportError):
            get_transport("missing")
