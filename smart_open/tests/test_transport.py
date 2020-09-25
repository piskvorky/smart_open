# -*- coding: utf-8 -*-
import pytest
import unittest

from smart_open.transport import register_transport, get_transport


class TransportTest(unittest.TestCase):

    def test_registry_requires_declared_schemes(self):
        with pytest.raises(ValueError):
            register_transport('smart_open.tests.fixtures.no_schemes_transport')

    def test_registry_errors_on_double_register_scheme(self):
        register_transport('smart_open.tests.fixtures.good_transport')
        with pytest.raises(AssertionError):
            register_transport('smart_open.tests.fixtures.good_transport')

    def test_registry_errors_get_transport_for_module_with_missing_deps(self):
        register_transport('smart_open.tests.fixtures.missing_deps_transport')
        with pytest.raises(ImportError):
            get_transport("missing")
