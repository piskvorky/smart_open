# -*- coding: utf-8 -*-
import pytest
import unittest

from smart_open.transport import register_transport, get_transport


class TransportTest(unittest.TestCase):

    def test_registry_requires_declared_schemes(self):
        with pytest.raises(ValueError):
            register_transport('smart_open.tests.fixtures.no_schemes_transport', [])

    def test_registry_asserts_known_schemes_match_module(self):
        with pytest.raises(ValueError):
            register_transport('smart_open.tests.fixtures.good_transport', ['other'])

    def test_registry_errors_on_double_register_scheme(self):
        register_transport('smart_open.tests.fixtures.good_transport', ['foo'])
        with pytest.raises(AssertionError):
            register_transport('smart_open.tests.fixtures.good_transport', ['foo'])

    def test_registry_lazily_shows_import_error_on_blind_scheme(self):
        register_transport('smart_open.tests.fixtures.import_error_transport', ['bar'])
        with pytest.raises(ImportError):
            get_transport('bar')
