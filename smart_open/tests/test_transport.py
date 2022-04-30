# -*- coding: utf-8 -*-
from importlib.metadata import EntryPoint
import pytest
import unittest

from smart_open.transport import (
    register_transport, get_transport, _REGISTRY, _ERRORS, _register_transport_entry_point
)


def unregister_transport(x):
    if x in _REGISTRY:
        del _REGISTRY[x]
    if x in _ERRORS:
        del _ERRORS[x]


def assert_transport_not_registered(scheme):
    with pytest.raises(NotImplementedError):
        get_transport(scheme)


def assert_transport_registered(scheme):
    transport = get_transport(scheme)
    assert transport.SCHEME == scheme


class TransportTest(unittest.TestCase):
    def tearDown(self):
        unregister_transport("foo")
        unregister_transport("missing")

    def test_registry_requires_declared_schemes(self):
        with pytest.raises(ValueError):
            register_transport('smart_open.tests.fixtures.no_schemes_transport')

    def test_registry_valid_transport(self):
        assert_transport_not_registered("foo")
        register_transport('smart_open.tests.fixtures.good_transport')
        assert_transport_registered("foo")

    def test_registry_errors_on_double_register_scheme(self):
        register_transport('smart_open.tests.fixtures.good_transport')
        with pytest.raises(AssertionError):
            register_transport('smart_open.tests.fixtures.good_transport')

    def test_registry_errors_get_transport_for_module_with_missing_deps(self):
        register_transport('smart_open.tests.fixtures.missing_deps_transport')
        with pytest.raises(ImportError):
            get_transport("missing")

    def test_register_entry_point_valid(self):
        assert_transport_not_registered("foo")
        _register_transport_entry_point(EntryPoint(
            "foo",
            "smart_open.tests.fixtures.good_transport",
            "smart_open_transport",
        ))
        assert_transport_registered("foo")

    def test_register_entry_point_catch_bad_data(self):
        _register_transport_entry_point(EntryPoint(
            "invalid",
            "smart_open.some_totaly_invalid_module",
            "smart_open_transport",
        ))

    def test_register_entry_point_for_module_with_missing_deps(self):
        assert_transport_not_registered("missing")
        _register_transport_entry_point(EntryPoint(
            "missing",
            "smart_open.tests.fixtures.missing_deps_transport",
            "smart_open_transport",
        ))
        with pytest.raises(ImportError):
            get_transport("missing")
