# -*- coding: utf-8 -*-

import pytest

from smart_open.utils import importlib_metadata
from smart_open.compression import _COMPRESSOR_REGISTRY, _register_compressor_entry_point

EntryPoint = importlib_metadata.EntryPoint


def unregister_compressor(ext):
    if ext in _COMPRESSOR_REGISTRY:
        del _COMPRESSOR_REGISTRY[ext]


@pytest.fixture(autouse=True)
def cleanup_compressor():
    unregister_compressor(".foo")
    unregister_compressor(".bar")


def test_register_valid_entry_point():
    assert ".foo" not in _COMPRESSOR_REGISTRY
    assert ".bar" not in _COMPRESSOR_REGISTRY
    _register_compressor_entry_point(EntryPoint(
        "foo",
        "smart_open.tests.fixtures.compressor:handle_bar",
        "smart_open_compressor",
    ))
    _register_compressor_entry_point(EntryPoint(
        "bar",
        "smart_open.tests.fixtures.compressor:handle_bar",
        "smart_open_compressor",
    ))
    assert ".foo" in _COMPRESSOR_REGISTRY
    assert ".bar" in _COMPRESSOR_REGISTRY


def test_register_invalid_entry_point_name_do_not_crash():
    _register_compressor_entry_point(EntryPoint(
        "",
        "smart_open.tests.fixtures.compressor:handle_foo",
        "smart_open_compressor",
    ))
    assert "" not in _COMPRESSOR_REGISTRY
    assert "." not in _COMPRESSOR_REGISTRY


def test_register_invalid_entry_point_value_do_not_crash():
    _register_compressor_entry_point(EntryPoint(
        "foo",
        "smart_open.tests.fixtures.compressor:handle_invalid",
        "smart_open_compressor",
    ))
    assert ".foo" not in _COMPRESSOR_REGISTRY
