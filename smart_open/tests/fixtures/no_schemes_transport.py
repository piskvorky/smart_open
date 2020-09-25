# -*- coding: utf-8 -*-
"""A transport that is missing the required SCHEME/SCHEMAS attributes"""
import io

open = io.open


def parse_uri(uri_as_string):   # pragma: no cover
    ...


def open_uri(uri_as_string, mode, transport_params):   # pragma: no cover
    ...
