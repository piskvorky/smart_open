# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

import os
import tempfile
import pytest

import smart_open
import smart_open.ssh


def explode(*args, **kwargs):
    raise RuntimeError("this function should never have been called")


@pytest.mark.skipif("SMART_OPEN_SSH" not in os.environ, reason="this test only works on the dev machine")
def test():
    with smart_open.open("ssh://misha@localhost/Users/misha/git/smart_open/README.rst") as fin:
        readme = fin.read()

    assert 'smart_open — utils for streaming large files in Python' in readme

    #
    # Ensure the cache is being used
    #
    assert ('localhost', 'misha') in smart_open.ssh._SSH

    try:
        connect_ssh = smart_open.ssh._connect_ssh
        smart_open.ssh._connect_ssh = explode

        with smart_open.open("ssh://misha@localhost/Users/misha/git/smart_open/howto.md") as fin:
            howto = fin.read()

        assert 'How-to Guides' in howto
    finally:
        smart_open.ssh._connect_ssh = connect_ssh
