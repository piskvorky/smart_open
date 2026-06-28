#
# Copyright (C) 2022 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

import os

import pytest

import smart_open
import smart_open.ssh


def explode(*args, **kwargs):  # noqa: ARG001  # interface conformance
    """Raise to prove this stub was hit when it should not have been called."""
    msg = "this function should never have been called"
    raise RuntimeError(msg)


@pytest.mark.skipif("SMART_OPEN_SSH" not in os.environ, reason="this test only works on the dev machine")
def test():
    """Confirm the SSH connection cache reuses an existing connection."""
    with smart_open.open("ssh://misha@localhost/Users/misha/git/smart_open/README.md") as fin:
        readme = fin.read()

    assert "smart_open — utils for streaming large files in Python" in readme

    #
    # Ensure the cache is being used
    #
    assert ("localhost", "misha") in smart_open.ssh._SSH  # integration test reaches into private state

    try:
        connect_ssh = smart_open.ssh._connect_ssh  # integration test reaches into private state
        smart_open.ssh._connect_ssh = explode  # integration test reaches into private state

        with smart_open.open("ssh://misha@localhost/Users/misha/git/smart_open/HOWTO.md") as fin:
            howto = fin.read()

        assert "How-to Guides" in howto
    finally:
        smart_open.ssh._connect_ssh = connect_ssh  # integration test reaches into private state
