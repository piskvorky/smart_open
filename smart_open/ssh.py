#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions from the MIT License (MIT).
#

"""Implements I/O streams over SSH.

Examples
--------

>>> with open('/proc/version_signature', host='1.2.3.4') as conn:
...     print(conn.read())
b'Ubuntu 4.4.0-1061.70-aws 4.4.131'

Similarly, from a command line::

    $ python -c "from smart_open import ssh;print(ssh.open('/proc/version_signature', host='1.2.3.4').read())"
    b'Ubuntu 4.4.0-1061.70-aws 4.4.131'

"""

import getpass
import logging
import warnings

logger = logging.getLogger(__name__)

#
# Global storage for SSH connections.
#
_SSH = {}

SCHEMES = ("ssh", "scp", "sftp")
"""Supported URL schemes."""

DEFAULT_PORT = 22


def _connect(hostname, username, port):
    try:
        import paramiko
    except ImportError:
        warnings.warn(
            'paramiko missing, opening SSH/SCP/SFTP paths will be disabled. '
            '`pip install paramiko` to suppress'
        )
        raise

    key = (hostname, username)
    ssh = _SSH.get(key)
    if ssh is None:
        ssh = _SSH[key] = paramiko.client.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname, port, username)
    return ssh


def open(path, mode='r', host=None, user=None, port=DEFAULT_PORT):
    """Open a file on a remote machine over SSH.

    Expects authentication to be already set up via existing keys on the local machine.

    Parameters
    ----------
    path: str
        The path to the file to open on the remote machine.
    mode: str, optional
        The mode to use for opening the file.
    host: str, optional
        The hostname of the remote machine.  May not be None.
    user: str, optional
        The username to use to login to the remote machine.
        If None, defaults to the name of the current user.
    port: int, optional
        The port to connect to.

    Returns
    -------
    A file-like object.

    Important
    ---------
    If you specify a previously unseen host, then its host key will be added to
    the local ~/.ssh/known_hosts *automatically*.

    """
    if not host:
        raise ValueError('you must specify the host to connect to')
    if not user:
        user = getpass.getuser()
    conn = _connect(host, user, port)
    sftp_client = conn.get_transport().open_sftp_client()
    return sftp_client.open(path, mode)
