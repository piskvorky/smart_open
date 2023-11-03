# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
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
import os
import logging
import urllib.parse

try:
    import paramiko
except ImportError:
    MISSING_DEPS = True

import smart_open.utils

logger = logging.getLogger(__name__)

#
# Global storage for SSH connections.
#
_SSH = {}

SCHEMES = ("ssh", "scp", "sftp")
"""Supported URL schemes."""

DEFAULT_PORT = 22

URI_EXAMPLES = (
    'ssh://username@host/path/file',
    'ssh://username@host//path/file',
    'scp://username@host/path/file',
    'sftp://username@host/path/file',
)

#
# Global storage for SSH config files.
#
_SSH_CONFIG_FILES = [os.path.expanduser("~/.ssh/config")]


def _unquote(text):
    return text and urllib.parse.unquote(text)


def _str2bool(string):
    if string == "no":
        return False
    if string == "yes":
        return True
    raise ValueError(f"Expected 'yes' / 'no', got {string}.")


def parse_uri(uri_as_string):
    split_uri = urllib.parse.urlsplit(uri_as_string)
    assert split_uri.scheme in SCHEMES
    return dict(
        scheme=split_uri.scheme,
        uri_path=_unquote(split_uri.path),
        user=_unquote(split_uri.username),
        host=split_uri.hostname,
        port=int(split_uri.port) if split_uri.port else None,
        password=_unquote(split_uri.password),
    )


def open_uri(uri, mode, transport_params):
    smart_open.utils.check_kwargs(open, transport_params)
    parsed_uri = parse_uri(uri)
    uri_path = parsed_uri.pop('uri_path')
    parsed_uri.pop('scheme')
    return open(uri_path, mode, transport_params=transport_params, **parsed_uri)


def _connect_ssh(hostname, username, port, password, transport_params):
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    kwargs = transport_params.get('connect_kwargs', {}).copy()
    if 'key_filename' not in kwargs:
        kwargs.setdefault('password', password)
    kwargs.setdefault('username', username)
    ssh.connect(hostname, port, **kwargs)
    return ssh


def _maybe_fetch_config(host, username=None, password=None, port=None, transport_params=None):
    # If all fields are set, return as-is.
    if not any(arg is None for arg in (host, username, password, port, transport_params)):
        return host, username, password, port, transport_params
    
    if not host:
        raise ValueError('you must specify the host to connect to')
    if not transport_params:
        transport_params = {}
    if "connect_kwargs" not in transport_params:
        transport_params["connect_kwargs"] = {}
    
    # Attempt to load an OpenSSH config.
    # NOTE: connections configured in this way are not guaranteed to perform exactly as
    # they do in typical usage due to mismatches between the set of OpenSSH configuration
    # options and those that Paramiko supports. We provide a best attempt, 
    # and support:
    # - hostname -> address resolution
    # - username inference
    # - port inference
    # - identityfile inference
    # - connection timeout inference
    # - compression selection
    # - GSS configuration
    for config_filename in _SSH_CONFIG_FILES:
        print(config_filename)
        if os.path.exists(config_filename):
            try:
                cfg = paramiko.SSHConfig.from_path(config_filename)
            except PermissionError:
                continue
            if host in cfg.get_hostnames():
                cfg = cfg.lookup(host)
                host = cfg["hostname"]
                if username is None:
                    username = cfg.get("user", None)
                if port is None and cfg.get("port", None) is not None:
                    port = int(cfg["port"])

                # Special case, as we can have multiple identity files, so we check that the
                # identityfile list has len > 0. This should be redundant, but keeping it for safety.
                if (transport_params["connect_kwargs"].get("key_filename", None) is None and
                    "identityfile" in cfg and len(cfg.get("identityfile", []))
                ):
                    transport_params["connect_kwargs"]["key_filename"] = cfg["identityfile"]

                # Map parameters from config to their required values for Paramiko's `connect` fn.
                _connect_kwarg_map = dict(
                    timeout=dict(key="connecttimeout", type=float),
                    compress=dict(key="compression", type=_str2bool),
                    gss_auth=dict(key="gssapiauthentication", type=_str2bool),
                    gss_kex=dict(key="gssapikeyexchange", type=_str2bool),
                    gss_deleg_creds=dict(key="gssapidelegatecredentials", type=_str2bool),
                    gss_trust_dns=dict(key="gssapitrustdns", type=_str2bool)
                )
                for target, field in _connect_kwarg_map.items():
                    if (
                        transport_params["connect_kwargs"].get(target, None) is None and field["key"] in cfg
                    ):
                        transport_params["connect_kwargs"][target] = field["type"](cfg[field["key"]])

    if port is None:
        port = DEFAULT_PORT
    if not username:
        username = getpass.getuser()
    return host, username, password, port, transport_params


def open(path, mode='r', host=None, user=None, password=None, port=None, transport_params=None):
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
    password: str, optional
        The password to use to login to the remote machine.
    port: int, optional
        The port to connect to.
    transport_params: dict, optional
        Any additional settings to be passed to paramiko.SSHClient.connect

    Returns
    -------
    A file-like object.

    Important
    ---------
    If you specify a previously unseen host, then its host key will be added to
    the local ~/.ssh/known_hosts *automatically*.

    If ``username`` or ``password`` are specified in *both* the uri and
    ``transport_params``, ``transport_params`` will take precedence
    """
    
    host, user, password, port, transport_params = _maybe_fetch_config(host, user, password, port, transport_params)

    key = (host, user)

    attempts = 2
    for attempt in range(attempts):
        try:
            ssh = _SSH[key]
        except KeyError:
            ssh = _SSH[key] = _connect_ssh(host, user, port, password, transport_params)

        try:
            transport = ssh.get_transport()
            sftp_client = transport.open_sftp_client()
            break
        except paramiko.SSHException as ex:
            connection_timed_out = ex.args and ex.args[0] == 'SSH session not active'
            if attempt == attempts - 1 or not connection_timed_out:
                raise

            #
            # Try again.  Delete the connection from the cache to force a
            # reconnect in the next attempt.
            #
            del _SSH[key]

    fobj = sftp_client.open(path, mode)
    fobj.name = path
    return fobj
