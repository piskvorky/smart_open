#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements I/O streams over SSH.

Example:
    >>> with open("/proc/version_signature", host="1.2.3.4") as conn:
    ...     print(conn.read())
    b'Ubuntu 4.4.0-1061.70-aws 4.4.131'

    Similarly, from a command line::

        $ python -c "from smart_open import ssh;print(ssh.open('/proc/version_signature', host='1.2.3.4').read())"
        b'Ubuntu 4.4.0-1061.70-aws 4.4.131'
"""

from __future__ import annotations

import contextlib
import getpass
import logging
import urllib.parse
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

try:
    import paramiko
except ImportError:
    MISSING_DEPS = True

import smart_open.utils

if TYPE_CHECKING:
    from collections.abc import Callable

    from smart_open._typing import TransportParams

logger = logging.getLogger(__name__)

#
# Global storage for SSH connections.
#
_SSH: dict[tuple[str, str], paramiko.SSHClient] = {}

SCHEMES = ("ssh", "scp", "sftp")
"""Supported URL schemes."""

DEFAULT_PORT = 22

URI_EXAMPLES = (
    "ssh://username@host/path/file",
    "ssh://username@host//path/file",
    "scp://username@host/path/file",
    "sftp://username@host/path/file",
)

#
# Global storage for SSH config files.
#
_SSH_CONFIG_FILES = [str(Path("~/.ssh/config").expanduser())]


def _unquote(text: str | None) -> str | None:
    return text and urllib.parse.unquote(text)


def _str2bool(string: str) -> bool:
    if string == "no":
        return False
    if string == "yes":
        return True
    msg = f"Expected 'yes' / 'no', got {string}."
    raise ValueError(msg)


#
# The parameter names used by Paramiko (and smart_open) slightly differ to
# those used in ~/.ssh/config, so we use a mapping to bridge the gap.
#
# The keys are option names as they appear in Paramiko (and smart_open)
# The values are a tuples containing:
#
# 1. their corresponding names in the ~/.ssh/config file
# 2. a callable to convert the parameter value from a string to the appropriate type
#
_PARAMIKO_CONFIG_MAP: dict[str, tuple[str, Callable]] = {
    "timeout": ("connecttimeout", float),
    "compress": ("compression", _str2bool),
    "gss_auth": ("gssapiauthentication", _str2bool),
    "gss_kex": ("gssapikeyexchange", _str2bool),
    "gss_deleg_creds": ("gssapidelegatecredentials", _str2bool),
    "gss_trust_dns": ("gssapitrustdns", _str2bool),
}


class _SSHUri(TypedDict):
    scheme: str
    uri_path: str | None
    user: str | None
    host: str | None
    port: int | None
    password: str | None


def parse_uri(uri_as_string: str) -> _SSHUri:
    """Parse an ``ssh://``/``scp://``/``sftp://`` URI into connection components."""
    split_uri = urllib.parse.urlsplit(uri_as_string)
    assert split_uri.scheme in SCHEMES  # noqa: S101  # internal precondition; misuse should crash loudly
    return {
        "scheme": split_uri.scheme,
        "uri_path": _unquote(split_uri.path),
        "user": _unquote(split_uri.username),
        "host": split_uri.hostname,
        "port": int(split_uri.port) if split_uri.port else None,
        "password": _unquote(split_uri.password),
    }


def open_uri(uri: str, mode: str, transport_params: TransportParams) -> paramiko.SFTPFile:
    """Open an SSH/SCP/SFTP URI using the given mode and transport params."""
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    parsed_uri: dict[str, Any] = dict(parse_uri(uri))
    uri_path = parsed_uri.pop("uri_path")
    parsed_uri.pop("scheme")
    final_params = {**parsed_uri, **kwargs}  # transport_params takes precedence over uri
    return open(uri_path, mode, **final_params)


def _connect_ssh(
    hostname: str,
    username: str | None,
    port: int | None,
    password: str | None,
    connect_kwargs: dict[str, Any] | None,
) -> paramiko.SSHClient:
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # noqa: S507  # documented smart_open default
    kwargs = (connect_kwargs or {}).copy()
    if "key_filename" not in kwargs:
        kwargs.setdefault("password", password)
    kwargs.setdefault("username", username)
    ssh.connect(hostname, port if port is not None else DEFAULT_PORT, **kwargs)
    return ssh


def _maybe_fetch_config(  # noqa: C901, PLR0912  # legacy internal helper; refactor in a dedicated PR
    host: str | None,
    username: str | None = None,
    password: str | None = None,
    port: int | None = None,
    connect_kwargs: dict[str, Any] | None = None,
) -> tuple[str | None, str | None, str | None, int | None, dict[str, Any] | None]:
    # If all fields are set, return as-is.
    if not any(arg is None for arg in (host, username, password, port, connect_kwargs)):
        return host, username, password, port, connect_kwargs

    if not host:
        msg = "you must specify the host to connect to"
        raise ValueError(msg)

    # Attempt to load an OpenSSH config.
    #
    # Connections configured in this way are not guaranteed to perform exactly
    # as they do in typical usage due to mismatches between the set of OpenSSH
    # configuration options and those that Paramiko supports. We provide a best
    # attempt, and support:
    #
    # - hostname -> address resolution
    # - username inference
    # - port inference
    # - identityfile inference
    # - connection timeout inference
    # - compression selection
    # - GSS configuration
    #
    connect_params = (connect_kwargs or {}).copy()
    config_files = [f for f in _SSH_CONFIG_FILES if Path(f).exists()]
    #
    # This is the actual name of the host.  The input host may actually be an
    # alias.
    #
    actual_hostname = ""

    for config_filename in config_files:
        try:
            cfg = paramiko.SSHConfig.from_path(config_filename)
        except PermissionError:
            continue

        if host not in cfg.get_hostnames():
            continue

        cfg = cfg.lookup(host)
        if username is None:
            username = cfg.get("user", None)

        if not actual_hostname:
            actual_hostname = cfg["hostname"]

        if port is None:
            # Nb. ignore missing/invalid port numbers
            with contextlib.suppress(KeyError, ValueError):
                port = int(cfg["port"])

        #
        # Special case, as we can have multiple identity files, so we check
        # that the identityfile list has len > 0. This should be redundant, but
        # keeping it for safety.
        #
        if connect_params.get("key_filename") is None:
            identityfile = cfg.get("identityfile", [])
            if len(identityfile):
                connect_params["key_filename"] = identityfile

        for param_name, (sshcfg_name, from_str) in _PARAMIKO_CONFIG_MAP.items():
            if connect_params.get(param_name) is None and sshcfg_name in cfg:
                connect_params[param_name] = from_str(cfg[sshcfg_name])

        #
        # Continue working through other config files, if there are any,
        # as they may contain more options for our host
        #

    if port is None:
        port = DEFAULT_PORT

    if not username:
        username = getpass.getuser()

    if actual_hostname:
        host = actual_hostname

    return host, username, password, port, connect_params


def open(  # noqa: PLR0913  # legacy public API; refactor in a dedicated PR
    path: str | None,
    mode: str = "r",
    host: str | None = None,
    user: str | None = None,
    password: str | None = None,
    port: int | None = None,
    connect_kwargs: dict[str, Any] | None = None,
    prefetch_kwargs: dict[str, Any] | None = None,
    buffer_size: int = -1,
) -> paramiko.SFTPFile:
    """Open a file on a remote machine over SSH.

    Expects authentication to be already set up via existing keys on the local machine.

    Args:
        path: The path to the file to open on the remote machine.
        mode: The mode to use for opening the file.
        host: The hostname of the remote machine. May not be None.
        user: The username to use to login to the remote machine.
            If None, defaults to the name of the current user.
        password: The password to use to login to the remote machine.
        port: The port to connect to.
        connect_kwargs: Any additional settings to be passed to paramiko.SSHClient.connect.
        prefetch_kwargs: Any additional settings to be passed to paramiko.SFTPFile.prefetch.
            The presence of this dict (even if empty) triggers prefetching.
        buffer_size: Passed to the bufsize argument of paramiko.SFTPClient.open.

    Returns:
        A file-like object.

    Raises:
        paramiko.SSHException: If the SSH session is no longer active and
            cannot be re-established within the retry limit.

    Note:
        If you specify a previously unseen host, then its host key will be added to
        the local ~/.ssh/known_hosts *automatically*.

        If ``username`` or ``password`` are specified in *both* the uri and
        ``transport_params``, ``transport_params`` will take precedence.
    """
    host, user, password, port, connect_kwargs = _maybe_fetch_config(
        host, user, password, port, connect_kwargs
    )
    assert host is not None  # noqa: S101  # _maybe_fetch_config raises if host is falsy
    assert user is not None  # noqa: S101  # _maybe_fetch_config defaults user via getpass.getuser()
    assert path is not None  # noqa: S101  # callers always provide a remote path to open

    key = (host, user)

    sftp_client: paramiko.SFTPClient | None = None
    attempts = 2
    for attempt in range(attempts):
        try:
            ssh = _SSH[key]
            # Validate that the cached connection is still an active connection
            #   and if not, refresh the connection
            transport = ssh.get_transport()
            assert transport is not None  # noqa: S101  # a cached connection has a transport
            if not transport.active:
                ssh.close()
                ssh = _SSH[key] = _connect_ssh(host, user, port, password, connect_kwargs)
        except KeyError:
            ssh = _SSH[key] = _connect_ssh(host, user, port, password, connect_kwargs)

        try:
            transport = ssh.get_transport()
            assert transport is not None  # noqa: S101  # _connect_ssh established an active transport
            sftp_client = transport.open_sftp_client()
            break
        except paramiko.SSHException as ex:
            connection_timed_out = ex.args and ex.args[0] == "SSH session not active"
            if attempt == attempts - 1 or not connection_timed_out:
                raise

            #
            # Try again.  Delete the connection from the cache to force a
            # reconnect in the next attempt.
            #
            del _SSH[key]

    assert sftp_client is not None  # noqa: S101  # the loop above either sets this or raises
    fobj = sftp_client.open(path, mode=mode, bufsize=buffer_size)
    fobj.name = path
    if prefetch_kwargs is not None:
        fobj.prefetch(**prefetch_kwargs)
    return fobj
