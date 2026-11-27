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
import re
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
_SSH: dict[tuple, paramiko.SSHClient] = {}

#
# Host keys accepted because ~/.ssh/config opts out of StrictHostKeyChecking,
# so later connections in this process, like reconnects, reject any other key.
#
_ACCEPTED_HOST_KEYS: dict[str, paramiko.PKey] = {}


class _AcceptFirstHostKey:
    """Paramiko missing host key policy that trusts the first key of a host for the rest of the process."""

    def missing_host_key(self, client: paramiko.SSHClient, hostname: str, key: paramiko.PKey) -> None:
        """Accept the first key seen for hostname, and reject any other key for it."""
        # setdefault is atomic, so concurrent first connections to a host agree on one key
        accepted = _ACCEPTED_HOST_KEYS.setdefault(hostname, key)
        if accepted != key:
            raise paramiko.BadHostKeyException(hostname, key, accepted)


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

#
# Paramiko substitutes %h and %n into "Match exec" commands unquoted and runs them
# through a shell, so a hostname with shell syntax would run as part of that command.
# These are the characters OpenSSH rejects in a hostname since 9.6 (CVE-2023-51385).
#
_UNSAFE_HOSTNAME = re.compile(r"""^-|[\x00-\x20\x7f\s'`"$\\;&<>|(){}]""")


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


def _maybe_fetch_config(  # noqa: C901  # legacy internal helper; refactor in a dedicated PR
    host: str | None,
    username: str | None = None,
    password: str | None = None,
    port: int | None = None,
    connect_kwargs: dict[str, Any] | None = None,
) -> tuple[str, str, str | None, int, dict[str, Any], bool]:
    if not host:
        msg = "you must specify the host to connect to"
        raise ValueError(msg)
    if _UNSAFE_HOSTNAME.search(host):
        msg = f"invalid characters in SSH hostname: {host!r}"
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
    # - StrictHostKeyChecking no, off and accept-new
    #
    connect_params = (connect_kwargs or {}).copy()
    config_files = [f for f in _SSH_CONFIG_FILES if Path(f).exists()]
    config = paramiko.SSHConfig()
    for config_filename in config_files:
        with contextlib.suppress(PermissionError), Path(config_filename).open() as config_file:
            config.parse(config_file)

    # Resolve once, so Paramiko applies first-value precedence before adding
    # its default HostName when no matching stanza explicitly supplies one.
    cfg = config.lookup(host)
    host = cfg["hostname"]
    if username is None:
        username = cfg.get("user", None)

    if port is None:
        # Nb. ignore missing/invalid port numbers
        with contextlib.suppress(KeyError, ValueError):
            port = int(cfg["port"])

    if password is None and connect_params.get("key_filename") is None:
        identityfile = cfg.get("identityfile", [])
        if len(identityfile):
            connect_params["key_filename"] = identityfile

    for param_name, (sshcfg_name, from_str) in _PARAMIKO_CONFIG_MAP.items():
        if connect_params.get(param_name) is None and sshcfg_name in cfg:
            connect_params[param_name] = from_str(cfg[sshcfg_name])

    # Paramiko, like OpenSSH without a terminal to ask on, rejects hosts missing from known_hosts.
    # These values opt out by accepting their key for this process. Changed keys are still rejected.
    auto_add_host_keys = cfg.get("stricthostkeychecking", "").lower() in ("no", "off", "false", "accept-new")

    if port is None:
        port = DEFAULT_PORT

    if not username:
        username = getpass.getuser()

    return host, username, password, port, connect_params, auto_add_host_keys


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
    known_hosts: str | None = None,
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
        known_hosts: Path to a known_hosts file whose host keys are trusted on top of ~/.ssh/known_hosts.
            It is only read, so it may live on a read only filesystem.

    Returns:
        A file-like object.

    Raises:
        paramiko.SSHException: If the SSH session is no longer active and
            cannot be re-established within the retry limit.

    Note:
        Like OpenSSH without a terminal, a host whose key is not in ~/.ssh/known_hosts
        or ``known_hosts`` is rejected, unless ~/.ssh/config sets ``StrictHostKeyChecking no`` or ``accept-new``
        for that host. Its first key is then trusted for the rest of the process, but not saved,
        so a later connection to that host that presents another key is rejected.

        If ``username`` or ``password`` are specified in *both* the uri and
        ``transport_params``, ``transport_params`` will take precedence.
    """
    host, user, password, port, connect_kwargs, auto_add_host_keys = _maybe_fetch_config(
        host, user, password, port, connect_kwargs
    )
    assert path is not None  # noqa: S101  # callers always provide a remote path to open

    # Credentials and connect options are part of the key, so a client authenticated
    # by one call is never reused for a call that supplied different credentials.
    # sock is left out: a fresh socket per call would otherwise never hit the cache.
    options = {k: v for k, v in connect_kwargs.items() if k != "sock"}
    key = (host, user, port, password, repr(sorted(options.items())), auto_add_host_keys, known_hosts)

    def connect() -> paramiko.SSHClient:
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        if known_hosts:
            ssh.load_system_host_keys(known_hosts)  # adds to ~/.ssh/known_hosts, and is never written to
        # accept new hosts only when ~/.ssh/config opts out of StrictHostKeyChecking
        ssh.set_missing_host_key_policy(
            _AcceptFirstHostKey() if auto_add_host_keys else paramiko.RejectPolicy()
        )
        kwargs = connect_kwargs.copy()
        if "key_filename" not in kwargs:
            kwargs.setdefault("password", password)
        kwargs.setdefault("username", user)
        # paramiko leaves the transport thread and socket running when connect fails, so close the client then
        with contextlib.ExitStack() as on_failure:
            on_failure.callback(ssh.close)
            ssh.connect(host, port, **kwargs)
            on_failure.pop_all()
        return ssh

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
                ssh = _SSH[key] = connect()
        except KeyError:
            ssh = _SSH[key] = connect()

        try:
            transport = ssh.get_transport()
            assert transport is not None  # noqa: S101  # connect() established an active transport
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
