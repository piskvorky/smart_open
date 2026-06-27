#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements I/O streams over FTP."""

from __future__ import annotations

import logging
import ssl
import types
import urllib.parse
from ftplib import FTP, FTP_TLS, error_reply
from typing import IO, TYPE_CHECKING, Any, TypedDict, cast

import smart_open.utils

if TYPE_CHECKING:
    from smart_open._typing import TransportParams

logger = logging.getLogger(__name__)

SCHEMES = ("ftp", "ftps")

"""Supported URL schemes."""

DEFAULT_PORT = 21

URI_EXAMPLES = (
    "ftp://username@host/path/file",
    "ftp://username:password@host/path/file",
    "ftp://username:password@host:port/path/file",
    "ftps://username@host/path/file",
    "ftps://username:password@host/path/file",
    "ftps://username:password@host:port/path/file",
)


class _FTPUri(TypedDict):
    scheme: str
    uri_path: str | None
    user: str | None
    host: str | None
    port: int
    password: str | None


def _unquote(text: str | None) -> str | None:
    return text and urllib.parse.unquote(text)


def parse_uri(uri_as_string: str) -> _FTPUri:
    """Parse an ``ftp://`` or ``ftps://`` URI into connection components."""
    split_uri = urllib.parse.urlsplit(uri_as_string)
    assert split_uri.scheme in SCHEMES  # noqa: S101  # internal precondition; misuse should crash loudly
    return {
        "scheme": split_uri.scheme,
        "uri_path": _unquote(split_uri.path),
        "user": _unquote(split_uri.username),
        "host": split_uri.hostname,
        "port": int(split_uri.port or DEFAULT_PORT),
        "password": _unquote(split_uri.password),
    }


def open_uri(uri: str, mode: str, transport_params: TransportParams) -> IO[Any]:
    """Open an FTP/FTPS URI using the given mode and transport params."""
    smart_open.utils.check_kwargs(open, transport_params)
    parsed_uri: dict[str, Any] = dict(parse_uri(uri))
    uri_path = parsed_uri.pop("uri_path")
    scheme = parsed_uri.pop("scheme")
    secure_conn = scheme == "ftps"
    return open(
        uri_path,
        mode,
        secure_connection=secure_conn,
        transport_params=transport_params,
        **parsed_uri,
    )


def convert_transport_params_to_args(transport_params: TransportParams) -> dict[str, Any]:
    """Return the subset of `transport_params` that the FTP client accepts."""
    supported_keywords = [
        "timeout",
        "source_address",
        "encoding",
    ]
    unsupported_keywords = [k for k in transport_params if k not in supported_keywords]
    kwargs = {k: v for (k, v) in transport_params.items() if k in supported_keywords}

    if unsupported_keywords:
        logger.warning("ignoring unsupported ftp keyword arguments: %r", unsupported_keywords)

    return kwargs


def _connect(  # noqa: PLR0913  # legacy internal helper; refactor in a dedicated PR
    hostname: str,
    username: str | None,
    port: int,
    password: str | None,
    secure_connection: bool,  # noqa: FBT001  # legacy internal helper
    transport_params: TransportParams,
) -> FTP | FTP_TLS:
    kwargs = convert_transport_params_to_args(transport_params)
    ftp: FTP | FTP_TLS
    if secure_connection:
        ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        ftp = FTP_TLS(context=ssl_context, **kwargs)  # noqa: S321  # this module's purpose
    else:
        ftp = FTP(**kwargs)  # noqa: S321  # this module's purpose
    try:
        ftp.connect(hostname, port)
    except Exception:
        logger.exception("Unable to connect to FTP server: try checking the host and port!")
        raise
    try:
        ftp.login(cast("str", username), cast("str", password))
    except error_reply:
        logger.exception("Unable to login to FTP server: try checking the username and password!")
        raise
    if isinstance(ftp, FTP_TLS):
        ftp.prot_p()
    return ftp


def open(  # noqa: PLR0913  # legacy public API; refactor in a dedicated PR
    path: str | None,
    mode: str = "rb",
    host: str | None = None,
    user: str | None = None,
    password: str | None = None,
    port: int = DEFAULT_PORT,
    secure_connection: bool = False,  # noqa: FBT001, FBT002  # public API
    transport_params: TransportParams | None = None,
) -> IO[Any]:
    """Open a file for reading or writing via FTP/FTPS.

    Args:
        path: The path on the remote server.
        mode: Must be "rb" or "wb".
        host: The host to connect to.
        user: The username to use for the connection.
        password: The password for the specified username.
        port: The port to connect to.
        secure_connection: True for FTPS, False for FTP.
        transport_params: Additional parameters for the FTP connection.
            Currently supported parameters: timeout, source_address, encoding.

    Returns:
        A file-like object for the remote FTP/FTPS file.

    Raises:
        ValueError: If `host` or `user` is not specified, or if `mode` is unsupported.
    """
    if not host:
        msg = "you must specify the host to connect to"
        raise ValueError(msg)
    if not user:
        msg = "you must specify the user"
        raise ValueError(msg)
    if not transport_params:
        transport_params = {}
    conn = _connect(host, user, port, password, secure_connection, transport_params)
    mode_to_ftp_cmds = {
        "rb": ("RETR", "rb"),
        "wb": ("STOR", "wb"),
        "ab": ("APPE", "wb"),
    }
    try:
        ftp_mode, file_obj_mode = mode_to_ftp_cmds[mode]
    except KeyError as err:
        msg = f"unsupported mode: {mode!r}"
        raise ValueError(msg) from err
    ftp_mode, file_obj_mode = mode_to_ftp_cmds[mode]
    conn.voidcmd("TYPE I")
    socket = conn.transfercmd(f"{ftp_mode} {path}")
    fobj: Any = socket.makefile(cast("Any", file_obj_mode))

    def full_close(self: Any) -> None:
        self.orig_close()
        self.socket.close()
        self.conn.close()

    fobj.orig_close = fobj.close
    fobj.socket = socket
    fobj.conn = conn
    fobj.close = types.MethodType(full_close, fobj)
    return fobj
