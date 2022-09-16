# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements I/O streams over FTP.
"""

import logging
import urllib.parse
import smart_open.utils
from ftplib import FTP, error_reply
import types
logger = logging.getLogger(__name__)

SCHEME = "ftp"

"""Supported URL schemes."""

DEFAULT_PORT = 21

URI_EXAMPLES = (
    "ftp://username@host/path/file",
    "ftp://username:password@host/path/file",
    "ftp://username:password@host:port/path/file",
)


def _unquote(text):
    return text and urllib.parse.unquote(text)


def parse_uri(uri_as_string):
    split_uri = urllib.parse.urlsplit(uri_as_string)
    assert split_uri.scheme == SCHEME, 'unexpected scheme: %r' % split_uri.scheme
    return dict(
        scheme=split_uri.scheme,
        uri_path=_unquote(split_uri.path),
        user=_unquote(split_uri.username),
        host=split_uri.hostname,
        port=int(split_uri.port or DEFAULT_PORT),
        password=_unquote(split_uri.password),
    )


def open_uri(uri, mode, transport_params):
    smart_open.utils.check_kwargs(open, transport_params)
    parsed_uri = parse_uri(uri)
    uri_path = parsed_uri.pop("uri_path")
    parsed_uri.pop("scheme")
    return open(uri_path, mode, transport_params=transport_params, **parsed_uri)


def convert_transport_params_to_args(transport_params):
    supported_keywords = [
        "timeout",
        "source_address",
        "encoding",
    ]
    unsupported_keywords = [k for k in transport_params if k not in supported_keywords]
    kwargs = {k: v for (k, v) in transport_params.items() if k in supported_keywords}

    if unsupported_keywords:
        logger.warning(
            "ignoring unsupported ftp keyword arguments: %r", unsupported_keywords
        )

    return kwargs


def _connect(hostname, username, port, password, transport_params):
    kwargs = convert_transport_params_to_args(transport_params)
    ftp = FTP(**kwargs)
    try:
        ftp.connect(hostname, port)
    except Exception as e:
        logger.error("Unable to connect to FTP server: try checking the host and port!")
        raise e
    try:
        ftp.login(username, password)
    except error_reply as e:
        logger.error("Unable to login to FTP server: try checking the username and password!")
        raise e
    return ftp


# transport paramaters can include any extra parameters that you want to be passed into FTP_TLS
def open(
    path,
    mode="r",
    host=None,
    user=None,
    password=None,
    port=DEFAULT_PORT,
    transport_params=None,
):
    if not host:
        raise ValueError("you must specify the host to connect to")
    if not user:
        raise ValueError("you must specify the user")
    if not transport_params:
        transport_params = {}
    conn = _connect(host, user, port, password, transport_params)
    mode_to_ftp_cmds = {
        "r": ("RETR", "r"),
        "rb": ("RETR", "rb"),
        "w": ("STOR", "w"),
        "wb": ("STOR", "wb"),
        "a": ("APPE", "w"),
        "ab": ("APPE", "wb")
    }
    try:
        ftp_mode, file_obj_mode = mode_to_ftp_cmds[mode]
    except KeyError:
        raise ValueError(f"unsupported mode: {mode!r}")
    ftp_mode, file_obj_mode = mode_to_ftp_cmds[mode]
    socket = conn.transfercmd(f"{ftp_mode} {path}")
    fobj = socket.makefile(file_obj_mode)

    def full_close(self):
        self.orig_close()
        self.socket.close()
        self.conn.close()
    fobj.orig_close = fobj.close
    fobj.socket = socket
    fobj.conn = conn
    fobj.close = types.MethodType(full_close, fobj)
    return fobj
