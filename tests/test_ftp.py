import ftplib
import socket
from unittest.mock import Mock

import pytest

import smart_open.ftp


@pytest.mark.parametrize("mode", ["wb", "ab"])
def test_failed_upload_reply_raises_on_close(monkeypatch, mode):
    """Raise the final reply of a rejected upload instead of losing data silently."""
    connection = Mock()
    connection.transfercmd.return_value, peer = socket.socketpair()
    connection.voidresp.side_effect = ftplib.error_temp("451 Failure writing to local file.")  # noqa: S321  # constructing a test exception
    monkeypatch.setattr(smart_open.ftp, "_connect", Mock(return_value=connection))
    with (
        peer,
        pytest.raises(ftplib.error_temp, match="451"),
        smart_open.ftp.open("file", mode, host="host", user="user") as fout,
    ):
        fout.write(b"data")
    fout.close()
    connection.voidresp.assert_called_once()
    connection.close.assert_called_once()
