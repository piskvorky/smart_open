import ftplib
from unittest.mock import Mock

import pytest

import smart_open.ftp


@pytest.mark.parametrize("operation", ["connect", "login"])
def test_failed_ftp_connection_closes_control_socket(monkeypatch, operation):
    """Release the control socket when connecting or authenticating fails."""
    connection = Mock()
    getattr(connection, operation).side_effect = ftplib.error_perm("denied")  # noqa: S321  # constructing a test exception
    monkeypatch.setattr(smart_open.ftp, "FTP", Mock(return_value=connection))  # mocked FTP construction
    with pytest.raises(ftplib.error_perm, match="denied"):
        smart_open.ftp._connect("host", "user", 21, "password", secure_connection=False, transport_params={})
    connection.close.assert_called_once()


def test_invalid_ftp_mode_does_not_connect(monkeypatch):
    """Validate a requested file mode before opening a connection."""
    connect = Mock()
    monkeypatch.setattr(smart_open.ftp, "_connect", connect)
    with pytest.raises(ValueError, match="unsupported mode"):
        smart_open.ftp.open("file", "invalid", host="host", user="user")
    connect.assert_not_called()


@pytest.mark.parametrize("operation", ["voidcmd", "transfercmd"])
def test_failed_ftp_transfer_setup_closes_connection(monkeypatch, operation):
    """Release the control socket if the server refuses transfer setup."""
    connection = Mock()
    getattr(connection, operation).side_effect = ftplib.error_perm("transfer denied")  # noqa: S321  # constructing a test exception
    monkeypatch.setattr(smart_open.ftp, "_connect", Mock(return_value=connection))
    with pytest.raises(ftplib.error_perm, match="transfer denied"):
        smart_open.ftp.open("file", "rb", host="host", user="user")
    connection.close.assert_called_once()
