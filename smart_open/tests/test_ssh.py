# -*- coding: utf-8 -*-

import logging
import unittest
from unittest import mock

from paramiko import SSHException

import smart_open.ssh


def mock_ssh(func):
    def wrapper(*args, **kwargs):
        smart_open.ssh._SSH.clear()
        return func(*args, **kwargs)

    return mock.patch("paramiko.SSHClient.get_transport")(
        mock.patch("paramiko.SSHClient.connect")(wrapper)
    )


class SSHOpen(unittest.TestCase):
    @mock_ssh
    def test_open(self, mock_connect, get_transp_mock):
        smart_open.open("ssh://user:pass@some-host/")
        mock_connect.assert_called_with("some-host", 22, username="user", password="pass")

    @mock_ssh
    def test_percent_encoding(self, mock_connect, get_transp_mock):
        smart_open.open("ssh://user%3a:pass%40@some-host/")
        mock_connect.assert_called_with("some-host", 22, username="user:", password="pass@")

    @mock_ssh
    def test_open_without_password(self, mock_connect, get_transp_mock):
        smart_open.open("ssh://user@some-host/")
        mock_connect.assert_called_with("some-host", 22, username="user", password=None)

    @mock_ssh
    def test_open_with_transport_params(self, mock_connect, get_transp_mock):
        smart_open.open(
            "ssh://user:pass@some-host/",
            transport_params={"connect_kwargs": {"username": "ubuntu", "password": "pwd"}},
        )
        mock_connect.assert_called_with("some-host", 22, username="ubuntu", password="pwd")

    @mock_ssh
    def test_open_with_key_filename(self, mock_connect, get_transp_mock):
        smart_open.open(
            "ssh://user@some-host/",
            transport_params={"connect_kwargs": {"key_filename": "key"}},
        )
        mock_connect.assert_called_with("some-host", 22, username="user", key_filename="key")

    @mock_ssh
    def test_reconnect_after_session_timeout(self, mock_connect, get_transp_mock):
        mock_sftp = get_transp_mock().open_sftp_client()
        get_transp_mock().open_sftp_client.reset_mock()

        def mocked_open_sftp():
            if len(mock_connect.call_args_list) < 2:  # simulate timeout until second connect()
                yield SSHException('SSH session not active')
            while True:
                yield mock_sftp

        get_transp_mock().open_sftp_client.side_effect = mocked_open_sftp()

        smart_open.open("ssh://user:pass@some-host/")
        mock_connect.assert_called_with("some-host", 22, username="user", password="pass")
        mock_sftp.open.assert_called_once()


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=logging.DEBUG)
    unittest.main()
