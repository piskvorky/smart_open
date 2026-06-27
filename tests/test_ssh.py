import logging
import os
import unittest
from unittest import mock

from paramiko import SSHException

import smart_open.ssh

_TEST_DATA_PATH = os.path.join(os.path.dirname(__file__), "test_data")  # noqa: PTH118, PTH120  # test fixture path join; test fixture dirname
_CONFIG_PATH = os.path.join(_TEST_DATA_PATH, "ssh.cfg")  # noqa: PTH118  # test fixture path join


def mock_ssh(func):
    """Mock ssh."""

    def wrapper(*args, **kwargs):
        smart_open.ssh._SSH.clear()  # test reaches into private state
        return func(*args, **kwargs)

    return mock.patch("paramiko.SSHClient.get_transport")(mock.patch("paramiko.SSHClient.connect")(wrapper))


class SSHOpen(unittest.TestCase):
    """Tests for opening ssh:// URIs."""

    def setUp(self):
        """SetUp."""
        self._cfg_files = smart_open.ssh._SSH_CONFIG_FILES  # test reaches into private state
        smart_open.ssh._SSH_CONFIG_FILES = [_CONFIG_PATH]  # test reaches into private state

    def tearDown(self):
        """TearDown."""
        smart_open.ssh._SSH_CONFIG_FILES = self._cfg_files  # test reaches into private state

    @mock_ssh
    def test_open(self, mock_connect, get_transp_mock):
        """Open."""
        smart_open.open("ssh://user:pass@some-host/")
        mock_connect.assert_called_with(
            "some-host", 22, username="user", password="pass"
        )  # fixture password kwarg

    @mock_ssh
    def test_percent_encoding(self, mock_connect, get_transp_mock):
        """Percent encoding."""
        smart_open.open("ssh://user%3a:pass%40@some-host/")
        mock_connect.assert_called_with(
            "some-host", 22, username="user:", password="pass@"
        )  # fixture password kwarg

    @mock_ssh
    def test_open_without_password(self, mock_connect, get_transp_mock):
        """Open without password."""
        smart_open.open("ssh://user@some-host/")
        mock_connect.assert_called_with("some-host", 22, username="user", password=None)

    @mock_ssh
    def test_open_with_transport_params(self, mock_connect, get_transp_mock):
        """Open with transport params."""
        smart_open.open(
            "ssh://user:pass@some-host/",
            transport_params={"connect_kwargs": {"username": "ubuntu", "password": "pwd"}},
        )
        mock_connect.assert_called_with(
            "some-host", 22, username="ubuntu", password="pwd"
        )  # fixture password kwarg

    @mock_ssh
    def test_open_with_key_filename(self, mock_connect, get_transp_mock):
        """Open with key filename."""
        smart_open.open(
            "ssh://user@some-host/",
            transport_params={"connect_kwargs": {"key_filename": "key"}},
        )
        mock_connect.assert_called_with("some-host", 22, username="user", key_filename="key")

    @mock_ssh
    def test_reconnect_after_session_timeout(self, mock_connect, get_transp_mock):
        """Reconnect after session timeout."""
        mock_sftp = get_transp_mock().open_sftp_client()
        get_transp_mock().open_sftp_client.reset_mock()

        def mocked_open_sftp():
            if (
                len(mock_connect.call_args_list) < 2  # test uses inline magic value
            ):  # simulate timeout until second connect()
                yield SSHException("SSH session not active")
            while True:
                yield mock_sftp

        get_transp_mock().open_sftp_client.side_effect = mocked_open_sftp()

        smart_open.open("ssh://user:pass@some-host/")
        mock_connect.assert_called_with(
            "some-host", 22, username="user", password="pass"
        )  # fixture password kwarg
        mock_sftp.open.assert_called_once()

    @mock_ssh
    def test_open_with_openssh_config(self, mock_connect, get_transp_mock):
        """Open with openssh config."""
        smart_open.open("ssh://another-host/")
        mock_connect.assert_called_with(
            "another-host-domain.com",
            2345,
            username="another-user",
            key_filename=["/path/to/key/file"],
            timeout=20.0,
            compress=True,
            gss_auth=False,
            gss_kex=False,
            gss_deleg_creds=False,
            gss_trust_dns=False,
        )

    @mock_ssh
    def test_open_with_openssh_config_override_port(self, mock_connect, get_transp_mock):
        """Open with openssh config override port."""
        smart_open.open("ssh://another-host:22/")
        mock_connect.assert_called_with(
            "another-host-domain.com",
            22,
            username="another-user",
            key_filename=["/path/to/key/file"],
            timeout=20.0,
            compress=True,
            gss_auth=False,
            gss_kex=False,
            gss_deleg_creds=False,
            gss_trust_dns=False,
        )

    @mock_ssh
    def test_open_with_openssh_config_override_port2(self, mock_connect, get_transp_mock):
        """Open with openssh config override port2."""
        smart_open.open("ssh://another-host/", transport_params={"port": 22})
        mock_connect.assert_called_with(
            "another-host-domain.com",
            22,
            username="another-user",
            key_filename=["/path/to/key/file"],
            timeout=20.0,
            compress=True,
            gss_auth=False,
            gss_kex=False,
            gss_deleg_creds=False,
            gss_trust_dns=False,
        )

    @mock_ssh
    def test_open_with_openssh_config_missing_port(self, mock_connect, get_transp_mock):
        """Open with openssh config missing port."""
        smart_open.open("ssh://another-host-missing-port/")
        mock_connect.assert_called_with(
            "another-host-domain.com",
            22,
            username="another-user",
            key_filename=["/path/to/key/file"],
            timeout=20.0,
            compress=True,
            gss_auth=False,
            gss_kex=False,
            gss_deleg_creds=False,
            gss_trust_dns=False,
        )

    @mock_ssh
    def test_open_with_openssh_config_override_user(self, mock_connect, get_transp_mock):
        """Open with openssh config override user."""
        smart_open.open("ssh://new-user@another-host/")
        mock_connect.assert_called_with(
            "another-host-domain.com",
            2345,
            username="new-user",
            key_filename=["/path/to/key/file"],
            timeout=20.0,
            compress=True,
            gss_auth=False,
            gss_kex=False,
            gss_deleg_creds=False,
            gss_trust_dns=False,
        )

    @mock_ssh
    def test_open_with_prefetch(self, mock_connect, get_transp_mock):
        """Open with prefetch."""
        smart_open.open(
            "ssh://user:pass@some-host/",
            transport_params={"prefetch_kwargs": {"max_concurrent_requests": 3}},
        )
        mock_sftp = get_transp_mock().open_sftp_client()
        mock_fobj = mock_sftp.open()
        mock_fobj.prefetch.assert_called_with(max_concurrent_requests=3)

    @mock_ssh
    def test_open_without_prefetch(self, mock_connect, get_transp_mock):
        """Open without prefetch."""
        smart_open.open("ssh://user:pass@some-host/")
        mock_sftp = get_transp_mock().open_sftp_client()
        mock_fobj = mock_sftp.open()
        mock_fobj.prefetch.assert_not_called()


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=logging.DEBUG)
    unittest.main()
