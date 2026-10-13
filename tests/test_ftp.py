# This code is distributed under the terms and conditions of the MIT License.

import contextlib
import ftplib
import socket
import socketserver
import ssl
import threading
from datetime import datetime, timedelta, timezone

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

import smart_open

PAYLOAD = b"FTP transfer data"


class _FTPHandler(socketserver.StreamRequestHandler):
    def handle(self):
        self.request.settimeout(3)
        self.wfile.write(b"220 test server\r\n")
        listener = None
        while command := self.rfile.readline():
            verb = command.split(b" ", 1)[0].strip()
            if verb == b"AUTH":
                self.wfile.write(b"234 start TLS\r\n")
                self.request = self.server.server_context.wrap_socket(self.request, server_side=True)
                self.rfile = self.request.makefile("rb")
                self.wfile = self.request.makefile("wb", buffering=0)
            elif verb in (b"PBSZ", b"PROT"):
                self.wfile.write(b"200 protection enabled\r\n")
            elif verb == b"USER":
                self.wfile.write(b"331 password required\r\n")
            elif verb == b"PASS":
                self.wfile.write(b"230 logged in\r\n")
            elif verb == b"TYPE":
                self.wfile.write(b"200 binary mode\r\n")
            elif verb == b"PASV":
                listener = socket.socket()
                listener.settimeout(3)
                listener.bind(("127.0.0.1", 0))
                listener.listen()
                port = listener.getsockname()[1]
                self.wfile.write(f"227 (127,0,0,1,{port // 256},{port % 256})\r\n".encode())
            elif verb in (b"RETR", b"STOR", b"APPE"):
                self.wfile.write(b"150 opening data connection\r\n")
                if not self.transfer(listener, verb):
                    self.server.completed.set()
                    continue
                message = "transfer complete" if self.server.status == 226 else "storage quota exceeded"
                self.wfile.write(f"{self.server.status} {message}\r\n".encode())
                self.server.completed.set()
            else:
                pytest.fail(f"Unexpected FTP command: {command!r}")

    def transfer(self, listener, verb):
        with listener:
            connection, _ = listener.accept()
            if self.server.server_context is not None:
                connection = self.server.server_context.wrap_socket(connection, server_side=True)
            with connection:
                connection.settimeout(3)
                if verb == b"RETR":
                    connection.sendall(PAYLOAD)
                else:
                    while data := connection.recv(1024):
                        self.server.received.extend(data)
                if self.server.server_context is not None:
                    try:
                        connection.unwrap()
                    except OSError:
                        self.wfile.write(b"426 TLS shutdown failed\r\n")
                        return False
        return True


@pytest.fixture(params=[("ftp", 226), ("ftp", 552), ("ftps", 226), ("ftps", 552)])
def ftp_server(request, tmp_path, monkeypatch):
    """Serve one passive FTP transfer with a configurable completion reply."""
    received = bytearray()
    completed = threading.Event()
    scheme, status = request.param
    server_context = None
    if scheme == "ftps":
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
        now = datetime.now(timezone.utc)
        certificate = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(days=1))
            .not_valid_after(now + timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
            .sign(key, hashes.SHA256())
        )
        cert_path, key_path = tmp_path / "cert.pem", tmp_path / "key.pem"
        cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
        key_path.write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
            )
        )
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(cert_path, key_path)
        client_context = ssl.create_default_context(cafile=str(cert_path))
        monkeypatch.setattr(smart_open.ftp.ssl, "create_default_context", lambda **_kwargs: client_context)

    with socketserver.TCPServer(("127.0.0.1", 0), _FTPHandler) as server:
        server.server_context = server_context
        server.status = status
        server.received = received
        server.completed = completed
        thread = threading.Thread(target=server.handle_request, daemon=True)
        thread.start()
        try:
            yield (
                f"{scheme}://user:pass@localhost:{server.server_address[1]}/file",
                status,
                received,
                completed,
            )
        finally:
            thread.join(5)
            assert not thread.is_alive()


@pytest.mark.parametrize("mode", ["rb", "wb", "ab"])
def test_transfer_completion(ftp_server, mode):
    """Observe completion errors after a data transfer, and close only once."""
    url, status, received, completed = ftp_server
    expectation = pytest.raises(ftplib.error_perm, match="552") if status == 552 else contextlib.nullcontext()
    with expectation, smart_open.open(url, mode, transport_params={"timeout": 2}) as stream:
        if mode == "rb":
            assert stream.read() == PAYLOAD
        else:
            assert stream.write(PAYLOAD) == len(PAYLOAD)
    assert completed.wait(2)
    assert stream.closed
    stream.close()
    if mode != "rb":
        assert received == PAYLOAD
