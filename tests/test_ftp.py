# This code is distributed under the terms and conditions of the MIT License.

import contextlib
import ftplib
import socket
import socketserver
import threading

import pytest

import smart_open

PAYLOAD = b"FTP transfer data"


@pytest.fixture(params=[226, 552])
def ftp_server(request):
    """Serve one passive FTP transfer with a configurable completion reply."""
    received = bytearray()
    completed = threading.Event()
    status = request.param

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            self.request.settimeout(3)
            self.wfile.write(b"220 test server\r\n")
            listener = None
            while command := self.rfile.readline():
                verb = command.split(b" ", 1)[0].strip()
                if verb == b"USER":
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
                    with listener:
                        connection, _ = listener.accept()
                        with connection:
                            connection.settimeout(3)
                            if verb == b"RETR":
                                connection.sendall(PAYLOAD)
                            else:
                                while data := connection.recv(1024):
                                    received.extend(data)
                    message = "transfer complete" if status == 226 else "storage quota exceeded"
                    self.wfile.write(f"{status} {message}\r\n".encode())
                    completed.set()
                else:
                    pytest.fail(f"Unexpected FTP command: {command!r}")

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.handle_request, daemon=True)
        thread.start()
        try:
            yield f"ftp://user:pass@127.0.0.1:{server.server_address[1]}/file", status, received, completed
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
