from __future__ import unicode_literals
import pytest
from smart_open import open
import ssl
from functools import partial

# localhost has self-signed cert, see ci_helpers/helpers.sh:create_ftp_ftps_servers
ssl.create_default_context = partial(ssl.create_default_context, cafile="/etc/vsftpd.pem")


@pytest.fixture(params=[("ftp", 21), ("ftps", 90)])
def server_info(request):
    return request.param

def test_nonbinary(server_info):
    server_type = server_info[0]
    port_num = server_info[1]
    file_contents = "Test Test \n new test \n another tests"
    appended_content1 = "Added \n to end"

    with open(f"{server_type}://user:123@localhost:{port_num}/file", "w") as f:
        f.write(file_contents)

    with open(f"{server_type}://user:123@localhost:{port_num}/file", "r") as f:
        read_contents = f.read()
        assert read_contents == file_contents
    
    with open(f"{server_type}://user:123@localhost:{port_num}/file", "a") as f:
        f.write(appended_content1)
    
    with open(f"{server_type}://user:123@localhost:{port_num}/file", "r") as f:
        read_contents = f.read()
        assert read_contents == file_contents + appended_content1

def test_binary(server_info):
    server_type = server_info[0]
    port_num = server_info[1]
    file_contents = b"Test Test \n new test \n another tests"
    appended_content1 = b"Added \n to end"

    with open(f"{server_type}://user:123@localhost:{port_num}/file2", "wb") as f:
        f.write(file_contents)

    with open(f"{server_type}://user:123@localhost:{port_num}/file2", "rb") as f:
        read_contents = f.read()
        assert read_contents == file_contents
    
    with open(f"{server_type}://user:123@localhost:{port_num}/file2", "ab") as f:
        f.write(appended_content1)
    
    with open(f"{server_type}://user:123@localhost:{port_num}/file2", "rb") as f:
        read_contents = f.read()
        assert read_contents == file_contents + appended_content1

def test_line_endings_non_binary(server_info):
    server_type = server_info[0]
    port_num = server_info[1]
    B_CLRF = b'\r\n'
    CLRF = '\r\n'
    file_contents = f"Test Test {CLRF} new test {CLRF} another tests{CLRF}"

    with open(f"{server_type}://user:123@localhost:{port_num}/file3", "w") as f:
        f.write(file_contents)

    with open(f"{server_type}://user:123@localhost:{port_num}/file3", "r") as f:    
        for line in f:
            assert not CLRF in line
    
    with open(f"{server_type}://user:123@localhost:{port_num}/file3", "rb") as f:    
        for line in f:
            assert B_CLRF in line

def test_line_endings_binary(server_info):
    server_type = server_info[0]
    port_num = server_info[1]
    B_CLRF = b'\r\n'
    CLRF = '\r\n'
    file_contents = f"Test Test {CLRF} new test {CLRF} another tests{CLRF}".encode('utf-8')

    with open(f"{server_type}://user:123@localhost:{port_num}/file4", "wb") as f:
        f.write(file_contents)

    with open(f"{server_type}://user:123@localhost:{port_num}/file4", "r") as f:    
        for line in f:
            assert not CLRF in line
    
    with open(f"{server_type}://user:123@localhost:{port_num}/file4", "rb") as f:    
        for line in f:
            assert B_CLRF in line
