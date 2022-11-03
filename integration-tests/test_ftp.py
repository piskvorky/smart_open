from __future__ import unicode_literals

from smart_open import open

def test_nonbinary():
    file_contents = "Test Test \n new test \n another tests"
    appended_content1 = "Added \n to end"

    with open("ftp://user:123@localhost:21/home/user/dir/file", "w") as f:
        f.write(file_contents)

    with open("ftp://user:123@localhost:21/home/user/dir/file", "r") as f:
        read_contents = f.read()
        assert read_contents == file_contents
    
    with open("ftp://user:123@localhost:21/home/user/dir/file", "a") as f:
        f.write(appended_content1)
    
    with open("ftp://user:123@localhost:21/home/user/dir/file", "r") as f:
        read_contents = f.read()
        assert read_contents == file_contents + appended_content1

def test_binary():
    file_contents = b"Test Test \n new test \n another tests"
    appended_content1 = b"Added \n to end"

    with open("ftp://user:123@localhost:21/home/user/dir/file2", "wb") as f:
        f.write(file_contents)

    with open("ftp://user:123@localhost:21/home/user/dir/file2", "rb") as f:
        read_contents = f.read()
        assert read_contents == file_contents
    
    with open("ftp://user:123@localhost:21/home/user/dir/file2", "ab") as f:
        f.write(appended_content1)
    
    with open("ftp://user:123@localhost:21/home/user/dir/file2", "rb") as f:
        read_contents = f.read()
        assert read_contents == file_contents + appended_content1

def test_line_endings_non_binary():
    B_CLRF = b'\r\n'
    CLRF = '\r\n'
    file_contents = f"Test Test {CLRF} new test {CLRF} another tests{CLRF}"

    with open("ftp://user:123@localhost:21/home/user/dir/file3", "w") as f:
        f.write(file_contents)

    with open("ftp://user:123@localhost:21/home/user/dir/file3", "r") as f:    
        for line in f:
            assert not CLRF in line
    
    with open("ftp://user:123@localhost:21/home/user/dir/file3", "rb") as f:    
        for line in f:
            assert B_CLRF in line

def test_line_endings_binary():
    B_CLRF = b'\r\n'
    CLRF = '\r\n'
    file_contents = f"Test Test {CLRF} new test {CLRF} another tests{CLRF}".encode('utf-8')

    with open("ftp://user:123@localhost:21/home/user/dir/file4", "wb") as f:
        f.write(file_contents)

    with open("ftp://user:123@localhost:21/home/user/dir/file4", "r") as f:    
        for line in f:
            assert not CLRF in line
    
    with open("ftp://user:123@localhost:21/home/user/dir/file4", "rb") as f:    
        for line in f:
            assert B_CLRF in line