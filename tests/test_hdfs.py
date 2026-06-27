#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import gzip
import os
import os.path
import subprocess
import sys
from unittest import mock

import pytest

import smart_open.hdfs

CURR_DIR = os.path.dirname(os.path.abspath(__file__))  # noqa: PTH100, PTH120  # test fixture abspath; test fixture dirname
if sys.platform.startswith("win"):
    pytest.skip("these tests don't work under Windows", allow_module_level=True)


#
# We want our mocks to emulate the real implementation as close as possible,
# so we use a Popen call during each test.  If we mocked using io.BytesIO, then
# it is possible the mocks would behave differently to what we expect in real
# use.
#
# Since these tests use cat, they will not work in an environment without cat,
# such as Windows.  The main line of this test submodule contains a simple
# cat implementation.  We need this because Windows' analog, type, does
# weird stuff with line endings (inserts CRLF).  Also, I don't know of a way
# to get type to echo standard input.
#
def cat(path=None):
    """Cat."""
    command = [sys.executable, os.path.abspath(__file__)]  # noqa: PTH100  # test fixture abspath
    if path:
        command.append(path)
    return subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)  # noqa: S603  # subprocess in test helper


CAP_PATH = os.path.join(CURR_DIR, "test_data", "crime-and-punishment.txt")  # noqa: PTH118  # test fixture path join
with open(CAP_PATH, encoding="utf-8") as fin:  # noqa: PTH123  # test fixture path open
    CRIME_AND_PUNISHMENT = fin.read()


@pytest.mark.parametrize(
    ("uri", "expected_path"),
    [
        ("hdfs:///tmp/test.txt", "/tmp/test.txt"),  # tmp path in test
        ("hdfs://host/tmp/test.txt", "hdfs://host/tmp/test.txt"),
        ("hdfs://host:8020/tmp/test.txt", "hdfs://host:8020/tmp/test.txt"),
        ("viewfs:///tmp/test.txt", "/tmp/test.txt"),  # tmp path in test
        ("viewfs://cluster/tmp/test.txt", "viewfs://cluster/tmp/test.txt"),
    ],
)
def test_parse_uri(uri, expected_path):
    """Parse uri."""
    parsed = smart_open.hdfs.parse_uri(uri)
    assert parsed["uri_path"] == expected_path


@pytest.mark.parametrize("uri", ["hdfs:///", "hdfs://"])
def test_parse_uri_invalid(uri):
    """Parse uri invalid."""
    with pytest.raises(RuntimeError):
        smart_open.hdfs.parse_uri(uri)


def test_sanity_read_bytes():
    """Sanity read bytes."""
    with open(CAP_PATH, "rb") as fin:  # noqa: PTH123  # test fixture path open
        lines = list(fin)
    assert len(lines) == 3  # test uses inline magic value


def test_sanity_read_text():
    """Sanity read text."""
    with open(CAP_PATH, encoding="utf-8") as fin:  # noqa: PTH123  # test fixture path open
        text = fin.read()

    expected = "В начале июля, в чрезвычайно жаркое время"  # noqa: RUF001  # fixture deliberately uses Cyrillic characters
    assert text[: len(expected)] == expected


@pytest.mark.parametrize("schema", [("hdfs",), ("viewfs",)])
def test_read(schema):
    """Read."""
    with mock.patch("subprocess.Popen", return_value=cat(CAP_PATH)):
        reader = smart_open.hdfs.CliRawInputBase(f"{schema}://dummy/url")
        as_bytes = reader.read()

    #
    # Not 100% sure why this is necessary on Windows platforms, but the
    # tests fail without it.  It may be a bug, but I don't have time to
    # investigate right now.
    #
    as_text = as_bytes.decode("utf-8").replace(os.linesep, "\n")
    assert as_text == CRIME_AND_PUNISHMENT


@pytest.mark.parametrize("schema", [("hdfs",), ("viewfs",)])
def test_read_75(schema):
    """Read 75."""
    with mock.patch("subprocess.Popen", return_value=cat(CAP_PATH)):
        reader = smart_open.hdfs.CliRawInputBase(f"{schema}://dummy/url")
        as_bytes = reader.read(75)

    as_text = as_bytes.decode("utf-8").replace(os.linesep, "\n")
    assert as_text == CRIME_AND_PUNISHMENT[: len(as_text)]


@pytest.mark.parametrize("schema", [("hdfs",), ("viewfs",)])
def test_unzip(schema):
    """Unzip."""
    with (
        mock.patch("subprocess.Popen", return_value=cat(CAP_PATH + ".gz")),
        gzip.GzipFile(fileobj=smart_open.hdfs.CliRawInputBase(f"{schema}://dummy/url")) as fin,
    ):
        as_bytes = fin.read()

    as_text = as_bytes.decode("utf-8")
    assert as_text == CRIME_AND_PUNISHMENT


@pytest.mark.parametrize("schema", [("hdfs",), ("viewfs",)])
def test_context_manager(schema):
    """Context manager."""
    with (
        mock.patch("subprocess.Popen", return_value=cat(CAP_PATH)),
        smart_open.hdfs.CliRawInputBase(f"{schema}://dummy/url") as fin,
    ):
        as_bytes = fin.read()

    as_text = as_bytes.decode("utf-8").replace("\r\n", "\n")
    assert as_text == CRIME_AND_PUNISHMENT


@pytest.mark.parametrize("schema", [("hdfs",), ("viewfs",)])
def test_write(schema):
    """Write."""
    expected = "мы в ответе за тех, кого приручили"
    mocked_cat = cat()

    payload = expected.encode("utf-8")
    with (
        mock.patch("subprocess.Popen", return_value=mocked_cat),
        smart_open.hdfs.CliRawOutputBase(f"{schema}://dummy/url") as fout,
    ):
        written = fout.write(payload)

    # CliRawOutputBase implements io.RawIOBase, whose write() contract is to
    # return the number of bytes written. Returning None breaks callers like
    # ray._private.external_storage that assert on the return value.
    assert written == len(payload)

    actual = mocked_cat.stdout.read().decode("utf-8")
    assert actual == expected


@pytest.mark.parametrize("schema", [("hdfs",), ("viewfs",)])
def test_write_zip(schema):
    """Write zip."""
    expected = "мы в ответе за тех, кого приручили"
    mocked_cat = cat()

    with (
        mock.patch("subprocess.Popen", return_value=mocked_cat),
        smart_open.hdfs.CliRawOutputBase(f"{schema}://dummy/url") as fout,
        gzip.GzipFile(fileobj=fout, mode="wb") as gz_fout,
    ):
        gz_fout.write(expected.encode("utf-8"))

    with gzip.GzipFile(fileobj=mocked_cat.stdout) as fin:
        actual = fin.read().decode("utf-8")

    assert actual == expected


def main():
    """Main."""
    try:
        path = sys.argv[1]
    except IndexError:
        bytez = sys.stdin.buffer.read()
    else:
        with open(path, "rb") as fin:  # noqa: PTH123  # test fixture path open
            bytez = fin.read()

    sys.stdout.buffer.write(bytez)
    sys.stdout.flush()


if __name__ == "__main__":
    main()
