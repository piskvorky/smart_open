#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

import logging
import unittest

import smart_open

GZIP_MAGIC = b"\x1f\x8b"
BASE_URL = "https://raw.githubusercontent.com/RaRe-Technologies/smart_open/master/tests/test_data/"


class ReadTest(unittest.TestCase):
    """HTTP read smoke tests against the published sample text."""

    def test_read_text(self):
        """Read a plain UTF-8 text URL via smart_open."""
        url = BASE_URL + "crime-and-punishment.txt"
        with smart_open.open(url, encoding="utf-8") as fin:
            text = fin.read()
        assert text.startswith("В начале июля, в чрезвычайно жаркое время,")  # noqa: RUF001  # Cyrillic fixture
        assert text.endswith("улизнуть, чтобы никто не видал.\n")

    def test_read_binary(self):
        """Read a plain text URL in binary mode via smart_open."""
        url = BASE_URL + "crime-and-punishment.txt"
        with smart_open.open(url, "rb") as fin:
            text = fin.read()
        assert text.startswith("В начале июля, в чрезвычайно".encode())  # noqa: RUF001  # Cyrillic fixture
        assert text.endswith("улизнуть, чтобы никто не видал.\n".encode())

    def test_read_gzip_text(self):
        """Read a gzip-compressed text URL with transparent decompression."""
        url = BASE_URL + "crime-and-punishment.txt.gz"
        with smart_open.open(url, encoding="utf-8") as fin:
            text = fin.read()
        assert text.startswith("В начале июля, в чрезвычайно жаркое время,")  # noqa: RUF001  # Cyrillic fixture
        assert text.endswith("улизнуть, чтобы никто не видал.\n")

    def test_read_gzip_binary(self):
        """Read a gzip URL with compression disabled and confirm gzip magic bytes."""
        url = BASE_URL + "crime-and-punishment.txt.gz"
        with smart_open.open(url, "rb", compression="disable") as fin:
            binary = fin.read()
        assert binary.startswith(GZIP_MAGIC)


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=logging.DEBUG)
    unittest.main()
