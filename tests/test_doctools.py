import builtins
import io
import sys
import unittest
from unittest import mock

from smart_open import doctools


class DocstringStdoutTests(unittest.TestCase):
    """The docstring builders run at import time and must leave sys.stdout alone."""

    def _stdout_seen_by_print(self, tweak):
        """Call ``tweak`` on a stub and return the sys.stdout value seen by each print call."""

        def stub():
            pass

        stub.__doc__ = doctools.PLACEHOLDER
        seen = []
        real_print = builtins.print

        def recording_print(*args, **kwargs):
            seen.append(sys.stdout)
            return real_print(*args, **kwargs)

        with mock.patch("builtins.print", recording_print):
            tweak(stub)

        assert stub.__doc__ != doctools.PLACEHOLDER
        return seen

    def test_tweak_open_docstring_does_not_rebind_stdout(self):
        """sys.stdout is process-global, so rebinding it diverts output from other threads."""
        sentinel = io.StringIO()
        with mock.patch("sys.stdout", sentinel):
            seen = self._stdout_seen_by_print(doctools.tweak_open_docstring)
        assert seen
        assert all(s is sentinel for s in seen)
        assert sentinel.getvalue() == ""

    def test_tweak_parse_uri_docstring_does_not_rebind_stdout(self):
        """sys.stdout is process-global, so rebinding it diverts output from other threads."""
        sentinel = io.StringIO()
        with mock.patch("sys.stdout", sentinel):
            seen = self._stdout_seen_by_print(doctools.tweak_parse_uri_docstring)
        assert seen
        assert all(s is sentinel for s in seen)
        assert sentinel.getvalue() == ""
