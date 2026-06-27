import os
import unittest

import pytest

from smart_open import open

skip_tests = "SMART_OPEN_TEST_MISSING_DEPS" not in os.environ


class PackageTests(unittest.TestCase):
    """Tests for the package's import-time error messages."""

    @pytest.mark.skipif(skip_tests, reason="requires missing dependencies")
    def test_azure_raises_helpful_error_with_missing_deps(self):
        """Azure raises helpful error with missing deps."""
        with pytest.raises(ImportError, match=r"pip install smart_open\[azure\]"):
            open("azure://foo/bar")

    @pytest.mark.skipif(skip_tests, reason="requires missing dependencies")
    def test_aws_raises_helpful_error_with_missing_deps(self):
        """Aws raises helpful error with missing deps."""
        match = r"pip install smart_open\[s3\]"
        with pytest.raises(ImportError, match=match):
            open("s3://foo/bar")

    @pytest.mark.skipif(skip_tests, reason="requires missing dependencies")
    def test_gcs_raises_helpful_error_with_missing_deps(self):
        """Gcs raises helpful error with missing deps."""
        with pytest.raises(ImportError, match=r"pip install smart_open\[gcs\]"):
            open("gcs://foo/bar")
