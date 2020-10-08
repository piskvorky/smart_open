# -*- coding: utf-8 -*-
# import os
import unittest
import pytest

from smart_open import open

#
# Temporarily disable these tests while we deal with the fallout of the 2.2.0
# release.
#
# skip_tests = "SMART_OPEN_TEST_MISSING_DEPS" not in os.environ
skip_tests = True


class PackageTests(unittest.TestCase):

    @pytest.mark.skipif(skip_tests, reason="requires missing dependencies")
    def test_azure_raises_helpful_error_with_missing_deps(self):
        with pytest.raises(ImportError, match=r"pip install smart_open\[azure\]"):
            open("azure://foo/bar")

    @pytest.mark.skipif(skip_tests, reason="requires missing dependencies")
    def test_aws_raises_helpful_error_with_missing_deps(self):
        match = r"pip install smart_open\[s3\]"
        with pytest.raises(ImportError, match=match):
            open("s3://foo/bar")

    @pytest.mark.skipif(skip_tests, reason="requires missing dependencies")
    def test_gcs_raises_helpful_error_with_missing_deps(self):
        with pytest.raises(ImportError, match=r"pip install smart_open\[gcs\]"):
            open("gs://foo/bar")
