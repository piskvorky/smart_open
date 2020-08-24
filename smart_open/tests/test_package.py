# -*- coding: utf-8 -*-
import os
import unittest
import pytest

skip_tests = "SMART_OPEN_TEST_MISSING_DEPS" not in os.environ


class PackageTests(unittest.TestCase):

    @pytest.mark.skipif(skip_tests, reason="requires missing dependencies")
    def test_azure_raises_helpful_error_with_missing_deps(self):
        with pytest.raises(ImportError, match=r"pip install smart_open\[azure\]"):
            from smart_open import azure # noqa

    @pytest.mark.skipif(skip_tests, reason="requires missing dependencies")
    def test_aws_raises_helpful_error_with_missing_deps(self):
        match = r"pip install smart_open\[aws\]"
        with pytest.raises(ImportError, match=match):
            from smart_open import s3 # noqa
        with pytest.raises(ImportError, match=match):
            from smart_open import s3_iter_bucket
            s3_iter_bucket('fake-name')
        with pytest.raises(ImportError, match=match):
            from smart_open import smart_open
            smart_open('fake-name', profile_name="will produce an error importing s3")

    @pytest.mark.skipif(skip_tests, reason="requires missing dependencies")
    def test_gcs_raises_helpful_error_with_missing_deps(self):
        with pytest.raises(ImportError, match=r"pip install smart_open\[gcs\]"):
            from smart_open import gcs # noqa
