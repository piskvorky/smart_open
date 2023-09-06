# -*- coding: utf-8 -*-
import functools
import logging
import unittest
import uuid
import time

import boto3
import moto

from smart_open import open


BUCKET_NAME = "test-smartopen"
KEY_NAME = "test-key"


logger = logging.getLogger(__name__)


_resource = functools.partial(boto3.resource, region_name="us-east-1")


def get_versions(bucket, key):
    """Return object versions in chronological order."""
    return [
        v.id
        for v in sorted(
            _resource("s3").Bucket(bucket).object_versions.filter(Prefix=key),
            key=lambda version: version.last_modified,
        )
    ]


@moto.mock_s3
class TestVersionId(unittest.TestCase):
    def setUp(self):
        #
        # Each run of this test reuses the BUCKET_NAME, but works with a
        # different key for isolation.
        #
        resource = _resource("s3")
        resource.create_bucket(Bucket=BUCKET_NAME).wait_until_exists()
        resource.BucketVersioning(BUCKET_NAME).enable()

        self.key = "test-write-key-{}".format(uuid.uuid4().hex)
        self.url = "s3://%s/%s" % (BUCKET_NAME, self.key)
        self.test_ver1 = "String version 1.0".encode("utf8")
        self.test_ver2 = "String version 2.0".encode("utf8")

        bucket = resource.Bucket(BUCKET_NAME)
        bucket.put_object(Key=self.key, Body=self.test_ver1)
        logging.critical(
            "versions after first write: %r", get_versions(BUCKET_NAME, self.key)
        )

        time.sleep(3)

        bucket.put_object(Key=self.key, Body=self.test_ver2)
        self.versions = get_versions(BUCKET_NAME, self.key)
        logging.critical(
            "versions after second write: %r", get_versions(BUCKET_NAME, self.key)
        )

        assert len(self.versions) == 2

    def test_good_id(self):
        """Does passing the version_id parameter into the s3 submodule work correctly when reading?"""
        params = {"version_id": self.versions[0]}
        with open(self.url, mode="rb", transport_params=params) as fin:
            actual = fin.read()
        self.assertEqual(actual, self.test_ver1)

    def test_bad_id(self):
        """Does passing an invalid version_id exception into the s3 submodule get handled correctly?"""
        params = {"version_id": "bad-version-does-not-exist"}
        with self.assertRaises(IOError):
            open(self.url, "rb", transport_params=params)

    def test_bad_mode(self):
        """Do we correctly handle non-None version when writing?"""
        params = {"version_id": self.versions[0]}
        with self.assertRaises(ValueError):
            open(self.url, "wb", transport_params=params)

    def test_no_version(self):
        """Passing in no version at all gives the newest version of the file?"""
        with open(self.url, "rb") as fin:
            actual = fin.read()
        self.assertEqual(actual, self.test_ver2)

    def test_newest_version(self):
        """Passing in the newest version explicitly gives the most recent content?"""
        params = {"version_id": self.versions[1]}
        with open(self.url, mode="rb", transport_params=params) as fin:
            actual = fin.read()
        self.assertEqual(actual, self.test_ver2)

    def test_oldest_version(self):
        """Passing in the oldest version gives the oldest content?"""
        params = {"version_id": self.versions[0]}
        with open(self.url, mode="rb", transport_params=params) as fin:
            actual = fin.read()
        self.assertEqual(actual, self.test_ver1)

    def test_version_to_boto3(self):
        """Passing in the oldest version gives the oldest content?"""
        self.versions = get_versions(BUCKET_NAME, self.key)
        params = {"version_id": self.versions[0]}
        with open(self.url, mode="rb", transport_params=params) as fin:
            returned_obj = fin.to_boto3(_resource("s3"))

        boto3_body = boto3_body = returned_obj.get()["Body"].read()
        self.assertEqual(boto3_body, self.test_ver1)


if __name__ == "__main__":
    unittest.main()
