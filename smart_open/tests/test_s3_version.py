# -*- coding: utf-8 -*-
import logging
import os
import time
import unittest
import uuid

import boto3
import moto

from smart_open import open
from smart_open.tests.test_s3 import ensure_bucket_exists, cleanup_bucket


BUCKET_NAME = 'test-smartopen-{}'.format(uuid.uuid4().hex)
KEY_NAME = 'test-key'
DISABLE_MOCKS = os.environ.get('SO_DISABLE_MOCKS') == "1"


logger = logging.getLogger(__name__)


def maybe_mock_s3(func):
    if DISABLE_MOCKS:
        return func
    else:
        return moto.mock_s3(func)


@maybe_mock_s3
def setUpModule():
    '''Called once by unittest when initializing this module.  Sets up the
    test S3 bucket.

    '''
    boto3.resource('s3').create_bucket(Bucket=BUCKET_NAME)
    ensure_bucket_exists(BUCKET_NAME)
    boto3.resource('s3').BucketVersioning(BUCKET_NAME).enable()


def get_versions(bucket, key):
    """Return object versions in chronological order."""
    return [
        v.id
        for v in sorted(
            boto3.resource('s3').Bucket(bucket).object_versions.filter(Prefix=key),
            key=lambda version: version.last_modified,
        )
    ]


@maybe_mock_s3
class TestVersionId(unittest.TestCase):

    def setUp(self):
        ensure_bucket_exists(BUCKET_NAME)
        #
        # Each run of this test reuses the BUCKET_NAME, but works with a
        # different key for isolation.
        #
        self.key = 'test-write-key-{}'.format(uuid.uuid4().hex)
        self.url = "s3://%s/%s" % (BUCKET_NAME, self.key)
        self.test_ver1 = u"String version 1.0".encode('utf8')
        self.test_ver2 = u"String version 2.0".encode('utf8')

        bucket = boto3.resource('s3').Bucket(BUCKET_NAME)
        bucket.put_object(Key=self.key, Body=self.test_ver1)

        logging.critical('versions after first write: %r', get_versions(BUCKET_NAME, self.key))

        if DISABLE_MOCKS:
            #
            # I suspect there is a race condition that's messing up the
            # order of the versions in the test.
            #
            time.sleep(5)

        bucket.put_object(Key=self.key, Body=self.test_ver2)

        self.versions = get_versions(BUCKET_NAME, self.key)
        logging.critical('versions after second write: %r', get_versions(BUCKET_NAME, self.key))

        assert len(self.versions) == 2

    def test_good_id(self):
        """Does passing the version_id parameter into the s3 submodule work correctly when reading?"""
        params = {'version_id': self.versions[0]}
        with open(self.url, mode='rb', transport_params=params) as fin:
            actual = fin.read()
        self.assertEqual(actual, self.test_ver1)

    def test_bad_id(self):
        """Does passing an invalid version_id exception into the s3 submodule get handled correctly?"""
        params = {'version_id': 'bad-version-does-not-exist'}
        with self.assertRaises(IOError):
            open(self.url, 'rb', transport_params=params)

    def test_bad_mode(self):
        """Do we correctly handle non-None version when writing?"""
        params = {'version_id': self.versions[0]}
        with self.assertRaises(ValueError):
            open(self.url, 'wb', transport_params=params)

    def test_no_version(self):
        """Passing in no version at all gives the newest version of the file?"""
        with open(self.url, 'rb') as fin:
            actual = fin.read()
        self.assertEqual(actual, self.test_ver2)

    def test_newest_version(self):
        """Passing in the newest version explicitly gives the most recent content?"""
        params = {'version_id': self.versions[1]}
        with open(self.url, mode='rb', transport_params=params) as fin:
            actual = fin.read()
        self.assertEqual(actual, self.test_ver2)

    def test_oldset_version(self):
        """Passing in the oldest version gives the oldest content?"""
        params = {'version_id': self.versions[0]}
        with open(self.url, mode='rb', transport_params=params) as fin:
            actual = fin.read()
        self.assertEqual(actual, self.test_ver1)


if __name__ == '__main__':
    unittest.main()
