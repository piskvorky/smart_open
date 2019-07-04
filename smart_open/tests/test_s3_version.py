# -*- coding: utf-8 -*-
import logging
import os
import time
import unittest
import uuid

import boto3
import botocore.client
import moto

from smart_open import open


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
    boto3.resource('s3').BucketVersioning(BUCKET_NAME).enable()


def put_to_bucket(contents, num_attempts=12, sleep_time=5):
    # fake (or not) connection, bucket and key
    logger.debug('%r', locals())

    #
    # In real life, it can take a few seconds for the bucket to become ready.
    # If we try to write to the key while the bucket while it isn't ready, we
    # will get a ClientError: NoSuchBucket.
    #
    for attempt in range(num_attempts):
        try:
            boto3.resource('s3').Object(BUCKET_NAME, KEY_NAME).put(Body=contents)
            return
        except botocore.exceptions.ClientError as err:
            logger.error('caught %r, retrying', err)
            time.sleep(sleep_time)

    assert False, 'failed to create bucket %s after %d attempts' % (BUCKET_NAME, num_attempts)


@maybe_mock_s3
class TestVersionId(unittest.TestCase):

    def setUp(self):
        self.WRITE_KEY_NAME = 'test-write-key-{}'.format(uuid.uuid4().hex)
        self.test_ver1 = u"String version 1.0".encode('utf8')
        self.test_ver2 = u"String version 2.0".encode('utf8')
        # write into key
        with open("s3://"+BUCKET_NAME+"/"+self.WRITE_KEY_NAME, 'wb') as fout:
            fout.write(self.test_ver1)
        with open("s3://" + BUCKET_NAME + "/" + self.WRITE_KEY_NAME, 'wb') as fout:
            fout.write(self.test_ver2)

    def test_good_id(self):
        """Does passing the version_id parameter into the s3 submodule work correctly when reading?"""
        versions = boto3.resource('s3').Bucket(BUCKET_NAME).object_versions.filter(Prefix=self.WRITE_KEY_NAME)
        check_version = list(versions)[0].get()['VersionId']
        transport_params = {'version_id': check_version}
        with open("s3://%s/%s" % (BUCKET_NAME, self.WRITE_KEY_NAME), mode='rb', transport_params=transport_params) as fin:
            expected = fin.read()
        self.assertEqual(expected, self.test_ver1)

    def test_bad_id(self):
        """Does passing an invalid version_id exception into the s3 submodule get handled correctly?"""
        transport_params = {'version_id': 'bad-version-does-not-exist'}
        with self.assertRaises(IOError):
            open("s3://%s/%s" % (BUCKET_NAME, self.WRITE_KEY_NAME), 'rb', transport_params=transport_params)

    def test_bad_mode(self):
        """Do we correctly handle non-None version when writing?"""
        versions = boto3.resource('s3').Bucket(BUCKET_NAME).object_versions.filter(Prefix=self.WRITE_KEY_NAME)
        check_version = list(versions)[0].get()['VersionId']
        transport_params = {'version_id': check_version}
        with self.assertRaises(ValueError):
            open("s3://%s/%s" % (BUCKET_NAME, self.WRITE_KEY_NAME), 'wb', transport_params=transport_params)

    def test_no_version(self):
        """Passing in no version at all gives the newest version of the file?"""
        with open("s3://%s/%s" % (BUCKET_NAME, self.WRITE_KEY_NAME), 'rb') as fin:
            expected = fin.read()
        self.assertEqual(expected, self.test_ver2)

    def test_newest_version(self):
        """Passing in the newest version explicitly gives the same as above?"""
        versions = boto3.resource('s3').Bucket(BUCKET_NAME).object_versions.filter(Prefix=self.WRITE_KEY_NAME)
        newest_version = list(versions)[-1].get()['VersionId']
        transport_params = {'version_id': newest_version}
        with open("s3://%s/%s" % (BUCKET_NAME, self.WRITE_KEY_NAME), 'rb', transport_params=transport_params) as fin:
            expected = fin.read()
        self.assertEqual(expected, self.test_ver2)

    def test_oldset_version(self):
        """Passing in the oldest version gives you the oldest version?"""
        versions = boto3.resource('s3').Bucket(BUCKET_NAME).object_versions.filter(Prefix=self.WRITE_KEY_NAME)
        oldest_version = list(versions)[0].get()['VersionId']
        transport_params = {'version_id': oldest_version}
        with open("s3://%s/%s" % (BUCKET_NAME, self.WRITE_KEY_NAME), 'rb', transport_params=transport_params) as fin:
            expected = fin.read()
        self.assertEqual(expected, self.test_ver1)


if __name__ == '__main__':
    unittest.main()
