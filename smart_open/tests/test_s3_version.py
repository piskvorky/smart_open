# -*- coding: utf-8 -*-
import gzip
import io
import logging
import os
import time
import unittest
import uuid
import warnings

import boto.s3.bucket
import boto3
import botocore.client
import mock
import moto
import six

import smart_open

BUCKET_NAME = 'test-smartopen-{}'.format(uuid.uuid4().hex)
KEY_NAME = 'test-key'
WRITE_KEY_NAME = 'test-write-key'
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
        self.test_ver0 = u"String version 1.0".encode('utf8')
        self.test_ver1 = u"String version 2.0".encode('utf8')
        # write into key
        with smart_open.s3.BufferedOutputBase(BUCKET_NAME, WRITE_KEY_NAME) as fout:
            fout.write(self.test_ver0)
        with smart_open.s3.BufferedOutputBase(BUCKET_NAME, WRITE_KEY_NAME) as fout:
            fout.write(self.test_ver1)

    def test_good_id(self):
        """Does version_id into s3 work correctly?"""
        versions = boto3.resource('s3').Bucket(BUCKET_NAME).object_versions.filter(Prefix=WRITE_KEY_NAME)
        check_version = list(versions)[0].get()['VersionId']
        with smart_open.s3.SeekableBufferedInputBase(BUCKET_NAME, WRITE_KEY_NAME,check_version) as fin:
            expected = fin.read()
        self.assertEqual(expected, self.test_ver0)

    def test_bad_id(self):
        """Does version_id exception into s3 work correctly?"""
        versions = boto3.resource('s3').Bucket(BUCKET_NAME).object_versions.filter(Prefix=WRITE_KEY_NAME)
        check_version = list(versions)[0].get()['VersionId']

        with self.assertRaises(IOError):
            smart_open.s3.open(BUCKET_NAME, WRITE_KEY_NAME, 'rb', version_id=check_version+check_version)

if __name__ == '__main__':
    unittest.main()