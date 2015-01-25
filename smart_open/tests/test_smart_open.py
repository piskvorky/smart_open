#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).


import unittest
import logging

import boto
import mock
from moto import mock_s3

import smart_open
from smart_open import smart_open_lib

class ParseUriTest(unittest.TestCase):
    """
    Test ParseUri class.

    """
    def test_scheme(self):
        """Do URIs schemes parse correctly?"""
        # supported schemes
        for scheme in ("s3", "s3n", "hdfs", "file"):
            parsed_uri = smart_open.ParseUri(scheme + "://mybucket/mykey")
            self.assertEqual(parsed_uri.scheme, scheme)

        # unsupported scheme => NotImplementedError
        self.assertRaises(NotImplementedError, smart_open.ParseUri, "http://mybucket/mykey")

        # unknown scheme => default_scheme
        parsed_uri = smart_open.ParseUri("blah blah")
        self.assertEqual(parsed_uri.scheme, "file")


    def test_s3_uri(self):
        """Do S3 URIs parse correctly?"""
        # correct uri without credentials
        parsed_uri = smart_open.ParseUri("s3://mybucket/mykey")
        self.assertEqual(parsed_uri.scheme, "s3")
        self.assertEqual(parsed_uri.bucket_id, "mybucket")
        self.assertEqual(parsed_uri.key_id, "mykey")
        self.assertEqual(parsed_uri.access_id, None)
        self.assertEqual(parsed_uri.access_secret, None)

        # correct uri, key contains slash
        parsed_uri = smart_open.ParseUri("s3://mybucket/mydir/mykey")
        self.assertEqual(parsed_uri.scheme, "s3")
        self.assertEqual(parsed_uri.bucket_id, "mybucket")
        self.assertEqual(parsed_uri.key_id, "mydir/mykey")
        self.assertEqual(parsed_uri.access_id, None)
        self.assertEqual(parsed_uri.access_secret, None)

        # correct uri with credentials
        parsed_uri = smart_open.ParseUri("s3://ACCESSID456:acces/sse_cr-et@mybucket/mykey")
        self.assertEqual(parsed_uri.scheme, "s3")
        self.assertEqual(parsed_uri.bucket_id, "mybucket")
        self.assertEqual(parsed_uri.key_id, "mykey")
        self.assertEqual(parsed_uri.access_id, "ACCESSID456")
        self.assertEqual(parsed_uri.access_secret, "acces/sse_cr-et")

        # correct uri, contains credentials
        parsed_uri = smart_open.ParseUri("s3://accessid:access/secret@mybucket/mykey")
        self.assertEqual(parsed_uri.scheme, "s3")
        self.assertEqual(parsed_uri.bucket_id, "mybucket")
        self.assertEqual(parsed_uri.key_id, "mykey")
        self.assertEqual(parsed_uri.access_id, "accessid")
        self.assertEqual(parsed_uri.access_secret, "access/secret")

        # incorrect uri - only one '@' in uri is allowed
        self.assertRaises(RuntimeError, smart_open.ParseUri, "s3://access_id@access_secret@mybucket/mykey")


class SmartOpenReadTest(unittest.TestCase):
    """
    Test reading from files under various schemes.

    """
    # TODO: add more complex test for file://
    @mock.patch('smart_open.smart_open_lib.file_smart_open')
    def test_file(self, mock_smart_open):
        """Is file:// line iterator called correctly?"""
        smart_open_object = smart_open.smart_open("file:///tmp/test.txt", "rb")
        smart_open_object.__iter__()
        # called with the correct path?
        mock_smart_open.assert_called_with("/tmp/test.txt", "rb")


    # couldn't find any project for mocking up HDFS data
    # TODO: we want to test also a content of the files, not just fnc call params
    @mock.patch('smart_open.smart_open_lib.subprocess')
    def test_hdfs(self, mock_subprocess):
        """Is HDFS line iterator called correctly?"""
        mock_subprocess.PIPE.return_value = "test"
        smart_open_object = smart_open.HdfsOpenRead(smart_open.ParseUri("hdfs:///tmp/test.txt"))
        smart_open_object.__iter__()
        # called with the correct params?
        mock_subprocess.Popen.assert_called_with(["hadoop", "fs", "-cat", "/tmp/test.txt"], stdout=mock_subprocess.PIPE)


    @mock.patch('smart_open.smart_open_lib.boto')
    @mock.patch('smart_open.smart_open_lib.s3_iter_lines')
    def test_s3_boto(self, mock_s3_iter_lines, mock_boto):
        """Is S3 line iterator called correctly?"""
        # no credentials
        smart_open_object = smart_open.S3OpenRead(smart_open.ParseUri("s3://mybucket/mykey"))
        smart_open_object.__iter__()
        mock_boto.connect_s3.assert_called_with(aws_access_key_id=None, aws_secret_access_key=None)

        # with credential
        smart_open_object = smart_open.S3OpenRead(smart_open.ParseUri("s3://access_id:access_secret@mybucket/mykey"))
        smart_open_object.__iter__()
        mock_boto.connect_s3.assert_called_with(aws_access_key_id="access_id", aws_secret_access_key="access_secret")

        # lookup bucket, key; call s3_iter_lines
        smart_open_object = smart_open.S3OpenRead(smart_open.ParseUri("s3://access_id:access_secret@mybucket/mykey"))
        smart_open_object.__iter__()
        mock_boto.connect_s3().lookup.assert_called_with("mybucket")
        mock_boto.connect_s3().lookup().lookup.assert_called_with("mykey")
        self.assertTrue(mock_s3_iter_lines.called)


    @mock_s3
    def test_s3_iter_moto(self):
        """Are S3 files iterated over correctly?"""
        # a list of strings to test with
        expected = ["*" * 5 * 1024**2] + ['0123456789'] * 1024 + ["test"]

        # create fake bucket and fake key
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        # lower the multipart upload size, to speed up the tests
        smart_open_lib.S3_MIN_PART_SIZE = 5 * 1024**2
        with smart_open.smart_open("s3://mybucket/mykey", "wb") as fout:
            # write a single huge line (=full multipart upload)
            fout.write("%s\n" % expected[0])

            # write lots of small lines
            for lineno, line in enumerate(expected[1:-1]):
                fout.write('%s\n' % line)

            # a plain line at the end, no newline at the end
            fout.write("%s" % expected[-1])

        # connect to fake s3 and read from the fake key we filled above
        smart_open_object = smart_open.S3OpenRead(smart_open.ParseUri("s3://mybucket/mykey"))
        output = [line.rstrip('\n') for line in smart_open_object]
        self.assertEqual(output, expected)

        # same thing but using a context manager
        with smart_open.S3OpenRead(smart_open.ParseUri("s3://mybucket/mykey")) as smart_open_object:
            output = [line.rstrip('\n') for line in smart_open_object]
            self.assertEqual(output, expected)


class S3IterLinesTest(unittest.TestCase):
    """
    Test s3_iter_lines.

    """
    @mock_s3
    def test_s3_iter_lines_with_key(self):
        """Does s3_iter_lines give correct content?"""
        # create fake bucket and fake key
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        test_string = "hello žluťoučký world!\nhow are you?"
        with smart_open.smart_open("s3://mybucket/mykey", "wb") as fin:
            fin.write(test_string)

        # obtain boto key object
        mykey = conn.get_bucket("mybucket").get_key("mykey")

        # call s3_iter_lines and check output
        output = list(smart_open.s3_iter_lines(mykey))

        self.assertEqual(''.join(output), test_string)


    @mock_s3
    def test_s3_iter_lines_without_key(self):
        """Does s3_iter_lines fail on invalid input?"""
        # cannot use context manager for assertRaise in py2.6
        try:
            for i in smart_open.s3_iter_lines(None):
                pass
        except TypeError:
            pass
        else:
            self.fail("s3_iter_lines extected to fail on non-Key inputs")

        try:
            for i in smart_open.s3_iter_lines("test"):
                pass
        except TypeError:
            pass
        else:
            self.fail("s3_iter_lines extected to fail on non-Key inputs")


class SmartOpenTest(unittest.TestCase):
    """
    Test reading and writing from/into files.

    """
    @mock.patch('smart_open.smart_open_lib.boto')
    @mock.patch('smart_open.smart_open_lib.file_smart_open')
    def test_file_mode_mock(self, mock_file, mock_boto):
        """Are file:// open modes passed correctly?"""
        # incorrect file mode
        self.assertRaises(NotImplementedError, smart_open.smart_open, "s3://bucket/key", "x")

        # correct read modes
        smart_open.smart_open("blah", "r")
        mock_file.assert_called_with("blah", "r")

        smart_open.smart_open("blah", "rb")
        mock_file.assert_called_with("blah", "rb")

        # correct write modes, incorrect scheme
        self.assertRaises(NotImplementedError, smart_open.smart_open, "hdfs:///blah.txt", "wb")
        self.assertRaises(NotImplementedError, smart_open.smart_open, "http:///blah.txt", "w")

        # correct write mode, correct file:// URI
        smart_open.smart_open("blah", "w")
        mock_file.assert_called_with("blah", "w")

        smart_open.smart_open("file:///some/file.txt", "wb")
        mock_file.assert_called_with("/some/file.txt", "wb")

    @mock.patch('smart_open.smart_open_lib.boto')
    @mock.patch('smart_open.smart_open_lib.S3OpenWrite')
    def test_s3_mode_mock(self, mock_write, mock_boto):
        """Are s3:// open modes passed correctly?"""
        # correct write mode, correct s3 URI
        smart_open.smart_open("s3://mybucket/mykey", "w")
        mock_boto.connect_s3.assert_called_with(aws_access_key_id=None, aws_secret_access_key=None)
        mock_boto.connect_s3().lookup.return_value = True
        mock_boto.connect_s3().get_bucket.assert_called_with("mybucket")
        self.assertTrue(mock_write.called)

    @mock_s3
    def test_s3_modes_moto(self):
        """Do s3:// open modes work correctly?"""
        # fake bucket and key
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        test_string = "second test"

        # correct write mode, correct s3 URI
        with smart_open.smart_open("s3://mybucket/newkey", "wb") as fin:
            fin.write(test_string)

        output = list(smart_open.smart_open("s3://mybucket/newkey", "rb"))

        self.assertEqual(output, [test_string])


class S3OpenWriteTest(unittest.TestCase):
    """
    Test writing into s3 files.

    """
    @mock_s3
    def test_write_01(self):
        """Does writing into s3 work correctly?"""
        # fake connection, bucket and key
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        mybucket = conn.get_bucket("mybucket")
        mykey = boto.s3.key.Key()
        mykey.name = "testkey"
        test_string = "žluťoučký koníček"

        # write into key
        with smart_open.S3OpenWrite(mybucket, mykey) as fin:
            fin.write(test_string)

        # read key and test content
        output = list(smart_open.smart_open("s3://mybucket/testkey", "rb"))

        self.assertEqual(output, [test_string])

    @mock_s3
    def test_write_01a(self):
        """Does s3 write fail on incorrect input?"""
        # fake connection, bucket and key
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        mybucket = conn.get_bucket("mybucket")
        mykey = boto.s3.key.Key()
        mykey.name = "testkey"

        try:
            with smart_open.S3OpenWrite(mybucket, mykey) as fin:
                fin.write(None)
        except TypeError:
            pass
        else:
            self.fail()


    @mock_s3
    def test_write_02(self):
        """Does s3 write unicode conversion work?"""
        # fake connection, bucket and key
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        mybucket = conn.get_bucket("mybucket")
        mykey = boto.s3.key.Key()
        mykey.name = "testkey"

        smart_open_write = smart_open.S3OpenWrite(mybucket, mykey)
        with smart_open_write as fin:
            fin.write(u"testžížáč")
            self.assertEqual(fin.total_size, 14)


    @mock_s3
    def test_write_03(self):
        """Does s3 multipart chunking work correctly?"""
        # fake connection, bucket and key
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        mybucket = conn.get_bucket("mybucket")
        mykey = boto.s3.key.Key()
        mykey.name = "testkey"

        # write
        smart_open_write = smart_open.S3OpenWrite(mybucket, mykey, min_part_size=10)
        with smart_open_write as fin:
            fin.write(u"test")
            self.assertEqual(fin.chunk_bytes, 4)

            fin.write(u"test\n")
            self.assertEqual(fin.chunk_bytes, 9)
            self.assertEqual(fin.parts, 0)

            fin.write(u"test")
            self.assertEqual(fin.chunk_bytes, 0)
            self.assertEqual(fin.parts, 1)

        # read back the same key and check its content
        output = list(smart_open.smart_open("s3://mybucket/testkey"))

        self.assertEqual(output, ["testtest\n", "test"])


class S3IterBucketTest(unittest.TestCase):
    """
    Test parallel iteration of given bucket.

    """
    def test_s3_iter_bucket_process_key_mock(self):
        """Is s3_iter_bucket_process_key called correctly?"""
        attrs = {"name" : "fileA", "get_contents_as_string.return_value" : "contentA"}
        mykey = mock.Mock(spec=["name", "get_contents_as_string"])
        mykey.configure_mock(**attrs)

        key, content = smart_open.s3_iter_bucket_process_key(mykey)
        self.assertEqual(key, mykey)
        self.assertEqual(content, "contentA")


    @mock_s3
    def test_s3_iter_bucket_process_key_moto(self):
        """Does s3_iter_bucket_process_key work correctly?"""
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        mybucket = conn.get_bucket("mybucket")

        mykey = boto.s3.key.Key(mybucket)
        mykey.key = "mykey"
        mykey.set_contents_from_string("contentA")

        key, content = smart_open.s3_iter_bucket_process_key(mykey)
        self.assertEqual(key, mykey)
        self.assertEqual(content, "contentA")


    @mock.patch('smart_open.multiprocessing.pool')
    def test_s3_iter_bucket_mock(self, mock_pool):
        """Is s3_iter_bucket called correctly?"""
        attrs = {"name" : "fileA", "get_contents_as_string.return_value" : "contentA"}
        mykey = mock.Mock(spec=["name", "get_contents_as_string"])
        mykey.configure_mock(**attrs)

        attrs = {"list.return_value" : [mykey]}
        mybucket = mock.Mock(spec=["list"])
        mybucket.configure_mock(**attrs)

        for key, content in smart_open.s3_iter_bucket(mybucket):
            mock_pool.Pool.assert_called_with(processes=16)
            mock_pool.Pool().imap_unordered.assert_called_with()

        mock_pool.Pool.assert_called_with(processes=16)
        self.assertTrue(mock_pool.Pool().imap_unordered.called)


    @mock_s3
    def test_s3_iter_bucket_moto(self):
        """Does s3_iter_bucket work correctly?"""
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        mybucket = conn.get_bucket("mybucket")

        # first, create some keys in the bucket
        expected = {}
        for key_no in range(200):
            key_name = "mykey%s" % key_no
            with smart_open.smart_open("s3://mybucket/%s" % key_name, 'wb') as fout:
                content = '\n'.join("line%i%i" % (key_no, line_no) for line_no in range(10))
                fout.write(content)
                expected[key_name] = content

        # read all keys + their content back, in parallel, using s3_iter_bucket
        result = dict(smart_open.s3_iter_bucket(mybucket))
        self.assertEqual(expected, result)

        # read some of the keys back, in parallel, using s3_iter_bucket
        result = dict(smart_open.s3_iter_bucket(mybucket, accept_key=lambda fname: fname.endswith('4')))
        self.assertEqual(result, dict((k, c) for k, c in expected.items() if k.endswith('4')))

        # read some of the keys back, in parallel, using s3_iter_bucket
        result = dict(smart_open.s3_iter_bucket(mybucket, key_limit=10))
        self.assertEqual(len(result), min(len(expected), 10))

        for workers in [1, 4, 8, 16, 64]:
            self.assertEqual(dict(smart_open.s3_iter_bucket(mybucket, workers=workers)), expected)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    unittest.main()
