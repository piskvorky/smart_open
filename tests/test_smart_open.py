#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).

import boto
import mock
import os
import random
import unittest
import string

from moto import mock_s3

import smart_open


class ParseURLTest(unittest.TestCase):
    """
    Test ParseURL class

    """
    def test_scheme(self):
        """
        Test correct parsing of URI scheme.

        """
        # supported schemes
        for scheme in ("s3", "s3n", "hdfs", "file"):
            parsed_url = smart_open.ParseURL(scheme + "://mybucket/mykey")
            self.assertEqual(parsed_url.scheme, scheme)

        # unsupported scheme => NotImplementedError
        self.assertRaises(NotImplementedError, smart_open.ParseURL, "http://mybucket/mykey")

        # unknown scheme => default_scheme
        parsed_url = smart_open.ParseURL("blah blah")
        self.assertEqual(parsed_url.scheme, "file")


    def test_s3_uri(self):
        """
        Test parsing of S3 URI.

        """
        # correct uri without credentials
        parsed_url = smart_open.ParseURL("s3://mybucket/mykey")
        self.assertEqual(parsed_url.scheme, "s3")
        self.assertEqual(parsed_url.bucket_id, "mybucket")
        self.assertEqual(parsed_url.key_id, "mykey")
        self.assertEqual(parsed_url.access_id, None)
        self.assertEqual(parsed_url.access_secret, None)

        # correct uri, key contains slash
        parsed_url = smart_open.ParseURL("s3://mybucket/mydir/mykey")
        self.assertEqual(parsed_url.scheme, "s3")
        self.assertEqual(parsed_url.bucket_id, "mybucket")
        self.assertEqual(parsed_url.key_id, "mydir/mykey")
        self.assertEqual(parsed_url.access_id, None)
        self.assertEqual(parsed_url.access_secret, None)

        # correct uri with credentials
        # pouzit skutocne hodnoty pre credentials
        parsed_url = smart_open.ParseURL("s3://ACCESSID456:acces/sse_cr-et@mybucket/mykey")
        self.assertEqual(parsed_url.scheme, "s3")
        self.assertEqual(parsed_url.bucket_id, "mybucket")
        self.assertEqual(parsed_url.key_id, "mykey")
        self.assertEqual(parsed_url.access_id, "ACCESSID456")
        self.assertEqual(parsed_url.access_secret, "acces/sse_cr-et")

        # correct uri, credentials contains 
        parsed_url = smart_open.ParseURL("s3://accessid:access/secret@mybucket/mykey")
        self.assertEqual(parsed_url.scheme, "s3")
        self.assertEqual(parsed_url.bucket_id, "mybucket")
        self.assertEqual(parsed_url.key_id, "mykey")
        self.assertEqual(parsed_url.access_id, "accessid")
        self.assertEqual(parsed_url.access_secret, "access/secret")

        # incorrect uri - only one '@' in uri is allowed
        self.assertRaises(RuntimeError, smart_open.ParseURL, "s3://access_id@access_secret@mybucket/mykey")


class SmartOpenReadTest(unittest.TestCase):
    """
    Test reading from files

    """
    # TODO: add more complex test
    @mock.patch('smart_open.file_smart_open')
    def test_file(self, mock_smart_open):
        """
        Test FILE files.
        Check if file_smart_open obtain correct filepath.
    
        """
        smart_open_object = smart_open.SmartOpenRead("file:///tmp/test.txt")
        smart_open_object.__iter__()
        mock_smart_open.assert_called_with("/tmp/test.txt")


    # TODO: couldn't find any project for testing HDFS
    # TODO: we want to test also a content of the files, not just calling
    @mock.patch('smart_open.subprocess')
    def test_hdfs(self, mock_subprocess):
        """
        Test iterator for HDFS files.
        Check if subprocess.Popen obtain correct filepath.
    
        """
        mock_subprocess.PIPE.return_value = "test"
        smart_open_object = smart_open.SmartOpenRead("hdfs:///tmp/test.txt")
        smart_open_object.__iter__()
        mock_subprocess.Popen.assert_called_with(["hadoop", "fs", "-cat", "/tmp/test.txt"], stdout=mock_subprocess.PIPE)


    @mock.patch('smart_open.boto')
    @mock.patch('smart_open.s3_iter_lines')
    def test_s3_boto(self, mock_s3_iter_lines, mock_boto):
        """
        Test iterator for S3 files.
        Check if boto.connect_s3 obtain correct credentials.
    
        """
        # no credentials
        smart_open_object = smart_open.SmartOpenRead("s3://mybucket/mykey")
        smart_open_object.__iter__()
        mock_boto.connect_s3.assert_called_with(aws_access_key_id=None, aws_secret_access_key=None)
    
        # with credential
        smart_open_object = smart_open.SmartOpenRead("s3://access_id:access_secret@mybucket/mykey")
        smart_open_object.__iter__()
        mock_boto.connect_s3.assert_called_with(aws_access_key_id="access_id", aws_secret_access_key="access_secret")
    
        # lookup bucket, key; call s3_iter_lines
        smart_open_object = smart_open.SmartOpenRead("s3://access_id:access_secret@mybucket/mykey")
        smart_open_object.__iter__()
        mock_boto.connect_s3().lookup.assert_called_with("mybucket")
        mock_boto.connect_s3().lookup().lookup.assert_called_with("mykey")
        self.assertTrue(mock_s3_iter_lines.called)


    @mock_s3
    def test_s3_moto(self):
        """
        Test iterator for S3 files using moto.
        
        """
        # create fake bucket and fake key
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        with smart_open.smart_open("s3://mybucket/mykey", "wb") as fin:
            fin.write("test")

        # connect to fake s3 and read from fake key
        smart_open_object = smart_open.SmartOpenRead("s3://mybucket/mykey")
        output = []
        for line in smart_open_object:
            output.append(line)
            break

        self.assertEqual("".join(output), "test")


class S3IterLinesTest(unittest.TestCase):
    """
    Test method s3_iter_lines.

    """
    @mock_s3
    def test_s3_iter_lines_with_key(self):
        """
        Test s3_iter_lines using moto.

        """
        # create fake bucket and fake key
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        with smart_open.smart_open("s3://mybucket/mykey", "wb") as fin:
            fin.write("test")

        # obtain boto key object
        mykey = conn.get_bucket("mybucket").get_key("mykey")

        # call s3_iter_lines and check output
        output = []
        for line in smart_open.s3_iter_lines(mykey):
            output.append(line)
            break

        self.assertEqual("".join(output), "test")


    @mock_s3
    def test_s3_iter_lines_without_key(self):
        """
        Call s3_iter_lines with invalid boto key object.
    
        """
        try:
            for i in smart_open.s3_iter_lines(None):
                pass
        except TypeError:
            pass
        else:
            self.fail()

        try:
            for i in smart_open.s3_iter_lines("test"):
                pass
        except TypeError:
            pass
        else:
            self.fail()


class IterLinesTest(unittest.TestCase):
    """
    Test reading from files.
    Method iter_lines just calls SmartOpenRead class.

    """
    @mock.patch('smart_open.SmartOpenRead')
    def test_iter_lines_mock(self, mock):
        """
        Test iter_lines using mock.
        Check if SmartOpenRead gets correct argument.

        """
        smart_open.iter_lines("blah")
        mock.assert_called_with("blah")


class SmartOpenTest(unittest.TestCase):
    """
    Test reading and writing from/into files.

    """
    @mock.patch('smart_open.boto')
    @mock.patch('smart_open.SmartOpenRead')
    @mock.patch('smart_open.SmartOpenWrite')
    def test_file_mode_mock(self, mock_write, mock_read, mock_boto):
        """
        Test supported file modes using mock.

        """
        # incorrect file mode
        self.assertRaises(NotImplementedError, smart_open.smart_open, "blah", "x")

        # correct read modes
        smart_open.smart_open("blah", "r")
        mock_read.assert_called_with("blah")

        smart_open.smart_open("blah", "rb")
        mock_read.assert_called_with("blah")

        # correct write modes, incorrect scheme
        self.assertRaises(NotImplementedError, smart_open.smart_open, "blah", "w")
        self.assertRaises(NotImplementedError, smart_open.smart_open, "file:///blah.txt", "w")
        self.assertRaises(NotImplementedError, smart_open.smart_open, "hdfs:///blah.txt", "wb")
        self.assertRaises(NotImplementedError, smart_open.smart_open, "http:///blah.txt", "w")

        # correct write mode, correct s3 url
        smart_open.smart_open("s3://mybucket/mykey", "w")
        mock_boto.connect_s3.assert_called_with(aws_access_key_id=None, aws_secret_access_key=None)
        mock_boto.connect_s3().lookup.return_value = True
        mock_boto.connect_s3().lookup.assert_called_with("mybucket")
        self.assertTrue(mock_write.called)


    @mock_s3
    def test_file_mode_mock(self):
        """
        Test supported file modes using moto.
        
        """
        # fake bucket and key
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")

        # correct write mode, correct s3 url
        with smart_open.smart_open("s3://mybucket/newkey", "wb") as fin:
            fin.write("second test")

        output = []
        for line in smart_open.smart_open("s3://mybucket/newkey", "rb"):
            output.append(line)
            break

        self.assertEqual("".join(output), "second test")


class SmartOpenWriteTest(unittest.TestCase):
    """
    Test writing into s3 files.

    """
    @mock_s3
    def test_write_01(self):
        """
        Test writing into s3 files using moto.

        """
        # fake connection, bucket and key
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        mybucket = conn.get_bucket("mybucket")
        mykey = boto.s3.key.Key()
        mykey.name = "testkey"

        # write into key
        with smart_open.SmartOpenWrite(mybucket, mykey) as fin:
            fin.write("test")

        # read key and test content
        output = []
        for line in smart_open.smart_open("s3://mybucket/testkey", "rb"):
            output.append(line)
            break

        self.assertEqual("".join(output), "test")


    @mock_s3
    def test_write_01a(self):
        """
        Test write with incorrect input.

        """
        # fake connection, bucket and key
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        mybucket = conn.get_bucket("mybucket")
        mykey = boto.s3.key.Key()
        mykey.name = "testkey"

        try:
            with smart_open.SmartOpenWrite(mybucket, mykey) as fin:
                fin.write(None)
        except TypeError:
            pass
        else:
            self.fail()


    @mock_s3
    def test_write_02(self):
        """
        Test writing of unicode input into s3 files.

        """
        # fake connection, bucket and key
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        mybucket = conn.get_bucket("mybucket")
        mykey = boto.s3.key.Key()
        mykey.name = "testkey"

        smart_open_write = smart_open.SmartOpenWrite(mybucket, mykey)
        with smart_open_write as fin:
            fin.write(u"testžížáč")
            self.assertEqual(fin.total_size, 14)


    @mock_s3
    def test_write_03(self):
        """
        Test writing into s3 files.

        """
        # fake connection, bucket and key
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        mybucket = conn.get_bucket("mybucket")
        mykey = boto.s3.key.Key()
        mykey.name = "testkey"

        # write
        smart_open_write = smart_open.SmartOpenWrite(mybucket, mykey, min_chunk_size=10)
        with smart_open_write as fin:
            fin.write(u"test")
            self.assertEqual(fin.chunk_bytes, 4)

            fin.write(u"test")
            self.assertEqual(fin.chunk_bytes, 8)
            self.assertEqual(fin.parts, 0)

            fin.write(u"test")
            self.assertEqual(fin.chunk_bytes, 0)
            self.assertEqual(fin.parts, 1)

        # read key and test content
        output = []
        for line in smart_open.smart_open("s3://mybucket/testkey", "rb"):
            output.append(line)
            break

        self.assertEqual("".join(output), "testtesttest")


class S3StoreLinesTest(unittest.TestCase):
    """
    Test writing into s3 files.

    """
    def test_s3_store_lines_01(self):
        """
        Test input arguments.

        """
        # no url and no bucket raises RuntimeError
        self.assertRaises(RuntimeError, smart_open.s3_store_lines, None, url=None)

        # given url must have s3 scheme
        self.assertRaises(NotImplementedError, smart_open.s3_store_lines, None, url="a")


    @mock.patch('smart_open.boto')
    @mock.patch('smart_open.SmartOpenWrite')
    def test_s3_store_lines_02(self, mock_write, mock_boto):
        """
        Test s3_store_lines with given URL.

        """
        smart_open.s3_store_lines(["sentence1", "sentence2"], url="s3://mybucket/mykey")
        mock_boto.connect_s3.assert_called_with(aws_access_key_id=None, aws_secret_access_key=None)
        mock_boto.connect_s3().lookup.assert_called_with("mybucket")
        self.assertTrue(mock_write.called)


    @mock_s3
    def test_s3_store_lines_02_moto(self):
        """
        Test s3_store_lines with given URL using moto.

        """
        # fake connection and bucket
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")

        # store lines
        smart_open.s3_store_lines(list(["sentence1", "sentence2"]), url="s3://mybucket/mykey")

        # read and check data
        output = []
        for line in smart_open.smart_open("s3://mybucket/mykey", "rb"):
            output.append(line)

        self.assertEqual("".join(output), "sentence1\nsentence2\n")


    @mock.patch('smart_open.boto')
    @mock.patch('smart_open.SmartOpenWrite')
    def test_s3_store_lines_03(self, mock_write, mock_boto):
        """
        Test s3_store_lines with given bucket.

        """
        mybucket = mock.Mock()
        smart_open.s3_store_lines(["sentence1", "sentence2"], outbucket=mybucket)
        self.assertFalse(mock_boto.connect_s3.called)


    @mock_s3
    def test_s3_store_lines_03_moto(self):
        """
        Test s3_store_lines with given bucket.

        """
        # fake connection and bucket
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        mybucket = conn.get_bucket("mybucket")

        # correct call
        smart_open.s3_store_lines(["sentence1", "sentence2"], outbucket=mybucket, outkey="mykey")

        # read and check data
        output = []
        for line in smart_open.smart_open("s3://mybucket/mykey", "rb"):
            output.append(line)

        self.assertEqual("".join(output), "sentence1\nsentence2\n")



class S3IterBucketTest(unittest.TestCase):
    """
    Test parallel iteration of given bucket.

    """
    def test_s3_iter_bucket_process_key_mock(self):
        """
        Test s3_iter_bucket_process_key using mock.

        """
        attrs = {"name" : "fileA", "get_contents_as_string.return_value" : "contentA"}
        mykey = mock.Mock(spec=["name", "get_contents_as_string"])
        mykey.configure_mock(**attrs)

        (key, content) = smart_open.s3_iter_bucket_process_key(mykey)
        self.assertEqual(key, mykey)
        self.assertEqual(content, "contentA")


    @mock_s3
    def test_s3_iter_bucket_process_key_moto(self):
        """
        Test s3_iter_bucket_process_key using moto.

        """
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        mybucket = conn.get_bucket("mybucket")

        mykey = boto.s3.key.Key(mybucket)
        mykey.key = "mykey"
        mykey.set_contents_from_string("contentA")

        (key, content) = smart_open.s3_iter_bucket_process_key(mykey)
        self.assertEqual(key, mykey)
        self.assertEqual(content, "contentA")


    @mock.patch('smart_open.multiprocessing.pool')
    def test_s3_iter_bucket_mock(self, mock_pool):
        """
        Test parallel bucket iteration using mock.

        """
        attrs = {"name" : "fileA", "get_contents_as_string.return_value" : "contentA"}
        mykey = mock.Mock(spec=["name", "get_contents_as_string"])
        mykey.configure_mock(**attrs)

        attrs = {"list.return_value" : [mykey]}
        mybucket = mock.Mock(spec=["list"])
        mybucket.configure_mock(**attrs)

        for (key, content) in smart_open.s3_iter_bucket(mybucket):
            mock_pool.Pool.assert_called_with(processes=16)
            mock_pool.Pool().imap_unordered.assert_called_with()

        mock_pool.Pool.assert_called_with(processes=16)
        self.assertTrue(mock_pool.Pool().imap_unordered.called)


    # TODO: add more keys (min. 16)
    @mock_s3
    def test_s3_iter_bucket_moto(self):
        """
        Test parallel bucket iteration using mock.

        """
        conn = boto.connect_s3()
        conn.create_bucket("mybucket")
        mybucket = conn.get_bucket("mybucket")

        smart_open.s3_store_lines(["sentence11", "sentence12"], url="s3://mybucket/mykey01")
        smart_open.s3_store_lines(["sentence21", "sentence22"], url="s3://mybucket/mykey02")
        smart_open.s3_store_lines(["sentence31", "sentence32"], url="s3://mybucket/mykey03")
        smart_open.s3_store_lines(["sentence41", "sentence42"], url="s3://mybucket/mykey04")
        smart_open.s3_store_lines(["sentence51", "sentence52"], url="s3://mybucket/mykey05")

        for (key, content) in smart_open.s3_iter_bucket(mybucket):
            if key == "mykey01":
                self.assertEqual(content, "sentence11\nsentence12\n")

            if key == "mykey02":
                self.assertEqual(content, "sentence21\nsentence22\n")

            if key == "mykey03":
                self.assertEqual(content, "sentence31\nsentence32\n")

            if key == "mykey04":
                self.assertEqual(content, "sentence41\nsentence42\n")

            if key == "mykey05":
                self.assertEqual(content, "sentence51\nsentence52\n")
