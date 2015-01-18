#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).

import mock
import moto
import os
import random
import unittest
import string

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
        with self.assertRaises(NotImplementedError):
            smart_open.ParseURL("http://mybucket/mykey")

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
        parsed_url = smart_open.ParseURL("s3://accessid:accesssecret@mybucket/mykey")
        self.assertEqual(parsed_url.scheme, "s3")
        self.assertEqual(parsed_url.bucket_id, "mybucket")
        self.assertEqual(parsed_url.key_id, "mykey")
        self.assertEqual(parsed_url.access_id, "accessid")
        self.assertEqual(parsed_url.access_secret, "access_secret")

        # correct uri, credentials contains 
        parsed_url = smart_open.ParseURL("s3://accessid:access/secret@mybucket/mykey")
        self.assertEqual(parsed_url.scheme, "s3")
        self.assertEqual(parsed_url.bucket_id, "mybucket")
        self.assertEqual(parsed_url.key_id, "mykey")
        self.assertEqual(parsed_url.access_id, "accessid")
        self.assertEqual(parsed_url.access_secret, "access/secret")

        # incorrect uri - only one '@' in uri is allowed
        # TODO: check is bucket or key could contain '@'
        # TODO: asi to nefunguje v 2.6
        with self.assertRaises(RuntimeError):
            parsed_url = smart_open.ParseURL("s3://access_id@access_secret@mybucket/mykey")


class SmartOpenReadTest(unittest.TestCase):
    """
    Test reading from files

    """
    @mock.patch('smart_open.gensim.utils')
    def test_file(self, mock_gensim):
        """
        Test FILE files.
        Check if gensim.utils.smart_open obtain correct filepath.

        """
        smart_open_object = smart_open.SmartOpenRead("file:///tmp/test.txt")
        smart_open_object.__iter__()
        mock_gensim.smart_open.assert_called_with("/tmp/test.txt")

    # TODO: overit, ci existuje knihovna na testovanie HDFS
    @mock.patch('smart_open.subprocess')
    def test_hdfs(self, mock_subprocess):
        """
        Test iterator for HDFS files.
        Check if subprocess.Popen obtain correct filepath.

        """
        smart_open_object = smart_open.SmartOpenRead("hdfs:///tmp/test.txt")
        for line in smart_open_object:
            break
        mock_subprocess.PIPE.return_value = "test"
        mock_subprocess.Popen.assert_called_with(["hadoop", "fs", "-cat", "/tmp/test.txt"], stdout=mock_subprocess.PIPE)

    # TODO: knihovna MOTO na testovanie BOTO
    @mock.patch('smart_open.boto')
    @mock.patch('smart_open.s3_iter_lines')
    def test_s3(self, mock_s3_iter_lines, mock_boto):
        """
        Test iterator for S3 files
        Check if boto.connect_s3 obtain correct credentials

        """
        # no credentials
        smart_open_object = smart_open.SmartOpenRead("s3://mybucket/mykey")
        for line in smart_open_object:
            break
        mock_boto.connect_s3.assert_called_with(aws_access_key_id=None, aws_secret_access_key=None)

        # with credential
        smart_open_object = smart_open.SmartOpenRead("s3://access_id:access_secret@mybucket/mykey")
        for line in smart_open_object:
            break
        mock_boto.connect_s3.assert_called_with(aws_access_key_id="access_id", aws_secret_access_key="access_secret")

        # lookup bucket, key; call s3_iter_lines
        smart_open_object = smart_open.SmartOpenRead("s3://access_id:access_secret@mybucket/mykey")
        for line in smart_open_object:
            break
        mock_boto.connect_s3().lookup.assert_called_with("mybucket")
        mock_boto.connect_s3().lookup().lookup.assert_called_with("mykey")
        self.assertTrue(mock_s3_iter_lines.called)


class S3IterLinesTest(unittest.TestCase):
    """
    Test method s3_iter_lines.

    """

    # TODO: v Moto vyrobit fake key
    def test_s3_iter_lines(self):
        """
        Test s3_iter_lines

        """
        test_sequence = "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n"
        input_sequence = (str(i) + "\n" for i in range(10))
        output_sequence = list(smart_open.s3_iter_lines(input_sequence))
        self.assertEqual("".join(output_sequence), test_sequence)

    # TODO: test ked na vstupe nema boto object


class IterLinesTest(unittest.TestCase):
    """
    Test reading from files.
    Method iter_lines just calls SmartOpenRead class.

    """
    @mock.patch('smart_open.SmartOpenRead')
    def test_iter_lines(self, mock):
        """
        Test iter_lines.
        Check if SmartOpenRead gets correct argument.

        """
        smart_open.iter_lines("blah")
        mock.assert_called_with("blah")


class SmartOpenTest(unittest.TestCase):
    """
    Test reading and writing from/into files

    """

    @mock.patch('smart_open.boto')
    @mock.patch('smart_open.SmartOpenRead')
    @mock.patch('smart_open.SmartOpenWrite')
    def test_file_mode(self, mock_write, mock_read, mock_boto):
        """
        Test supported file modes
        
        """
        # incorrect file mode
        with self.assertRaises(NotImplementedError):
            smart_open.smart_open("blah", "x")

        # correct read modes
        smart_open.smart_open("blah", "r")
        mock_read.assert_called_with("blah")

        smart_open.smart_open("blah", "rb")
        mock_read.assert_called_with("blah")

        # correct write modes, incorrect scheme
        with self.assertRaises(NotImplementedError):
            smart_open.smart_open("blah", "w")

        with self.assertRaises(NotImplementedError):
            smart_open.smart_open("file:///blah.txt", "w")

        with self.assertRaises(NotImplementedError):
            smart_open.smart_open("hdfs:///blah.txt", "wb")

        with self.assertRaises(NotImplementedError):
            smart_open.smart_open("http:///blah.txt", "w")

        # TODO: moto
        # correct write mode, correct s3 url
        smart_open.smart_open("s3://mybucket/mykey", "w")
        mock_boto.connect_s3.assert_called_with(aws_access_key_id=None, aws_secret_access_key=None)
        mock_boto.connect_s3().lookup.return_value = True
        mock_boto.connect_s3().lookup.assert_called_with("mybucket")
        self.assertTrue(mock_write.called)


class SmartOpenWriteTest(unittest.TestCase):
    """
    Test writing into s3 files.

    """
    def test_write_01(self):
        """
        Test writing into s3 files.

        """
        # check incorrect input
        mybucket = mock.Mock(spec=["initiate_multipart_upload", "cancel_multipart_upload", "key_name", "id", "upload_part_from_file", "complete_upload"])
        mykey = mock.Mock()
        with self.assertRaises(TypeError):
            with smart_open.SmartOpenWrite(mybucket, mykey) as fin:
                fin.write(None)

    def test_write_02(self):
        """
        Test writing into s3 files.

        """
        mybucket = mock.Mock(spec=["initiate_multipart_upload", "cancel_multipart_upload", "key_name", "id", "upload_part_from_file", "complete_upload"])
        mykey = mock.Mock()

        smart_open_write = smart_open.SmartOpenWrite(mybucket, mykey)
        with smart_open_write as fin:
            fin.write(u"testžížáč")
            self.assertEqual(fin.total_size, 14)
            self.assertFalse(fin.mp.upload_part_from_file.called)

        self.assertTrue(smart_open_write.mp.complete_upload.called)

    def test_write_03(self):
        """
        Test writing into s3 files.

        """
        mybucket = mock.Mock(spec=["initiate_multipart_upload", "cancel_multipart_upload", "key_name", "id", "upload_part_from_file", "complete_upload"])
        mykey = mock.Mock()

        smart_open_write = smart_open.SmartOpenWrite(mybucket, mykey, min_chunk_size=10)
        with smart_open_write as fin:
            fin.write(u"test")
            self.assertEqual(fin.chunk_bytes, 4)
            self.assertFalse(fin.mp.upload_part_from_file.called)

            fin.write(u"test")
            self.assertEqual(fin.chunk_bytes, 8)
            self.assertFalse(fin.mp.upload_part_from_file.called)
            self.assertEqual(fin.parts, 0)

            fin.write(u"test")
            self.assertEqual(fin.chunk_bytes, 0)
            self.assertTrue(fin.mp.upload_part_from_file.called)
            self.assertEqual(fin.parts, 1)

        self.assertTrue(smart_open_write.mp.complete_upload.called)

        # TODO: moto na testovanie obsahu
        
        # TODO: test na volanie cancel_upload
        #  - vyhodime vynimku


class S3StoreLinesTest(unittest.TestCase):
    """
    Test writing into s3 files.

    """
    def test_s3_store_lines_01(self):
        """
        Test controlling of input arguments.

        """
        # no url and no bucket raises RuntimeError
        with self.assertRaises(RuntimeError):
            smart_open.s3_store_lines(None, url=None)

        # given url must have s3 scheme
        with self.assertRaises(NotImplementedError):
            smart_open.s3_store_lines(None, url="a")

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
        
        # TODO - moto

    @mock.patch('smart_open.boto')
    @mock.patch('smart_open.SmartOpenWrite')
    def test_s3_store_lines_03(self, mock_write, mock_boto):
        """
        Test s3_store_lines with given bucket.

        """
        mybucket = mock.Mock()
        smart_open.s3_store_lines(["sentence1", "sentence2"], outbucket=mybucket)
        self.assertFalse(mock_boto.connect_s3.called)

        # TODO - moto


class S3IterBucketTest(unittest.TestCase):
    """
    Test parallel iteration of given bucket

    """
    def test_s3_iter_bucket_process_key(self):
        """
        Test s3_iter_bucket_process_key

        """
        attrs = {"name" : "fileA", "get_contents_as_string.return_value" : "contentA"}
        mykey = mock.Mock(spec=["name", "get_contents_as_string"])
        mykey.configure_mock(**attrs)

        (key, content) = smart_open.s3_iter_bucket_process_key(mykey)
        self.assertEqual(key, mykey)
        self.assertEqual(content, "contentA")


    @mock.patch('smart_open.multiprocessing.pool')
    def test_s3_iter_bucket_01(self, mock_pool):
        """
        Test parallel bucket iteration

        """
        # TODO - moto
        # TODO - ma amazon nejaky testovaci bucket?
        

        attrs = {"name" : "fileA", "get_contents_as_string.return_value" : "contentA"}
        mykey = mock.Mock(spec=["name", "get_contents_as_string"])
        mykey.configure_mock(**attrs)

        attrs = {"list.return_value" : [mykey]}
        mybucket = mock.Mock(spec=["list"])
        mybucket.configure_mock(**attrs)

        for (key, content) in smart_open.s3_iter_bucket(mybucket):
            mock_pool.Pool.assert_called_with(processes=16)
            mock_pool.Pool().imap_unordered.assert_called_with()
            #self.assertEqual(key, mykey)
            #self.assertEqual(content, "contentB")

        mock_pool.Pool.assert_called_with(processes=16)
        self.assertTrue(mock_pool.Pool().imap_unordered.called)

    # TODO
    #def test_s3_iter_bucket_02(self):
    #    """
    #    Test parallel bucket iteration
    #
    #    """
    #    attrs = {"name" : "fileA", "get_contents_as_string.return_value" : "contentA"}
    #    mykey = mock.Mock(spec=["name", "get_contents_as_string"])
    #    mykey.configure_mock(**attrs)
    #
    #    attrs = {"list.return_value" : [mykey]}
    #    mybucket = mock.Mock(spec=["list"])
    #    mybucket.configure_mock(**attrs)
    #
    #    for (key, content) in smart_open.s3_iter_bucket(mybucket):
    #        self.assertEqual(key, mykey)
    #        self.assertEqual(content, "contentB")
