# -*- coding: utf-8 -*-
import contextlib
import logging
import gzip
import io
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest

import boto
import moto

import smart_open
import smart_open.s3

_LOGGER = logging.getLogger(__name__)


def create_bucket_and_key(bucket_name='mybucket', key_name='mykey', contents=None):
    # fake connection, bucket and key
    _LOGGER.debug('%r', locals())
    conn = boto.connect_s3()
    conn.create_bucket(bucket_name)
    mybucket = conn.get_bucket(bucket_name)
    mykey = boto.s3.key.Key()
    mykey.name = key_name
    mykey.bucket = mybucket
    if contents is not None:
        _LOGGER.debug('len(contents): %r', len(contents))
        mykey.set_contents_from_string(contents)
    return mybucket, mykey


@moto.mock_s3
class BufferedInputBaseTest(unittest.TestCase):
    def setUp(self):
        # lower the multipart upload size, to speed up these tests
        self.old_min_part_size = smart_open.s3.DEFAULT_MIN_PART_SIZE
        smart_open.s3.DEFAULT_MIN_PART_SIZE = 5 * 1024**2

    def tearDown(self):
        smart_open.s3.DEFAULT_MIN_PART_SIZE = self.old_min_part_size

    def test_iter(self):
        """Are S3 files iterated over correctly?"""
        # a list of strings to test with
        expected = u"hello wořld\nhow are you?".encode('utf8')
        bucket, key = create_bucket_and_key(contents=expected)

        # connect to fake s3 and read from the fake key we filled above
        fin = smart_open.s3.BufferedInputBase('mybucket', 'mykey')
        output = [line.rstrip(b'\n') for line in fin]
        self.assertEqual(output, expected.split(b'\n'))

    def test_iter_context_manager(self):
        # same thing but using a context manager
        expected = u"hello wořld\nhow are you?".encode('utf8')
        bucket, key = create_bucket_and_key(contents=expected)
        with smart_open.s3.BufferedInputBase('mybucket', 'mykey') as fin:
            output = [line.rstrip(b'\n') for line in fin]
            self.assertEqual(output, expected.split(b'\n'))

    def test_read(self):
        """Are S3 files read correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        bucket, key = create_bucket_and_key(contents=content)
        _LOGGER.debug('content: %r len: %r', content, len(content))

        fin = smart_open.s3.BufferedInputBase('mybucket', 'mykey')
        self.assertEqual(content[:6], fin.read(6))
        self.assertEqual(content[6:14], fin.read(8))  # ř is 2 bytes
        self.assertEqual(content[14:], fin.read())  # read the rest

    def test_seek_beginning(self):
        """Does seeking to the beginning of S3 files work correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        bucket, key = create_bucket_and_key(contents=content)

        fin = smart_open.s3.BufferedInputBase('mybucket', 'mykey')
        self.assertEqual(content[:6], fin.read(6))
        self.assertEqual(content[6:14], fin.read(8))  # ř is 2 bytes

        fin.seek(0)
        self.assertEqual(content, fin.read())  # no size given => read whole file

        fin.seek(0)
        self.assertEqual(content, fin.read(-1))  # same thing

    def test_seek_start(self):
        """Does seeking from the start of S3 files work correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        bucket, key = create_bucket_and_key(contents=content)

        fin = smart_open.s3.BufferedInputBase('mybucket', 'mykey')
        seek = fin.seek(6)
        self.assertEqual(seek, 6)
        self.assertEqual(fin.tell(), 6)
        self.assertEqual(fin.read(6), u'wořld'.encode('utf-8'))

    def test_seek_current(self):
        """Does seeking from the middle of S3 files work correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        bucket, key = create_bucket_and_key(contents=content)

        fin = smart_open.s3.BufferedInputBase('mybucket', 'mykey')
        self.assertEqual(fin.read(5), b'hello')
        seek = fin.seek(1, whence=smart_open.s3.CURRENT)
        self.assertEqual(seek, 6)
        self.assertEqual(fin.read(6), u'wořld'.encode('utf-8'))

    def test_seek_end(self):
        """Does seeking from the end of S3 files work correctly?"""
        content = u"hello wořld\nhow are you?".encode('utf8')
        bucket, key = create_bucket_and_key(contents=content)

        fin = smart_open.s3.BufferedInputBase('mybucket', 'mykey')
        seek = fin.seek(-4, whence=smart_open.s3.END)
        self.assertEqual(seek, len(content) - 4)
        self.assertEqual(fin.read(), b'you?')

    def test_detect_eof(self):
        content = u"hello wořld\nhow are you?".encode('utf8')
        bucket, key = create_bucket_and_key(contents=content)

        fin = smart_open.s3.BufferedInputBase('mybucket', 'mykey')
        fin.read()
        eof = fin.tell()
        self.assertEqual(eof, len(content))
        fin.seek(0, whence=smart_open.s3.END)
        self.assertEqual(eof, fin.tell())

    def test_read_gzip(self):
        expected = u'раcцветали яблони и груши, поплыли туманы над рекой...'.encode('utf-8')
        buf = io.BytesIO()
        buf.close = lambda: None  # keep buffer open so that we can .getvalue()
        with contextlib.closing(gzip.GzipFile(fileobj=buf, mode='w')) as zipfile:
            zipfile.write(expected)
        bucket, key = create_bucket_and_key(contents=buf.getvalue())

        #
        # Make sure we're reading things correctly.
        #
        with smart_open.s3.BufferedInputBase('mybucket', 'mykey') as fin:
            self.assertEqual(fin.read(), buf.getvalue())

        #
        # Make sure the buffer we wrote is legitimate gzip.
        #
        sanity_buf = io.BytesIO(buf.getvalue())
        with contextlib.closing(gzip.GzipFile(fileobj=sanity_buf)) as zipfile:
            self.assertEqual(zipfile.read(), expected)

        _LOGGER.debug('starting actual test')
        with smart_open.s3.BufferedInputBase('mybucket', 'mykey') as fin:
            with contextlib.closing(gzip.GzipFile(fileobj=fin)) as zipfile:
                actual = zipfile.read()

        self.assertEqual(expected, actual)


@moto.mock_s3
class BufferedOutputBaseTest(unittest.TestCase):
    """
    Test writing into s3 files.

    """
    def test_write_01(self):
        """Does writing into s3 work correctly?"""
        mybucket, mykey = create_bucket_and_key()
        test_string = u"žluťoučký koníček".encode('utf8')

        # write into key
        with smart_open.s3.BufferedOutputBase('mybucket', 'writekey') as fout:
            fout.write(test_string)

        # read key and test content
        output = list(smart_open.smart_open("s3://mybucket/writekey", "rb"))

        self.assertEqual(output, [test_string])

    def test_write_01a(self):
        """Does s3 write fail on incorrect input?"""
        mybucket, mykey = create_bucket_and_key()

        try:
            with smart_open.s3.BufferedOutputBase('mybucket', 'writekey') as fin:
                fin.write(None)
        except TypeError:
            pass
        else:
            self.fail()

    def test_write_02(self):
        """Does s3 write unicode-utf8 conversion work?"""
        mybucket, mykey = create_bucket_and_key()

        smart_open_write = smart_open.s3.BufferedOutputBase('mybucket', 'writekey')
        smart_open_write.tell()
        _LOGGER.info("smart_open_write: %r", smart_open_write)
        with smart_open_write as fout:
            fout.write(u"testžížáč".encode("utf-8"))
            self.assertEqual(fout.tell(), 14)

    def test_write_03(self):
        """Does s3 multipart chunking work correctly?"""
        mybucket, mykey = create_bucket_and_key()

        # write
        smart_open_write = smart_open.s3.BufferedOutputBase(
            'mybucket', 'writekey', min_part_size=10
        )
        with smart_open_write as fout:
            fout.write(b"test")
            self.assertEqual(fout._buf.tell(), 4)

            fout.write(b"test\n")
            self.assertEqual(fout._buf.tell(), 9)
            self.assertEqual(fout._total_parts, 0)

            fout.write(b"test")
            self.assertEqual(fout._buf.tell(), 0)
            self.assertEqual(fout._total_parts, 1)

        # read back the same key and check its content
        output = list(smart_open.smart_open("s3://mybucket/writekey"))
        self.assertEqual(output, [b"testtest\n", b"test"])

    def test_write_04(self):
        """Does writing no data cause key with an empty value to be created?"""
        mybucket, mykey = create_bucket_and_key()

        smart_open_write = smart_open.s3.BufferedOutputBase('mybucket', 'writekey')
        with smart_open_write as fout:  # noqa
            pass

        # read back the same key and check its content
        output = list(smart_open.smart_open("s3://mybucket/writekey"))

        self.assertEqual(output, [])

    def test_gzip(self):
        create_bucket_and_key()

        expected = u'а не спеть ли мне песню... о любви'.encode('utf-8')
        with smart_open.s3.BufferedOutputBase('mybucket', 'writekey') as fout:
            with contextlib.closing(gzip.GzipFile(fileobj=fout, mode='w')) as zipfile:
                zipfile.write(expected)

        with smart_open.s3.BufferedInputBase('mybucket', 'writekey') as fin:
            with contextlib.closing(gzip.GzipFile(fileobj=fin)) as zipfile:
                actual = zipfile.read()

        self.assertEqual(expected, actual)

    def test_text_iterator(self):
        expected = u"выйду ночью в поле с конём".split(u' ')
        create_bucket_and_key(contents="\n".join(expected).encode('utf-8'))
        with smart_open.s3.open('mybucket', 'mykey', 'r') as fin:
            actual = [line.rstrip() for line in fin]
        self.assertEqual(expected, actual)

    def test_binary_iterator(self):
        expected = u"выйду ночью в поле с конём".encode('utf-8').split(b' ')
        create_bucket_and_key(contents=b"\n".join(expected))
        with smart_open.s3.open('mybucket', 'mykey', 'rb') as fin:
            actual = [line.rstrip() for line in fin]
        self.assertEqual(expected, actual)


class ClampTest(unittest.TestCase):
    def test(self):
        self.assertEqual(smart_open.s3._clamp(5, 0, 10), 5)
        self.assertEqual(smart_open.s3._clamp(11, 0, 10), 10)
        self.assertEqual(smart_open.s3._clamp(-1, 0, 10), 0)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    unittest.main()
