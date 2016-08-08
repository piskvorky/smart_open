import smart_open.gzipstreamfile
import unittest
import os.path as P
import hashlib
import logging
import io
import mock
import zlib

logger = logging.getLogger(__name__)

CURR_DIR = P.abspath(P.dirname(__file__))


class TestSequenceFunctions(unittest.TestCase):

    def test_warc_md5sum(self):
        """Does the WARC md5 checksum match the expected value?"""
        fpath = P.join(CURR_DIR, 'test_data/crlf_at_1k_boundary.warc.gz')
        for read_size in [1, 2, 10, 28, 42, 100, 256, 512, 800]:
            f = open(fpath, "rb")
            gz = smart_open.gzipstreamfile.GzipStreamFile(f)
            data = []
            tmp = gz.read(read_size)
            while tmp:
                data.append(tmp)
                tmp = gz.read(read_size)
                self.assertTrue(tmp is not None)
            data = b''.join(data)
            m = hashlib.md5(data)
            assert m.hexdigest() == '18473e60f8c7c98d29d65bf805736a0d', \
                'Failed with read size of {0}'.format(read_size)
            f.close()


def mock_decompressobj(*args, **kwargs):
    # Crafting the payload to reproduce a bug is tricky since both the
    # compressed and decompressed streams must trigger a certain condition wrt
    # to io.DEFAULT_BUFFER_SIZE.  We use mock to reproduce the bug easier.
    decoder = mock.Mock()
    decoder.decompress = lambda x: x
    decoder.unused_data = None
    return decoder


class S3ReadStreamInnerTest(unittest.TestCase):

    @mock.patch("zlib.decompressobj", mock_decompressobj)
    def test_read_from_internal_buffer(self):
        """The reader should correctly return bytes in its unused buffer."""
        stream = io.BytesIO(b"0" * io.DEFAULT_BUFFER_SIZE * 2)
        reader = smart_open.gzipstreamfile.GzipStreamFileInner(stream)

        ret = reader.read(io.DEFAULT_BUFFER_SIZE // 2)
        self.assertEquals(len(ret), io.DEFAULT_BUFFER_SIZE // 2)

        ret = reader.read(io.DEFAULT_BUFFER_SIZE // 4)
        self.assertEquals(len(ret), io.DEFAULT_BUFFER_SIZE // 4)

        ret = reader.read()
        self.assertEquals(len(ret), io.DEFAULT_BUFFER_SIZE * 5 / 4)

    @mock.patch("zlib.decompressobj", mock_decompressobj)
    def test_read_from_closed_stream(self):
        """The reader should correctly handle reaching the end of the
        stream."""
        stream = io.BytesIO(b"0" * io.DEFAULT_BUFFER_SIZE)
        reader = smart_open.gzipstreamfile.GzipStreamFileInner(stream)

        ret = reader.read(io.DEFAULT_BUFFER_SIZE * 2)
        self.assertEquals(len(ret), io.DEFAULT_BUFFER_SIZE)

        ret = reader.read()
        self.assertEquals(len(ret), 0)

    @mock.patch("zlib.decompressobj", mock_decompressobj)
    def test_buffer_flushed_after_eof(self):
        """The buffer should be empty after we've requested to read until
        EOF."""
        stream = io.BytesIO(b"0" * io.DEFAULT_BUFFER_SIZE * 2)
        reader = smart_open.gzipstreamfile.GzipStreamFileInner(stream)
        self.assertEquals(len(reader.read(io.DEFAULT_BUFFER_SIZE)),
                          io.DEFAULT_BUFFER_SIZE)
        self.assertEquals(len(reader.read(io.DEFAULT_BUFFER_SIZE)),
                          io.DEFAULT_BUFFER_SIZE)
        self.assertEquals(len(reader.unused_buffer), 0)
        self.assertEquals(len(reader.read()), 0)

    def test_read_until_eof(self):
        """The reader should correctly read until EOF."""
        fpath = P.join(CURR_DIR, 'test_data/crlf_at_1k_boundary.warc.gz')
        with open(fpath, "rb") as fin:
            expected = zlib.decompress(fin.read(), 16 + zlib.MAX_WBITS)

        #
        # Test reading all at once.
        #
        with open(fpath, "rb") as fin:
            reader = smart_open.gzipstreamfile.GzipStreamFileInner(fin)
            actual = reader.read()
        self.assertEquals(expected, actual)

        #
        # Test reading in smaller chunks.
        #
        with open(fpath, "rb") as fin:
            reader = smart_open.gzipstreamfile.GzipStreamFileInner(fin)
            actual = reader.read(io.DEFAULT_BUFFER_SIZE // 2)
            actual += reader.read(io.DEFAULT_BUFFER_SIZE // 4)
            actual += reader.read()
        self.assertEquals(expected, actual)

if __name__ == '__main__':
    unittest.main()
