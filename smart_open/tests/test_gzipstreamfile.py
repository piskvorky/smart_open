import gzipstream.gzipstreamfile
import unittest
import os.path as P
import hashlib

CURR_DIR = P.abspath(P.dirname(__file__))


class TestSequenceFunctions(unittest.TestCase):

    def test_warc_md5sum(self):
        """Does the WARC md5 checksum match the expected value?"""
        fpath = P.join(CURR_DIR, 'test_data/crlf_at_1k_boundary.warc.gz')
        for read_size in [1, 2, 10, 28, 42, 100, 256, 512, 800]:
            f = open(fpath, "rb")
            gz = gzipstream.gzipstreamfile.GzipStreamFile(f)
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

if __name__ == '__main__':
    unittest.main()
