from .. import gzipstreamfile
import unittest

GzipStreamFile = gzipstreamfile.GzipStreamFile


class TestSequenceFunctions(unittest.TestCase):
  def test_warc_md5sum(self):
    for read_size in [1, 2, 10, 28, 42, 100, 256, 512, 800]:
      f = open('gzipstream/tests/test_data/crlf_at_1k_boundary.warc.gz')
      gz = GzipStreamFile(f)
      data = []
      tmp = gz.read(read_size)
      while tmp:
        data.append(tmp)
        tmp = gz.read(read_size)
      data = ''.join(data)
      import hashlib
      m = hashlib.md5(data)
      assert m.hexdigest() == '18473e60f8c7c98d29d65bf805736a0d', 'Failed with read size of {}'.format(read_size)

if __name__ == '__main__':
  unittest.main()
