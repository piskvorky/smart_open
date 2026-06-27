from smart_open import open


def read_bytes(url, limit):
    """Read ``limit`` one-byte chunks from ``url`` and return them as a list."""
    with open(url, "rb") as fin:
        return [fin.read(1) for _i in range(limit)]


def test(benchmark):
    """Benchmark many small reads against a large S3 object."""
    #
    # This file is around 850MB.
    #
    url = (
        "s3://commoncrawl/crawl-data/CC-MAIN-2019-51/segments/1575541319511.97"
        "/warc/CC-MAIN-20191216093448-20191216121448-00559.warc.gz"
    )
    limit = 1000000
    bytes_ = benchmark(read_bytes, url, limit)
    assert len(bytes_) == limit
