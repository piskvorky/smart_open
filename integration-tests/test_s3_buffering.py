from smart_open import open


def read_bytes(url, limit):
    bytes_ = []
    with open(url, 'rb') as fin:
        for i in range(limit):
            bytes_.append(fin.read(1))

    return bytes_


def test(benchmark):
    #
    # This file is around 850MB.
    #
    url = (
        's3://commoncrawl/crawl-data/CC-MAIN-2019-51/segments/1575541319511.97'
        '/warc/CC-MAIN-20191216093448-20191216121448-00559.warc.gz'
    )
    limit = 1000000
    bytes_ = benchmark(read_bytes, url, limit)
    assert len(bytes_) == limit
