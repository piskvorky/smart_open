from smart_open import open


def read_lines(url, limit):
    lines = []
    with open(url, 'r', errors='ignore') as fin:
        for i, l in enumerate(fin):
            if i == limit:
                break
            lines.append(l)

    return lines


def test(benchmark):
    #
    # This file is around 850MB.
    #
    url = (
        's3://commoncrawl/crawl-data/CC-MAIN-2019-51/segments/1575541319511.97'
        '/warc/CC-MAIN-20191216093448-20191216121448-00559.warc.gz'
    )
    limit = 1000000
    lines = benchmark(read_lines, url, limit)
    assert len(lines) == limit
