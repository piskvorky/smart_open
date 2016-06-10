#!/usr/bin/env python
import smart_open

URL = "s3://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2014-35/\
segments/1408500800168.29/warc/\
CC-MAIN-20140820021320-00000-ip-10-180-136-8.ec2.internal.warc.gz"

with smart_open.smart_open(URL) as fin:
    for i, line in enumerate(fin):
        print line.rstrip("\n")
        if i > 10:
            break
