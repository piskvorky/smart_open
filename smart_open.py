#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT)
#

"""
TODO
"""

import boto
import gensim
import itertools
import logging
import subprocess
import urlparse

from cStringIO import StringIO
from multiprocessing.pool import Pool
from sys import version_info

# Minimum version required version 2.6;
# python 2.5 has a syntax which is already incompatible
# but newer pythons in 2 series ara easily forward compatible
_major_version = version_info[0]
if _major_version < 3:      # py <= 2.x
    if version_info[1] < 6: # py <= 2.5
        raise ImportError("smart_open requires python 2.6 or higher (python 3.3 or higher supported)")

try:
    from cPickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL
except ImportError:
    from pickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)

class ParseURL(object):
    def __init__(self, url, default_scheme = "file"):

        uri = urlparse.urlsplit(url)
        self.scheme = uri.scheme if uri.scheme else default_scheme
        

        if self.scheme == "hdfs":
            self.uri.path = uri.path
            return

        if self.scheme in ("s3", "s3n"):
            self.bucket_id = (uri.netloc + uri.path).split('@')
            self.key_id = ""

            if len(self.bucket_id) == 1:
                # URI without credentials: s3://bucket/object
                self.bucket_id, self.key_id = self.bucket_id[0].split('/', 1)
                self.access_id, self.access_secret = None, None
            elif len(self.bucket_id) == 2 and len(self.bucket_id[0].split(':')) == 2:
                # URI in full format: s3://key:secret@bucket/object
                acc, self.bucket_id = self.bucket_id
                self.access_id, self.access_secret = acc.split(':')
                self.bucket_id, self.key_id = self.bucket_id.split('/', 1)
            else:
                raise RuntimeError("invalid S3 URI: %s" % url)
            return

        if self.scheme == 'file':
            self.uri.path = uri.path
            return

        raise NotImplementedError("unknown URI scheme in %r" % input_uri)


class SmartOpenRead(object):
    def __init__(self, url, default_scheme = "file"):
        self.parsed_url = ParseURL(url, default_scheme)

        if not self.parsed_url.scheme in ("hdfs", "s3", "s3n", "file"):
            raise NotImplementedError("unknown URI scheme in %r" % parsed_url.scheme)

    def __iter__(self):
        if self.parsed_url.scheme == "hdfs":
            hdfs = subprocess.Popen(["hadoop", "fs", "-cat", parsed_url.uri.path], stdout = subprocess.PIPE)
            return hdfs.stdout

        if self.parsed_url.scheme in ("s3", "s3n"):
            s3_connection = boto.connect_s3(aws_access_key_id = self.parsed_url.access_id, aws_secret_access_key = self.parsed_url.access_secret)
            return s3_iter_lines(s3_connection.lookup(self.parsed_url.bucket_id).lookup(self.parsed_url.key_id))

        if self.parsed_url.scheme == "file":
            return gensim.utils.smart_open(parsed_url.uri.path)

        raise NotImplementedError("unknown URI scheme in %r" % self.parsed_url.scheme)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        return None

    #def read(self, size=-1):
    #    # return whole content as string
    #    if size < 0:
    #        size = None
    #
    #    # skip first self.start bytes
    #    chunk = itertools.islice(self.key, size)
    #    return "".join(chunk)

def s3_iter_lines(key):
    """
    Stream an object from s3 line by line (generator).

    `key` is a boto Key object.

    """

    buf = ''
    # keep reading chunks of bytes into the buffer
    for chunk in key:
        buf += chunk

        start = 0
        # process all lines within the current buffer
        while True:
            end = buf.find('\n', start) + 1
            if end:
                yield buf[start : end]
                start = end
            else:
                # no more newlines => break out to read more data from s3 into the buffer
                buf = buf[start : ]
                break
    # process the last line, too
    if buf:
        yield buf


def iter_lines(url, default_scheme = "file"):
    """
    Iterate over lines from a single file, pointed to by `url` path. The iteration
    is memory efficient (streaming) and suitable for large files.

    The `url` path can be either:

    1. local filesystem; examples: "/home/joe/lines.txt", "/home/joe/lines.txt.gz",
       "file:///home/joe/lines.txt.bz2"
    2. HDFS; example "hdfs:///some/path/lines.txt"
    3. Amazon's S3; example "s3://my_bucket/lines.txt",
       "s3://my_aws_key_id:key_secret@my_bucket/lines.txt"

    """

    smart_open = SmartOpenRead(url)
    return smart_open.__iter__()

def smart_open(url, mode = "rb"):
    """
    Iterate over lines from a single file, pointed to by `url` path. The iteration
    is memory efficient (streaming) and suitable for large files.

    The `url` path can be either:

    1. local filesystem; examples: "/home/joe/lines.txt", "/home/joe/lines.txt.gz",
       "file:///home/joe/lines.txt.bz2"
    2. HDFS; example "hdfs:///some/path/lines.txt"
    3. Amazon's S3; example "s3://my_bucket/lines.txt",
       "s3://my_aws_key_id:key_secret@my_bucket/lines.txt"

    """

    if (mode == "rb"):
        return SmartOpenRead(url)

    if (mode == "wb"):
        parsed_url = ParseURL(url)

        # prepare boto bucket and boto key objects
        s3_connection = boto.connect_s3(aws_access_key_id = parsed_url.access_id, aws_secret_access_key = parsed_url.access_secret)
        outbucket = s3_connection.lookup(parsed_url.bucket_id)
        outkey = boto.s3.key.Key(outbucket)
        outkey.key = parsed_url.key_id

        # return SmartOpenWrite object
        return SmartOpenWrite(outbucket, outkey)

    raise NotImplementedError("unknown file mode %s" % mode)

class SmartOpenWrite(object):
    def __init__(self, outbucket, outkey, delimiter = "\n", min_chunk_size = 5 * 1024 ** 2):
        self.outbucket = outbucket
        self.outkey = outkey
        self.min_chunk_size = min_chunk_size
        self.delimiter = delimiter

        if min_chunk_size < 5 * 1024 ** 2:
            logger.warning("s3 requires minimum part size >= 5MB; multipart upload may fail")

    def __enter__(self):
        # initialize mulitpart upload
        self.mp = self.outbucket.initiate_multipart_upload(self.outkey)

        # initialize stats
        self.lines = []
        self.total_size = 0
        self.chunk_bytes = 0
        self.parts = 0

        return self

    def write(self, line):
        if isinstance(line, unicode):
            line = line.encode('utf8')

        assert isinstance(line, str), "input must be a sequence of strings"

        self.lines.append(line + self.delimiter)
        self.chunk_bytes += len(line)
        self.total_size += len(line)

        if self.chunk_bytes >= self.min_chunk_size:
            buff = "".join(self.lines)
            logger.info("uploading part #%i, %i bytes (total %.1fGB)" % (self.parts, len(buff), self.total_size / 1024.0 ** 3))
            mp.upload_part_from_file(StringIO(buff), part_num = self.parts + 1)
            self.parts += 1
            self.lines, self.chunk_bytes = [], 0

    def __exit__(self, type, value, traceback):
        buff = "".join(self.lines)
        if buff:
            logger.info("uploading last part #%i, %i bytes (total %.1fGB)" % (self.parts, len(buff), self.total_size / 1024.0 ** 3))
            self.mp.upload_part_from_file(StringIO(buff), part_num = self.parts + 1)

        if self.total_size:
            self.mp.complete_upload()
        else:
            # AWS complains with "The XML you provided was not well-formed or did not validate against our published schema"
            # when the input is completely empty => abort the upload
            logger.warning("empty input, ignoring multipart upload")
            outbucket.cancel_multipart_upload(self.mp.key_name, self.mp.id)

        return None

def s3_store_lines(input_data, url = "", outbucket = None, outkey = None, delimiter = "\n", min_chunk_size = 50 * 1024 ** 2):
    """
    Stream lines (strings, or unicode which will be converted to utf8 strings) from
    the `input_data` iterator into a single s3 object `outkey` (string) under `outbucket`
    (boto Bucket object).

    Optionally put `delimiter` after each line (set this to empty string to disable;
    default is the newline character).

    This works around a number of s3 annoyances, such as its inability to store too
    large files (=uses multipart upload), inability to store too small multiparts
    (=merges parts into blocks of sufficient size), empty files etc.

    Suitable for processing very large inputs/outputs, in limited RAM.

    """

    if url == "" and not outbucket:
        raise RuntimeError("`input_uri` or `outbucket` must be defined: %s" % input_uri)

    if url != "":
        parsed_url = ParseURL(url)
        s3_connection = boto.connect_s3(aws_access_key_id = parsed_url.access_id, aws_secret_access_key = parsed_url.access_secret)
        outbucket = s3_connection.lookup(parsed_url.bucket_id)
        outkey = boto.s3.key.Key(outbucket)
        outkey.key = parsed_url.key_id
    else:
        outkey_id = outkey
        outkey = boto.s3.key.Key(outbucket)
        outkey.key = outkey_id

    logger.info("streaming input into %s/%s, using %.1fMB chunks and %r delimiter" %
        (outbucket, outkey, min_chunk_size / 1024.0 ** 2, delimiter))

    with SmartOpenWrite(outbucket, outkey, delimiter, min_chunk_size) as fout:
        for lineno, line in enumerate(input_data):
            fout.write(line)


def s3_iter_bucket_process_key(key):
    """
    Conceptually part of `s3_iter_bucket`, but must remain top-level method because
    of pickling visibility.

    """
    return key, key.get_contents_as_string()


def s3_iter_bucket(bucket, prefix='', accept_key=lambda key: True, key_limit=None, workers=16):
    """
    Iterate over all objects in the given input `bucket` in parallel, yielding out
    (key name, key content) 2-tuples (generator).

    `accept_key` must be a function of one parameter = unicode string = key name.

    """
    logger.info("iterating over keys from %s with %i workers" % (bucket, workers))

    total_size, key_no = 0, -1
    keys = (key for key in bucket.list(prefix=prefix) if accept_key(key.name))

    pool = Pool(processes=workers)
    for key_no, (key, content) in enumerate(pool.imap_unordered(s3_iter_bucket_process_key, keys)):
        if key_no % 1000 == 0:
            logger.info("yielding key #%i: %s, size %i (total %.1fMB)" %
                (key_no, key, len(content), total_size / 1024.0**2))

        yield key.name, content
        key.close()
        total_size += len(content)

        if key_limit is not None and key_no + 1 >= key_limit:
            # we were asked to output only a limited number of keys => we're done
            break
    pool.terminate()

    logger.info("processed %i keys, total size %i" % (key_no + 1, total_size))
