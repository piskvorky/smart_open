"""Prepare an S3 bucket for our integration tests.

Once the bucket is initialized, the tests in test_s3_ported.py should pass.
"""

import gzip
import sys

import boto3


def _build_contents():
    hello_bytes = u"hello wořld\nhow are you?".encode('utf8')
    yield 'hello.txt', hello_bytes
    yield 'multiline.txt', b'englishman\nin\nnew\nyork\n'
    yield 'hello.txt.gz', gzip.compress(hello_bytes)

    for i in range(100):
        key = 'iter_bucket/%02d.txt' % i
        body = '\n'.join("line%i%i" % (i, line_no) for line_no in range(10)).encode('utf8')
        yield key, body


CONTENTS = dict(_build_contents())


def main():
    bucket_name = sys.argv[1]

    bucket = boto3.resource('s3').Bucket(bucket_name)

    #
    # Assume the bucket exists.  Creating it ourselves and dealing with
    # timing issues is too much of a PITA.
    #
    for key in bucket.objects.all():
        key.delete()

    for (key, body) in CONTENTS.items():
        bucket.put_object(Key=key, Body=body)


if __name__ == '__main__':
    main()
