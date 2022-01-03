import contextlib
import os
import random
import string
import uuid

from pytest import fixture
import oss2

import smart_open

_BUCKET = os.environ.get('OSS_BUCKET')
assert _BUCKET is not None, 'please set the OSS_BUCKET environment variable'

_KEY = os.environ.get('OSS_KEY')
assert _KEY is not None, 'please set the OSS_KEY environment variable'

_AK = os.environ.get('OSS_AK')
assert _AK is not None, 'please set the OSS_AK environment variable'

_SK = os.environ.get('OSS_SK')
assert _SK is not None, 'please set the OSS_SK environment variable'

_ENDPOINT = os.environ.get('OSS_ENDPOINT', 'https://oss-cn-hangzhou.aliyuncs.com')
assert _ENDPOINT is not None, 'please set the OSS_ENDPOINT environment variable'


def get_uuid():
    return str(uuid.uuid4())[:6]


def _get_oss_bucket(bucket_name, endpoint, ak, sk):
    return oss2.Bucket(oss2.Auth(ak, sk), endpoint, bucket_name)


def _get_obj_iter(oss_bucket, prefix):
    for info in oss2.ObjectIterator(oss_bucket,
                                    prefix=prefix,
                                    delimiter='/',
                                    max_keys=100):
        try:
            yield info.key
        except (oss2.exceptions.NoSuchKey, oss2.exceptions.NotFound) as e:
            continue
        except Exception as e:
            raise e


def _delete_obj_by_prefix(oss_bucket, prefix):
    for obj_key in _get_obj_iter(oss_bucket, prefix):
        oss_bucket.delete_object(obj_key)


#
# https://stackoverflow.com/questions/13484726/safe-enough-8-character-short-unique-random-string
#
def _random_string(length=8):
    alphabet = string.ascii_lowercase + string.digits
    return ''.join(random.choices(alphabet, k=length))


@fixture
def oss_bucket():
    return _get_oss_bucket(_BUCKET, _ENDPOINT, _AK, _SK)


@contextlib.contextmanager
def temporary(oss_bucket):
    """Yields a URL than can be used for temporary writing.

    Removes all content under the URL when exiting.
    """
    key = '%s/%s' % (_KEY, _random_string())
    yield 'oss://%s/%s' % (_BUCKET, key)

    # oss_bucket = _get_oss_bucket(_BUCKET, _ENDPOINT, _AK, _SK)
    _delete_obj_by_prefix(oss_bucket, prefix=key)


def _test_case(function):
    def inner(benchmark, oss_bucket):
        with temporary(oss_bucket) as uri:
            return function(benchmark, oss_bucket, uri)
    return inner


def write_read(uri, content, write_mode, read_mode, encoding=None, oss_bucket=None, **kwargs):
    transport_params = dict(kwargs)
    transport_params.update(client=oss_bucket)

    # with open(url, 'wb', transport_params={'client': oss_client}) as fout:
    with smart_open.open(uri, write_mode, encoding=encoding, transport_params=transport_params) as fout:
        fout.write(content)
    with smart_open.open(uri, read_mode, encoding=encoding, transport_params=transport_params) as fin:
        actual = fin.read()
    return actual

@_test_case
def test_oss_readwrite_text(benchmark, oss_bucket, uri):
    text = 'с гранатою в кармане, с чекою в руке'
    actual = benchmark(write_read, uri, text, 'w', 'r', 'utf-8', oss_bucket)
    assert actual == text
