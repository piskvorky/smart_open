#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
# flake8: noqa


"""
Utilities for streaming from several file-like data storages: S3 / HDFS / standard
filesystem / compressed files..., using a single, Pythonic API.

The streaming makes heavy use of generators and pipes, to avoid loading
full file contents into memory, allowing work with arbitrarily large files.

The main methods are:

* `smart_open()`, which opens the given file for reading/writing
* `s3_iter_bucket()`, which goes over all keys in an S3 bucket in parallel

"""

import logging
import os
import subprocess
import sys
import requests
import io


IS_PY2 = (sys.version_info[0] == 2)

if IS_PY2:
    import cStringIO as StringIO
    import httplib
elif sys.version_info[0] == 3:
    import io as StringIO
    import http.client as httplib

from boto.compat import BytesIO, urlsplit, six
import boto.s3.connection
import boto.s3.key
from ssl import SSLError

logger = logging.getLogger(__name__)

# Multiprocessing is unavailable in App Engine (and possibly other sandboxes).
# The only method currently relying on it is s3_iter_bucket, which is instructed
# whether to use it by the MULTIPROCESSING flag.
MULTIPROCESSING = False
try:
    import multiprocessing.pool
    MULTIPROCESSING = True
except ImportError:
    logger.warning("multiprocessing could not be imported and won't be used")
    from itertools import imap

import gzip


S3_MIN_PART_SIZE = 50 * 1024**2  # minimum part size for S3 multipart uploads
WEBHDFS_MIN_PART_SIZE = 50 * 1024**2  # minimum part size for HDFS multipart uploads

S3_MODES = ("r", "rb", "w", "wb")
"""Allowed I/O modes for working with S3."""


def smart_open(uri, mode="rb", **kw):
    """
    Open the given S3 / HDFS / filesystem file pointed to by `uri` for reading or writing.

    The only supported modes for now are 'rb' (read, default) and 'wb' (replace & write).

    The reads/writes are memory efficient (streamed) and therefore suitable for
    arbitrarily large files.

    The `uri` can be either:

    1. a URI for the local filesystem (compressed ``.gz`` or ``.bz2`` files handled automatically):
       `./lines.txt`, `/home/joe/lines.txt.gz`, `file:///home/joe/lines.txt.bz2`
    2. a URI for HDFS: `hdfs:///some/path/lines.txt`
    3. a URI for Amazon's S3 (can also supply credentials inside the URI):
       `s3://my_bucket/lines.txt`, `s3://my_aws_key_id:key_secret@my_bucket/lines.txt`
    4. an instance of the boto.s3.key.Key class.

    Examples::

      >>> # stream lines from http; you can use context managers too:
      >>> with smart_open.smart_open('http://www.google.com') as fin:
      ...     for line in fin:
      ...         print line

      >>> # stream lines from S3; you can use context managers too:
      >>> with smart_open.smart_open('s3://mybucket/mykey.txt') as fin:
      ...     for line in fin:
      ...         print line

      >>> # you can also use a boto.s3.key.Key instance directly:
      >>> key = boto.connect_s3().get_bucket("my_bucket").get_key("my_key")
      >>> with smart_open.smart_open(key) as fin:
      ...     for line in fin:
      ...         print line

      >>> # stream line-by-line from an HDFS file
      >>> for line in smart_open.smart_open('hdfs:///user/hadoop/my_file.txt'):
      ...    print line

      >>> # stream content *into* S3:
      >>> with smart_open.smart_open('s3://mybucket/mykey.txt', 'wb') as fout:
      ...     for line in ['first line', 'second line', 'third line']:
      ...          fout.write(line + '\n')

      >>> # stream from/to (compressed) local files:
      >>> for line in smart_open.smart_open('/home/radim/my_file.txt'):
      ...    print line
      >>> for line in smart_open.smart_open('/home/radim/my_file.txt.gz'):
      ...    print line
      >>> with smart_open.smart_open('/home/radim/my_file.txt.gz', 'wb') as fout:
      ...    fout.write("hello world!\n")
      >>> with smart_open.smart_open('/home/radim/another.txt.bz2', 'wb') as fout:
      ...    fout.write("good bye!\n")
      >>> # stream from/to (compressed) local files with Expand ~ and ~user constructions:
      >>> for line in smart_open.smart_open('~/my_file.txt'):
      ...    print line
      >>> for line in smart_open.smart_open('my_file.txt'):
      ...    print line

    """

    # validate mode parameter
    if not isinstance(mode, six.string_types):
        raise TypeError('mode should be a string')

    if isinstance(uri, six.string_types):
        # this method just routes the request to classes handling the specific storage
        # schemes, depending on the URI protocol in `uri`
        parsed_uri = ParseUri(uri)

        if parsed_uri.scheme in ("file", ):
            # local files -- both read & write supported
            # compression, if any, is determined by the filename extension (.gz, .bz2)
            return file_smart_open(parsed_uri.uri_path, mode)
        elif parsed_uri.scheme in ("s3", "s3n"):
            return s3_open_uri(parsed_uri, mode, **kw)
        elif parsed_uri.scheme in ("hdfs", ):
            if mode in ('r', 'rb'):
                return HdfsOpenRead(parsed_uri, **kw)
            if mode in ('w', 'wb'):
                return HdfsOpenWrite(parsed_uri, **kw)
            else:
                raise NotImplementedError("file mode %s not supported for %r scheme", mode, parsed_uri.scheme)
        elif parsed_uri.scheme in ("webhdfs", ):
            if mode in ('r', 'rb'):
                return WebHdfsOpenRead(parsed_uri, **kw)
            elif mode in ('w', 'wb'):
                return WebHdfsOpenWrite(parsed_uri, **kw)
            else:
                raise NotImplementedError("file mode %s not supported for %r scheme", mode, parsed_uri.scheme)
        elif parsed_uri.scheme.startswith('http'):
            if mode in ('r', 'rb'):
                return HttpOpenRead(parsed_uri, **kw)
            else:
                raise NotImplementedError("file mode %s not supported for %r scheme", mode, parsed_uri.scheme)
        else:
            raise NotImplementedError("scheme %r is not supported", parsed_uri.scheme)
    elif isinstance(uri, boto.s3.key.Key):
        return s3_open_key(uri, mode, **kw)
    elif hasattr(uri, 'read'):
        # simply pass-through if already a file-like
        return uri
    else:
        raise TypeError('don\'t know how to handle uri %s' % repr(uri))


class ParseUri(object):
    """
    Parse the given URI.

    Supported URI schemes are "file", "s3", "s3n", "s3u" and "hdfs".

      * s3 and s3n are treated the same way.
      * s3u is s3 but without SSL.

    Valid URI examples::

      * s3://my_bucket/my_key
      * s3://my_key:my_secret@my_bucket/my_key
      * s3://my_key:my_secret@my_server:my_port@my_bucket/my_key
      * hdfs:///path/file
      * hdfs://path/file
      * webhdfs://host:port/path/file
      * ./local/path/file
      * ~/local/path/file
      * local/path/file
      * ./local/path/file.gz
      * file:///home/user/file
      * file:///home/user/file.bz2

    """
    def __init__(self, uri, default_scheme="file"):
        """
        Assume `default_scheme` if no scheme given in `uri`.

        """
        if os.name == 'nt':
            # urlsplit doesn't work on Windows -- it parses the drive as the scheme...
            if '://' not in uri:
                # no protocol given => assume a local file
                uri = 'file://' + uri
        parsed_uri = urlsplit(uri, allow_fragments=False)
        self.scheme = parsed_uri.scheme if parsed_uri.scheme else default_scheme

        if self.scheme == "hdfs":
            self.uri_path = parsed_uri.netloc + parsed_uri.path
            self.uri_path = "/" + self.uri_path.lstrip("/")

            if not self.uri_path:
                raise RuntimeError("invalid HDFS URI: %s" % uri)
        elif self.scheme == "webhdfs":
            self.uri_path = parsed_uri.netloc + "/webhdfs/v1" + parsed_uri.path
            if parsed_uri.query:
                self.uri_path += "?" + parsed_uri.query

            if not self.uri_path:
                raise RuntimeError("invalid WebHDFS URI: %s" % uri)
        elif self.scheme in ("s3", "s3n", "s3u"):
            self.bucket_id = (parsed_uri.netloc + parsed_uri.path).split('@')
            self.key_id = None
            self.port = 443
            self.host = boto.config.get('s3', 'host', 's3.amazonaws.com')
            self.ordinary_calling_format = False
            if len(self.bucket_id) == 1:
                # URI without credentials: s3://bucket/object
                self.bucket_id, self.key_id = self.bucket_id[0].split('/', 1)
                # "None" credentials are interpreted as "look for credentials in other locations" by boto
                self.access_id, self.access_secret = None, None
            elif len(self.bucket_id) == 2 and len(self.bucket_id[0].split(':')) == 2:
                # URI in full format: s3://key:secret@bucket/object
                # access key id: [A-Z0-9]{20}
                # secret access key: [A-Za-z0-9/+=]{40}
                acc, self.bucket_id = self.bucket_id
                self.access_id, self.access_secret = acc.split(':')
                self.bucket_id, self.key_id = self.bucket_id.split('/', 1)
            elif len(self.bucket_id) == 3 and len(self.bucket_id[0].split(':')) == 2:
                # or URI in extended format: s3://key:secret@server[:port]@bucket/object
                acc,  server, self.bucket_id = self.bucket_id
                self.access_id, self.access_secret = acc.split(':')
                self.bucket_id, self.key_id = self.bucket_id.split('/', 1)
                server = server.split(':')
                self.ordinary_calling_format = True
                self.host = server[0]
                if len(server) == 2:
                    self.port = int(server[1])
            else:
                # more than 2 '@' means invalid uri
                # Bucket names must be at least 3 and no more than 63 characters long.
                # Bucket names must be a series of one or more labels.
                # Adjacent labels are separated by a single period (.).
                # Bucket names can contain lowercase letters, numbers, and hyphens.
                # Each label must start and end with a lowercase letter or a number.
                raise RuntimeError("invalid S3 URI: %s" % uri)
        elif self.scheme == 'file':
            self.uri_path = parsed_uri.netloc + parsed_uri.path

            # '~/tmp' may be expanded to '/Users/username/tmp'
            self.uri_path = os.path.expanduser(self.uri_path)

            if not self.uri_path:
                raise RuntimeError("invalid file URI: %s" % uri)
        elif self.scheme.startswith('http'):
            self.uri_path = uri
        else:
            raise NotImplementedError("unknown URI scheme %r in %r" % (self.scheme, uri))


class HdfsOpenRead(object):
    """
    Implement streamed reader from HDFS, as an iterable & context manager.

    """
    def __init__(self, parsed_uri):
        if parsed_uri.scheme not in ("hdfs"):
            raise TypeError("can only process HDFS files")
        self.parsed_uri = parsed_uri

    def __iter__(self):
        hdfs = subprocess.Popen(["hdfs", "dfs", "-cat", self.parsed_uri.uri_path], stdout=subprocess.PIPE)
        return hdfs.stdout

    def read(self, size=None):
        raise NotImplementedError("read() not implemented yet")

    def seek(self, offset, whence=None):
        raise NotImplementedError("seek() not implemented yet")

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass


class HdfsOpenWrite(object):
    """
    Implement streamed writer from HDFS, as an iterable & context manager.

    """
    def __init__(self, parsed_uri):
        if parsed_uri.scheme not in ("hdfs"):
            raise TypeError("can only process HDFS files")
        self.parsed_uri = parsed_uri
        self.out_pipe = subprocess.Popen(["hdfs","dfs","-put","-f","-",self.parsed_uri.uri_path], stdin=subprocess.PIPE)

    def write(self, b):
        self.out_pipe.stdin.write(b)

    def seek(self, offset, whence=None):
        raise NotImplementedError("seek() not implemented yet")

    def __enter__(self):
        return self

    def close(self):
        self.out_pipe.stdin.close()

    def __exit__(self, type, value, traceback):
        self.close()


class WebHdfsOpenRead(object):
    """
    Implement streamed reader from WebHDFS, as an iterable & context manager.
    NOTE: it does not support kerberos authentication yet

    """
    def __init__(self, parsed_uri):
        if parsed_uri.scheme not in ("webhdfs"):
            raise TypeError("can only process WebHDFS files")
        self.parsed_uri = parsed_uri
        self.offset = 0

    def __iter__(self):
        payload = {"op": "OPEN"}
        response = requests.get("http://" + self.parsed_uri.uri_path, params=payload, stream=True)
        return response.iter_lines()

    def read(self, size=None):
        """
        Read the specific number of bytes from the file

        Note read() and line iteration (`for line in self: ...`) each have their
        own file position, so they are independent. Doing a `read` will not affect
        the line iteration, and vice versa.
        """
        if not size or size < 0:
            payload = {"op": "OPEN", "offset": self.offset}
            self.offset = 0
        else:
            payload = {"op": "OPEN", "offset": self.offset, "length": size}
            self.offset = self.offset + size
        response = requests.get("http://" + self.parsed_uri.uri_path, params=payload, stream=True)
        return response.content

    def seek(self, offset, whence=0):
        """
        Seek to the specified position.

        Only seeking to the beginning (offset=0) supported for now.

        """
        if whence == 0 and offset == 0:
            self.offset = 0
        elif whence == 0:
            self.offset = offset
        else:
            raise NotImplementedError("operations with whence not implemented yet")

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass


def make_closing(base, **attrs):
    """
    Add support for `with Base(attrs) as fout:` to the base class if it's missing.
    The base class' `close()` method will be called on context exit, to always close the file properly.

    This is needed for gzip.GzipFile, bz2.BZ2File etc in older Pythons (<=2.6), which otherwise
    raise "AttributeError: GzipFile instance has no attribute '__exit__'".

    """
    if not hasattr(base, '__enter__'):
        attrs['__enter__'] = lambda self: self
    if not hasattr(base, '__exit__'):
        attrs['__exit__'] = lambda self, type, value, traceback: self.close()
    return type('Closing' + base.__name__, (base, object), attrs)


def compression_wrapper(file_obj, filename, mode):
    """
    This function will wrap the file_obj with an appropriate
    [de]compression mechanism based on the extension of the filename.

    file_obj must either be a filehandle object, or a class which behaves
        like one.

    If the filename extension isn't recognized, will simply return the original
    file_obj.
    """
    _, ext = os.path.splitext(filename)
    if ext == '.bz2':
        if IS_PY2:
            from bz2file import BZ2File
        else:
            from bz2 import BZ2File
        return make_closing(BZ2File)(file_obj, mode)

    elif ext == '.gz':
        from gzip import GzipFile
        return make_closing(GzipFile)(fileobj=file_obj, mode=mode)

    else:
        return file_obj


def file_smart_open(fname, mode='rb'):
    """
    Stream from/to local filesystem, transparently (de)compressing gzip and bz2
    files if necessary.

    """
    return compression_wrapper(open(fname, mode), fname, mode)


class HttpReadStream(object):
    """
    Implement streamed reader from a web site, as an iterable & context manager.
    Supports Kerberos and Basic HTTP authentication.

    As long as you don't mix different access patterns (readline vs readlines vs
    read(n) vs read() vs iteration) this will load efficiently in memory.

    """
    def __init__(self, url, mode='r', kerberos=False, user=None, password=None):
        """
        If Kerberos is True, will attempt to use the local Kerberos credentials.
        Otherwise, will try to use "basic" HTTP authentication via username/password.

        If none of those are set, will connect unauthenticated.
        """
        if kerberos:
            import requests_kerberos
            auth = requests_kerberos.HTTPKerberosAuth()
        elif user is not None and password is not None:
            auth = (user, password)
        else:
            auth = None
        
        self.response = requests.get(url, auth=auth, stream=True)

        if not self.response.ok:
            self.response.raise_for_status()

        self.mode = mode
        self._read_buffer = None
        self._read_iter = None
        self._readline_iter = None

    def __iter__(self):
        return self.response.iter_lines()

    def binary_content(self):
        """Return the content of the request as bytes."""
        return self.response.content

    def readline(self):
        """
        Mimics the readline call to a filehandle object.
        """
        if self._readline_iter is None:
            self._readline_iter = self.response.iter_lines()

        try:
            return next(self._readline_iter)
        except StopIteration:
            # When readline runs out of data, it just returns an empty string
            return ''

    def readlines(self):
        """
        Mimics the readlines call to a filehandle object.
        """
        return list(self.response.iter_lines())

    def seek(self):
        raise NotImplementedError('seek() is not implemented')

    def read(self, size=None):
        """
        Mimics the read call to a filehandle object.
        """
        if size is None:
            return self.response.content
        else:
            if self._read_iter is None:
                self._read_iter = self.response.iter_content(size)
                self._read_buffer = next(self._read_iter)
            
            while len(self._read_buffer) < size:
                try:
                    self._read_buffer += next(self._read_iter)
                except StopIteration:
                    # Oops, ran out of data early.
                    retval = self._read_buffer
                    self._read_buffer = ''
                    if len(retval) == 0:
                        # When read runs out of data, it just returns empty
                        return ''
                    else:
                        return retval
            
            # If we got here, it means we have enough data in the buffer
            # to return to the caller.
            retval = self._read_buffer[:size]
            self._read_buffer = self._read_buffer[size:]
            return retval

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        self.response.close()


def HttpOpenRead(parsed_uri, mode='r', **kwargs):
    if parsed_uri.scheme not in ('http', 'https'):
        raise TypeError("can only process http/https urls")
    if mode not in ('r', 'rb'):
        raise NotImplementedError('Streaming write to http not supported')

    url = parsed_uri.uri_path

    response = HttpReadStream(url, **kwargs)

    fname = urlsplit(url, allow_fragments=False).path.split('/')[-1]

    if fname.endswith('.gz'):
        #  Gzip needs a seek-able filehandle, so we need to buffer it.
        buffer = make_closing(io.BytesIO)(response.binary_content())
        return compression_wrapper(buffer, fname, mode)
    else:
        return compression_wrapper(response, fname, mode)


class WebHdfsOpenWrite(object):
    """
    Context manager for writing into webhdfs files

    """
    def __init__(self, parsed_uri, min_part_size=WEBHDFS_MIN_PART_SIZE):
        if parsed_uri.scheme not in ("webhdfs"):
            raise TypeError("can only process WebHDFS files")
        self.parsed_uri = parsed_uri
        self.closed = False
        self.min_part_size = min_part_size
        # creating empty file first
        payload = {"op": "CREATE", "overwrite": True}
        init_response = requests.put("http://" + self.parsed_uri.uri_path, params=payload, allow_redirects=False)
        if not init_response.status_code == httplib.TEMPORARY_REDIRECT:
            raise WebHdfsException(str(init_response.status_code) + "\n" + init_response.content)
        uri = init_response.headers['location']
        response = requests.put(uri, data="", headers={'content-type': 'application/octet-stream'})
        if not response.status_code == httplib.CREATED:
            raise WebHdfsException(str(response.status_code) + "\n" + response.content)
        self.lines = []
        self.parts = 0
        self.chunk_bytes = 0
        self.total_size = 0

    def upload(self, data):
        payload = {"op": "APPEND"}
        init_response = requests.post("http://" + self.parsed_uri.uri_path, params=payload, allow_redirects=False)
        if not init_response.status_code == httplib.TEMPORARY_REDIRECT:
            raise WebHdfsException(str(init_response.status_code) + "\n" + init_response.content)
        uri = init_response.headers['location']
        response = requests.post(uri, data=data, headers={'content-type': 'application/octet-stream'})
        if not response.status_code == httplib.OK:
            raise WebHdfsException(str(response.status_code) + "\n" + response.content)

    def write(self, b):
        """
        Write the given bytes (binary string) into the WebHDFS file from constructor.

        """
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if isinstance(b, six.text_type):
            # not part of API: also accept unicode => encode it as utf8
            b = b.encode('utf8')

        if not isinstance(b, six.binary_type):
            raise TypeError("input must be a binary string")

        self.lines.append(b)
        self.chunk_bytes += len(b)
        self.total_size += len(b)

        if self.chunk_bytes >= self.min_part_size:
            buff = b"".join(self.lines)
            logger.info("uploading part #%i, %i bytes (total %.3fGB)" % (self.parts, len(buff), self.total_size / 1024.0 ** 3))
            self.upload(buff)
            logger.debug("upload of part #%i finished" % self.parts)
            self.parts += 1
            self.lines, self.chunk_bytes = [], 0

    def seek(self, offset, whence=None):
        raise NotImplementedError("seek() not implemented yet")

    def close(self):
        buff = b"".join(self.lines)
        if buff:
            logger.info("uploading last part #%i, %i bytes (total %.3fGB)" % (self.parts, len(buff), self.total_size / 1024.0 ** 3))
            self.upload(buff)
            logger.debug("upload of last part #%i finished" % self.parts)
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


def s3_iter_bucket_process_key_with_kwargs(kwargs):
    return s3_iter_bucket_process_key(**kwargs)


def s3_iter_bucket_process_key(key, retries=3):
    """
    Conceptually part of `s3_iter_bucket`, but must remain top-level method because
    of pickling visibility.

    """
    # Sometimes, https://github.com/boto/boto/issues/2409 can happen because of network issues on either side.
    # Retry up to 3 times to ensure its not a transient issue.
    for x in range(0, retries + 1):
        try:
            return key, key.get_contents_as_string()
        except SSLError:
            # Actually fail on last pass through the loop
            if x == retries:
                raise
            # Otherwise, try again, as this might be a transient timeout
            pass


def s3_iter_bucket(bucket, prefix='', accept_key=lambda key: True, key_limit=None, workers=16, retries=3):
    """
    Iterate and download all S3 files under `bucket/prefix`, yielding out
    `(key, key content)` 2-tuples (generator).

    `accept_key` is a function that accepts a key name (unicode string) and
    returns True/False, signalling whether the given key should be downloaded out or
    not (default: accept all keys).

    If `key_limit` is given, stop after yielding out that many results.

    The keys are processed in parallel, using `workers` processes (default: 16),
    to speed up downloads greatly. If multiprocessing is not available, thus
    MULTIPROCESSING is False, this parameter will be ignored.

    Example::

      >>> mybucket = boto.connect_s3().get_bucket('mybucket')

      >>> # get all JSON files under "mybucket/foo/"
      >>> for key, content in s3_iter_bucket(mybucket, prefix='foo/', accept_key=lambda key: key.endswith('.json')):
      ...     print key, len(content)

      >>> # limit to 10k files, using 32 parallel workers (default is 16)
      >>> for key, content in s3_iter_bucket(mybucket, key_limit=10000, workers=32):
      ...     print key, len(content)

    """
    total_size, key_no = 0, -1
    keys = ({'key': key, 'retries': retries} for key in bucket.list(prefix=prefix) if accept_key(key.name))

    if MULTIPROCESSING:
        logger.info("iterating over keys from %s with %i workers" % (bucket, workers))
        pool = multiprocessing.pool.Pool(processes=workers)
        iterator = pool.imap_unordered(s3_iter_bucket_process_key_with_kwargs, keys)
    else:
        logger.info("iterating over keys from %s without multiprocessing" % bucket)
        iterator = imap(s3_iter_bucket_process_key_with_kwargs, keys)

    for key_no, (key, content) in enumerate(iterator):
        if key_no % 1000 == 0:
            logger.info("yielding key #%i: %s, size %i (total %.1fMB)" %
                (key_no, key, len(content), total_size / 1024.0 ** 2))

        yield key, content
        key.close()
        total_size += len(content)

        if key_limit is not None and key_no + 1 >= key_limit:
            # we were asked to output only a limited number of keys => we're done
            break

    if MULTIPROCESSING:
        pool.terminate()

    logger.info("processed %i keys, total size %i" % (key_no + 1, total_size))


def s3_check_key(key):
    """Raise TypeError if key is not an S3 key."""
    has_bucket = hasattr(key, "bucket")
    has_name = hasattr(key, "name")
    has_read = hasattr(key, "read")
    has_close = hasattr(key, "close")
    logger.debug("key: %r", key)
    logger.debug(
        "has_bucket: %r has_name: %r has_read: %r has_close: %r",
        has_bucket, has_name, has_read, has_close
    )
    if not (has_bucket and has_name and has_read and has_close):
        raise TypeError("can only process S3 keys")


class S3BufferedInputBase(io.BufferedIOBase):
    """Reads bytes from S3.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(self, key):
        s3_check_key(key)

        self.key = key
        self.unused_buffer = b''
        self.finished = False
        self.current_pos = 0

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        """Flush and close this stream."""
        logger.debug("close: called")
        self.finished = True
        self.key.close()

    def readable(self):
        """Return True if the stream can be read from."""
        return True

    def seekable(self):
        """If False, seek(), tell() and truncate() will raise IOError.
        
        We offer only limited seek support, and no truncate support."""
        return True

    def seek(self, offset, whence=0):
        """Seek to the specified position.

        Only seeking to to the beginning of the stream is supported."""
        if offset or whence:
            raise IOError("can only seek to the beginning of the stream")
        self.key.close(fast=True)
        self.unused_buffer = b""
        self.finished = False
        self.current_pos = 0
        return self.current_pos

    def tell(self):
        """Return the current stream position."""
        return self.current_pos

    def truncate(self, size=None):
        raise IOError("truncate() not supported")

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")

    def readall(self):
        """Read and return all the bytes from the stream until EOF."""
        #
        # This method is here because boto.s3.Key.read() reads the entire
        # file, which isn't expected behavior.
        #
        # https://github.com/boto/boto/issues/3311
        #
        logger.debug("readall(): called")
        while not self.finished:
            raw = self.key.read(io.DEFAULT_BUFFER_SIZE)
            if len(raw) > 0:
                self.unused_buffer += raw
            else:
                self.finished = True
        result = self.unused_buffer
        self.unused_buffer = b""
        self.current_pos += len(result)
        return result

    def read(self, size=-1):
        """Read up to size bytes from the object and return them."""
        if size <= 0:
            return self.readall()

        #
        # Return unused data first
        #
        if len(self.unused_buffer) >= size:
            return self.__read_from_unused_buffer(size)

        #
        # If the stream is finished, return what we have.
        #
        if self.key.closed or self.finished:
            self.finished = True
            return self.__read_from_unused_buffer(size)

        #
        # Fill our buffer to the required size.
        #
        while len(self.unused_buffer) < size and not self.finished:
            raw = self.key.read(io.DEFAULT_BUFFER_SIZE)
            if len(raw):
                self.unused_buffer += raw
            else:
                self.finished = True

        return self.__read_from_unused_buffer(size)

    def read1(self, n=-1):
        """Read and return up to n bytes using only one low-level operation.

        Use most one call to the underlying raw stream’s read() method."""
        raise io.UnsupportedOperation("read1 is not supported")

    def readinto(self, b):
        """Read up to len(b) bytes into b, and return the number of bytes
        read."""
        data = self.read(len(b))
        if not data:
            return 0
        b[:len(data)] = data
        return len(data)

    def terminate(self):
        """Do nothing."""

    #
    # Internal methods.
    #
    def __read_from_unused_buffer(self, size):
        """Remove at most size bytes from our buffer and return them."""
        part = self.unused_buffer[:size]
        self.unused_buffer = self.unused_buffer[size:]
        self.current_pos += len(part)
        return part


class S3BufferedOutputBase(io.BufferedIOBase):
    """Writes bytes to S3.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(self, key, min_part_size=S3_MIN_PART_SIZE):
        s3_check_key(key)

        if min_part_size < 5 * 1024 ** 2:
            logger.warning("S3 requires minimum part size >= 5MB; \
multipart upload may fail")

        self.key = key
        self.min_part_size = min_part_size

        self.mp = self.key.bucket.initiate_multipart_upload(self.key)

        # initialize stats
        self.buf = io.BytesIO()
        self.total_bytes = 0
        self.total_parts = 0

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        logger.debug("closing")
        if self.buf.tell():
            self.__upload_next_part()

        if self.total_bytes:
            self.mp.complete_upload()
            logger.debug("completed multipart upload")
        elif self.mp:
            #
            # AWS complains with "The XML you provided was not well-formed or
            # did not validate against our published schema" when the input is
            # completely empty => abort the upload, no file created.
            #
            logger.info("empty input, ignoring multipart upload")
            assert self.mp, "no multipart upload in progress"
            self.key.bucket.cancel_multipart_upload(
                self.mp.key_name, self.mp.id
            )
            #
            # So, instead, create an empty file like this
            #
            logger.info("setting an empty value for the key")
            self.key.set_contents_from_string(b'')

            logger.debug("wrote empty file")

        logger.debug("successfully closed")

    def writable(self):
        """Return True if the stream supports writing."""
        return True

    def tell(self):
        """Return the current stream position."""
        return self.total_bytes

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")

    def write(self, b):
        """Write the given bytes (binary string) to the S3 file.

        There's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away."""
        if isinstance(b, six.text_type):
            #
            # not part of API: also accept unicode => encode it as utf8
            #
            logger.warning("implicitly encoding unicode to UTF-8 byte string")
            b = b.encode('utf8')

        if not isinstance(b, six.binary_type):
            raise TypeError("input must be a binary string, got: %r", b)

        logger.debug("writing %r bytes to %r", len(b), self.buf)

        self.buf.write(b)
        self.total_bytes += len(b)

        if self.buf.tell() >= self.min_part_size:
            self.__upload_next_part()

        return len(b)

    def terminate(self):
        """Cancel the underlying multipart upload."""
        assert self.mp, "no multipart upload in progress"
        self.key.bucket.cancel_multipart_upload(self.mp.key_name, self.mp.id)
        self.mp = None

    #
    # Internal methods.
    #
    def __upload_next_part(self):
        part_num = self.total_parts + 1
        logger.info(
            "uploading part #%i, %i bytes (total %.3fGB)" % (
                part_num, self.buf.tell(), self.total_bytes / 1024.0 ** 3
            )
        )
        self.buf.seek(0)
        self.mp.upload_part_from_file(self.buf, part_num=part_num)
        logger.debug("upload of part #%i finished" % part_num)

        self.total_parts += 1
        self.buf = io.BytesIO()

    def __exit__(self, exc_type, value, traceback):
        if exc_type is not None:
            self.terminate()
        else:
            self.close()


def s3_open_key(key, mode, **kwargs):
    s3_check_key(key)

    if mode not in S3_MODES:
        raise NotImplementedError("unknown mode: %r not in %r", mode, S3_MODES)

    buffer_size = kwargs.pop("buffer_size", io.DEFAULT_BUFFER_SIZE)
    encoding = kwargs.pop("encoding", "utf-8")
    errors = kwargs.pop("errors", None)
    newline = kwargs.pop("newline", None)
    line_buffering = kwargs.pop("line_buffering", False)
    ignore_extension = kwargs.pop("ignore_extension", False)
    s3_min_part_size = kwargs.pop("s3_min_part_size", S3_MIN_PART_SIZE)

    #
    # TODO: is it really worth tightly coupling with gzip here?
    #
    # Without coupling:
    #
    #   with open("s3://bucket/key.tar.gz") as fileobj:
    #       gz = gzip.GzipFile(fileobj=fileobj, **kwargs)
    #       file_content = gz.read()
    #
    # With coupling:
    #
    #   with open("s3://bucket/key.tar.gz", **kwargs) as gz:
    #       file_content = gz.read()
    #
    # We're saving people a line's worth of work, at the expense of
    # having to:
    #
    #   1) estimate the compression from the file extension,
    #   2) integrating a separate library
    #   3) marshalling args/kwargs -- these will be different for each
    #   compressor we support, and
    #   4) testing/maintaining the integrated code.
    #
    # Is the single line we save really worth it?
    # Can we do it at a higher level in the library, or even outside of the
    # library altogether?
    #
    if mode in ["r", "rb"] and is_gzip(key.name) and not ignore_extension:
        fileobj = S3BufferedInputBase(key)
        return gzip.GzipFile(fileobj=fileobj, mode="rb")
    elif mode in ["w", "wb"] and is_gzip(key.name) and not ignore_extension:
        fileobj = S3BufferedOutputBase(key)
        return gzip.GzipFile(fileobj=fileobj, mode="wb")
    elif mode == "rb":
        return S3BufferedInputBase(key)
    elif mode == "r":
        return io.TextIOWrapper(
            S3BufferedInputBase(key), encoding=encoding, errors=errors,
            newline=newline, line_buffering=line_buffering
        )
    elif mode == "wb":
        return S3BufferedOutputBase(key, min_part_size=s3_min_part_size)
    elif mode == "w":
        return io.TextIOWrapper(
            S3BufferedOutputBase(key), encoding=encoding,
            errors=errors, newline=newline, line_buffering=line_buffering
        )


def s3_open_uri(parsed_uri, mode, **kwargs):
    """Open an S3 connection to the resource specified in parsed_uri."""
    if mode not in S3_MODES:
        raise NotImplementedError("unknown mode: %r not in %r", mode, S3_MODES)

    logger.debug(
        "bucket_id: %r key_id: %r", parsed_uri.bucket_id, parsed_uri.key_id
    )

    #
    # Get an S3 host. It is required for sigv4 operations.
    #
    host = kwargs.pop('host', None)
    if not host:
        host = boto.config.get('s3', 'host', 's3.amazonaws.com')

    #
    # For credential order of precedence see
    # http://boto.cloudhackers.com/en/latest/boto_config_tut.html#credentials
    #
    s3_connection = boto.connect_s3(
        aws_access_key_id=parsed_uri.access_id,
        host=host,
        aws_secret_access_key=parsed_uri.access_secret,
        profile_name=kwargs.pop('profile_name', None)
    )

    bucket = s3_connection.get_bucket(parsed_uri.bucket_id)
    key = bucket.get_key(parsed_uri.key_id, validate=mode in ["r", "rb"])
    logger.debug("bucket: %r key: %r", bucket, key)

    if key is None:
        raise KeyError(parsed_uri.key_id)

    return s3_open_key(key, mode, **kwargs)


def is_gzip(name):
    """Return True if the name indicates that the file is compressed with
    gzip."""
    return name.endswith(".gz")


class WebHdfsException(Exception):
    def __init__(self, msg=str()):
        self.msg = msg
        super(WebHdfsException, self).__init__(self.msg)
