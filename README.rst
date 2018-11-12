======================================================
smart_open — utils for streaming large files in Python
======================================================

|License|_ |Travis|_

.. |License| image:: https://img.shields.io/pypi/l/smart_open.svg
.. |Travis| image:: https://travis-ci.org/RaRe-Technologies/smart_open.svg?branch=master
.. _Travis: https://travis-ci.org/RaRe-Technologies/smart_open
.. _License: https://github.com/RaRe-Technologies/smart_open/blob/master/LICENSE

What?
=====

``smart_open`` is a Python 2 & Python 3 library for **efficient streaming of very large files** from/to S3, HDFS, WebHDFS, HTTP, or local (compressed) files. It's a drop-in replacement for Python's built-in ``open()``: it can do anything ``open`` can (100% compatible, falls back to native ``open`` wherever possible), plus lots of nifty extra stuff on top.

``smart_open`` is well-tested, well-documented and sports a simple, Pythonic API:

.. code-block:: python

  >>> from smart_open import smart_open

  >>> # stream lines from an S3 object
  >>> for line in smart_open('s3://mybucket/mykey.txt', 'rb'):
  ...    print(line.decode('utf8'))

  >>> # stream from/to compressed files, with transparent (de)compression:
  >>> for line in smart_open('./foo.txt.gz', encoding='utf8'):
  ...    print(line)

  >>> # can use context managers too:
  >>> with smart_open('/home/radim/foo.txt.bz2', 'wb') as fout:
  ...    fout.write(u"some content\n".encode('utf8'))

  >>> with smart_open('s3://mybucket/mykey.txt', 'rb') as fin:
  ...     for line in fin:
  ...         print(line.decode('utf8'))
  ...     fin.seek(0)  # seek to the beginning
  ...     b1000 = fin.read(1000)  # read 1000 bytes

  >>> # stream from HDFS
  >>> for line in smart_open('hdfs://user/hadoop/my_file.txt', encoding='utf8'):
  ...     print(line)

  >>> # stream from HTTP
  >>> for line in smart_open('http://example.com/index.html'):
  ...     print(line)

  >>> # stream from WebHDFS
  >>> for line in smart_open('webhdfs://host:port/user/hadoop/my_file.txt'):
  ...     print(line)

  >>> # stream content *into* S3 (write mode):
  >>> with smart_open('s3://mybucket/mykey.txt', 'wb') as fout:
  ...     for line in [b'first line\n', b'second line\n', b'third line\n']:
  ...          fout.write(line)

  >>> # stream content *into* HDFS (write mode):
  >>> with smart_open('hdfs://host:port/user/hadoop/my_file.txt', 'wb') as fout:
  ...     for line in [b'first line\n', b'second line\n', b'third line\n']:
  ...          fout.write(line)

  >>> # stream content *into* WebHDFS (write mode):
  >>> with smart_open('webhdfs://host:port/user/hadoop/my_file.txt', 'wb') as fout:
  ...     for line in [b'first line\n', b'second line\n', b'third line\n']:
  ...          fout.write(line)

  >>> # stream using a completely custom s3 server, like s3proxy:
  >>> for line in smart_open('s3u://user:secret@host:port@mybucket/mykey.txt', 'rb'):
  ...    print(line.decode('utf8'))

  >>> # you can also use a boto.s3.key.Key instance directly:
  >>> key = boto.connect_s3().get_bucket("my_bucket").get_key("my_key")
  >>> with smart_open(key, 'rb') as fin:
  ...     for line in fin:
  ...         print(line.decode('utf8'))
 
  >>> # Stream to Digital Ocean Spaces bucket providing credentials from boto profile
  >>> with smart_open('s3://bucket-for-experiments/file.txt', 'wb', endpoint_url='https://ams3.digitaloceanspaces.com', profile_name='digitalocean') as fout:
  ...     fout.write(b'here we stand')

Why?
----

Working with large S3 files using Amazon's default Python library, `boto <http://docs.pythonboto.org/en/latest/>`_ and `boto3 <https://boto3.readthedocs.io/en/latest/>`_, is a pain. Its ``key.set_contents_from_string()`` and ``key.get_contents_as_string()`` methods only work for small files (loaded in RAM, no streaming).
There are nasty hidden gotchas when using ``boto``'s multipart upload functionality that is needed for large files, and a lot of boilerplate.

``smart_open`` shields you from that. It builds on boto3 but offers a cleaner, Pythonic API. The result is less code for you to write and fewer bugs to make.

Installation
------------
::

    pip install smart_open

Or, if you prefer to install from the `source tar.gz <http://pypi.python.org/pypi/smart_open>`_::

    python setup.py test  # run unit tests
    python setup.py install

To run the unit tests (optional), you'll also need to install `mock <https://pypi.python.org/pypi/mock>`_ , `moto <https://github.com/spulec/moto>`_ and `responses <https://github.com/getsentry/responses>`_ (``pip install mock moto responses``). The tests are also run automatically with `Travis CI <https://travis-ci.org/RaRe-Technologies/smart_open>`_ on every commit push & pull request.

S3-Specific Options
-------------------

The S3 reader supports gzipped content transparently, as long as the key is obviously a gzipped file (e.g. ends with ".gz").

There are a few optional keyword arguments that are useful only for S3 access.

The **host** and **profile** arguments are both passed to `boto.s3_connect()` as keyword arguments:

.. code-block:: python

  >>> smart_open('s3://', host='s3.amazonaws.com')
  >>> smart_open('s3://', profile_name='my-profile')

The **s3_session** argument allows you to provide a custom `boto3.Session` instance for connecting to S3:

.. code-block:: python

  >>> smart_open('s3://', s3_session=boto3.Session())


The **s3_upload** argument accepts a dict of any parameters accepted by `initiate_multipart_upload <https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.ObjectSummary.initiate_multipart_upload/>`_:

.. code-block:: python

  >>> smart_open('s3://', s3_upload={ 'ServerSideEncryption': 'AES256' })

Since going over all (or select) keys in an S3 bucket is a very common operation,
there's also an extra method ``smart_open.s3_iter_bucket()`` that does this efficiently,
**processing the bucket keys in parallel** (using multiprocessing):

.. code-block:: python

  >>> from smart_open import smart_open, s3_iter_bucket
  >>> # get all JSON files under "mybucket/foo/"
  >>> bucket = boto.connect_s3().get_bucket('mybucket')
  >>> for key, content in s3_iter_bucket(bucket, prefix='foo/', accept_key=lambda key: key.endswith('.json')):
  ...     print(key, len(content))

For more info (S3 credentials in URI, minimum S3 part size...) and full method signatures, check out the API docs:

.. code-block:: python

  >>> import smart_open
  >>> help(smart_open.smart_open_lib)


Comments, bug reports
---------------------

``smart_open`` lives on `Github <https://github.com/RaRe-Technologies/smart_open>`_. You can file
issues or pull requests there. Suggestions, pull requests and improvements welcome!

----------------

``smart_open`` is open source software released under the `MIT license <https://github.com/piskvorky/smart_open/blob/master/LICENSE>`_.
Copyright (c) 2015-now `Radim Řehůřek <https://radimrehurek.com>`_.
