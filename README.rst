=============================================
smart_open -- utils for streaming large files
=============================================

|License|_ |Travis|_


.. |License| image:: https://img.shields.io/pypi/l/smart_open.svg
.. |Travis| image:: https://travis-ci.org/RaRe-Technologies/smart_open.svg?branch=master
.. _Travis: https://travis-ci.org/RaRe-Technologies/smart_open
.. _License: https://github.com/RaRe-Technologies/smart_open/blob/master/LICENSE

What?
=====

``smart_open`` is a Python 2 & Python 3 library for **efficient streaming of very large files** from/to S3, HDFS, WebHDFS, HTTP, or local (compressed) files.
It is well tested (using `moto <https://github.com/spulec/moto>`_), well documented and sports a simple, Pythonic API:

.. code-block:: python

  >>> # stream from/to compressed files, with transparent (de)compression:
  >>> for line in smart_open.smart_open('./foo.txt.gz', 'rb'):
  ...    print(line.decode('utf8'))

  >>> with smart_open.smart_open('/home/radim/foo.txt.bz2', 'wb') as fout:
  ...    fout.write(b"some content\n")

  >>> # stream lines from an S3 object
  >>> for line in smart_open.smart_open('s3://mybucket/mykey.txt'):
  ...    print(line.decode('utf8'))

  >>> # using a completely custom s3 server, like s3proxy:
  >>> for line in smart_open.smart_open('s3u://user:secret@host:port@mybucket/mykey.txt', 'rb'):
  ...    print(line.decode('utf8'))

  >>> # you can also use a boto.s3.key.Key instance directly:
  >>> key = boto.connect_s3().get_bucket("my_bucket").get_key("my_key")
  >>> with smart_open.smart_open(key, 'rb') as fin:
  ...     for line in fin:
  ...         print(line.decode('utf8'))

  >>> # can use context managers too:
  >>> with smart_open.smart_open('s3://mybucket/mykey.txt', 'rb') as fin:
  ...     for line in fin:
  ...         print(line.decode('utf8'))
  ...     fin.seek(0)  # seek back to the beginning
  ...     x = fin.read(1000)  # read 1000 bytes

  >>> # stream from HDFS
  >>> for line in smart_open.smart_open('hdfs://user/hadoop/my_file.txt', 'rb'):
  ...     print(line.decode('utf8'))

  >>> # stream from HTTP
  >>> for line in smart_open.smart_open('http://example.com/index.html', 'rb'):
  ...     print(line.decode('utf8'))

  >>> # stream from WebHDFS
  >>> for line in smart_open.smart_open('webhdfs://host:port/user/hadoop/my_file.txt', 'rb'):
  ...     print(line.decode('utf8'))

  >>> # stream content *into* S3 (write mode):
  >>> with smart_open.smart_open('s3://mybucket/mykey.txt', 'wb') as fout:
  ...     for line in ['first line\n', 'second line\n', 'third line\n']:
  ...          fout.write(line.encode('utf8'))

  >>> # stream content *into* HDFS (write mode):
  >>> with smart_open.smart_open('hdfs://host:port/user/hadoop/my_file.txt', 'wb') as fout:
  ...     for line in ['first line\n', 'second line'n', 'third line'n']:
  ...          fout.write(line.encode('utf8'))

  >>> # stream content *into* WebHDFS (write mode):
  >>> with smart_open.smart_open('webhdfs://host:port/user/hadoop/my_file.txt', 'wb') as fout:
  ...     for line in ['first line\n', 'second line\n', 'third line\n']:
  ...          fout.write(line.encode('utf8'))

Since going over all (or select) keys in an S3 bucket is a very common operation,
there's also an extra method ``smart_open.s3_iter_bucket()`` that does this efficiently,
**processing the bucket keys in parallel** (using multiprocessing):

.. code-block:: python

  >>> # get all JSON files under "mybucket/foo/"
  >>> bucket = boto.connect_s3().get_bucket('mybucket')
  >>> for key, content in s3_iter_bucket(bucket, prefix='foo/', accept_key=lambda key: key.endswith('.json')):
  ...     print(key, len(content))

For more info (S3 credentials in URI, minimum S3 part size...) and full method signatures, check out the API docs:

.. code-block:: python

  >>> import smart_open
  >>> help(smart_open.smart_open_lib)

S3-Specific Options
-------------------

There are a few optional keyword arguments that are useful only for S3 access.

The **host** and **profile** arguments are both passed to `boto.s3_connect()` as keyword arguments:

.. code-block:: python

  >>> smart_open.smart_open('s3://', host='s3.amazonaws.com')
  >>> smart_open.smart_open('s3://', profile_name='my-profile')


The **s3_session** argument allows you to provide a custom `boto3.Session` instance for connecting to S3:

.. code-block:: python

  >>> smart_open.smart_open('s3://', s3_session=boto3.Session())


The **s3_upload** argument accepts a dict of any parameters accepted by `initiate_multipart_upload <https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.ObjectSummary.initiate_multipart_upload/>`_:

.. code-block:: python

  >>> smart_open.smart_open('s3://', s3_upload={ 'ServerSideEncryption': 'AES256' })


The S3 reader supports gzipped content, as long as the key is obviously a gzipped file (e.g. ends with ".gz").

Why?
----

Working with large S3 files using Amazon's default Python library, `boto <http://docs.pythonboto.org/en/latest/>`_, is a pain. Its ``key.set_contents_from_string()`` and ``key.get_contents_as_string()`` methods only work for small files (loaded in RAM, no streaming).
There are nasty hidden gotchas when using ``boto``'s multipart upload functionality, and a lot of boilerplate.

``smart_open`` shields you from that. It builds on boto but offers a cleaner API. The result is less code for you to write and fewer bugs to make.

Installation
------------
::

    pip install smart_open

Or, if you prefer to install from the `source tar.gz <http://pypi.python.org/pypi/smart_open>`_::

    python setup.py test  # run unit tests
    python setup.py install

To run the unit tests (optional), you'll also need to install `mock <https://pypi.python.org/pypi/mock>`_ , `moto <https://github.com/spulec/moto>`_ and `responses <https://github.com/getsentry/responses>` (``pip install mock moto responses``). The tests are also run automatically with `Travis CI <https://travis-ci.org/RaRe-Technologies/smart_open>`_ on every commit push & pull request.

Todo
----

``smart_open`` is an ongoing effort. Suggestions, pull request and improvements welcome!

Comments, bug reports
---------------------

``smart_open`` lives on `Github <https://github.com/RaRe-Technologies/smart_open>`_. You can file
issues or pull requests there.

----------------

``smart_open`` is open source software released under the `MIT license <https://github.com/piskvorky/smart_open/blob/master/LICENSE>`_.
Copyright (c) 2015-now `Radim Řehůřek <https://radimrehurek.com>`_.
