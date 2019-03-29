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

``smart_open`` is a Python 2 & Python 3 library for **efficient streaming of very large files** from/to S3, HDFS, WebHDFS, HTTP, or local storage. It supports transparent, on-the-fly (de-)compression for a variety of different formats.

``smart_open`` is a drop-in replacement for Python's built-in ``open()``: it can do anything ``open`` can (100% compatible, falls back to native ``open`` wherever possible), plus lots of nifty extra stuff on top.

``smart_open`` is well-tested, well-documented, and has a simple, Pythonic API:


.. _doctools_before_examples:

.. code-block:: python

  >>> from smart_open import open
  >>>
  >>> # stream lines from an S3 object
  >>> for line in open('s3://commoncrawl/robots.txt'):
  ...    print(repr(line))
  ...    break
  'User-Agent: *\n'

  >>> # stream from/to compressed files, with transparent (de)compression:
  >>> for line in open('smart_open/tests/test_data/1984.txt.gz', encoding='utf-8'):
  ...    print(repr(line))
  'It was a bright cold day in April, and the clocks were striking thirteen.\n'
  'Winston Smith, his chin nuzzled into his breast in an effort to escape the vile\n'
  'wind, slipped quickly through the glass doors of Victory Mansions, though not\n'
  'quickly enough to prevent a swirl of gritty dust from entering along with him.\n'

  >>> # can use context managers too:
  >>> with open('smart_open/tests/test_data/1984.txt.gz') as fin:
  ...    with open('smart_open/tests/test_data/1984.txt.bz2', 'w') as fout:
  ...        for line in fin:
  ...           fout.write(line)

  >>> # can use any IOBase operations, like seek
  >>> with open('s3://commoncrawl/robots.txt', 'rb') as fin:
  ...     for line in fin:
  ...         print(repr(line.decode('utf-8')))
  ...         break
  ...     offset = fin.seek(0)  # seek to the beginning
  ...     print(fin.read(4))
  'User-Agent: *\n'
  b'User'

  >>> # stream from HTTP
  >>> for line in open('http://example.com/index.html'):
  ...     print(repr(line))
  ...     break
  '<!doctype html>\n'

Other examples of URLs that ``smart_open`` accepts::

    s3://my_bucket/my_key
    s3://my_key:my_secret@my_bucket/my_key
    s3://my_key:my_secret@my_server:my_port@my_bucket/my_key
    hdfs:///path/file
    hdfs://path/file
    webhdfs://host:port/path/file
    ./local/path/file
    ~/local/path/file
    local/path/file
    ./local/path/file.gz
    file:///home/user/file
    file:///home/user/file.bz2
    [ssh|scp|sftp]://username@host//path/file
    [ssh|scp|sftp]://username@host/path/file
    file:///home/user/file.xz

.. _doctools_after_examples:

For detailed API info, see the online help:

.. code-block:: python

    help('smart_open')

or click `here <https://github.com/RaRe-Technologies/smart_open/blob/master/help.txt>`__ to view the help in your browser.

More examples:

.. code-block:: python

    >>> import boto3
    >>>
    >>> # stream content *into* S3 (write mode) using a custom session
    >>> url = 's3://smart-open-py37-benchmark-results/test.txt'
    >>> lines = [b'first line\n', b'second line\n', b'third line\n']
    >>> transport_params = {'session': boto3.Session(profile_name='smart_open')}
    >>> with open(url, 'wb', transport_params=transport_params) as fout:
    ...     for line in lines:
    ...         bytes_written = fout.write(line)

.. code-block:: python

    # stream from HDFS
    for line in open('hdfs://user/hadoop/my_file.txt', encoding='utf8'):
        print(line)

    # stream from WebHDFS
    for line in open('webhdfs://host:port/user/hadoop/my_file.txt'):
        print(line)

    # stream content *into* HDFS (write mode):
    with open('hdfs://host:port/user/hadoop/my_file.txt', 'wb') as fout:
        fout.write(b'hello world')

    # stream content *into* WebHDFS (write mode):
    with open('webhdfs://host:port/user/hadoop/my_file.txt', 'wb') as fout:
        fout.write(b'hello world')

    # stream from a completely custom s3 server, like s3proxy:
    for line in open('s3u://user:secret@host:port@mybucket/mykey.txt'):
        print(line)

    # Stream to Digital Ocean Spaces bucket providing credentials from boto profile
    transport_params = {
        'session': boto3.Session(profile_name='digitalocean'),
        'resource_kwargs': {
            'endpoint_url': 'https://ams3.digitaloceanspaces.com',
        }
    }
    with open('s3://bucket/key.txt', 'wb', transport_params=transport_params) as fout:
        fout.write(b'here we stand')

Why?
----

Working with large S3 files using Amazon's default Python library, `boto <http://docs.pythonboto.org/en/latest/>`_ and `boto3 <https://boto3.readthedocs.io/en/latest/>`_, is a pain.
Its ``key.set_contents_from_string()`` and ``key.get_contents_as_string()`` methods only work for small files (loaded in RAM, no streaming).
There are nasty hidden gotchas when using ``boto``'s multipart upload functionality that is needed for large files, and a lot of boilerplate.

``smart_open`` shields you from that. It builds on boto3 but offers a cleaner, Pythonic API. The result is less code for you to write and fewer bugs to make.

Installation
------------
::

    pip install smart_open

Or, if you prefer to install from the `source tar.gz <http://pypi.python.org/pypi/smart_open>`_::

    python setup.py test  # run unit tests
    python setup.py install

To run the unit tests (optional), you'll also need to install `mock <https://pypi.python.org/pypi/mock>`_ , `moto <https://github.com/spulec/moto>`_ and `responses <https://github.com/getsentry/responses>`_ (``pip install mock moto responses``).
The tests are also run automatically with `Travis CI <https://travis-ci.org/RaRe-Technologies/smart_open>`_ on every commit push & pull request.

Supported Compression Formats
-----------------------------

``smart_open`` allows reading and writing gzip, bzip2 and xz files.
They are transparently handled over HTTP, S3, and other protocols, too, based on the extension of the file being opened.
You can easily add support for other file extensions and compression formats:

.. code-block:: python

    def _handle_lzma(file_obj, mode):
        import lzma
        return lzma.LZMAFile(filename=file_obj, mode=mode, format=lzma.FORMAT_ALONE)

    from smart_open import open, register_compressor
    register_compressor('.lzma', _handle_lzma)
    with open('file.lzma', ...) as fin:
        pass

Transport-specific Options
--------------------------

``smart_open`` supports a wide range of transport options out of the box, including:

- S3
- HTTP, HTTPS (read-only)
- SSH, SCP and SFTP
- WebHDFS

Each option involves setting up its own set of parameters.
For example, for accessing S3, you often need to set up authentication, like API keys or a profile name.
``smart_open``'s ``open`` function accepts a keyword argument ``transport_params`` which accepts additional parameters for the transport layer.
Here are some examples of using this parameter:

.. code-block:: python

  >>> import boto3
  >>> fin = open('s3://commoncrawl/robots.txt', transport_params=dict(session=boto3.Session()))
  >>> fin = open('s3://commoncrawl/robots.txt', transport_params=dict(buffer_size=1024))

For the full list of keyword arguments supported by each transport option, see the documentation:

.. code-block:: python

  help('smart_open.open')

S3 Credentials
--------------

``smart_open`` uses the ``boto3`` library to talk to S3.
``boto3`` has several `mechanisms <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html>`__ for determining the credentials to use.
By default, ``smart_open`` will defer to ``boto3`` and let the latter take care of the credentials.
There are several ways to override this behavior.

The first is to pass a ``boto3.Session`` object as a transport parameter to the ``open`` function.
You can customize the credentials when constructing the session.
``smart_open`` will then use the session when talking to S3.

.. code-block:: python

    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN,
    )
    fin = open('s3://bucket/key', transport_params=dict(session=session), ...)

Your second option is to specify the credentials within the S3 URL itself:

.. code-block:: python

    fin = open('s3://aws_access_key_id:aws_secret_access_key@bucket/key', ...)

*Important*: The two methods above are **mutually exclusive**. If you pass an AWS session *and* the URL contains credentials, ``smart_open`` will ignore the latter.

Iterating Over an S3 Bucket's Contents
--------------------------------------

Since going over all (or select) keys in an S3 bucket is a very common operation, there's also an extra function ``smart_open.s3_iter_bucket()`` that does this efficiently, **processing the bucket keys in parallel** (using multiprocessing):

.. code-block:: python

  >>> from smart_open import s3_iter_bucket
  >>> # get data corresponding to 2010 and later under "silo-open-data/annual/monthly_rain"
  >>> # we use workers=1 for reproducibility; you should use as many workers as you have cores
  >>> bucket = 'silo-open-data'
  >>> prefix = 'annual/monthly_rain/'
  >>> for key, content in s3_iter_bucket(bucket, prefix=prefix, accept_key=lambda key: '/201' in key, workers=1, key_limit=3):
  ...     print(key, round(len(content) / 2**20))
  annual/monthly_rain/2010.monthly_rain.nc 14
  annual/monthly_rain/2011.monthly_rain.nc 14
  annual/monthly_rain/2012.monthly_rain.nc 14

Comments, bug reports
---------------------

``smart_open`` lives on `Github <https://github.com/RaRe-Technologies/smart_open>`_. You can file
issues or pull requests there. Suggestions, pull requests and improvements welcome!

----------------

``smart_open`` is open source software released under the `MIT license <https://github.com/piskvorky/smart_open/blob/master/LICENSE>`_.
Copyright (c) 2015-now `Radim Řehůřek <https://radimrehurek.com>`_.
