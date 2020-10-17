======================================================
smart_open — utils for streaming large files in Python
======================================================


|License|_ |Travis|_ |Appveyor|_ |Coveralls|_ |Downloads|_

.. |License| image:: https://img.shields.io/pypi/l/smart_open.svg
.. |Travis| image:: https://travis-ci.org/RaRe-Technologies/smart_open.svg?branch=develop
.. |Appveyor| image:: https://ci.appveyor.com/api/projects/status/47py3gwhmt9tr74w/branch/develop?svg=true
.. |Coveralls| image:: https://coveralls.io/repos/github/RaRe-Technologies/smart_open/badge.svg?branch=develop
.. |Downloads| image:: https://pepy.tech/badge/smart-open/month
.. _License: https://github.com/RaRe-Technologies/smart_open/blob/master/LICENSE
.. _Travis: https://travis-ci.org/RaRe-Technologies/smart_open
.. _Appveyor: https://ci.appveyor.com/project/piskvorky/smart-open
.. _Coveralls: https://coveralls.io/github/RaRe-Technologies/smart_open?branch=HEAD
.. _Downloads: https://pypi.org/project/smart-open/


What?
=====

``smart_open`` is a Python 3 library for **efficient streaming of very large files** from/to storages such as S3, GCS, Azure Blob Storage, HDFS, WebHDFS, HTTP, HTTPS, SFTP, or local filesystem. It supports transparent, on-the-fly (de-)compression for a variety of different formats.

``smart_open`` is a drop-in replacement for Python's built-in ``open()``: it can do anything ``open`` can (100% compatible, falls back to native ``open`` wherever possible), plus lots of nifty extra stuff on top.

**Python 2.7 is no longer supported. If you need Python 2.7, please use** `smart_open 1.10.1 <https://github.com/RaRe-Technologies/smart_open/releases/tag/1.10.0>`_, **the last version to support Python 2.**

Why?
====

Working with large remote files, for example using Amazon's `boto3 <https://boto3.amazonaws.com/v1/documentation/api/latest/index.html>`_ Python library, is a pain.
``boto3``'s ``Object.upload_fileobj()`` and ``Object.download_fileobj()`` methods require gotcha-prone boilerplate to use successfully, such as constructing file-like object wrappers.
``smart_open`` shields you from that. It builds on boto3 and other remote storage libraries, but offers a **clean unified Pythonic API**. The result is less code for you to write and fewer bugs to make.


How?
=====

``smart_open`` is well-tested, well-documented, and has a simple Pythonic API:


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

.. _doctools_after_examples:

Other examples of URLs that ``smart_open`` accepts::

    s3://my_bucket/my_key
    s3://my_key:my_secret@my_bucket/my_key
    s3://my_key:my_secret@my_server:my_port@my_bucket/my_key
    gs://my_bucket/my_blob
    azure://my_bucket/my_blob
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
    [ssh|scp|sftp]://username:password@host/path/file


Documentation
=============

Installation
------------
::

    pip install smart_open  // Install with no cloud dependencies
    pip install smart_open[s3] // Install S3 deps
    pip install smart_open[gcp] // Install GCP deps
    pip install smart_open[azure] // Install Azure deps
    pip install smart_open[all] // Installs all cloud dependencies

Or, if you prefer to install from the `source tar.gz <http://pypi.python.org/pypi/smart_open>`_::

    python setup.py test  # run unit tests
    python setup.py install

To run the unit tests (optional), you'll also need to install some other dependencies: see setup.py or run `pip install .[test]`.
The tests are also run automatically with `Travis CI <https://travis-ci.org/RaRe-Technologies/smart_open>`_ on every commit push & pull request.

If you're upgrading from ``smart_open`` versions 1.8.0 and below, please check out the `Migration Guide <MIGRATING_FROM_OLDER_VERSIONS.rst>`_.

Version ``3.0`` will introduce a backwards incompatible installation method with regards to the cloud dependencies.
If you want to maintain backwards compatibility (installing all dependencies) install this package via ``smart_open[all]`` now
and once the change is made you should not have any issues. If all you care about is AWS dependencies for example you can install via ``smart_open[s3]`` and
once the dependency change is made you will simply drop the unwanted dependencies. You can read more about the motivations `here <https://github.com/RaRe-Technologies/smart_open/issues/443>`_


Built-in help
-------------

For detailed API info, see the online help:

.. code-block:: python

    help('smart_open')

or click `here <https://github.com/RaRe-Technologies/smart_open/blob/master/help.txt>`__ to view the help in your browser.

More examples
-------------

.. code-block:: python

    >>> import os, boto3
    >>>
    >>> # stream content *into* S3 (write mode) using a custom session
    >>> session = boto3.Session(
    ...     aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    ...     aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    ... )
    >>> url = 's3://smart-open-py37-benchmark-results/test.txt'
    >>> with open(url, 'wb', transport_params={'session': session}) as fout:
    ...     bytes_written = fout.write(b'hello world!')
    ...     print(bytes_written)
    12

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

    # Stream to Digital Ocean Spaces bucket providing credentials from boto3 profile
    transport_params = {
        'session': boto3.Session(profile_name='digitalocean'),
        'resource_kwargs': {
            'endpoint_url': 'https://ams3.digitaloceanspaces.com',
        }
    }
    with open('s3://bucket/key.txt', 'wb', transport_params=transport_params) as fout:
        fout.write(b'here we stand')

    # stream from GCS
    for line in open('gs://my_bucket/my_file.txt'):
        print(line)

    # stream content *into* GCS (write mode):
    with open('gs://my_bucket/my_file.txt', 'wb') as fout:
        fout.write(b'hello world')

    # stream from Azure Blob Storage
    connect_str = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    transport_params = {
        client: azure.storage.blob.BlobServiceClient.from_connection_string(connect_str)
    }
    for line in open('azure://mycontainer/myfile.txt', transport_params=transport_params):
        print(line)

    # stream content *into* Azure Blob Storage (write mode):
    connect_str = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    transport_params = {
        client: azure.storage.blob.BlobServiceClient.from_connection_string(connect_str)
    }
    with open('azure://mycontainer/my_file.txt', 'wb', transport_params=transport_params) as fout:
        fout.write(b'hello world')

Supported Compression Formats
-----------------------------

``smart_open`` allows reading and writing gzip and bzip2 files.
They are transparently handled over HTTP, S3, and other protocols, too, based on the extension of the file being opened.
You can easily add support for other file extensions and compression formats.
For example, to open xz-compressed files:

.. code-block:: python

    >>> import lzma, os
    >>> from smart_open import open, register_compressor

    >>> def _handle_xz(file_obj, mode):
    ...      return lzma.LZMAFile(filename=file_obj, mode=mode, format=lzma.FORMAT_XZ)

    >>> register_compressor('.xz', _handle_xz)

    >>> with open('smart_open/tests/test_data/crime-and-punishment.txt.xz') as fin:
    ...     text = fin.read()
    >>> print(len(text))
    1696

``lzma`` is in the standard library in Python 3.3 and greater.
For 2.7, use `backports.lzma`_.

.. _backports.lzma: https://pypi.org/project/backports.lzma/


Transport-specific Options
--------------------------

``smart_open`` supports a wide range of transport options out of the box, including:

- S3
- HTTP, HTTPS (read-only)
- SSH, SCP and SFTP
- WebHDFS
- GCS
- Azure Blob Storage

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

*Important*: ``smart_open`` ignores configuration files from the older ``boto`` library.
Port your old ``boto`` settings to ``boto3`` in order to use them with ``smart_open``.

Iterating Over an S3 Bucket's Contents
--------------------------------------

Since going over all (or select) keys in an S3 bucket is a very common operation, there's also an extra function ``smart_open.s3.iter_bucket()`` that does this efficiently, **processing the bucket keys in parallel** (using multiprocessing):

.. code-block:: python

  >>> from smart_open import s3
  >>> # get data corresponding to 2010 and later under "silo-open-data/annual/monthly_rain"
  >>> # we use workers=1 for reproducibility; you should use as many workers as you have cores
  >>> bucket = 'silo-open-data'
  >>> prefix = 'annual/monthly_rain/'
  >>> for key, content in s3.iter_bucket(bucket, prefix=prefix, accept_key=lambda key: '/201' in key, workers=1, key_limit=3):
  ...     print(key, round(len(content) / 2**20))
  annual/monthly_rain/2010.monthly_rain.nc 13
  annual/monthly_rain/2011.monthly_rain.nc 13
  annual/monthly_rain/2012.monthly_rain.nc 13

GCS Credentials
---------------
``smart_open`` uses the ``google-cloud-storage`` library to talk to GCS.
``google-cloud-storage`` uses the ``google-cloud`` package under the hood to handle authentication.
There are several `options <https://google-cloud-python.readthedocs.io/en/0.32.0/core/auth.html>`__ to provide
credentials.
By default, ``smart_open`` will defer to ``google-cloud-storage`` and let it take care of the credentials.

To override this behavior, pass a ``google.cloud.storage.Client`` object as a transport parameter to the ``open`` function.
You can `customize the credentials <https://google-cloud-python.readthedocs.io/en/0.32.0/core/client.html>`__
when constructing the client. ``smart_open`` will then use the client when talking to GCS. To follow allow with
the example below, `refer to Google's guide <https://cloud.google.com/storage/docs/reference/libraries#setting_up_authentication>`__
to setting up GCS authentication with a service account.

.. code-block:: python

    import os
    from google.cloud.storage import Client
    service_account_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    client = Client.from_service_account_json(service_account_path)
    fin = open('gs://gcp-public-data-landsat/index.csv.gz', transport_params=dict(client=client))

If you need more credential options, you can create an explicit ``google.auth.credentials.Credentials`` object
and pass it to the Client. To create an API token for use in the example below, refer to the
`GCS authentication guide <https://cloud.google.com/storage/docs/authentication#apiauth>`__.

.. code-block:: python

	import os
	from google.auth.credentials import Credentials
	from google.cloud.storage import Client
	token = os.environ['GOOGLE_API_TOKEN']
	credentials = Credentials(token=token)
	client = Client(credentials=credentials)
	fin = open('gs://gcp-public-data-landsat/index.csv.gz', transport_params=dict(client=client))

Azure Credentials
-----------------

``smart_open`` uses the ``azure-storage-blob`` library to talk to Azure Blob Storage.
By default, ``smart_open`` will defer to ``azure-storage-blob`` and let it take care of the credentials.

Azure Blob Storage does not have any ways of inferring credentials therefore, passing a ``azure.storage.blob.BlobServiceClient``
object as a transport parameter to the ``open`` function is required.
You can `customize the credentials <https://docs.microsoft.com/en-us/azure/storage/common/storage-samples-python#authentication>`__
when constructing the client. ``smart_open`` will then use the client when talking to. To follow allow with
the example below, `refer to Azure's guide <https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal>`__
to setting up authentication.

.. code-block:: python

    import os
    from azure.storage.blob import BlobServiceClient
    azure_storage_connection_string = os.environ['AZURE_STORAGE_CONNECTION_STRING']
    client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
    fin = open('azure://my_container/my_blob.txt', transport_params=dict(client=client))

If you need more credential options, refer to the
`Azure Storage authentication guide <https://docs.microsoft.com/en-us/azure/storage/common/storage-samples-python#authentication>`__.

File-like Binary Streams
------------------------

The ``open`` function also accepts file-like objects.
This is useful when you already have a `binary file <https://docs.python.org/3/glossary.html#term-binary-file>`_ open, and would like to wrap it with transparent decompression:


.. code-block:: python

    >>> import io, gzip
    >>>
    >>> # Prepare some gzipped binary data in memory, as an example.
    >>> # Any binary file will do; we're using BytesIO here for simplicity.
    >>> buf = io.BytesIO()
    >>> with gzip.GzipFile(fileobj=buf, mode='w') as fout:
    ...     _ = fout.write(b'this is a bytestring')
    >>> _ = buf.seek(0)
    >>>
    >>> # Use case starts here.
    >>> buf.name = 'file.gz'  # add a .name attribute so smart_open knows what compressor to use
    >>> import smart_open
    >>> smart_open.open(buf, 'rb').read()  # will gzip-decompress transparently!
    b'this is a bytestring'


In this case, ``smart_open`` relied on the ``.name`` attribute of our `binary I/O stream <https://docs.python.org/3/library/io.html#binary-i-o>`_ ``buf`` object to determine which decompressor to use.
If your file object doesn't have one, set the ``.name`` attribute to an appropriate value.
Furthermore, that value has to end with a **known** file extension (see the ``register_compressor`` function).
Otherwise, the transparent decompression will not occur.

Drop-in replacement of ``pathlib.Path.open``
--------------------------------------------

``smart_open.open`` can also be used with ``Path`` objects.
The built-in `Path.open()` is not able to read text from compressed files, so use ``patch_pathlib`` to replace it with `smart_open.open()` instead.
This can be helpful when e.g. working with compressed files.

.. code-block:: python

    >>> from pathlib import Path
    >>> from smart_open.smart_open_lib import patch_pathlib
    >>>
    >>> _ = patch_pathlib()  # replace `Path.open` with `smart_open.open`
    >>>
    >>> path = Path("smart_open/tests/test_data/crime-and-punishment.txt.gz")
    >>>
    >>> with path.open("r") as infile:
    ...     print(infile.readline()[:41])
    В начале июля, в чрезвычайно жаркое время

How do I ...?
=============

See `this document <howto.md>`__.

Extending ``smart_open``
========================

See `this document <extending.md>`__.

Comments, bug reports
=====================

``smart_open`` lives on `Github <https://github.com/RaRe-Technologies/smart_open>`_. You can file
issues or pull requests there. Suggestions, pull requests and improvements welcome!

----------------

``smart_open`` is open source software released under the `MIT license <https://github.com/piskvorky/smart_open/blob/master/LICENSE>`_.
Copyright (c) 2015-now `Radim Řehůřek <https://radimrehurek.com>`_.
