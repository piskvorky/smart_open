Migrating to v8.0.0
===================

Version 8.0.0 drops several long-deprecated APIs and backwards-compat shims that had been emitting warnings (or were no-ops) for years.
Tracked under `#926 <https://github.com/piskvorky/smart_open/issues/926>`_.

``smart_open.s3_iter_bucket`` removed
-------------------------------------

Tracked in `#927 <https://github.com/piskvorky/smart_open/pull/927>`_.
The top-level ``smart_open.s3_iter_bucket`` wrapper has been removed.
Import ``iter_bucket`` from ``smart_open.s3`` instead:

.. code-block:: diff

   - from smart_open import s3_iter_bucket
   + from smart_open.s3 import iter_bucket as s3_iter_bucket

Top-level ``smart_open.smart_open()`` removed
---------------------------------------------

Tracked in `#928 <https://github.com/piskvorky/smart_open/pull/928>`_.
The compatibility wrapper around ``smart_open.open()`` has been removed.
Call ``smart_open.open()`` directly; if you were using ``ignore_extension=True``, switch to ``compression='disable'``:

.. code-block:: diff

   - fin = smart_open.smart_open('s3://bucket/key.gz', 'rb', ignore_extension=True)
   + fin = smart_open.open('s3://bucket/key.gz', 'rb', compression='disable')

``smart_open_lib`` backwards-compat re-exports removed
------------------------------------------------------

Tracked in `#929 <https://github.com/piskvorky/smart_open/pull/929>`_.
The underscored re-export aliases in ``smart_open.smart_open_lib`` are gone.
Import the canonical names from ``smart_open.utils`` (the public ``smart_open.register_compressor`` continues to work):

.. code-block:: diff

   - from smart_open.smart_open_lib import _check_kwargs, _inspect_kwargs
   + from smart_open.utils import check_kwargs, inspect_kwargs

``s3.iter_bucket`` ``session_kwargs`` is now a dict
---------------------------------------------------

Tracked in `#930 <https://github.com/piskvorky/smart_open/pull/930>`_.
``smart_open.s3.iter_bucket`` no longer accepts session keyword arguments via ``**session_kwargs``.
Pass a single ``session_kwargs`` dict instead:

.. code-block:: diff

     smart_open.s3.iter_bucket(
         bucket,
   -     aws_access_key_id='id',
   -     aws_secret_access_key='secret',
   +     session_kwargs={
   +         'aws_access_key_id': 'id',
   +         'aws_secret_access_key': 'secret',
   +     },
     )

``concurrency.create_pool`` and ``ConcurrentFuturesPool`` removed
-----------------------------------------------------------------

Tracked in `#931 <https://github.com/piskvorky/smart_open/pull/931>`_.
The ``create_pool`` context manager and ``ConcurrentFuturesPool`` class are gone.
Use ``smart_open.concurrency.ThreadPoolExecutor`` directly:

.. code-block:: diff

   - with smart_open.concurrency.create_pool(processes=8) as pool:
   -     for result in pool.imap_unordered(fn, items):
   -         ...
   + with smart_open.concurrency.ThreadPoolExecutor(max_workers=8) as pool:
   +     for result in pool.imap(fn, items):
   +         ...

``compression.tweak_close()`` removed
-------------------------------------

Tracked in `#933 <https://github.com/piskvorky/smart_open/pull/933>`_.
The ``smart_open.compression.tweak_close()`` helper has been removed.
``smart_open.open().__exit__`` already calls ``__exit__`` on the underlying filestream, so the helper's behaviour is provided by the standard exit path — drop the call.

GCS ``buffer_size`` and ``line_terminator`` parameters removed
--------------------------------------------------------------

Tracked in `#935 <https://github.com/piskvorky/smart_open/pull/935>`_.
``smart_open.gcs.open()`` and ``smart_open.gcs.Reader()`` no longer accept ``buffer_size`` or ``line_terminator`` (``Reader`` only).
They have been no-ops emitting a ``UserWarning`` for years.
Drop them from your call sites:

.. code-block:: diff

   - smart_open.gcs.open(bucket, blob, 'rb', buffer_size=8192)
   + smart_open.gcs.open(bucket, blob, 'rb')

GCS writer ``terminate()`` no longer exists
-------------------------------------------

Tracked in `#936 <https://github.com/piskvorky/smart_open/pull/936>`_.
The no-op ``.terminate()`` monkey-patch on the google-cloud-storage blob writer has been removed.
It had been a silent no-op for years (`Google deprecated resumable-upload termination upstream <https://cloud.google.com/storage/docs/resumable-uploads>`_).
Rely on the writer's context manager or ``.close()`` for normal completion.

S3 ``open_uri`` deprecated transport-parameter warnings removed
---------------------------------------------------------------

Tracked in `#937 <https://github.com/piskvorky/smart_open/pull/937>`_.
The ``UserWarning`` that ``smart_open.s3.open_uri`` emitted for the legacy resource-API transport parameters (``multipart_upload_kwargs``, ``object_kwargs``, ``resource``, ``resource_kwargs``, ``session``, ``singlepart_upload_kwargs``) is gone.
These parameters had already been unsupported since v5.0.0 — see `Migrating to the new client-based S3 API`_ below for the actual translation recipes.

S3 URIs no longer accept embedded ``host[:port]`` or the ``s3u`` scheme
----------------------------------------------------------------------

Tracked in `#385 <https://github.com/piskvorky/smart_open/issues/385>`_.
The non-standard ``s3://key:secret@host:port@bucket/key`` form (and its ``s3u://`` http variant) is no longer parsed.
It broke `RFC 3986 <https://www.rfc-editor.org/rfc/rfc3986>`_ by stuffing two ``@`` separators into the authority, made S3 URI parsing the most fiddly code in the library, and was rarely used in practice.
Build a boto3 client with the desired ``endpoint_url`` (and credentials) and pass it via ``transport_params['client']`` instead:

.. code-block:: diff

   - smart_open.open('s3://key:secret@host:1234@bucket/key')
   + client = boto3.client(
   +     's3',
   +     endpoint_url='https://host:1234',
   +     aws_access_key_id='key',
   +     aws_secret_access_key='secret',
   + )
   + smart_open.open('s3://bucket/key', transport_params={'client': client})

For an ``s3u://`` URL (http endpoint), pass ``endpoint_url='http://host:1234'`` to the client.
The ``s3``, ``s3n``, and ``s3a`` schemes continue to work, as does the ``s3://key:secret@bucket/key`` form for embedding credentials in the URL.

GCS canonical scheme is now ``gcs://``
--------------------------------------

Tracked in `#598 <https://github.com/piskvorky/smart_open/issues/598>`_.
Documentation and examples now use ``gcs://bucket/blob`` as the canonical GCS URI form, matching the ``smart_open.gcs`` module name and the ``smart_open[gcs]`` extras.
The ``gs://`` scheme keeps working as a backwards-compatible alias — no code change required if you are already using it — but prefer ``gcs://`` in new code:

.. code-block:: diff

   - smart_open.open('gs://my_bucket/my_file.txt')
   + smart_open.open('gcs://my_bucket/my_file.txt')


Migrating to the new compression parameter
==========================================

smart_open versions 6.0.0 and above no longer support the ``ignore_ext`` parameter.
Use the ``compression`` parameter instead:

.. code-block:: python

    fin = smart_open.open("/path/file.gz", ignore_ext=True)  # No
    fin = smart_open.open("/path/file.gz", compression="disable")  # Yes
    
    fin = smart_open.open("/path/file.gz", ignore_ext=False)  # No
    fin = smart_open.open("/path/file.gz")  # Yes
    fin = smart_open.open("/path/file.gz", compression="infer_from_extension")  # Yes, if you want to be explicit
    
    fin = smart_open.open("/path/file", compression=".gz")  # Yes


Migrating to the new client-based S3 API
========================================

Version of smart_open prior to 5.0.0 used the boto3 `resource API`_ for communicating with S3.
This API was easy to integrate for smart_open developers, but this came at a cost: it was not thread- or multiprocess-safe.
Furthermore, as smart_open supported more and more options, the transport parameter list grew, making it less maintainable.

Starting with version 5.0.0, smart_open uses the `client API`_ instead of the resource API.
Functionally, very little changes for the smart_open user. 
The only difference is in passing transport parameters to the S3 backend.

More specifically, the following S3 transport parameters are no longer supported:

- `multipart_upload_kwargs`
- `object_kwargs`
- `resource`
- `resource_kwargs`
- `session`
- `singlepart_upload_kwargs`

**If you weren't using the above parameters, nothing changes for you.**

However, if you were using any of the above, then you need to adjust your code.
Here are some quick recipes below.

If you were previously passing `session`, then construct an S3 client from the session and pass that instead.
For example, before:

.. code-block:: python

    smart_open.open('s3://bucket/key', transport_params={'session': session})

After:

.. code-block:: python

    smart_open.open('s3://bucket/key', transport_params={'client': session.client('s3')})

If you were passing `resource`, then replace the resource with a client, and pass that instead.
For example, before:

.. code-block:: python

    resource = session.resource('s3', **resource_kwargs)
    smart_open.open('s3://bucket/key', transport_params={'resource': resource})

After:

.. code-block:: python

    client = session.client('s3')
    smart_open.open('s3://bucket/key', transport_params={'client': client})

If you were passing any of the `*_kwargs` parameters, you will need to include them in `client_kwargs`, keeping in mind the following transformations.

========================== ====================================== ==========================
Parameter name             Resource API method                    Client API function
========================== ====================================== ==========================
`multipart_upload_kwargs`  `S3.Object.initiate_multipart_upload`_ `S3.Client.create_multipart_upload`_
`object_kwargs`            `S3.Object.get`_                       `S3.Client.get_object`_
`resource_kwargs`          S3.resource                            `S3.client`_
`singlepart_upload_kwargs` `S3.Object.put`_                       `S3.Client.put_object`_
========================== ====================================== ==========================

Most of the above is self-explanatory, with the exception of `resource_kwargs`.
These were previously used mostly for passing a custom endpoint URL.

The `client_kwargs` dict can thus contain the following members:

- `S3.Client`: initializer parameters, e.g. those to pass directly to the `boto3.client` function, such as `endpoint_url`.
- `S3.Client.create_multipart_upload`
- `S3.Client.get_object`
- `S3.Client.put_object`

Here's a before-and-after example for connecting to a custom endpoint.  Before:

.. code-block:: python

    session = boto3.Session(profile_name='digitalocean')
    resource_kwargs = {'endpoint_url': 'https://ams3.digitaloceanspaces.com'}
    with open('s3://bucket/key.txt', 'wb', transport_params={'resource_kwarg': resource_kwargs}) as fout:
        fout.write(b'here we stand')

After:

.. code-block:: python

    session = boto3.Session(profile_name='digitalocean')
    client = session.client('s3', endpoint_url='https://ams3.digitaloceanspaces.com')
    with open('s3://bucket/key.txt', 'wb', transport_params={'client': client}) as fout:
        fout.write(b'here we stand')

See `README <README.rst>`_ and `HOWTO <howto.md>`_ for more examples.

.. _resource API: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#service-resource
.. _S3.Object.initiate_multipart_upload: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.initiate_multipart_upload
.. _S3.Object.get: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.ObjectSummary.get
.. _S3.Object.put: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.ObjectSummary.put

.. _client API: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client
.. _S3.Client: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client
.. _S3.Client.create_multipart_upload: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.create_multipart_upload
.. _S3.Client.get_object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
.. _S3.Client.put_object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object

Migrating to the new dependency management subsystem
====================================================

Smart_open has grown over the years to cover a lot of different storages, each with a different set of library dependencies. Not everybody needs *all* of them, so to make each smart_open installation leaner and faster, version 3.0.0 introduced a new, backward-incompatible installation method:

* smart_open < 3.0.0: All dependencies were installed by default. No way to select just a subset during installation.
* smart_open >= 3.0.0: No dependencies installed by default. Install the ones you need with e.g. ``pip install smart_open[s3]`` (only AWS), or ``smart_open[all]`` (install everything = same behaviour as < 3.0.0; use this for backward compatibility). 

You can read more about the motivation and internal discussions for this change  `here <https://github.com/piskvorky/smart_open/issues/443>`_.

Migrating to the new ``open`` function
======================================

Since 1.8.1, there is a ``smart_open.open`` function that replaces ``smart_open.smart_open``.
The new function offers several advantages over the old one:

- 100% compatible with the built-in ``open`` function (aka ``io.open``): it accepts all
  the parameters that the built-in ``open`` accepts.
- The default open mode is now "r", the same as for the built-in ``open``.
  The default for the old ``smart_open.smart_open`` function used to be "rb".
- Fully documented keyword parameters (try ``help("smart_open.open")``)

The instructions below will help you migrate to the new function painlessly.

First, update your imports:

.. code-block:: python

  >>> from smart_open import smart_open  # before
  >>> from smart_open import open  # after

In general, ``smart_open`` uses ``io.open`` directly, where possible, so if your
code already uses ``open`` for local file I/O, then it will continue to work.
If you want to continue using the built-in ``open`` function for e.g. debugging,
then you can ``import smart_open`` and use ``smart_open.open``.

**The default read mode is now "r" (read text).**
If your code was implicitly relying on the default mode being "rb" (read
binary), you'll need to update it and pass "rb" explicitly.

Before:

.. code-block:: python

  >>> import smart_open
  >>> smart_open.smart_open('s3://commoncrawl/robots.txt').read(32)  # 'rb' used to be the default
  b'User-Agent: *\nDisallow: /'

After:

.. code-block:: python

  >>> import smart_open
  >>> smart_open.open('s3://commoncrawl/robots.txt', 'rb').read(32)
  b'User-Agent: *\nDisallow: /'

The ``ignore_extension`` keyword parameter is now called ``ignore_ext``.
It behaves identically otherwise.

The most significant change is in the handling on keyword parameters for the
transport layer, e.g. HTTP, S3, etc. The old function accepted these directly:

.. code-block:: python

  >>> url = 's3://smart-open-py37-benchmark-results/test.txt'
  >>> session = boto3.Session(profile_name='smart_open')
  >>> smart_open.smart_open(url, 'r', session=session).read(32)
  'first line\nsecond line\nthird lin'

The new function accepts a ``transport_params`` keyword argument.  It's a dict.
Put your transport parameters in that dictionary.

.. code-block:: python

  >>> url = 's3://smart-open-py37-benchmark-results/test.txt'
  >>> params = {'session': boto3.Session(profile_name='smart_open')}
  >>> open(url, 'r', transport_params=params).read(32)
  'first line\nsecond line\nthird lin'

Renamed parameters:

- ``s3_upload`` ->  ``multipart_upload_kwargs``
- ``s3_session`` -> ``session``

Removed parameters:

- ``profile_name``

**The profile_name parameter has been removed.**
Pass an entire ``boto3.Session`` object instead.

Before:

.. code-block:: python

  >>> url = 's3://smart-open-py37-benchmark-results/test.txt'
  >>> smart_open.smart_open(url, 'r', profile_name='smart_open').read(32)
  'first line\nsecond line\nthird lin'

After:

.. code-block:: python

  >>> url = 's3://smart-open-py37-benchmark-results/test.txt'
  >>> params = {'session': boto3.Session(profile_name='smart_open')}
  >>> open(url, 'r', transport_params=params).read(32)
  'first line\nsecond line\nthird lin'

See ``help("smart_open.open")`` for the full list of acceptable parameter names,
or view the help online `here <https://github.com/piskvorky/smart_open/blob/master/help.txt>`__.

If you pass an invalid parameter name, the ``smart_open.open`` function will warn you about it.
Keep an eye on your logs for WARNING messages from ``smart_open``.
