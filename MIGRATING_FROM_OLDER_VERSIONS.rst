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

You can read more about the motivation and internal discussions for this change  `here <https://github.com/RaRe-Technologies/smart_open/issues/443>`_.

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
or view the help online `here <https://github.com/RaRe-Technologies/smart_open/blob/master/help.txt>`__.

If you pass an invalid parameter name, the ``smart_open.open`` function will warn you about it.
Keep an eye on your logs for WARNING messages from ``smart_open``.
