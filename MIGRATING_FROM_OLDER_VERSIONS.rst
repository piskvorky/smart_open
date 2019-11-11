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

