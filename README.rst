======================================================================
smart_open -- Utils for streaming large files (S3, HDFS, gzip, bz2...)
======================================================================

|Travis|_
|Downloads|_
|License|_

.. |Travis| image:: https://api.travis-ci.org/piskvorky/smart_open.png?branch=master
.. |Downloads| image:: https://pypip.in/d/smart_open/badge.png?style=flat
.. |License| image:: https://pypip.in/license/smart_open/badge.png?style=flat
.. _Travis: https://travis-ci.org/piskvorky/smart_open
.. _Downloads: https://pypi.python.org/pypi/smart_open
.. _License: https://github.com/piskvorky/smart_open/blob/master/LICENSE

What?
=====

``smart_open`` is a Python library for **efficient streaming of (very large) files from/to S3**. It is well tested (using `moto <https://github.com/spulec/moto>`_), well documented and has a dead simple API:

FIXME EXAMPES

Why?
----

Amazon's standard Python library, `boto <http://docs.pythonboto.org/en/latest/>`_ contains all the necessary building blocks for streaming, but has a really clumsy interface. There are nasty hidden gotchas when you want to stream large files from/to S3 (as opposed to simple in-memory read/write with ``key.set_contents_from_string()`` and ``key.get_contents_as_string()``).

``smart_open`` shields you from that, offering a cleaner API. The result is less code for you to write and fewer bugs to make.


Installation
------------

The module has no dependencies beyond 2.6 <= Python < 3.0 and ``boto``:

    pip install smart_open

Or, if you prefer to install from the `source tar.gz <http://pypi.python.org/pypi/smart_open>`_ ::

    python setup.py test # run unit tests
    python setup.py install

To run the unit tests (optional), you'll also need to install `mock <https://pypi.python.org/pypi/mock>`_ and `moto <https://github.com/spulec/moto>`_.

Todo
----

* improve ``smart_open`` support for HDFS (streaming from/to Hadoop File System)
* migrate ``smart_open`` streaming of gzip/bz2 files from gensim
* better document support for the default ``file://`` scheme
* add py3k support

Documentation
-------------

FIXME TODO ``help()``


Comments, bug reports
---------------------

``smart_open`` lives on `github <https://github.com/piskvorky/smart_open>`_. You can file
issues or pull requests there.
