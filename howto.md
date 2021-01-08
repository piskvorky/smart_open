# How-to Guides

The howtos are **goal-oriented guides** that demonstrate **how to solve a specific problem** using `smart_open`.

## How to Add a New Guide

The guides are code snippets compatible with Python's [doctest](https://docs.python.org/2/library/doctest.html) module.
Lines that start with `>>>` and `...` are Python commands to run via the interpreter.
Lines without the above prefixes are expected standard output from the commands.
The `doctest` module runs the commands and ensures that their output matches the expected values.

```python
>>> foo = 'bar'
>>> print(foo)
bar

```

Some tips:

- Enclose the snippets with markdowns triple backticks to get free syntax highlighting
- End your example with a blank line to let `doctest` know the triple backticks aren't part of the example

Finally, ensure all the guides still work by running:

    python -m doctest howto.md

The above command shouldn't print anything to standard output/error and return zero.

## How to Read/Write Zip Files

`smart_open` does not support reading/writing zip files out of the box.
However, you can easily integrate `smart_open` with the standard library's [zipfile](https://docs.python.org/3.5/library/zipfile.html) module:

- `smart_open` handles the I/O
- `zipfile` handles the compression, decompression, and file member lookup

Reading example:

```python
>>> from smart_open import open
>>> import zipfile
>>> with open('sampledata/hello.zip', 'rb') as fin:
...     with zipfile.ZipFile(fin) as zip:
...         for info in zip.infolist():
...             file_bytes = zip.read(info.filename)
...             print('%r: %r' % (info.filename, file_bytes.decode('utf-8')))
'hello/': ''
'hello/en.txt': 'hello world!\n'
'hello/ru.txt': 'здравствуй, мир!\n'

```

Writing example:

```python
>>> from smart_open import open
>>> import os
>>> import tempfile
>>> import zipfile
>>> tmp = tempfile.NamedTemporaryFile(prefix='smart_open-howto-', suffix='.zip', delete=False)
>>> with open(tmp.name, 'wb') as fout:
... 	with zipfile.ZipFile(fout, 'w') as zip:
...			zip.writestr('hello/en.txt', 'hello world!\n')
...			zip.writestr('hello/ru.txt', 'здравствуй, мир!\n')
>>> os.unlink(tmp.name)  # comment this line to keep the file for later

```

## How to access S3 anonymously

The `boto3` library that `smart_open` uses for accessing S3 signs each request using your `boto3` credentials.
If you'd like to access S3 without using an S3 account, then you need disable this signing mechanism.

```python
>>> import botocore
>>> import botocore.client
>>> from smart_open import open
>>> config = botocore.client.Config(signature_version=botocore.UNSIGNED)
>>> params = {'resource_kwargs': {'config': config}}
>>> with open('s3://commoncrawl/robots.txt', transport_params=params) as fin:
...    fin.readline()
'User-Agent: *\n'

```
## How to Access S3 Object Properties

When working with AWS S3, you may want to look beyond the abstraction
provided by `smart_open` and communicate with `boto3` directly in order to
satisfy your use case.

For example:

- Access the object's properties, such as the content type, timestamp of the last change, etc.
- Access version information for the object (versioned buckets only)
- Copy the object to another location
- Apply an ACL to the object
- and anything else specified in the [boto3 S3 Object API](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#object).

To enable such use cases, the file-like objects returned by `smart_open` have a special `to_boto3` method.
This returns a `boto3.s3.Object` that you can work with directly.
For example, let's get the content type of a publicly available file:

```python
>>> from smart_open import open
>>> with open('s3://commoncrawl/robots.txt') as fin:
...    print(fin.readline().rstrip())
...    boto3_s3_object = fin.to_boto3()
...    print(repr(boto3_s3_object))
...    print(boto3_s3_object.content_type)  # Using the boto3 API here
User-Agent: *
s3.Object(bucket_name='commoncrawl', key='robots.txt')
text/plain

```

This works only when reading and writing via S3.

## How to Access a Specific Version of an S3 Object

The ``version_id`` transport parameter enables you to get the desired version of the object from an S3 bucket.

.. Important::
    S3 disables version control by default.
    Before using the ``version_id`` parameter, you must explicitly enable version control for your S3 bucket.
    Read https://docs.aws.amazon.com/AmazonS3/latest/dev/Versioning.html for details.

```python
>>> import boto3
>>> from smart_open import open
>>> versions = ['KiQpZPsKI5Dm2oJZy_RzskTOtl2snjBg', 'N0GJcE3TQCKtkaS.gF.MUBZS85Gs3hzn']
>>> for v in versions:
...     with open('s3://smart-open-versioned/demo.txt', transport_params={'version_id': v}) as fin:
...         print(v, repr(fin.read()))
KiQpZPsKI5Dm2oJZy_RzskTOtl2snjBg 'second version\n'
N0GJcE3TQCKtkaS.gF.MUBZS85Gs3hzn 'first version\n'

>>> # If you don't specify a version, smart_open will read the most recent one
>>> with open('s3://smart-open-versioned/demo.txt') as fin:
...     print(repr(fin.read()))
'second version\n'

```

This works only when reading via S3.

## How to Access the Underlying boto3 Object

At some stage in your workflow, you may opt to work with `boto3` directly.
You can do this by calling to the `to_boto3()` method.
You can then interact with the object using the `boto3` API:


```python
>>> with open('s3://commoncrawl/robots.txt') as fin:
...     boto3_object = fin.to_boto3()
...     print(boto3_object)
...     print(boto3_object.get()['LastModified'])
s3.Object(bucket_name='commoncrawl', key='robots.txt')
2016-05-21 18:17:43+00:00

```

This works only when reading and writing via S3.

For versioned objects, the returned object will be slightly different:

```
>>> params = {'version_id': 'KiQpZPsKI5Dm2oJZy_RzskTOtl2snjBg'}
>>> with open('s3://smart-open-versioned/demo.txt', transport_params=params) as fin:
...     print(fin.to_boto3())
s3.ObjectVersion(bucket_name='smart-open-versioned', object_key='demo.txt', id='KiQpZPsKI5Dm2oJZy_RzskTOtl2snjBg')

```

## How to Specify the Request Payer (S3 only)

Some public buckets require you to [pay for S3 requests for the data in the bucket](https://docs.aws.amazon.com/AmazonS3/latest/dev/RequesterPaysBuckets.html).
This relieves the bucket owner of the data transfer costs, and spreads them among the consumers of the data.

To access such buckets, you need to pass some special transport parameters:

```python
>>> from smart_open import open
>>> params = {'object_kwargs': {'RequestPayer': 'requester'}}
>>> with open('s3://arxiv/pdf/arXiv_pdf_manifest.xml', transport_params=params) as fin:
...    print(fin.readline())
<?xml version='1.0' standalone='yes'?>
<BLANKLINE>

```

This works only when reading and writing via S3.

## How to Make S3 I/O Robust to Network Errors

Boto3 has a [built-in mechanism](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) for retrying after a recoverable error.
You can fine-tune it using several ways:

### Pre-configuring a boto3 resource and then passing the resource to smart_open

```python
>>> import boto3
>>> import botocore.config
>>> import smart_open
>>> config = botocore.config.Config(retries={'mode': 'standard'})
>>> resource = boto3.resource('s3', config=config)
>>> tp = {'resource': resource}
>>> with smart_open.open('s3://commoncrawl/robots.txt', transport_params=tp) as fin:
...     print(fin.readline())
User-Agent: *
```

### Directly passing configuration as transport parameters to smart_open

```python
>>> import boto3
>>> import botocore.config
>>> import smart_open
>>> config = botocore.config.Config(retries={'mode': 'standard'})
>>> tp = {'resource_kwargs': {'config': config}}
>>> with smart_open.open('s3://commoncrawl/robots.txt', transport_params=tp) as fin:
...     print(fin.readline())
User-Agent: *
```

To verify your settings have effect:

```python
import logging
logging.getLogger('smart_open.s3').setLevel(logging.DEBUG)
```

and check the log output of your code.
