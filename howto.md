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

```python
>>> params = {'version_id': 'KiQpZPsKI5Dm2oJZy_RzskTOtl2snjBg'}
>>> with open('s3://smart-open-versioned/demo.txt', transport_params=params) as fin:
...     print(fin.to_boto3())
s3.ObjectVersion(bucket_name='smart-open-versioned', object_key='demo.txt', id='KiQpZPsKI5Dm2oJZy_RzskTOtl2snjBg')

```

## How to Read from S3 Efficiently

Under the covers, `smart_open` uses the [boto3 resource API](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html) to read from S3.
By default, calling `smart_open.open` with an S3 URL will create its own boto3 session and resource.
These are expensive operations: they require both CPU time to construct the objects from a low-level API definition, and memory to store the objects once they have been created.
It is possible to save both CPU time and memory by sharing the same resource across multiple `smart_open.open` calls, for example:

```python
>>> import boto3
>>> from smart_open import open
>>> tp = {'resource': boto3.resource('s3')}
>>> for month in (1, 2, 3):
...     url = 's3://nyc-tlc/trip data/yellow_tripdata_2020-%02d.csv' % month
...     with open(url, transport_params=tp) as fin:
...         _ = fin.readline()  # skip CSV header
...         print(fin.readline().strip())
1,2020-01-01 00:28:15,2020-01-01 00:33:03,1,1.20,1,N,238,239,1,6,3,0.5,1.47,0,0.3,11.27,2.5
1,2020-02-01 00:17:35,2020-02-01 00:30:32,1,2.60,1,N,145,7,1,11,0.5,0.5,2.45,0,0.3,14.75,0
1,2020-03-01 00:31:13,2020-03-01 01:01:42,1,4.70,1,N,88,255,1,22,3,0.5,2,0,0.3,27.8,2.5

```

The above sharing is safe because it is all happening in the same thread and subprocess (see below for details).

## How to Work in a Parallelized Environment

Under the covers, `smart_open` uses the [boto3 resource API](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html) to read from S3.
This API is not thread-safe or multiprocess-safe.
Do not share the same `smart_open` objects across different threads or subprocesses.
`smart_open` will create its own session and resource objects for each individual `open` call, so you don't have to worry about managing boto3 objects.
This comes at a price: each session and resource requires CPU time to create and memory to store, so be wary of keeping hundreds of threads or subprocesses reading/writing from/to S3.

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

## How to Read/Write from localstack

[localstack](https://github.com/localstack/localstack) is a convenient test framework for developing cloud apps.
You run it locally on your machine and behaves almost identically to the real AWS.
This makes it useful for testing your code offline, without requiring you to set up mocks or test harnesses.

First, install localstack and start it:

    $ pip install localstack
    $ localstack start

The start command is blocking, so you'll need to run it in a separate terminal session or run it in the background.
Before we can read/write, we'll need to create a bucket:

    $ aws --endpoint-url http://localhost:4566 s3api create-bucket --bucket mybucket

where `http://localhost:4566` is the default host/port that localstack uses to listen for requests.

You can now read/write to the bucket the same way you would to a real S3 bucket:

```python
>>> from smart_open import open
>>> tparams = {'resource_kwargs': {'endpoint_url': 'http://localhost:4566'}}
>>> with open('s3://mybucket/hello.txt', 'wt', transport_params=tparams) as fout:
...     fout.write('hello world!')
>>> with open('s3://mybucket/hello.txt', 'rt', transport_params=tparams) as fin:
...     fin.read()
'hello world!'

```

You can also access it using the CLI:

    $ aws --endpoint-url http://localhost:4566 s3 ls s3://mybucket/
    2020-12-09 15:56:22         12 hello.txt
