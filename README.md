# smart_open — utils for streaming large files in Python

![License](https://img.shields.io/pypi/l/smart_open.svg)\_
![GHA](https://github.com/RaRe-Technologies/smart_open/workflows/Test/badge.svg)\_
![Coveralls](https://coveralls.io/repos/github/RaRe-Technologies/smart_open/badge.svg?branch=develop)\_
![Downloads](https://pepy.tech/badge/smart-open/month)\_

## What?

`smart_open` is a Python 3 library for **efficient streaming of very
large files** from/to storages such as S3, GCS, Azure Blob Storage,
HDFS, WebHDFS, HTTP, HTTPS, SFTP, or local filesystem. It supports
transparent, on-the-fly (de-)compression for a variety of different
formats.

`smart_open` is a drop-in replacement for Python's built-in `open()`: it
can do anything `open` can (100% compatible, falls back to native `open`
wherever possible), plus lots of nifty extra stuff on top.

**Python 2.7 is no longer supported. If you need Python 2.7, please
use** [smart_open
1.10.1](https://github.com/RaRe-Technologies/smart_open/releases/tag/1.10.0),
**the last version to support Python 2.**

## Why?

Working with large remote files, for example using Amazon's
[boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
Python library, is a pain. `boto3`'s `Object.upload_fileobj()` and
`Object.download_fileobj()` methods require gotcha-prone boilerplate to
use successfully, such as constructing file-like object wrappers.
`smart_open` shields you from that. It builds on boto3 and other remote
storage libraries, but offers a **clean unified Pythonic API**. The
result is less code for you to write and fewer bugs to make.

## How?

`smart_open` is well-tested, well-documented, and has a simple Pythonic
API:

<div id="doctools_before_examples">

``` python
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
74
80
78
79

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
```

</div>

<div id="doctools_after_examples">

Other examples of URLs that `smart_open` accepts:

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

</div>

## Documentation

### Installation

`smart_open` supports a wide range of storage solutions, including AWS
S3, Google Cloud and Azure. Each individual solution has its own
dependencies. By default, `smart_open` does not install any
dependencies, in order to keep the installation size small. You can
install these dependencies explicitly using:

    pip install smart_open[azure] # Install Azure deps
    pip install smart_open[gcs] # Install GCS deps
    pip install smart_open[s3] # Install S3 deps

Or, if you don't mind installing a large number of third party
libraries, you can install all dependencies using:

    pip install smart_open[all]

Be warned that this option increases the installation size
significantly, e.g. over 100MB.

If you're upgrading from `smart_open` versions 2.x and below, please
check out the [Migration Guide](MIGRATING_FROM_OLDER_VERSIONS.rst).

### Built-in help

For detailed API info, see the online help:

``` python
help('smart_open')
```

or click
[here](https://github.com/RaRe-Technologies/smart_open/blob/master/help.txt)
to view the help in your browser.

### More examples

For the sake of simplicity, the examples below assume you have all the
dependencies installed, i.e. you have done:

    pip install smart_open[all]

``` python
>>> import os, boto3
>>> from smart_open import open
>>>
>>> # stream content *into* S3 (write mode) using a custom session
>>> session = boto3.Session(
...     aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
...     aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
... )
>>> url = 's3://smart-open-py37-benchmark-results/test.txt'
>>> with open(url, 'wb', transport_params={'client': session.client('s3')}) as fout:
...     bytes_written = fout.write(b'hello world!')
...     print(bytes_written)
12
```

``` python
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
session = boto3.Session(profile_name='digitalocean')
client = session.client('s3', endpoint_url='https://ams3.digitaloceanspaces.com')
transport_params = {'client': client}
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
    'client': azure.storage.blob.BlobServiceClient.from_connection_string(connect_str),
}
for line in open('azure://mycontainer/myfile.txt', transport_params=transport_params):
    print(line)

# stream content *into* Azure Blob Storage (write mode):
connect_str = os.environ['AZURE_STORAGE_CONNECTION_STRING']
transport_params = {
    'client': azure.storage.blob.BlobServiceClient.from_connection_string(connect_str),
}
with open('azure://mycontainer/my_file.txt', 'wb', transport_params=transport_params) as fout:
    fout.write(b'hello world')
```

### Compression Handling

The top-level <span class="title-ref">compression</span> parameter
controls compression/decompression behavior when reading and writing.
The supported values for this parameter are:

-   `infer_from_extension` (default behavior)
-   `disable`
-   `.gz`
-   `.bz2`

By default, `smart_open` determines the compression algorithm to use
based on the file extension.

``` python
>>> from smart_open import open, register_compressor
>>> with open('smart_open/tests/test_data/1984.txt.gz') as fin:
...     print(fin.read(32))
It was a bright cold day in Apri
```

You can override this behavior to either disable compression, or
explicitly specify the algorithm to use. To disable compression:

``` python
>>> from smart_open import open, register_compressor
>>> with open('smart_open/tests/test_data/1984.txt.gz', 'rb', compression='disable') as fin:
...     print(fin.read(32))
b'\x1f\x8b\x08\x08\x85F\x94\\\x00\x031984.txt\x005\x8f=r\xc3@\x08\x85{\x9d\xe2\x1d@'
```

To specify the algorithm explicitly (e.g. for non-standard file
extensions):

``` python
>>> from smart_open import open, register_compressor
>>> with open('smart_open/tests/test_data/1984.txt.gzip', compression='.gz') as fin:
...     print(fin.read(32))
It was a bright cold day in Apri
```

You can also easily add support for other file extensions and
compression formats. For example, to open xz-compressed files:

``` python
>>> import lzma, os
>>> from smart_open import open, register_compressor

>>> def _handle_xz(file_obj, mode):
...      return lzma.LZMAFile(filename=file_obj, mode=mode, format=lzma.FORMAT_XZ)

>>> register_compressor('.xz', _handle_xz)

>>> with open('smart_open/tests/test_data/1984.txt.xz') as fin:
...     print(fin.read(32))
It was a bright cold day in Apri
```

`lzma` is in the standard library in Python 3.3 and greater. For 2.7,
use [backports.lzma](https://pypi.org/project/backports.lzma/).

### Transport-specific Options

`smart_open` supports a wide range of transport options out of the box,
including:

-   S3
-   HTTP, HTTPS (read-only)
-   SSH, SCP and SFTP
-   WebHDFS
-   GCS
-   Azure Blob Storage

Each option involves setting up its own set of parameters. For example,
for accessing S3, you often need to set up authentication, like API keys
or a profile name. `smart_open`'s `open` function accepts a keyword
argument `transport_params` which accepts additional parameters for the
transport layer. Here are some examples of using this parameter:

``` python
>>> import boto3
>>> fin = open('s3://commoncrawl/robots.txt', transport_params=dict(client=boto3.client('s3')))
>>> fin = open('s3://commoncrawl/robots.txt', transport_params=dict(buffer_size=1024))
```

For the full list of keyword arguments supported by each transport
option, see the documentation:

``` python
help('smart_open.open')
```

### S3 Credentials

`smart_open` uses the `boto3` library to talk to S3. `boto3` has several
[mechanisms](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html)
for determining the credentials to use. By default, `smart_open` will
defer to `boto3` and let the latter take care of the credentials. There
are several ways to override this behavior.

The first is to pass a `boto3.Client` object as a transport parameter to
the `open` function. You can customize the credentials when constructing
the session for the client. `smart_open` will then use the session when
talking to S3.

``` python
session = boto3.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN,
)
client = session.client('s3', endpoint_url=..., config=...)
fin = open('s3://bucket/key', transport_params={'client': client})
```

Your second option is to specify the credentials within the S3 URL
itself:

``` python
fin = open('s3://aws_access_key_id:aws_secret_access_key@bucket/key', ...)
```

*Important*: The two methods above are **mutually exclusive**. If you
pass an AWS client *and* the URL contains credentials, `smart_open` will
ignore the latter.

*Important*: `smart_open` ignores configuration files from the older
`boto` library. Port your old `boto` settings to `boto3` in order to use
them with `smart_open`.

### S3 Advanced Usage

Additional keyword arguments can be propagated to the boto3 methods that
are used by `smart_open` under the hood using the `client_kwargs`
transport parameter.

For instance, to upload a blob with Metadata, ACL, StorageClass, these
keyword arguments can be passed to `create_multipart_upload`
([docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.create_multipart_upload)).

``` python
kwargs = {'Metadata': {'version': 2}, 'ACL': 'authenticated-read', 'StorageClass': 'STANDARD_IA'}
fout = open('s3://bucket/key', 'wb', transport_params={'client_kwargs': {'S3.Client.create_multipart_upload': kwargs}})
```

### Iterating Over an S3 Bucket's Contents

Since going over all (or select) keys in an S3 bucket is a very common
operation, there's also an extra function `smart_open.s3.iter_bucket()`
that does this efficiently, **processing the bucket keys in parallel**
(using multiprocessing):

``` python
>>> from smart_open import s3
>>> # we use workers=1 for reproducibility; you should use as many workers as you have cores
>>> bucket = 'silo-open-data'
>>> prefix = 'Official/annual/monthly_rain/'
>>> for key, content in s3.iter_bucket(bucket, prefix=prefix, accept_key=lambda key: '/201' in key, workers=1, key_limit=3):
...     print(key, round(len(content) / 2**20))
Official/annual/monthly_rain/2010.monthly_rain.nc 13
Official/annual/monthly_rain/2011.monthly_rain.nc 13
Official/annual/monthly_rain/2012.monthly_rain.nc 13
```

### GCS Credentials

`smart_open` uses the `google-cloud-storage` library to talk to GCS.
`google-cloud-storage` uses the `google-cloud` package under the hood to
handle authentication. There are several
[options](https://googleapis.dev/python/google-api-core/latest/auth.html)
to provide credentials. By default, `smart_open` will defer to
`google-cloud-storage` and let it take care of the credentials.

To override this behavior, pass a `google.cloud.storage.Client` object
as a transport parameter to the `open` function. You can [customize the
credentials](https://googleapis.dev/python/storage/latest/client.html)
when constructing the client. `smart_open` will then use the client when
talking to GCS. To follow allow with the example below, [refer to
Google's
guide](https://cloud.google.com/storage/docs/reference/libraries#setting_up_authentication)
to setting up GCS authentication with a service account.

``` python
import os
from google.cloud.storage import Client
service_account_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
client = Client.from_service_account_json(service_account_path)
fin = open('gs://gcp-public-data-landsat/index.csv.gz', transport_params=dict(client=client))
```

If you need more credential options, you can create an explicit
`google.auth.credentials.Credentials` object and pass it to the Client.
To create an API token for use in the example below, refer to the [GCS
authentication
guide](https://cloud.google.com/storage/docs/authentication#apiauth).

``` python
import os
from google.auth.credentials import Credentials
from google.cloud.storage import Client
token = os.environ['GOOGLE_API_TOKEN']
credentials = Credentials(token=token)
client = Client(credentials=credentials)
fin = open('gs://gcp-public-data-landsat/index.csv.gz', transport_params={'client': client})
```

### GCS Advanced Usage

Additional keyword arguments can be propagated to the GCS open method
([docs](https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob#google_cloud_storage_blob_Blob_open)),
which is used by `smart_open` under the hood, using the
`blob_open_kwargs` transport parameter.

Additionally keyword arguments can be propagated to the GCS `get_blob`
method
([docs](https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.bucket.Bucket#google_cloud_storage_bucket_Bucket_get_blob))
when in a read-mode, using the `get_blob_kwargs` transport parameter.

Additional blob properties
([docs](https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob#properties))
can be set before an upload, as long as they are not read-only, using
the `blob_properties` transport parameter.

``` python
open_kwargs = {'predefined_acl': 'authenticated-read'}
properties = {'metadata': {'version': 2}, 'storage_class': 'COLDLINE'}
fout = open('gs://bucket/key', 'wb', transport_params={'blob_open_kwargs': open_kwargs, 'blob_properties': properties})
```

### Azure Credentials

`smart_open` uses the `azure-storage-blob` library to talk to Azure Blob
Storage. By default, `smart_open` will defer to `azure-storage-blob` and
let it take care of the credentials.

Azure Blob Storage does not have any ways of inferring credentials
therefore, passing a `azure.storage.blob.BlobServiceClient` object as a
transport parameter to the `open` function is required. You can
[customize the
credentials](https://docs.microsoft.com/en-us/azure/storage/common/storage-samples-python#authentication)
when constructing the client. `smart_open` will then use the client when
talking to. To follow allow with the example below, [refer to Azure's
guide](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal)
to setting up authentication.

``` python
import os
from azure.storage.blob import BlobServiceClient
azure_storage_connection_string = os.environ['AZURE_STORAGE_CONNECTION_STRING']
client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
fin = open('azure://my_container/my_blob.txt', transport_params={'client': client})
```

If you need more credential options, refer to the [Azure Storage
authentication
guide](https://docs.microsoft.com/en-us/azure/storage/common/storage-samples-python#authentication).

### Azure Advanced Usage

Additional keyword arguments can be propagated to the
`commit_block_list` method
([docs](https://azuresdkdocs.blob.core.windows.net/$web/python/azure-storage-blob/12.14.1/azure.storage.blob.html#azure.storage.blob.BlobClient.commit_block_list)),
which is used by `smart_open` under the hood for uploads, using the
`blob_kwargs` transport parameter.

``` python
kwargs = {'metadata': {'version': 2}}
fout = open('azure://container/key', 'wb', transport_params={'blob_kwargs': kwargs})
```

### Drop-in replacement of `pathlib.Path.open`

`smart_open.open` can also be used with `Path` objects. The built-in
<span class="title-ref">Path.open()</span> is not able to read text from
compressed files, so use `patch_pathlib` to replace it with <span
class="title-ref">smart_open.open()</span> instead. This can be helpful
when e.g. working with compressed files.

``` python
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
```

## How do I ...?

See [this document](howto.md).

## Extending `smart_open`

See [this document](extending.md).

## Testing `smart_open`

`smart_open` comes with a comprehensive suite of unit tests. Before you
can run the test suite, install the test dependencies:

    pip install -e .[test]

Now, you can run the unit tests:

    pytest smart_open

The tests are also run automatically with [Travis
CI](https://travis-ci.org/RaRe-Technologies/smart_open) on every commit
push & pull request.

## Comments, bug reports

`smart_open` lives on
[Github](https://github.com/RaRe-Technologies/smart_open). You can file
issues or pull requests there. Suggestions, pull requests and
improvements welcome!

------------------------------------------------------------------------

`smart_open` is open source software released under the [MIT
license](https://github.com/piskvorky/smart_open/blob/master/LICENSE).
Copyright (c) 2015-now [Radim Řehůřek](https://radimrehurek.com).
