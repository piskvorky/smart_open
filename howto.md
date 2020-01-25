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
