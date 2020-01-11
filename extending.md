# Extending `smart_open`

This document targets potential contributors to `smart_open`.
Currently, there are two main directions for extending existing `smart_open` functionality:

1. Add a new transport mechanism
2. Add a new compression format

## New transport mechanisms

Each transport mechanism lives in its own submodule.
For example, currently we have:

- `smart_open.file`
- `smart_open.s3`
- `smart_open.ssh`
- ... and others

So, to implement a new transport mechanism, you need to create a new module.
Your module should expose the following:

```python
XXX_SCHEMA = ...
"""The name of the mechanism, e.g. s3, ssh, etc.

This is the part that goes before the `://` in a URL, e.g. `s3://`."""

def parse_uri(uri_as_str):
    """Parse the specified URI into a dict.

    At a bare minimum, the dict must have `schema` member.
    """
    return dict(schema=XXX_SCHEMA, ...)


def open_uri(uri_as_str, mode, transport_params):
    """Return a file-like object pointing to the URI."""
    ...
```

Have a look at the existing mechanisms to see how they work.
You may define other functions and classes as necessary for your implementation.

Once your module is working, register it in the `smart_open/smart_open_lib.py` file.
The `_generate_transport()` generator builds a dictionary that maps schemes to the modules that implement functionality for them.
Include your new mechanism in that generator, and `smart_open` will be able to use it.

## New compression mechanisms

The compression layer is self-contained in the `smart_open.compression` submodule.

To add support for a new compressor:

- Create a new function to handle your compression format (given an extension)
- Add your compressor to the registry

For example:

```python
def _handle_xz(file_obj, mode):
    import lzma
    return lzma.LZMAFile(filename=file_obj, mode=mode, format=lzma.FORMAT_XZ)

register_compressor('.xz', _handle_xz)
```

There are many compression formats out there, and supporting all of them is beyond the scope of `smart_open`.
We want our code's functionality to cover the bare minimum required to satisfy 80% of our users.
We leave the remaining 20% of users with the ability to deal with compression in their own code, using the trivial mechanism described above.
