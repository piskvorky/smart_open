# Extending `smart_open`

This document targets potential contributors to `smart_open`.
Currently, there are two main directions for extending existing `smart_open` functionality:

1. Add a new transport mechanism
2. Add a new compression format

The first is by far the more challenging, and also the more welcome.

## New transport mechanisms

Each transport mechanism lives in its own submodule.
For example, currently we have:

- `smart_open.file`
- `smart_open.s3`
- `smart_open.ssh`
- ... and others

So, to implement a new transport mechanism, you need to create a new module.
Your module must expose the following:

```python
SCHEMA = ...
"""The name of the mechanism, e.g. s3, ssh, etc.

This is the part that goes before the `://` in a URL, e.g. `s3://`."""

URI_EXAMPLES = ('xxx://foo/bar', 'zzz://baz/boz')
"""This will appear in the documentation of the the `parse_uri` function."""


def parse_uri(uri_as_str):
    """Parse the specified URI into a dict.

    At a bare minimum, the dict must have `schema` member.
    """
    return dict(schema=XXX_SCHEMA, ...)


def open_uri(uri_as_str, mode, transport_params):
    """Return a file-like object pointing to the URI.

    Parameters:

    uri_as_str: str
        The URI to open
    mode: str
        Either "rb" or "wb".  You don't need to implement text modes,
        `smart_open` does that for you, outside of the transport layer.
    transport_params: dict
        Any additional parameters to pass to the `open` function (see below).

    """
    #
    # Parse the URI using parse_uri
    # Consolidate the parsed URI with transport_params, if needed
    # Pass everything to the open function (see below).
    #
    ...


def open(..., mode, param1=None, param2=None, paramN=None):
    """This function does the hard work.

    The keyword parameters are the transport_params from the `open_uri`
    function.

    """
    ...
```

Have a look at the existing mechanisms to see how they work.
You may define other functions and classes as necessary for your implementation.

Once your module is working, register it in the `smart_open/transport.py` file.
The `register_transport()` function updates a mapping from schemes to the modules that implement functionality for them.

Once you've registered your new transport module, the following will happen automagically:

1. `smart_open` will be able to open any URI supported by your module
2. The docstring for the `smart_open.open` function will contain a section
   detailing the parameters for your transport module.
3. The docstring for the `parse_uri` function will include the schemas and
   examples supported by your module.

You can confirm the documentation changes by running:

    python -c 'help("smart_open")'

and verify that documentation for your new submodule shows up.

### What's the difference between the `open_uri` and `open` functions?

There are several key differences between the two.

First, the parameters to `open_uri` are the same for _all transports_.
On the other hand, the parameters to the `open` function can differ from transport to transport.

Second, the responsibilities of the two functions are also different.
The `open` function opens the remote object.
The `open_uri` function deals with parsing transport-specific details out of the URI, and then delegates to `open`.

The `open` function contains documentation for transport parameters.
This documentation gets parsed by the `doctools` module and appears in various docstrings.

Some of these differences are by design; others as a consequence of evolution.

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
