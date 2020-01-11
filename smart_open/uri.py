import collections


Uri = collections.namedtuple(
    'Uri',
    (
        'scheme',
        'uri_path',
        'bucket_id',
        'key_id',
        'port',
        'host',
        'ordinary_calling_format',
        'access_id',
        'access_secret',
        'user',
        'password',
    )
)
"""Represents all the options that we parse from user input.

Some of the above options only make sense for certain protocols, e.g.
bucket_id is only for S3.
"""
#
# Set the default values for all Uri fields to be None.  This allows us to only
# specify the relevant fields when constructing a Uri.
#
# https://stackoverflow.com/questions/11351032/namedtuple-and-default-values-for-optional-keyword-arguments
#
Uri.__new__.__defaults__ = (None,) * len(Uri._fields)
