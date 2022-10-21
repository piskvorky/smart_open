# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 Radim Rehurek <radim@rare-technologies.com>
# Author(s): Sridhar Balachandriah, Hewlett Packard Enterprise Development LP.
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

'''
Implements reading and writing to/from DAOS

The main entry point is the :func:`~smart_open.daos.open` function.

Uses the pyDAOS KV store under the covers.

'''

import io
import logging
import smart_open.utils

try:
    from pydaos import (DCont, DObjNotFound)
except ImportError:
    MISSING_DEPS = True

logger = logging.getLogger(__name__)

SCHEME = "daos"

URI_EXAMPLES = (
    'daos://daos_pool_name/daos_container_name/offset',
)


def parse_uri(uri_as_string):
    split_uri = smart_open.utils.safe_urlsplit(uri_as_string)
    assert split_uri.scheme in SCHEME

    uri_path = split_uri.netloc + split_uri.path
    values = uri_path.split('/')
    pool = values[0].lstrip()
    container = values[1].lstrip()
    offset = values[2].lstrip()

    if not uri_path:
        raise RuntimeError("invalid DAOS URI: %r" % uri_as_string)

    return dict(scheme=split_uri.scheme, pool=pool,
                container=container, offset=offset)


def open_uri(uri, mode, transport_params):
    parsed_uri = parse_uri(uri)
    return open(parsed_uri['pool'], parsed_uri['container'],
                parsed_uri['offset'], mode, transport_params)


def open(pool, container, offset, mode, transport_params):

    dict_name = "dict-pyDAOS"
    tparams = dict(transport_params)
    if 'dict_name' in tparams:
        dict_name = tparams['dict_name']
    else:
        dict_name = "dict-pyDAOS"

    if mode == 'rb':
        fobj = PYDAOSRead(pool, container, offset, dict_name)
        return fobj
    elif mode == 'wb':
        fobj = PYDAOSWrite(pool, container, offset, dict_name)
        return fobj
    else:
        raise NotImplementedError('daos support\
                          for mode %r not implemented' % mode)


class PYDAOSWrite(io.BufferedIOBase):
    '''
    Writes bytes to pyDAOS kv store.
    Implements the io.RawIOBase interface of the standard library.
    '''
    def __init__(self, pool, container, offset, dict_name):
        self._pool = pool
        self._cont = container
        self._dict_name = dict_name
        self._offset = offset

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    def flush(self):
        pass

    #
    # Override some methods from io.IOBase.
    #

    def close(self):
        logger.debug("successfully closed")

    def seekable(self):
        #
        # If False, seek(), tell() and truncate() will raise IOError.
        #
        return True

    def tell(self):
        # Return the current position within the file.
        return self._current_pos

    def write(self, b):
        daos_cont = DCont(self._pool, self._cont, None)
        try:
            daos_dict = daos_cont.get(self._dict_name)
        except DObjNotFound:
            daos_dict = daos_cont.dict(self._dict_name)

        daos_dict.put(self._offset, b)
        self._current_pos = len(b)
        return len(b)

    #
    # io.IOBase methods.
    #

    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")


class _PYDAOSRawReader(object):
    # Read from the pyDAOS KV Store Object

    def __init__(self, dobj, size):
        self._dobj = dobj
        self._size = size
        self._position = 0

    def seek(self, position):
        '''
        Seek to the position (byte offset) in the  pyDAOS KV store Object.
        :param int position: The byte offset from the beginning of the object.
        Returns the position after seeking.
        '''
        self._position = position
        return self._position

    def read(self, size=-1):
        if self._position >= self._size:
            return b''
        binary = self._download_dobj_chunk(size)
        self._position += len(binary)
        return binary

    def _download_dobj_chunk(self, size):
        if self._size == self._position:
            #
            # When reading, we can't seek to the first byte of an empty file.
            # Similarly, we can't seek past the last byte.  Do nothing here.
            #
            return b''
        elif size == -1:
            binary = self._dobj[self._position:-1]
        else:
            binary = self._dobj[self._position:size]
        return binary


class PYDAOSRead(io.BufferedIOBase):
    '''
    Reads bytes from DAOS KV Store.

    Implements the io.BufferedIOBase interface of the standard library.
    '''

    def __init__(self, pool, container, offset, dict_name, defer_seek=False):

        self._pool = pool
        self._cont = container
        self._dict_name = dict_name
        self._offset = offset

        daos_cont = DCont(self._pool, self._cont, None)
        try:
            daos_dict = daos_cont.get(self._dict_name)
        except DObjNotFound:
            daos_dict = daos_cont.dict(self._dict_name)

        self._obj = daos_dict[self._offset]
        self._position = 0
        self._current_part = smart_open.bytebuffer.ByteBuffer(len(self._obj))
        self._size = len(self._obj)
        self._current_pos = self._offset

        # _dobj is the key in the KV Store
        self._dobj = self._offset

        self._raw_reader = _PYDAOSRawReader(self._obj, self._size)
        self._position = 0
        self._current_part = smart_open.bytebuffer.ByteBuffer(len(self._obj))

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        logger.debug("close: called")
        self._dobj = None
        self._raw_reader = None
        self._obj = None
        self._position = 0
        self._current_part = None
        self._size = None
        self._current_pos = None

    def readable(self):
        # Return True if the stream can be read from.
        return True

    def seekable(self):
        #
        # If False, seek(), tell() and truncate() will raise IOError.
        # We offer only seek support, and no truncate support.
        #
        return True

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        # Unsupported.
        raise io.UnsupportedOperation

    def seek(self, offset, whence=smart_open.constants.WHENCE_START):
        #
        # Seek to the specified position.
        #
        # offset: The offset in bytes.
        # whence: Where the offset is from.
        #
        # Returns the position after seeking.
        #
        logger.debug('seeking to offset: %r whence: %r', offset, whence)

        if whence == smart_open.constants.WHENCE_START:
            new_position = offset
        elif whence == smart_open.constants.WHENCE_CURRENT:
            new_position = self._position + offset
        else:
            new_position = self._size + offset
        self._position = new_position
        self._raw_reader.seek(new_position)
        logger.debug('current_pos: %r', self._position)

        self._current_part.empty()
        return self._position

    def tell(self):
        # Return the current position within the file.
        return self._position

    def truncate(self, size=None):
        # Unsupported.
        raise io.UnsupportedOperation

    def read(self, size=-1):
        # Read up to size bytes from the object and return them.
        if size == 0:
            return b''
        elif size < 0:
            self._position = self._size
            return self._read_from_object() + self._raw_reader.read()

        #
        # Return unused data first
        #
        if len(self._current_part) >= size:
            return self._read_from_object(size)

        if self._position == self._size:
            return self._read_from_object()

        self._fill_buffer()
        return self._read_from_object(size)

    #
    # Internal methods.
    #
    def _read_from_object(self, size=-1):
        # Remove at most size bytes from our buffer and return them.
        size = size if size >= 0 else len(self._current_part)
        part = self._current_part.read(size)
        self._position += len(part)
        return part

    def _fill_buffer(self, size=-1):
        size = max(size, self._current_part._chunk_size)
        while len(self._current_part) <\
                (size and not self._position == self._size):
            bytes_read = self._current_part.fill(self._raw_reader)
            if bytes_read == 0:
                return True
