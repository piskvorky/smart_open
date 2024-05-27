# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 Sergei Sokolov <sv.sokolov@gmail.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements file-like objects for reading and writing from/to HUAWEI Object Storage Service (OBS)."""
from __future__ import annotations

import io
import logging
import os
import struct
import sys
from typing import Optional, Tuple, List

from smart_open.utils import set_defaults

try:
    import obs.client
    from obs.searchmethod import get_token
    from obs import loadtoken
except ImportError:
    MISSING_DEPS = True

import smart_open.bytebuffer
import smart_open.utils

from smart_open import constants

logger = logging.getLogger(__name__)

SCHEMES = ('obs',)

URI_EXAMPLES = (
    'obs://bucket_id.server:port/object_key',
)

DEFAULT_CHUNK_SIZE = 65536
DEFAULT_HTTP_PROTOCOL = 'https'
DEFAULT_SECURITY_PROVIDER_POLICY = 'ENV'

ENV_VAR_USE_CLIENT_WRITE_MODE = 'SMART_OPEN_OBS_USE_CLIENT_WRITE_MODE'
ENV_VAR_DECRYPT_AK_SK = 'SMART_OPEN_OBS_DECRYPT_AK_SK'
ENV_VAR_SCC_LIB_PATH = 'SMART_OPEN_OBS_SCC_LIB_PATH'
ENV_VAR_SCC_CONF_PATH = 'SMART_OPEN_OBS_SCC_CONF_PATH'

default_client_kwargs = {
    'security_provider_policy': DEFAULT_SECURITY_PROVIDER_POLICY,
}


def parse_uri(uri_as_string):
    split_uri = smart_open.utils.safe_urlsplit(uri_as_string)
    assert split_uri.scheme in SCHEMES

    bucket_id, server = split_uri.netloc.split('.', 1)
    object_key = split_uri.path[1:]

    return dict(
        scheme=split_uri.scheme,
        bucket_id=bucket_id,
        object_key=object_key,
        server=server,
    )


def open_uri(uri, mode, transport_params):
    parsed_uri = parse_uri(uri)
    kwargs = _prepare_open_kwargs(parsed_uri=parsed_uri,
                                  transport_params=transport_params)
    return open(parsed_uri['bucket_id'], parsed_uri['object_key'], mode, **kwargs)


def _prepare_open_kwargs(parsed_uri: dict, transport_params: dict) -> dict:
    kwargs = smart_open.utils.check_kwargs(open, transport_params)

    http_protocol = transport_params.get('http_protocol', DEFAULT_HTTP_PROTOCOL)
    client_kwargs = {
        'server': f'{http_protocol}://{parsed_uri["server"]}',
    }
    client_kwargs.update(default_client_kwargs)

    kwargs['client'] = transport_params.get('client', client_kwargs)

    default_kwarg = {
        'use_obs_client_write_mode':
            os.environ.get(ENV_VAR_USE_CLIENT_WRITE_MODE, 'false').lower() in ('true'),
        'decrypt_ak_sk':
            os.environ.get(ENV_VAR_DECRYPT_AK_SK, 'false').lower() in ('true'),
        'scc_lib_path':
            os.environ.get(ENV_VAR_SCC_LIB_PATH, None),
        'scc_conf_path':
            os.environ.get(ENV_VAR_SCC_CONF_PATH, None),
    }

    set_defaults(kwargs, default_kwarg)

    return kwargs


def open(
        bucket_id,
        object_key,
        mode,
        buffer_size=DEFAULT_CHUNK_SIZE,
        client: Optional[obs.ObsClient | dict] = None,
        headers: Optional[obs.PutObjectHeader | obs.GetObjectHeader] = None,
        use_obs_client_write_mode: bool = False,
        decrypt_ak_sk: bool = False,
        scc_lib_path: Optional[str] = None,
        scc_conf_path: Optional[str] = None):
    """Open an OBS object for reading or writing.

    Parameters
    ----------
    bucket_id: str
        The name of the bucket this object resides in.
    object_key: str
        The name of the key within the bucket.
    mode: str
        The mode for opening the object.  Must be either "rb" or "wb".
    buffer_size: int
        The buffer size to use when performing I/O.
    client: Optional[obs.ObsClient | dict]
        The initialized OBS client or dict with args that will be supplied to obs.ObsClient constructor.
        Please see docs for esdk-obs-python.
    headers: Optional[List[Tuple]]
        The optional additional headers of the request.
        Please see docs for esdk-obs-python.
    use_obs_client_write_mode: bool
        True if we will use readable object to get bytes. For writing mode only.
        Please see docs for ObsClient.putContent api
    decrypt_ak_sk: bool
        True if we need decrypt Access key, Secret key and Security token.
        It required to install CryptoAPI libs.
        https://support.huawei.com/enterprise/en/software/260510077-ESW2000847337
    scc_lib_path: Optional[str]
        The path to CryptoAPI libs.
    scc_conf_path: Optional[str]
        The path to scc.conf.
    """

    logger.debug('%r', locals())
    if mode not in constants.BINARY_MODES:
        raise NotImplementedError('bad mode: %r expected one of %r' % (mode, constants.BINARY_MODES))

    _client = client if isinstance(client, obs.ObsClient) else create_obs_client(
        client_config=client,
        decrypt_ak_sk=decrypt_ak_sk,
        scc_lib_path=scc_lib_path,
        scc_conf_path=scc_conf_path)

    if mode == constants.READ_BINARY:
        fileobj = ObsReader(bucket_id=bucket_id,
                            object_key=object_key,
                            client=_client,
                            headers=headers)
    elif mode == constants.WRITE_BINARY:
        fileobj = ObsWriter(bucket_id=bucket_id,
                            object_key=object_key,
                            client=_client,
                            headers=headers,
                            use_obs_client_write_mode=use_obs_client_write_mode)
    else:
        assert False, 'unexpected mode: %r' % mode
    return fileobj


def create_obs_client(client_config: dict,
                      decrypt_ak_sk: bool = False,
                      scc_lib_path: Optional[str] = None,
                      scc_conf_path: Optional[str] = None) -> obs.ObsClient:
    """Initializes the ObsClient.
    """
    if not decrypt_ak_sk:
        return obs.ObsClient(**client_config)

    decrypted_config = _decrypt_ak_sk(client_config=client_config,
                                      scc_lib_path=scc_lib_path,
                                      scc_conf_path=scc_conf_path)

    set_defaults(decrypted_config, client_config)
    return obs.ObsClient(**decrypted_config)


def _decrypt_ak_sk(client_config: dict,
                   scc_lib_path: Optional[str] = None,
                   scc_conf_path: Optional[str] = None) -> dict:
    crypto_provider = CryptoProvider(scc_lib_path=scc_lib_path,
                                     scc_conf_path=scc_conf_path)

    if 'access_key_id' in client_config:
        access_key_id = client_config.get('access_key_id')
        secret_access_key = client_config.get('secret_access_key')
        security_token = client_config.get('security_token', None)
    else:
        tokens = get_token(security_providers=loadtoken.ENV)
        access_key_id = tokens.get('accessKey')
        secret_access_key = tokens.get('secretKey')
        security_token = tokens.get('securityToken')

    return {
        access_key_id: crypto_provider.decrypt(access_key_id),
        secret_access_key: crypto_provider.decrypt(secret_access_key),
        security_token: crypto_provider.decrypt(security_token),
    }


class ObsReader(io.RawIOBase):
    """Read an OBS Object.
    """

    def __init__(self,
                 bucket_id: str,
                 object_key: str,
                 client: obs.ObsClient,
                 headers: Optional[obs.GetObjectHeader] = None,
                 buffer_size: int = DEFAULT_CHUNK_SIZE):
        self.name = object_key
        self.bucket_id = bucket_id
        self.object_key = object_key
        self.buffer_size = buffer_size
        self._client = client
        self._buffer = smart_open.bytebuffer.ByteBuffer(buffer_size)
        self._resp = self._client.getObject(bucketName=bucket_id,
                                            objectKey=object_key,
                                            headers=headers)
        if self._resp.status >= 300:
            raise RuntimeError(
                f'Failed to read: {self.object_key}! '
                f'errorCode: {self._resp.errorCode}, '
                f'errorMessage: {self._resp.errorMessage}')

    def readinto(self, __buffer):
        data = self.read(len(__buffer))
        if not data:
            return 0
        __buffer[:len(data)] = data
        return len(data)

    def readinto1(self, __buffer):
        return self.readinto(__buffer)

    def read(self, size=-1):
        if size == 0:
            return b''

        if self._resp is None:
            raise RuntimeError(f'No response received while reading: {self.object_key}')

        if size > 0:
            chunk = self._resp.body.response.read(size)
            return chunk
        else:
            while True:
                chunk = self._resp.body.response.read(self.buffer_size)
                if not chunk:
                    break
                self._buffer.fill(struct.unpack(str(len(chunk)) + 'c', chunk))
            return self._buffer.read()

    def read1(self, size=-1):
        return self.read(size)

    def close(self):
        self.__del__()

    def seekable(self):
        return False

    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def __del__(self):
        try:
            if self._client:
                self._resp = None
                self._client.close()
                self._client = None
        except Exception as ex:
            logger.warning(ex)


class ObsWriter(io.RawIOBase):
    """Write an OBS Object.

    If use_obs_client_write_mode set to False:
    this class buffers all of its input in memory until its `close` method is called.
    Only then the data will be written to OBS and the buffer is released.

    If use_obs_client_write_mode set to True:
    `write` method of the ObsWriter will accept any readable object or path to file.
    In this case will be used internal implementation in obs.ObsClient.putContent to read bytes
    Write to OBS will be triggered in `close` method.
    """

    def __init__(self,
                 bucket_id: str,
                 object_key: str,
                 client: obs.ObsClient,
                 headers: Optional[obs.PutObjectHeader] = None,
                 use_obs_client_write_mode: bool = False
                 ):
        self.name = object_key
        self.bucket_id = bucket_id
        self.object_key = object_key
        self._client = client
        self._headers = headers
        self._content: Optional[str | io.BytesIO | io.BufferedReader] = None
        self.use_obs_client_write_mode = use_obs_client_write_mode

    def write(self, __buffer):
        if not __buffer:
            return None

        if self.use_obs_client_write_mode:
            self._content = __buffer
        else:
            if not self._content:
                self._content = io.BytesIO()
            self._content.write(__buffer)
        return None

    def close(self):
        if not self._content:
            self._client.close()
            return

        if isinstance(self._content, io.BytesIO):
            self._content.seek(0)

        self._client.putContent(bucketName=self.bucket_id,
                                objectKey=self.object_key,
                                content=self._content,
                                headers=self._headers)
        self._content = None

    def seekable(self):
        return False

    def writable(self):
        return self._content is not None

    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation


class CryptoProvider:
    """Decrypt Access Key, Secret Key, Security Token.

    This class use Huawei CloudGuard CSP seccomponent to decrypt AK, SK and ST.
    """

    def __init__(self, scc_lib_path: Optional[str] = None, scc_conf_path: Optional[str] = None):
        self._scc_lib_path = scc_lib_path
        self._scc_conf_path = scc_conf_path

        if scc_lib_path and scc_lib_path not in sys.path:
            sys.path.append(scc_lib_path)

        try:
            from CryptoAPI import CryptoAPI
        except ImportError:
            raise RuntimeError('Failed to use CryptoAPI module. Please install CloudGuard CSP seccomponent.')

        self._api = CryptoAPI()

        if self._scc_conf_path:
            self._api.initialize(self._scc_conf_path)
        else:
            self._api.initialize()

    def __del__(self):
        if self._api:
            self._api.finalize()

    def decrypt(self, encrypted: Optional[str]) -> Optional[str]:
        return self._api.decrypt(encrypted) if encrypted else None
