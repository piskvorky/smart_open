import io
import os
import unittest
import uuid
from unittest.mock import patch

import obs

import smart_open
from smart_open.obs import ObsReader

BUCKET_ID = 'test-smartopen-{}'.format(uuid.uuid4().hex)
OBJECT_KEY = 'hello.txt'


class ReadTest(unittest.TestCase):

    def setUp(self):
        self.test_string = u'ветер по морю гуляет...'

        response_wrapper = obs.model.ResponseWrapper(conn=None,
                                                     connHolder=None,
                                                     result=io.BytesIO(self.test_string.encode('utf-8')))
        body = obs.model.ObjectStream(response=response_wrapper)
        self.response = obs.model.GetResult(status=200, body=body)

    def test_read_never_returns_none(self):
        with patch.object(obs.ObsClient, 'getObject', return_value=self.response):
            reader = ObsReader(bucket_id=BUCKET_ID, object_key=OBJECT_KEY,
                               client=obs.ObsClient(server='server'))

            self.assertEqual(reader.read(), self.test_string.encode("utf-8"))
            self.assertEqual(reader.read(), b'')
            self.assertEqual(reader.read(), b'')


class WriteTest(unittest.TestCase):

    def setUp(self):
        self.texst_text = 'ветер по морю гуляет...'
        response_wrapper = obs.model.ResponseWrapper(conn=None, connHolder=None, result=io.BytesIO(b'ok'))
        body = obs.model.ObjectStream(response=response_wrapper)
        self.response = obs.model.GetResult(status=200, body=body)

    def test_write(self):
        with patch.object(obs.ObsClient, 'putContent', return_value=self.response) as mock_method:
            writer = smart_open.obs.ObsWriter(bucket_id=BUCKET_ID,
                                              object_key=OBJECT_KEY,
                                              client=obs.ObsClient(server='server'),
                                              headers=obs.PutObjectHeader(contentType='text/plain'))
            writer.write(u'ветер по морю '.encode('utf-8'))
            writer.write(u'гуляет...'.encode('utf-8'))
            writer.close()

        kwargs = mock_method.call_args.kwargs
        self.assertEqual(kwargs['bucketName'], BUCKET_ID)
        self.assertEqual(kwargs['objectKey'], OBJECT_KEY)
        self.assertEqual(kwargs['headers']['contentType'], 'text/plain')
        self.assertEqual(kwargs['content'].read(), self.texst_text.encode('utf-8'))

    def test_write_use_obs_client_write_mode(self):
        test_bytes = io.BytesIO(self.texst_text.encode('utf-8'))

        with patch.object(obs.ObsClient, 'putContent', return_value=self.response) as mock_method:
            writer = smart_open.obs.ObsWriter(bucket_id=BUCKET_ID,
                                              object_key=OBJECT_KEY,
                                              client=obs.ObsClient(server='server'),
                                              headers=obs.PutObjectHeader(contentType='text/plain'),
                                              use_obs_client_write_mode=True)
            writer.write(test_bytes)
            writer.close()

        kwargs = mock_method.call_args.kwargs
        self.assertEqual(kwargs['bucketName'], BUCKET_ID)
        self.assertEqual(kwargs['objectKey'], OBJECT_KEY)
        self.assertEqual(kwargs['headers']['contentType'], 'text/plain')
        self.assertEqual(kwargs['content'].read(), self.texst_text.encode('utf-8'))
        self.assertEqual(id(kwargs['content']), id(test_bytes))


class PrepareOpenKwargsTest(unittest.TestCase):
    def setUp(self):
        self.parsed_uri = dict(
            scheme='obs',
            bucket_id='bucket_id',
            object_key='object_key',
            server='server',
        )

    def tearDown(self):
        if os.environ.get(smart_open.obs.ENV_VAR_USE_CLIENT_WRITE_MODE, None):
            del os.environ[smart_open.obs.ENV_VAR_USE_CLIENT_WRITE_MODE]
        if os.environ.get(smart_open.obs.ENV_VAR_DECRYPT_AK_SK, None):
            del os.environ[smart_open.obs.ENV_VAR_DECRYPT_AK_SK]
        if os.environ.get(smart_open.obs.ENV_VAR_SCC_LIB_PATH, None):
            del os.environ[smart_open.obs.ENV_VAR_SCC_LIB_PATH]
        if os.environ.get(smart_open.obs.ENV_VAR_SCC_CONF_PATH, None):
            del os.environ[smart_open.obs.ENV_VAR_SCC_CONF_PATH]

    def test_prepare_open_kwargs_defaults(self):
        transport_parpams = {}
        actual = smart_open.obs._prepare_open_kwargs(parsed_uri=self.parsed_uri,
                                                     transport_params=transport_parpams)

        self.assertEqual(actual['decrypt_ak_sk'], False)
        self.assertIsNone(actual['scc_lib_path'])
        self.assertIsNone(actual['scc_conf_path'])
        self.assertIsNotNone(actual.get('client', None))
        self.assertEqual(actual['client'].get('server', None), f'https://{self.parsed_uri["server"]}')
        self.assertEqual(actual['client'].get('security_provider_policy', None), 'ENV')
        self.assertFalse(actual.get('use_obs_client_write_mode'))

    def test_prepare_open_kwargs_override(self):
        transport_parpams = {
            'decrypt_ak_sk': True,
            'scc_lib_path': 'scc_lib_path',
            'scc_conf_path': 'scc_conf_path',
            'use_obs_client_write_mode': True,
            'client': {
                'security_provider_policy': 'ECS',
                'server': 'https://server1'
            }
        }

        actual = smart_open.obs._prepare_open_kwargs(parsed_uri=self.parsed_uri,
                                                     transport_params=transport_parpams)

        self.assertEqual(actual['decrypt_ak_sk'], True)
        self.assertEqual(actual['scc_lib_path'], 'scc_lib_path')
        self.assertEqual(actual['scc_conf_path'], 'scc_conf_path')
        self.assertIsNotNone(actual.get('client', None))
        self.assertEqual(actual['client'].get('server', None), 'https://server1')
        self.assertEqual(actual['client'].get('security_provider_policy', None), 'ECS')
        self.assertTrue(actual.get('use_obs_client_write_mode'))

    def test_prepare_open_kwargs_override_env(self):
        os.environ[smart_open.obs.ENV_VAR_USE_CLIENT_WRITE_MODE] = 'True'
        os.environ[smart_open.obs.ENV_VAR_DECRYPT_AK_SK] = 'True'
        os.environ[smart_open.obs.ENV_VAR_SCC_LIB_PATH] = 'scc_lib_path'
        os.environ[smart_open.obs.ENV_VAR_SCC_CONF_PATH] = 'scc_conf_path'

        transport_parpams = {}

        actual = smart_open.obs._prepare_open_kwargs(parsed_uri=self.parsed_uri,
                                                     transport_params=transport_parpams)

        self.assertEqual(actual['decrypt_ak_sk'], True)
        self.assertEqual(actual['scc_lib_path'], 'scc_lib_path')
        self.assertEqual(actual['scc_conf_path'], 'scc_conf_path')
        self.assertIsNotNone(actual.get('client', None))
        self.assertEqual(actual['client'].get('server', None), f'https://{self.parsed_uri["server"]}')
        self.assertEqual(actual['client'].get('security_provider_policy', None), 'ENV')
        self.assertTrue(actual.get('use_obs_client_write_mode'))
