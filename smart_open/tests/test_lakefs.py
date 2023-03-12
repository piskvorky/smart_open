# -*- coding: utf-8 -*-
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import typing
import pytest
import lakefs_client
from lakefs_client import client, models, apis
import logging
from smart_open.lakefs import Reader


"""It needs docker compose to run lakefs locally:
https://docs.lakefs.io/quickstart/run.html

curl https://compose.lakefs.io | docker-compose -f - up
"""
_LAKEFS_HOST = "http://localhost:8000/api/v1"

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def lakefs():
    import shlex
    import subprocess
    from urllib3.exceptions import MaxRetryError

    compose = subprocess.Popen(
        shlex.split("curl https://compose.lakefs.io"), stdout=subprocess.PIPE
    )
    subprocess.Popen(shlex.split("docker-compose -f - up -d"), stdin=compose.stdout)
    compose.stdout.close()

    configuration = lakefs_client.Configuration(_LAKEFS_HOST)
    lfs_client = client.LakeFSClient(configuration)

    healthcheck: apis.HealthCheckApi = lfs_client.healthcheck
    api_available = False
    while not api_available:
        try:
            healthcheck.health_check()
            api_available = True
        except (lakefs_client.ApiException, MaxRetryError):
            continue

    comm_prefs = models.CommPrefsInput(
        email="test@company.com",
        feature_updates=True,
        security_updates=True,
    )
    username = models.Setup(username="admin")
    try:
        config: apis.ConfigApi = lfs_client.config
        _ = config.setup_comm_prefs(comm_prefs)
        credentials: models.CredentialsWithSecret = config.setup(username)
    except lakefs_client.ApiException as e:
        raise Exception(
            "Error setting up lakefs: %s\n" % e
        ) from lakefs_client.ApiException
    configuration = lakefs_client.Configuration(
        host=_LAKEFS_HOST,
        username=credentials.access_key_id,
        password=credentials.secret_access_key,
    )
    lfs_client = client.LakeFSClient(configuration)

    repositories_api: apis.RepositoriesApi = lfs_client.repositories
    new_repo = models.RepositoryCreation(
        name="repo", storage_namespace="local:///home/lakefs/", default_branch="main"
    )
    try:
        _: models.Repository = repositories_api.create_repository(new_repo)
    except lakefs_client.ApiException as e:
        raise Exception("Error creating repository: %s\n" % e) from e

    yield lfs_client

    compose = subprocess.Popen(
        shlex.split("curl https://compose.lakefs.io"), stdout=subprocess.PIPE
    )
    subprocess.Popen(shlex.split("docker-compose -f - down"), stdin=compose.stdout)
    compose.stdout.close()


@pytest.mark.parametrize(
    "uri, parsed",
    [
        ("lakefs://REPO/REF/file", dict(scheme='lakefs', repo='REPO', ref='REF', key='file')),
        ("lakefs://REPO/REF/1/file", dict(scheme='lakefs', repo='REPO', ref='REF', key='1/file')),
        pytest.param(
            "lakefs://REPO/REF/1/file", dict(scheme='lakefs', repo='REPO', ref='REF/1', key='file'),
            marks=pytest.mark.xfail),
    ]
    )
def test_parse_uri(uri, parsed):
    from smart_open.lakefs import parse_uri
    assert parsed == parse_uri(uri)


class TestReader:

    @pytest.fixture(scope="module")
    def repo(self, lakefs) -> models.Repository:
        repositories_api: apis.RepositoriesApi = lakefs.repositories
        return repositories_api.list_repositories().results[0]

    @pytest.fixture(scope="module")
    def put_to_repo(
            self,
            lakefs,
            repo: models.Repository,
        ) -> typing.Callable:

        def _put_to_repo(
            path: str,
            content: bytes,
            branch: str | None = None,
        ) -> typing.IO:
            from io import BytesIO

            objects: apis.ObjectsApi = lakefs.objects
            _branch = branch if branch else repo.default_branch
            stream = BytesIO(content)
            stream.name = path
            try:
                _ = objects.upload_object(repo.id, _branch, path, content=stream)
            except lakefs_client.ApiException as e:
                raise Exception("Error uploading object: %s\n" % e) from e
            return path, content

        return _put_to_repo

    @pytest.fixture(scope="module")
    def file(self, put_to_repo) -> typing.IO:
        path = "test/file.txt"
        content = "hello wořld\nhow are you?".encode("utf8")
        return put_to_repo(path, content)


    def test_iter(self, lakefs, repo, file):
        path, content = file
        fin = Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path)
        output = [line.rstrip(b"\n") for line in fin]
        assert output == content.split(b"\n")

    def test_iter_context_manager(self, lakefs, repo, file):
        path, content = file
        with Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path) as fin:
            output = [line.rstrip(b"\n") for line in fin]
        assert output == content.split(b"\n")

    def test_read(self, lakefs, repo, file):
        path, content = file
        fin = Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path)
        assert content[:6] == fin.read(6)
        assert content[6:6+8] == fin.read1(8)
        assert content[6+8:] == fin.read()

    def test_readinto(self, lakefs, repo, file):
        path, content = file
        fin = Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path)
        b = bytearray(6)
        assert len(b) == fin.readinto(b)
        assert content[:6] == b
        assert len(b) == fin.readinto1(b)
        assert content[6:6+6] == b

    def test_seek_beginning(self, lakefs, repo, file):
        path, content = file
        fin = Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path)
        assert content[:6] == fin.read(6)
        assert content[6:6+8] == fin.read(8)
        fin.seek(0)
        assert content == fin.read()
        fin.seek(0)
        assert content == fin.read(-1)

    def test_seek_start(self, lakefs, repo, file):
        path, _ = file
        fin = Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path)
        assert fin.seek(6) == 6
        assert fin.tell() == 6
        assert fin.read(6) == u'wořld'.encode('utf-8')

    def test_seek_current(self, lakefs, repo, file):
        from smart_open import constants

        path, _ = file
        fin = Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path)
        assert fin.read(5) == b'hello'
        assert fin.seek(1, whence=constants.WHENCE_CURRENT) == 6
        assert fin.read(6) == u'wořld'.encode('utf-8')

    def test_seek_end(self, lakefs, repo, file):
        from smart_open import constants

        path, content = file
        fin = Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path)
        assert fin.seek(-4, whence=constants.WHENCE_END) == len(content) - 4
        assert fin.read() == b'you?'

    def test_seek_past_end(self, lakefs, repo, file):
        path, content = file
        fin = Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path)
        assert fin.seek(60) == len(content)

    def test_detect_eof(self, lakefs, repo, file):
        from smart_open import constants

        path, content = file
        fin = Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path)
        fin.read()
        eof = fin.tell()
        assert eof == len(content)
        fin.seek(0, whence=constants.WHENCE_END)
        assert fin.tell() == eof
        fin.seek(eof)
        assert fin.tell() == eof

    def test_read_gzip(self, lakefs, repo, put_to_repo):
        from io import BytesIO
        import gzip

        expected = u'раcцветали яблони и груши, поплыли туманы над рекой...'.encode('utf-8')
        buf = BytesIO()
        buf.close = lambda: None  # keep buffer open so that we can .getvalue()
        with gzip.GzipFile(fileobj=buf, mode='w') as zipfile:
            zipfile.write(expected)
        path = "zip/file.zip"
        _ = put_to_repo(path, buf.getvalue())

        #
        # Make sure we're reading things correctly.
        #
        with Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path) as fin:
            assert fin.read() == buf.getvalue()

        #
        # Make sure the buffer we wrote is legitimate gzip.
        #
        sanity_buf = BytesIO(buf.getvalue())
        with gzip.GzipFile(fileobj=sanity_buf) as zipfile:
            assert zipfile.read() == expected

        logger.debug('starting actual test')
        with Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path) as fin:
            with gzip.GzipFile(fileobj=fin) as zipfile:
                assert zipfile.read() == expected

    def test_readline(self, lakefs, repo, put_to_repo):
        content = b'englishman\nin\nnew\nyork\n'
        path = "many_lines.txt"
        _ = put_to_repo(path, content)
        with Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path) as fin:
            fin.readline()
            assert fin.tell() == content.index(b'\n')+1
            fin.seek(0)
            assert list(fin) == [b'englishman\n', b'in\n', b'new\n', b'york\n']
            assert fin.tell() == len(content)

    def test_readline_tiny_buffer(self, lakefs, repo, put_to_repo):
        content = b'englishman\nin\nnew\nyork\n'
        path = "many_lines.txt"
        _ = put_to_repo(path, content)
        with Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path, buffer_size=8) as fin:
            assert list(fin) == [b'englishman\n', b'in\n', b'new\n', b'york\n']
            assert fin.tell() == len(content)

    def test_read0_does_not_return_data(self, lakefs, repo, file):
        path, _ = file
        with Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path) as fin:
            assert fin.read(0) == b''

    def test_read_past_end(self, lakefs, repo, file):
        path, content = file
        with Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path) as fin:
            assert fin.read(100) == content

    def test_read_empty_file(self, lakefs, repo, put_to_repo):
        content = b''
        path = "empty_file.txt"
        _ = put_to_repo(path, content)
        with Reader(client=lakefs, repo=repo.id, ref=repo.default_branch, path=path, buffer_size=8) as fin:
            assert fin.read() == b''
