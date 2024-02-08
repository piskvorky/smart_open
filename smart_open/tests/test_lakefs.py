# -*- coding: utf-8 -*-
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
from __future__ import annotations

import logging
from typing import Callable, Generator

import pytest
from lakefs_client import apis, configuration, exceptions, models
from lakefs_client import client as lfs_client

from smart_open.lakefs import open

"""It needs docker compose to run lakefs locally:
https://docs.lakefs.io/quickstart/run.html

curl https://compose.lakefs.io | docker-compose -f - up
"""
_LAKEFS_HOST = "http://localhost:8000/api/v1"

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def lakefs() -> Generator[lfs_client.LakeFSClient, None, None]:
    import os
    import shlex
    import subprocess

    from urllib3.exceptions import MaxRetryError

    cwd = os.path.dirname(os.path.realpath(__file__))
    subprocess.Popen(shlex.split("docker compose up -d"), cwd=cwd)

    conf = configuration.Configuration(_LAKEFS_HOST)
    client = lfs_client.LakeFSClient(conf)

    healthcheck: apis.HealthCheckApi = client.healthcheck
    api_available = False
    while not api_available:
        try:
            healthcheck.health_check()
            api_available = True
        except (exceptions.ApiException, MaxRetryError):
            continue

    comm_prefs = models.CommPrefsInput(
        email="test@company.com",
        feature_updates=True,
        security_updates=True,
    )
    username = models.Setup(username="admin")
    try:
        config: apis.ConfigApi = client.config
        _ = config.setup_comm_prefs(comm_prefs)
        credentials: models.CredentialsWithSecret = config.setup(username)
    except exceptions.ApiException as e:
        raise Exception("Error setting up lakefs: %s\n" % e) from e
    conf = configuration.Configuration(
        host=_LAKEFS_HOST,
        username=credentials.access_key_id,
        password=credentials.secret_access_key,
    )
    os.environ["LAKECTL_SERVER_ENDPOINT_URL"] = _LAKEFS_HOST
    os.environ["LAKECTL_CREDENTIALS_ACCESS_KEY_ID"] = credentials.access_key_id
    os.environ["LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY"] = credentials.secret_access_key

    client = lfs_client.LakeFSClient(conf)

    repositories_api: apis.RepositoriesApi = client.repositories
    new_repo = models.RepositoryCreation(
        name="repo", storage_namespace="local:///home/lakefs/", default_branch="main"
    )
    try:
        _: models.Repository = repositories_api.create_repository(new_repo)
    except exceptions.ApiException as e:
        raise Exception("Error creating repository: %s\n" % e) from e

    yield client

    subprocess.Popen(shlex.split("docker compose down"), cwd=cwd)


@pytest.fixture(scope="module")
def repo(lakefs) -> models.Repository:
    repositories_api: apis.RepositoriesApi = lakefs.repositories
    return repositories_api.list_repositories().results[0]


@pytest.mark.parametrize(
    "uri, parsed",
    [
        (
            "lakefs://REPO/REF/file",
            dict(scheme="lakefs", repo="REPO", ref="REF", key="file"),
        ),
        (
            "lakefs://REPO/REF/1/file",
            dict(scheme="lakefs", repo="REPO", ref="REF", key="1/file"),
        ),
        pytest.param(
            "lakefs://REPO/REF/1/file",
            dict(scheme="lakefs", repo="REPO", ref="REF/1", key="file"),
            marks=pytest.mark.xfail,
        ),
    ],
)
def test_parse_uri(uri, parsed):
    from dataclasses import asdict

    from smart_open.lakefs import parse_uri

    assert parsed == asdict(parse_uri(uri))


class TestReader:
    @pytest.fixture(scope="module")
    def put_to_repo(
        self,
        lakefs,
        repo: models.Repository,
    ) -> Callable:
        def _put_to_repo(
            path: str,
            content: bytes,
            branch: str | None = None,
        ) -> tuple[str, bytes]:
            from io import BytesIO

            objects: apis.ObjectsApi = lakefs.objects
            _branch = branch if branch else repo.default_branch
            stream = BytesIO(content)
            stream.name = path
            try:
                _ = objects.upload_object(repo.id, _branch, path, content=stream)
            except exceptions.ApiException as e:
                raise Exception("Error uploading object: %s\n" % e) from e
            return path, content

        return _put_to_repo

    @pytest.fixture(scope="module")
    def file(self, put_to_repo) -> tuple[str, bytes]:
        path = "test/file.txt"
        content = "hello wořld\nhow are you?".encode("utf8")
        return put_to_repo(path, content)

    def test_iter(self, lakefs, repo, file):
        path, content = file
        fin = open(repo.id, repo.default_branch, path, "rb", lakefs)
        output = [line.rstrip(b"\n") for line in fin]
        assert output == content.split(b"\n")

    def test_iter_context_manager(self, lakefs, repo, file):
        path, content = file
        with open(repo.id, repo.default_branch, path, "rb", lakefs) as fin:
            output = [line.rstrip(b"\n") for line in fin]
        assert output == content.split(b"\n")

    def test_read(self, lakefs, repo, file):
        path, content = file
        fin = open(repo.id, repo.default_branch, path, "rb", lakefs)
        assert content[:6] == fin.read(6)
        assert content[6:6 + 8] == fin.read1(8)
        assert content[6 + 8:] == fin.read()

    def test_readinto(self, lakefs, repo, file):
        path, content = file
        fin = open(repo.id, repo.default_branch, path, "rb", lakefs)
        b = bytearray(6)
        assert len(b) == fin.readinto(b)
        assert content[:6] == b
        assert len(b) == fin.readinto1(b)
        assert content[6:6 + 6] == b

    def test_seek_beginning(self, lakefs, repo, file):
        path, content = file
        fin = open(repo.id, repo.default_branch, path, "rb", lakefs)
        assert content[:6] == fin.read(6)
        assert content[6:6 + 8] == fin.read(8)
        fin.seek(0)
        assert content == fin.read()
        fin.seek(0)
        assert content == fin.read(-1)

    def test_seek_start(self, lakefs, repo, file):
        path, _ = file
        fin = open(repo.id, repo.default_branch, path, "rb", lakefs)
        assert fin.seek(6) == 6
        assert fin.tell() == 6
        assert fin.read(6) == "wořld".encode("utf-8")

    def test_seek_current(self, lakefs, repo, file):
        from smart_open import constants

        path, _ = file
        fin = open(repo.id, repo.default_branch, path, "rb", lakefs)
        assert fin.read(5) == b"hello"
        assert fin.seek(1, constants.WHENCE_CURRENT) == 6
        assert fin.read(6) == "wořld".encode("utf-8")

    def test_seek_end(self, lakefs, repo, file):
        from smart_open import constants

        path, content = file
        fin = open(repo.id, repo.default_branch, path, "rb", lakefs)
        assert fin.seek(-4, constants.WHENCE_END) == len(content) - 4
        assert fin.read() == b"you?"

    def test_seek_past_end(self, lakefs, repo, file):
        path, content = file
        fin = open(repo.id, repo.default_branch, path, "rb", lakefs)
        assert fin.seek(60) == len(content)

    def test_detect_eof(self, lakefs, repo, file):
        from smart_open import constants

        path, content = file
        fin = open(repo.id, repo.default_branch, path, "rb", lakefs)
        fin.read()
        eof = fin.tell()
        assert eof == len(content)
        fin.seek(0, constants.WHENCE_END)
        assert fin.tell() == eof
        fin.seek(eof)
        assert fin.tell() == eof

    def test_read_gzip(self, lakefs, repo, put_to_repo):
        import gzip
        from io import BytesIO

        expected = "раcцветали яблони и груши, поплыли туманы над рекой...".encode(
            "utf-8"
        )
        buf = BytesIO()
        buf.close = lambda: None  # keep buffer open so that we can .getvalue()
        with gzip.GzipFile(fileobj=buf, mode="w") as zipfile:
            zipfile.write(expected)
        path = "zip/file.zip"
        _ = put_to_repo(path, buf.getvalue())

        #
        # Make sure we're reading things correctly.
        #
        with open(repo.id, repo.default_branch, path, "rb", lakefs) as fin:
            assert fin.read() == buf.getvalue()

        #
        # Make sure the buffer we wrote is legitimate gzip.
        #
        sanity_buf = BytesIO(buf.getvalue())
        with gzip.GzipFile(fileobj=sanity_buf) as zipfile:
            assert zipfile.read() == expected

        with open(repo.id, repo.default_branch, path, "rb", lakefs) as fin:
            with gzip.GzipFile(fileobj=fin) as zipfile:
                assert zipfile.read() == expected

    def test_readline(self, lakefs, repo, put_to_repo):
        content = b"englishman\nin\nnew\nyork\n"
        path = "many_lines.txt"
        _ = put_to_repo(path, content)
        with open(repo.id, repo.default_branch, path, "rb", lakefs) as fin:
            fin.readline()
            assert fin.tell() == content.index(b"\n") + 1
            fin.seek(0)
            assert list(fin) == [b"englishman\n", b"in\n", b"new\n", b"york\n"]
            assert fin.tell() == len(content)

    def test_readline_tiny_buffer(self, lakefs, repo, put_to_repo):
        content = b"englishman\nin\nnew\nyork\n"
        path = "many_lines.txt"
        _ = put_to_repo(path, content)
        with open(
            repo.id, repo.default_branch, path, "rb", lakefs, buffer_size=8
        ) as fin:
            assert list(fin) == [b"englishman\n", b"in\n", b"new\n", b"york\n"]
            assert fin.tell() == len(content)

    def test_read0_does_not_return_data(self, lakefs, repo, file):
        path, _ = file
        with open(repo.id, repo.default_branch, path, "rb", lakefs) as fin:
            assert fin.read(0) == b""

    def test_read_past_end(self, lakefs, repo, file):
        path, content = file
        with open(repo.id, repo.default_branch, path, "rb", lakefs) as fin:
            assert fin.read(100) == content

    def test_read_empty_file(self, lakefs, repo, put_to_repo):
        content = b""
        path = "empty_file.txt"
        _ = put_to_repo(path, content)
        with open(
            repo.id, repo.default_branch, path, "rb", lakefs, buffer_size=8
        ) as fin:
            assert fin.read() == b""

    def test_open_with_transport_params(self, lakefs, repo, file):
        from smart_open import open

        path, content = file
        transport_params = {"client": lakefs}
        uri = f"lakefs://{repo.id}/{repo.default_branch}/{path}"
        with open(uri, transport_params=transport_params) as fin:
            assert fin.read() == content.decode()

    def test_open_with_envvar_credentials(self, lakefs, repo, file):
        from smart_open import open

        path, content = file
        uri = f"lakefs://{repo.id}/{repo.default_branch}/{path}"
        with open(uri) as fin:
            assert fin.read() == content.decode()


class TestWriter:
    def commits(self, lakefs, repo):
        refs: apis.RefsApi = lakefs.refs
        commit_list = refs.log_commits(repo.id, repo.default_branch)
        return commit_list.results

    def test_write(self, lakefs, repo):
        content = "ветер по морю гуляет...".encode("utf8")
        path = "write/1.txt"
        with open(repo.id, repo.default_branch, path, "wb", lakefs) as fout:
            assert fout.write(content) == len(content)

        with open(repo.id, repo.default_branch, path, "rb", lakefs) as fin:
            assert fin.read() == content

    def test_commit(self, lakefs, repo):
        content = "ветер по морю гуляет...".encode("utf8")
        path = "write/2.txt"
        message = "Modify file."
        with open(repo.id, repo.default_branch, path, "wb", lakefs, message) as fout:
            assert fout.write(content) == len(content)
        assert self.commits(lakefs, repo)[0].message == message
