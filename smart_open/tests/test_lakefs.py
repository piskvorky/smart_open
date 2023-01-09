# -*- coding: utf-8 -*-
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import pytest
import lakefs_client
from lakefs_client import client
from lakefs_client import models
from lakefs_client import apis
import logging


"""It needs docker compose to run lakefs locally:
https://docs.lakefs.io/quickstart/run.html

curl https://compose.lakefs.io | docker-compose -f - up
"""
_LAKEFS_HOST = "http://localhost:8000/api/v1"

logger = logging.getLogger(__name__)


def api_available(lfs_client: client.LakeFSClient):
    from urllib3.exceptions import MaxRetryError

    healthcheck: apis.HealthCheckApi = lfs_client.healthcheck
    try:
        healthcheck.health_check()
        return True
    except (lakefs_client.ApiException, MaxRetryError):
        return False


@pytest.fixture(scope="module")
def lakefs():
    import shlex
    import subprocess
    from time import sleep

    compose = subprocess.Popen(
        shlex.split("curl https://compose.lakefs.io"), stdout=subprocess.PIPE
    )
    subprocess.Popen(shlex.split("docker-compose -f - up -d"), stdin=compose.stdout)
    compose.stdout.close()

    configuration = lakefs_client.Configuration(_LAKEFS_HOST)
    lfs_client = client.LakeFSClient(configuration)

    while not api_available(lfs_client):
        sleep(1)

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
    yield client.LakeFSClient(configuration)

    compose = subprocess.Popen(
        shlex.split("curl https://compose.lakefs.io"), stdout=subprocess.PIPE
    )
    subprocess.Popen(shlex.split("docker-compose -f - down"), stdin=compose.stdout)
    compose.stdout.close()


def create_repo(lfs_client: client.LakeFSClient, repo_name: str) -> models.Repository:
    new_repo = models.RepositoryCreation(
        name=repo_name, storage_namespace="local:///home/lakefs/", default_branch="main"
    )
    try:
        repositories: apis.RepositoriesApi = lfs_client.repositories
        repository: models.Repository = repositories.create_repository(new_repo)
    except lakefs_client.ApiException as e:
        raise Exception("Error creating repository: %s\n" % e) from e
    return repository


def put_to_repo(
    lfs_client: client.LakeFSClient,
    repo: models.Repository,
    path: str,
    content: bytes,
    branch: str | None = None,
):
    from io import BytesIO

    objects: apis.ObjectsApi = lfs_client.objects
    _branch = branch if branch else repo.default_branch
    stream = BytesIO(content)
    stream.name = path
    try:
        obj_stats = objects.upload_object(repo.id, _branch, path, content=stream)
    except lakefs_client.ApiException as e:
        raise Exception("Error uploading object: %s\n" % e) from e
    return obj_stats


class TestReader:
    def test_read(self, lakefs: client.LakeFSClient):
        from smart_open.lakefs import Reader

        content = "hello wořld\nhow are you?".encode("utf8")
        path = "test.txt"
        repo_name = "test"
        repo = create_repo(lfs_client=lakefs, repo_name=repo_name)
        put_to_repo(lakefs, repo, path, content)
        logger.debug("content: %r, len: %r", content, len(content))

        fin = Reader(client=lakefs, repo=repo_name, ref=repo.default_branch, path=path)
        assert content[:6] == fin.read(6)
        assert content[6:14] == fin.read(8)
        assert content[14:] == fin.read()

    def test_iter(self, lakefs: client.LakeFSClient):
        from smart_open.lakefs import Reader

        content = "hello wořld\nhow are you?".encode("utf8")
        path = "test_iter.txt"
        repo_name = "test-iter"
        repo = create_repo(lfs_client=lakefs, repo_name=repo_name)
        put_to_repo(lakefs, repo, path, content)

        # connect to fake Azure Blob Storage and read from the fake key we filled above
        fin = Reader(client=lakefs, repo=repo_name, ref=repo.default_branch, path=path)
        output = [line.rstrip(b"\n") for line in fin]
        assert output == content.split(b"\n")
