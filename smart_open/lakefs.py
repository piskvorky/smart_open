from __future__ import annotations

import dataclasses
import functools
import io
import logging
import os
import re
import typing

try:
    from lakefs_client import apis, configuration, models
    from lakefs_client import client as lfs_client
except ImportError:
    MISSING_DEPS = True

from smart_open import constants, utils

SCHEME = "lakefs"

URI_EXAMPLES = (
    "lakefs://REPO/REF/file",
    "lakefs:///REPO/main/file.bz2",
)

DEFAULT_BUFFER_SIZE = 4 * 1024**2

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ParsedURI:
    scheme: str
    repo: str
    ref: str
    key: str


def parse_uri(uri_as_string: str) -> ParsedURI:
    """lakefs protocol URIs.

    lakeFS uses a specific format for path URIs. The URI lakefs://<REPO>/<REF>/<KEY>
    is a path to objects in the given repo and ref expression under key. This is used
    both for path prefixes and for full paths. In similar fashion, lakefs://<REPO>/<REF>
    identifies the repository at a ref expression, and lakefs://<REPO> identifes a repo.
    """
    sr = utils.safe_urlsplit(uri_as_string)
    if sr.scheme != SCHEME:
        raise ValueError(f"Scheme is not `lakefs` in {uri_as_string}")
    _pattern = r"^/(?P<ref>[^/]+)/(?P<key>.+)"
    _match = re.fullmatch(_pattern, sr.path)
    if _match is None:
        raise ValueError(
            f"Missing `branch/commit` and `path` in {uri_as_string}."
            "The URI should have the format of `lakefs://<REPO>/<REF>/<KEY>`"
        )
    else:
        ref, key = _match.groups()
        return ParsedURI(scheme=sr.scheme, repo=sr.netloc, ref=ref, key=key)


def open_uri(uri: str, mode: str, transport_params: dict) -> typing.IO:
    """Return a file-like object pointing to the URI.

    :param str uri: The URI to open
    :param str mode: Either "rb" or "wb".
    :param dict transport_params:  Any additional parameters to pass to `open`.

    :returns: file-like object.
    :rtype: file-like
    """
    parsed_uri = parse_uri(uri)
    kwargs = utils.check_kwargs(open, transport_params)
    return open(parsed_uri.repo, parsed_uri.ref, parsed_uri.key, mode, **kwargs)


def open(
    repo: str,
    ref: str,
    key: str,
    mode: str,
    client: lfs_client.LakeFSClient | None = None,
    commit_message: str | None = None,
    buffer_size: int = DEFAULT_BUFFER_SIZE,
):
    """Open a lakefs object for reading or writing.

    Parameters
    ----------
    repo: str
        The name of the repository this object resides in.
    ref: str
        The name of the branch or commit.
    key: str
        The path to the object for a given repo and branch.
    mode: str
        The mode for opening the object.  Must be either "rb" or "wb".
    client: lakefs_client.client.LakeFSClient
        The lakefs client to use.
    commit_message: str
        Only when writing. The message to include in the commit.
    buffer_size: int, optional
        The buffer size to use when performing I/O.
    """
    if client is None:
        try:
            conf = configuration.Configuration(
                host=os.environ["LAKECTL_SERVER_ENDPOINT_URL"],
                username=os.environ["LAKECTL_CREDENTIALS_ACCESS_KEY_ID"],
                password=os.environ["LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY"],
            )
            client = lfs_client.LakeFSClient(conf)
        except KeyError as e:
            raise ValueError(
                "Missing lakectl credentials. Please set "
                "LAKECTL_SERVER_ENDPOINT_URL, LAKECTL_CREDENTIALS_ACCESS_KEY_ID, "
                "and LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY"
            ) from e

    if mode == constants.READ_BINARY:
        raw = _RawReader(client, repo, ref, key)
        return io.BufferedReader(raw, buffer_size)
    elif mode == constants.WRITE_BINARY:
        raw_writer = _RawWriter(client, repo, ref, key, commit_message)
        return io.BufferedWriter(raw_writer, buffer_size)
    else:
        raise NotImplementedError(f"Lakefs support for mode {mode} not implemented")


class _RawReader(io.RawIOBase):
    """Read a lakeFS object.

    Provides low-level access to the underlying lakefs api.
    High level primites are implementedu using io.BufferedReader.
    """

    def __init__(
        self,
        client: lfs_client.LakeFSClient,
        repo: str,
        ref: str,
        key: str,
    ):
        self._client = client
        self._repo = repo
        self._ref = ref
        self._path = key
        self._position = 0
        self.name = key

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    @functools.cached_property
    def content_length(self) -> int:
        objects: apis.ObjectsApi = self._client.objects
        obj_stats: models.ObjectStats = objects.stat_object(
            self._repo, self._ref, self._path
        )
        return obj_stats.size_bytes

    @property
    def eof(self) -> bool:
        return self._position == self.content_length

    def seek(self, __offset: int, __whence: int = constants.WHENCE_START) -> int:
        """Seek to the specified position.

        :param int offset: The byte offset.
        :param int whence: Where the offset is from.

        :returns: The position after seeking.
        :rtype: int
        """
        if __whence not in constants.WHENCE_CHOICES:
            raise ValueError(
                "invalid whence, expected one of %r" % constants.WHENCE_CHOICES
            )

        if __whence == constants.WHENCE_START:
            start = max(0, __offset)
        elif __whence == constants.WHENCE_CURRENT:
            start = max(0, self._position + __offset)
        elif __whence == constants.WHENCE_END:
            start = max(0, self.content_length + __offset)

        self._position = min(start, self.content_length)

        return self._position

    def readinto(self, __buffer: bytes) -> int | None:
        """Read bytes into a pre-allocated bytes-like object __buffer.

        :param int size: number of bytes to read.

        :returns: the number of bytes read from lakefs
        :rtype: int
        """
        if self._position >= self.content_length:
            return 0
        size = len(__buffer)
        start_range = self._position
        end_range = min(self.content_length, (start_range + size)) - 1
        range = f"bytes={start_range}-{end_range}"
        objects: apis.ObjectsApi = self._client.objects
        data = objects.get_object(self._repo, self._ref, self._path, range=range).read()
        if not data:
            return 0
        self._position += len(data)
        __buffer[: len(data)] = data
        return len(data)


class _RawWriter(io.RawIOBase):
    """Write a lakefs object.

    Provides low-level access to the underlying lakefs api.
    High level primitives are implemented using io.BufferedReader.
    """

    def __init__(
        self,
        client: lfs_client.LakeFSClient,
        repo: str,
        ref: str,
        key: str,
        commit_message: str | None,
    ):
        self._client = client
        self._repo = repo
        self._ref = ref
        self._path = key
        if commit_message:
            self._message = commit_message
        else:
            self._message = f"Update {self._path}."
        self.name = key

    def writable(self) -> bool:
        return True

    def write(self, __b: bytes) -> int | None:
        objects: apis.ObjectsApi = self._client.objects
        commits: apis.CommitsApi = self._client.commits
        stream = io.BytesIO(__b)
        stream.name = self._path
        try:
            object_stats = objects.upload_object(
                self._repo, self._ref, self._path, content=stream
            )
            message = models.CommitCreation(self._message)
            _ = commits.commit(self._repo, self._ref, message)
        except lakefs_client.ApiException as e:
            raise Exception("Error uploading object: %s\n" % e) from e
        return object_stats.size_bytes
