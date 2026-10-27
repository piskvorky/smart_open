#
# Copyright (C) 2026 smart_open contributors
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import contextlib
import errno
import subprocess
import sys
from unittest import mock

import pytest

import smart_open


def _write_and_close(writer, payload, *, use_context):
    """Write buffered data and close through the requested public interface."""
    if use_context:
        with writer as stream:
            assert stream.write(payload) == len(payload)
    else:
        assert writer.write(payload) == len(payload)
        writer.close()


@pytest.mark.parametrize("mode", ["wb", "w"])
@pytest.mark.parametrize("use_context", [False, True])
def test_writer_close_after_broken_pipe(mode, use_context):
    """Close stdin and reap a failed put even when flushing the pipe fails."""
    command = [sys.executable, "-c", "import os, sys; os.close(sys.stdin.fileno()); sys.exit(7)"]
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)  # noqa: S603  # controlled CLI stand-in
    try:
        # The child closes its input pipe before stdout EOF; leave reaping to close().
        assert process.stdout.read() == b""
        assert process.returncode is None
        with mock.patch("smart_open.hdfs.subprocess.Popen", return_value=process):
            writer = smart_open.open("hdfs:///failed/file", mode)
        payload = b"payload" if mode == "wb" else "payload"
        with pytest.raises(OSError) as error:  # noqa: PT011  # native pipe error wording/type differs by OS; checked by errno below
            _write_and_close(writer, payload, use_context=use_context)
        # Preserve the native pipe error rather than replacing it with the exit status.
        assert error.value.errno in (errno.EPIPE, errno.EINVAL)
        assert process.stdin.closed
        assert process.returncode == 7
        assert writer.closed
        writer.close()
    finally:
        with contextlib.suppress(OSError):
            process.stdin.close()
        process.wait(timeout=5)
        process.stdout.close()


@pytest.mark.parametrize("mode", ["wb", "w"])
@pytest.mark.parametrize("exit_status", [0, 7])
def test_writer_close_flushes_before_waiting(mode, exit_status):
    """Send buffered data before waiting and still report failed put commands."""
    script = f"import sys; sys.stdout.buffer.write(sys.stdin.buffer.read()); sys.exit({exit_status})"
    command = [sys.executable, "-c", script]
    with (
        subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE) as process,  # noqa: S603  # controlled CLI stand-in
        mock.patch("smart_open.hdfs.subprocess.Popen", return_value=process),
    ):
        writer = smart_open.open("hdfs:///file", mode)
        payload = b"payload" if mode == "wb" else "payload"
        assert writer.write(payload) == len(payload)
        if exit_status:
            with pytest.raises(OSError, match="exit status 7"):
                writer.close()
        else:
            writer.close()
        assert writer.closed
        assert process.stdin.closed
        assert process.returncode == exit_status
        assert process.stdout.read() == b"payload"
        writer.close()
