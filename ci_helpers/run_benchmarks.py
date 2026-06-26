"""Runs benchmarks.

We only do this is AWS credentials are available, because without them, it
is impossible to run the benchmarks at all.
"""

import os
import platform
import subprocess
import uuid

import smart_open

if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
    required = ("SO_BUCKET",)
    for varname in required:
        assert varname in os.environ, "the following env vars must be set: {}".format(", ".join(required))

    os.environ["PYTEST_ADDOPTS"] = "--reruns 3 --reruns-delay 1"

    commit_hash = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode("utf-8").strip()

    #
    # This is a temporary key that test_s3 will use for I/O.
    #
    os.environ["SO_KEY"] = str(uuid.uuid4())
    subprocess.check_call(
        [
            "pytest",
            "-v",
            "integration-tests/test_s3.py",
            f"--benchmark-save={commit_hash}",
            "--numprocesses=0",  # disable pytest-xdist
        ]
    )

    url = "s3://{}/benchmark-results/{}".format(
        os.environ["SO_BUCKET"],
        commit_hash,
    )
    for root, _subdirs, files in os.walk(".benchmarks"):
        for f in files:
            if f.endswith(f"{commit_hash}.json"):
                out_url = f"{url}/{platform.python_version()}.json"
                with open(os.path.join(root, f)) as fin:
                    with smart_open.open(out_url, "wt") as fout:
                        fout.write(fin.read())
