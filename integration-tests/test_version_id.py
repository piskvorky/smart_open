"""Tests the version_id transport parameter for S3 against real S3."""

import boto3

from smart_open import open

BUCKET, KEY = "smart-open-versioned", "demo.txt"
"""Our have a public-readable bucket with a versioned object."""

URL = f"s3://{BUCKET}/{KEY}"


def assert_equal(a, b):
    """Assert two values are equal, formatting both into the failure message."""
    assert a == b, f"{a!r} != {b!r}"


def main():
    """Verify smart_open returns the correct content for each S3 object version."""
    versions = [v.id for v in boto3.resource("s3").Bucket(BUCKET).object_versions.filter(Prefix=KEY)]
    expected_versions = [
        "KiQpZPsKI5Dm2oJZy_RzskTOtl2snjBg",
        "N0GJcE3TQCKtkaS.gF.MUBZS85Gs3hzn",
    ]
    assert_equal(versions, expected_versions)

    contents = [open(URL, transport_params={"version_id": v}).read() for v in versions]
    expected_contents = ["second version\n", "first version\n"]
    assert_equal(contents, expected_contents)

    with open(URL) as fin:
        most_recent_contents = fin.read()
    assert_equal(most_recent_contents, expected_contents[0])

    print("OK")


if __name__ == "__main__":
    main()
