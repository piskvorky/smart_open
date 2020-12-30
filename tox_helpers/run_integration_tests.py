"""Runs integration tests."""
import os
import subprocess

os.environ['PYTEST_ADDOPTS'] = "--reruns 3 --reruns-delay 1"

subprocess.check_call(
    [
        'pytest',
        'integration-tests/test_207.py',
        'integration-tests/test_http.py',
    ]
)

if os.environ.get('AWS_ACCESS_KEY_ID') and os.environ.get('AWS_SECRET_ACCESS_KEY'):
    subprocess.check_call(['pytest', 'integration-tests/test_s3_ported.py'])
