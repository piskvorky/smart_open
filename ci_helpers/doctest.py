"""Runs the doctests, if the AWS credentials are available.

Without the credentials, skips the tests entirely, because otherwise they will fail.
"""
import os
import subprocess

if os.environ.get('AWS_ACCESS_KEY_ID') and os.environ.get('AWS_SECRET_ACCESS_KEY'):
    subprocess.check_call(['python', '-m', 'doctest', 'README.rst', '-v'])
