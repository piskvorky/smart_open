import os
import subprocess

os.environ['SMART_OPEN_TEST_MISSING_DEPS'] = '1'
command = [
    'pytest',
    'tests/test_package.py',
    '-v',
    '--cov', 'smart_open',
    '--cov-report', 'term-missing',
]
subprocess.check_call(command)
