#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).


import io
import os
import sys

from setuptools import setup, find_packages


def _get_version():
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(curr_dir, 'smart_open', 'version.py')) as fin:
        #
        # __version__ = '1.8.4'
        #
        line = fin.readline().strip()
        parts = line.split(' ')
        assert parts[0] == '__version__'
        assert parts[1] == '='
        return parts[2][1:-1]


#
# We cannot do "from smart_open.version import __version__" because that will
# require the dependencies for smart_open to already be in place, and that is
# not necessarily the case when running setup.py for the first time.
#
__version__ = _get_version()


def read(fname):
    return io.open(os.path.join(os.path.dirname(__file__), fname), encoding='utf-8').read()


tests_require = [
    'mock',
    'moto[server]',
    'pathlib2',
    'responses',
    'boto3',
    # Not used directly but allows boto GCE plugins to load.
    # https://github.com/GoogleCloudPlatform/compute-image-packages/issues/262
    'google-compute-engine==2.8.12',
    'paramiko',
    'parameterizedtestcase',
]

install_requires = [
    'requests',
    'boto3',
    'google-cloud-storage',
]
if sys.version_info[0] == 2:
    install_requires.append('bz2file')

setup(
    name='smart_open',
    version=__version__,
    description='Utils for streaming large files (S3, HDFS, GCS, gzip, bz2...)',
    long_description=read('README.rst'),

    packages=find_packages(),
    package_data={
        "smart_open.tests": ["test_data/*gz"],
    },

    author='Radim Rehurek',
    author_email='me@radimrehurek.com',
    maintainer='Radim Rehurek',
    maintainer_email='me@radimrehurek.com',

    url='https://github.com/piskvorky/smart_open',
    download_url='http://pypi.python.org/pypi/smart_open',

    keywords='file streaming, s3, hdfs, gcs',

    license='MIT',
    platforms='any',

    install_requires=install_requires,
    tests_require=tests_require,
    extras_require={
        'test': tests_require,
    },

    test_suite="smart_open.tests",

    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: System :: Distributed Computing',
        'Topic :: Database :: Front-Ends',
    ],
)
