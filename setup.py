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


def read(fname):
    return io.open(os.path.join(os.path.dirname(__file__), fname), encoding='utf-8').read()


#
# This code intentially duplicates a similar function in __init__.py.  The
# alternative would be to somehow import that module to access the function,
# which would be too messy for a setup.py script.
#
def _get_version():
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(curr_dir, 'smart_open', 'VERSION')) as fin:
        return fin.read().strip()


tests_require = [
    'mock',
    'moto==1.3.4',
    'pathlib2',
    'responses',
    # Temporary pin boto3 & botocore, because moto doesn't work with new version
    # See https://github.com/spulec/moto/issues/1793 and https://github.com/RaRe-Technologies/smart_open/issues/227
    'boto3 < 1.8.0',
    # 'botocore < 1.11.0'
    # Not used directly but allows boto GCE plugins to load.
    # https://github.com/GoogleCloudPlatform/compute-image-packages/issues/262
    'google-compute-engine==2.8.12'
]

install_requires = [
    'boto >= 2.32',
    'requests',
    'boto3',
]
if sys.version_info[0] == 2:
    install_requires.append('bz2file')

setup(
    name='smart_open',
    version=_get_version(),
    description='Utils for streaming large files (S3, HDFS, gzip, bz2...)',
    long_description=read('README.rst'),

    packages=find_packages(),
    package_data={
        "smart_open": ["VERSION"],
        "smart_open.tests": ["test_data/*gz"],
    },

    author='Radim Rehurek',
    author_email='me@radimrehurek.com',
    maintainer='Radim Rehurek',
    maintainer_email='me@radimrehurek.com',

    url='https://github.com/piskvorky/smart_open',
    download_url='http://pypi.python.org/pypi/smart_open',

    keywords='file streaming, s3, hdfs',

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
