#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).


import io
import os
from setuptools import setup, find_packages


def read(fname):
    return io.open(os.path.join(os.path.dirname(__file__), fname), encoding='utf-8').read()


tests_require = [
    'mock',
    'moto==0.4.31',
    'responses',
]

setup(
    name='smart_open',
    version='1.5.7',
    description='Utils for streaming large files (S3, HDFS, gzip, bz2...)',
    long_description=read('README.rst'),

    packages=find_packages(),
    package_data={"smart_open.tests": ["test_data/*gz"]},

    author='Radim Rehurek',
    author_email='me@radimrehurek.com',
    maintainer='Radim Rehurek',
    maintainer_email='me@radimrehurek.com',

    url='https://github.com/piskvorky/smart_open',
    download_url='http://pypi.python.org/pypi/smart_open',

    keywords='file streaming, s3, hdfs',

    license='MIT',
    platforms='any',

    install_requires=[
        'boto >= 2.32',
        'bz2file',
        'requests',
        'boto3'
    ],
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
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: System :: Distributed Computing',
        'Topic :: Database :: Front-Ends',
    ],
)
