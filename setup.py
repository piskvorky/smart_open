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

if sys.version_info < (2, 6):
    raise ImportError("smart_open requires python >= 2.6")


# TODO add ez_setup?
from setuptools import setup, find_packages

def read(fname):
    return io.open(os.path.join(os.path.dirname(__file__), fname), encoding='utf-8').read()


setup(
    name = 'smart_open',
    version = '1.3.3',
    description = 'Utils for streaming large files (S3, HDFS, gzip, bz2...)',
    long_description = read('README.rst'),

    packages=find_packages(),

    author = u'Radim Řehůřek',
    author_email = 'me@radimrehurek.com',
    maintainer = u'Radim Řehůřek',
    maintainer_email = 'me@radimrehurek.com',

    url = 'https://github.com/piskvorky/smart_open',
    download_url = 'http://pypi.python.org/pypi/smart_open',

    keywords = 'file streaming, s3, hdfs',

    license = 'MIT',
    platforms = 'any',

    install_requires=[
        'boto >= 2.32',
        'bz2file',
        'requests',
    ],

    test_suite="smart_open.tests",

    classifiers = [ # from http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: System :: Distributed Computing',
        'Topic :: Database :: Front-Ends',
    ],
)
