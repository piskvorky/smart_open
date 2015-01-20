#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Radim Rehurek <me@radimrehurek.com>


import os

# TODO add ez_setup?
from setuptools import setup, find_packages

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name = 'smart_open',
    version = '0.1.0',
    description = 'Utils for streaming large files (S3, HDFS, gzip, bz2...)',
    long_description = read('README.rst'),

    packages=find_packages(),

    author = u'Radim Řehůřek',
    author_email = 'radimrehurek@seznam.cz',
    maintainer = u'Vincent Kríž',
    maintainer_email = 'vincent.kriz@kamadu.eu',

    url = 'https://github.com/piskvorky/smart_open',
    download_url = 'http://pypi.python.org/pypi/smart_open',

    keywords = 'file streaming, s3, hdfs',

    license = 'MIT',
    platforms = 'any',

    install_requires=[
        'boto >= 2.35',
    ],

    test_suite="smart_open.tests",

    classifiers = [ # from http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: Public Domain',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: System :: Distributed Computing',
        'Topic :: Database :: Front-Ends',
    ],
)
