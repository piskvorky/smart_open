# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""
Sample code for HDFS integration tests.
Requires hadoop to be running on localhost, at the moment.
"""
import smart_open

with smart_open.smart_open("hdfs://user/root/input/core-site.xml") as fin:
    print(fin.read())

with smart_open.smart_open("hdfs://user/root/input/test.txt") as fin:
    print(fin.read())

with smart_open.smart_open("hdfs://user/root/input/test.txt?user.name=root", 'wb') as fout:
    fout.write(b'hello world')
