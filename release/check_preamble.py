# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Checks preambles of Python script files.

We want to ensure they all contain the appropriate license and copyright.

For the purposes of this script, the *preamble* is defined as the first
lines of the file starting with a hash (#).  Any line that does not start
with a hash ends the preamble.

Usage::

    python check_preamble.py --replace /path/to/template.py script.py

The above command reads the preamble from ``template.py``, and then copies
that preamble into ``script.py``.  If ``script.py`` already contains a
preamble, then the existing preamble will be replaced **entirely**.

Processing entire subdirectories with one command::

    find subdir1 subdir2 -iname "*.py" | xargs -n 1 python check_preamble.py --replace template.py

"""
import argparse
import logging
import os
import sys


def extract_preamble(fin):
    end_preamble = False
    preamble, body = [], []

    for line in fin:
        if end_preamble:
            body.append(line)
        elif line.startswith('#'):
            preamble.append(line)
        else:
            end_preamble = True
            body.append(line)

    return preamble, body


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path', help='the path of the file to check')
    parser.add_argument('--replace', help='replace the preamble with the one from this file')
    parser.add_argument('--loglevel', default=logging.INFO)
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)

    with open(args.path) as fin:
        preamble, body = extract_preamble(fin)

    for line in preamble:
        logging.info('%s: %s', args.path, line.rstrip())

    if not args.replace:
        sys.exit(0)

    with open(args.replace) as fin:
        preamble, _ = extract_preamble(fin)

    if os.access(args.path, os.X_OK):
        preamble.insert(0, '#!/usr/bin/env python\n')

    with open(args.path, 'w') as fout:
        for line in preamble + body:
            fout.write(line)


if __name__ == '__main__':
    main()
