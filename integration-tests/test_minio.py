# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
import logging
import boto3

from smart_open import open

#
# These are publicly available via play.min.io
#
KEY_ID = 'Q3AM3UQ867SPQQA43P2F'
SECRET_KEY = 'zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG'
ENDPOINT_URL = 'https://play.min.io:9000'


def read_boto3():
    """Read directly using boto3."""
    session = get_minio_session()
    s3 = session.resource('s3', endpoint_url=ENDPOINT_URL)

    obj = s3.Object('smart-open-test', 'README.rst')
    data = obj.get()['Body'].read()
    logging.info('read %d bytes via boto3', len(data))
    return data


def read_smart_open():
    url = 's3://Q3AM3UQ867SPQQA43P2F:zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG@play.min.io:9000@smart-open-test/README.rst'  # noqa

    #
    # If the default region is not us-east-1, we need to construct our own
    # session.  This is because smart_open will create a session in the default
    # region, which _must_ be us-east-1 for minio to work.
    #
    tp = {}
    if get_default_region() != 'us-east-1':
        logging.info('injecting custom session')
        tp['session'] = get_minio_session()
    with open(url, transport_params=tp) as fin:
        text = fin.read()
        logging.info('read %d characters via smart_open', len(text))
        return text


def get_minio_session():
    return boto3.Session(
        region_name='us-east-1',
        aws_access_key_id=KEY_ID,
        aws_secret_access_key=SECRET_KEY,
    )


def get_default_region():
    return boto3.Session().region_name


def main():
    logging.basicConfig(level=logging.INFO)
    from_boto3 = read_boto3()
    from_smart_open = read_smart_open()
    assert from_boto3.decode('utf-8') == from_smart_open


if __name__ == '__main__':
    main()
