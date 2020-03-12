"""Check that the environment variables contain valid boto3 credentials."""
import logging
import os
import boto3
import boto3.session


def check(session):
    client = session.client('s3')
    try:
        response = client.list_buckets()
    except Exception as e:
        logging.exception(e)
        return None
    else:
        return [b['Name'] for b in response['Buckets']]


def check_implicit():
    session = boto3.session.Session()
    buckets = check(session)
    if buckets:
        print('implicit check OK: %r' % buckets)
    else:
        print('implicit check failed')


def check_explicit():
    key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    if not (key_id and secret_key):
        print('no credentials found in os.environ, skipping explicit check')
        return

    session = boto3.session.Session(aws_access_key_id=key_id, aws_secret_access_key=secret_key)
    buckets = check(session)
    if buckets:
        print('explicit check OK: %r' % buckets)
    else:
        print('explicit check failed')


def main():
    check_implicit()
    check_explicit()


if __name__ == '__main__':
    main()
