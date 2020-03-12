"""Check that the environment variables contain valid boto3 credentials."""
import logging
import os
import boto3


def check_implicit():
    client = boto3.client('s3')
    try:
        response = client.list_buckets()
    except Exception as e:
        logging.exception(e)
    else:
        print([b['Name'] for b in response['Buckets']])
        print('implicit check OK')


def check_explicit():
    key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    if not (key_id and secret_key):
        print('no credentials found in os.environ, skipping explicit check')
        return

    client = boto3.client('s3', aws_access_key_id=key_id, aws_secret_access_key=secret_key)
    try:
        response = client.list_buckets()
    except Exception as e:
        logging.exception(e)
    else:
        print([b['Name'] for b in response['Buckets']])
        print('explicit check OK')


def main():
    check_implicit()
    check_explicit()


if __name__ == '__main__':
    main()
