"""Check that the environment variables contain valid boto3 credentials."""
import os
import boto3


def main():
    key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    if not (key_id and secret_key):
        print('no credentials found in os.environ, skipping test')
        return

    client = boto3.client('s3', aws_access_key_id=key_id, aws_secret_access_key=secret_key)
    client.list_buckets()
    print('OK')


if __name__ == '__main__':
    main()
