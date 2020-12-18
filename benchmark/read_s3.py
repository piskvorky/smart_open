import sys

import boto3
import smart_open

urls = [line.strip() for line in sys.stdin]

tp = {}
if 'create_session_and_resource':
    tp['session'] = boto3.Session()
    tp['resource'] = tp['session'].resource('s3')
elif 'create_resource' in sys.argv:
    tp['resource'] = boto3.resource('s3')
elif 'create_session' in sys.argv:
    tp['session'] = boto3.Session()

for i, url in enumerate(urls, 1):
    # print('%d/%d %s' % (i, len(urls), url))
    smart_open.open(url, transport_params=tp).read()
