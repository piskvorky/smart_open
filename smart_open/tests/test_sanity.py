import unittest

import boto3
import moto


class Test(unittest.TestCase):

    @moto.mock_s3()
    def test(self):
        resource = boto3.resource('s3')
        resource.create_bucket(Bucket='mybucket')
        resource.Bucket('mybucket').delete()
