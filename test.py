import sys

if len(sys.argv) != 5:
    sys.stderr.write("python test.py <ACCESS_KEY_ID> <ACCESS_SECRET_KEY> <BUCKET> <KEY>\n");
    sys.exit(1)

ACCESS_KEY_ID = sys.argv[1]
ACCESS_SECRET_KEY = sys.argv[2]
BUCKET_ID = sys.argv[3]
KEY_ID = sys.argv[4]

#
# 1. S3 reading.
#

#----------------------------------------------------------------------------

#import smart_open
#for line in smart_open.iter_lines("s3://%s:%s@%s/%s" % (ACCESS_KEY_ID, ACCESS_SECRET_KEY, BUCKET_ID, KEY_ID)):
#    print line

#import smart_open
#for line in smart_open.iter_lines("file://~/Work/RR/smart_open/test.txt"):
#    print line


#----------------------------------------------------------------------------
# same as above

#import smart_open
#for line in smart_open.smart_open("s3://%s:%s@%s/%s" % (ACCESS_KEY_ID, ACCESS_SECRET_KEY, BUCKET_ID, KEY_ID), "rb"):
#    print line

#----------------------------------------------------------------------------
# same as above, but wrapped in a context manager

#from smart_open import smart_open
#with smart_open("s3://%s:%s@%s/%s" % (ACCESS_KEY_ID, ACCESS_SECRET_KEY, BUCKET_ID, KEY_ID), "rb") as fin:
#    for line in fin:
#        print line

#----------------------------------------------------------------------------

#
# 2. S3 writing
#

#----------------------------------------------------------------------------
# stream a generator of lines = strings directly into an s3 key

#import smart_open
#smart_open.s3_store_lines(['sentence 1', 'sentence 2', 'sentence 3'], "s3://%s:%s@%s/%s" % (ACCESS_KEY_ID, ACCESS_SECRET_KEY, BUCKET_ID, KEY_ID))
#smart_open.s3_store_lines(("Pokus " + str(i) for i in range(100)), "s3://%s:%s@%s/%s" % (ACCESS_KEY_ID, ACCESS_SECRET_KEY, BUCKET_ID, KEY_ID))

#----------------------------------------------------------------------------
# same as above, but accepts already an established connection and bucket instead of path string (faster)
#mybucket = boto.connect_s3.get_bucket('mybucket')
#smart_open.s3_store_lines(['sentence 1', 'sentence 2', 'sentence 3'], bucket=mybucket, key='mykey.txt')

#import boto
#import smart_open
#s3 = boto.connect_s3(aws_access_key_id = ACCESS_KEY_ID, aws_secret_access_key = ACCESS_SECRET_KEY)
#mybucket = s3.get_bucket(BUCKET_ID)
#smart_open.s3_store_lines(['sentence 1', 'sentence 2', 'sentence 3'], outbucket = mybucket, outkey = KEY_ID)

#----------------------------------------------------------------------------

#import smart_open
#with smart_open.smart_open("s3://%s:%s@%s/%s" % (ACCESS_KEY_ID, ACCESS_SECRET_KEY, BUCKET_ID, KEY_ID), "wb") as fout:
#    for line in ['sentence 1', 'sentence 2', 'sentence 3']:
#        fout.write(line + "\n")

#----------------------------------------------------------------------------

#
# 3. S3 bucket iteration
#

#for file_name, file_content in s3_iter_bucket(mybucket, accept_key=lambda fname: fname.endswith('.json')):
#    print file_name, len(file_content)