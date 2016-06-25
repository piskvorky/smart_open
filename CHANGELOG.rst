* 1.3.4, 25th June 2016

  - Bundle gzipstream to enable streaming of gzipped content from S3
  - Update gzipstream to avoid deep recursion
  - Implemented readline for S3
  - Added pip requirements.txt

* 1.3.3, 16th May 2016

  - Accept an instance of boto.s3.key.Key to smart_open (PR #38, @asieira)
  - Allow passing `encrypt_key` and other parameters to `initiate_multipart_upload` (PR #63, @asieira)
  - Allow passing boto `host` and `profile_name` to smart_open (PR #71 #68, @robcowie)
  - Write an empty key to S3 even if nothing is written to S3OpenWrite (PR #61, @petedmarsh)
  - Support `LC_ALL=C` environment variable setup (PR #40, @nikicc)
  - Python 3.5 support

* 1.3.2, 3rd January 2016

  - Bug fix release to enable 'wb+' file mode (PR #50)


* 1.3.1, 18th December 2015

  - Disable multiprocessing if unavailable. Allows to run on Google Compute Engine. (PR #41, @nikicc)
  - Httpretty updated to allow LC_ALL=C locale config. (PR #39, @jsphpl)
  - Accept an instance of boto.s3.key.Key (PR #38, @asieira)


* 1.3.0, 19th September 2015

  - WebHDFS read/write (PR #29, @ziky90)
  - re-upload last S3 chunk in failed upload (PR #20, @andreycizov)
  - return the entire key in s3_iter_bucket instead of only the key name (PR #22, @salilb)
  - pass optional keywords on S3 write (PR #30, @val314159)
  - smart_open a no-op if passed a file-like object with a read attribute (PR #32, @gojomo)
  - various improvements to testing (PR #30, @val314159)


* 1.1.0, 1st February 2015

  - support for multistream bzip files (PR #9, @pombredanne)
  - introduce this CHANGELOG
