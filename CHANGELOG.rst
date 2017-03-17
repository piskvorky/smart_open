* 1.5.1, 16th Mar 2017

  - Bugfix for compressed formats (PR #110, @tmylk)

* 1.5.0, 14th Mar 2017

  - HTTP/HTTPS read support w/ Kerberos (PR #107, @robottwo)

* 1.4.0, 13th Feb 2017

  - HdfsOpenWrite implementation similar to read (PR #106, @skibaa)  
  - Support custom S3 server host, port, ssl. (PR #101, @robottwo)
  - Add retry around `s3_iter_bucket_process_key` to address S3 Read Timeout errors. (PR #96, @bbbco)  
  - Include tests data in sdist + install them. (PR #105, @cournape)
  
* 1.3.5, 5th October 2016

  - Add MANIFEST.in required for conda-forge recip (PR #90, @tmylk)
  - Fix #92. Allow hash in filename (PR #93, @tmylk)

* 1.3.4, 26th August 2016

  - Relative path support (PR #73, @yupbank)
  - Move gzipstream module to smart_open package (PR #81, @mpenkov)
  - Ensure reader objects never return None (PR #81, @mpenkov)
  - Ensure read functions never return more bytes than asked for (PR #84, @mpenkov)
  - Add support for reading gzipped objects until EOF, e.g. read() (PR #81, @mpenkov)
  - Add missing parameter to read_from_buffer call (PR #84, @mpenkov)
  - Add unit tests for gzipstream (PR #84, @mpenkov)
  - Bundle gzipstream to enable streaming of gzipped content from S3 (PR #73, @mpenkov)
  - Update gzipstream to avoid deep recursion (PR #73, @mpenkov)
  - Implemented readline for S3 (PR #73, @mpenkov)
  - Added pip requirements.txt (PR #73, @mpenkov)
  - Invert NO_MULTIPROCESSING flag (PR #79, @Janrain-Colin)
  - Add ability to add query to webhdfs uri. (PR #78, @ellimilial)

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
