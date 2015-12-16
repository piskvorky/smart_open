* 1.3.1, 16th December 2015

- Disable multiprocessing if unavailable. Allows to run on Google Compute Engine. (PR #41, @nikicc)
- Httpretty updated to allow LC_ALL=C locale config. (PR #39, @jsphpl)


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
