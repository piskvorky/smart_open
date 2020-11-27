# Unreleased

# 4.0.1, 27 Nov 2020

- Exclude `requests` from `install_requires` dependency list.
  If you need it, use `pip install smart_open[http]` or `pip install smart_open[webhdfs]`.

# 4.0.0, 24 Nov 2020

- Fix reading empty file or seeking past end of file for s3 backend (PR [#549](https://github.com/RaRe-Technologies/smart_open/pull/549), [@jcushman](https://github.com/jcushman))
- Fix handling of rt/wt mode when working with gzip compression (PR [#559](https://github.com/RaRe-Technologies/smart_open/pull/559), [@mpenkov](https://github.com/mpenkov))
- Bump minimum Python version to 3.6 (PR [#562](https://github.com/RaRe-Technologies/smart_open/pull/562), [@mpenkov](https://github.com/mpenkov))

# 3.0.0, 8 Oct 2020

This release modifies the behavior of setup.py with respect to dependencies.
Previously, `boto3` and other AWS-related packages were installed by default.
Now, in order to install them, you need to run either:

    pip install smart_open[s3]

to install the AWS dependencies only, or

    pip install smart_open[all]

to install all dependencies, including AWS, GCS, etc.

# 2.2.1, 1 Oct 2020

- Include S3 dependencies by default, because removing them in the 2.2.0 minor release was a mistake.

# 2.2.0, 25 Sep 2020

This release modifies the behavior of setup.py with respect to dependencies.
Previously, `boto3` and other AWS-related packages were installed by default.
Now, in order to install them, you need to run either:

    pip install smart_open[s3]

to install the AWS dependencies only, or

    pip install smart_open[all]

to install all dependencies, including AWS, GCS, etc.

Summary of changes:

- Correctly pass `newline` parameter to built-in `open` function (PR [#478](https://github.com/RaRe-Technologies/smart_open/pull/478), [@burkovae](https://github.com/burkovae))
- Remove boto as a dependency (PR [#523](https://github.com/RaRe-Technologies/smart_open/pull/523), [@isobit](https://github.com/isobit))
- Performance improvement: avoid redundant GetObject API queries in s3.Reader (PR [#495](https://github.com/RaRe-Technologies/smart_open/pull/495), [@jcushman](https://github.com/jcushman))
- Support installing smart_open without AWS dependencies (PR [#534](https://github.com/RaRe-Technologies/smart_open/pull/534), [@justindujardin](https://github.com/justindujardin))
- Take object version into account in `to_boto3` method (PR [#539](https://github.com/RaRe-Technologies/smart_open/pull/539), [@interpolatio](https://github.com/interpolatio))

## Deprecations

Functionality on the left hand side will be removed in future releases.
Use the functions on the right hand side instead.

- `smart_open.s3_iter_bucket` → `smart_open.s3.iter_bucket`

# 2.1.1, 27 Aug 2020

  - Bypass unnecessary GCS storage.buckets.get permission (PR [#516](https://github.com/RaRe-Technologies/smart_open/pull/516), [@gelioz](https://github.com/gelioz))
  - Allow SFTP connection with SSH key (PR [#522](https://github.com/RaRe-Technologies/smart_open/pull/522), [@rostskadat](https://github.com/rostskadat))

# 2.1.0, 1 July 2020

  - Azure storage blob support ([@nclsmitchell](https://github.com/nclsmitchell) and [@petedannemann](https://github.com/petedannemann))
  - Correctly pass `newline` parameter to built-in `open` function (PR [#478](https://github.com/RaRe-Technologies/smart_open/pull/478), [@burkovae](https://github.com/burkovae))
  - Ensure GCS objects always have a .name attribute (PR [#506](https://github.com/RaRe-Technologies/smart_open/pull/506), [@todor-markov](https://github.com/todor-markov))
  - Use exception chaining to convey the original cause of the exception (PR [#508](https://github.com/RaRe-Technologies/smart_open/pull/508), [@cool-RR](https://github.com/cool-RR))

# 2.0.0, 27 April 2020, "Python 3"

  - **This version supports Python 3 only** (3.5+).
    - If you still need Python 2, install the smart_open==1.10.1 legacy release instead.
  - Prevent smart_open from writing to logs on import (PR [#476](https://github.com/RaRe-Technologies/smart_open/pull/476), [@mpenkov](https://github.com/mpenkov))
  - Modify setup.py to explicitly support only Py3.5 and above (PR [#471](https://github.com/RaRe-Technologies/smart_open/pull/471), [@Amertz08](https://github.com/Amertz08))
  - Include all the test_data in setup.py (PR [#473](https://github.com/RaRe-Technologies/smart_open/pull/473), [@sikuan](https://github.com/sikuan))

# 1.10.1, 26 April 2020

  - This is the last version to support Python 2.7. Versions 1.11 and above will support Python 3 only.
  - Use only if you need Python 2.

# 1.11.1, 8 Apr 2020

  - Add missing boto dependency (Issue [#468](https://github.com/RaRe-Technologies/smart_open/issues/468))

# 1.11.0, 8 Apr 2020

  - Fix GCS multiple writes (PR [#421](https://github.com/RaRe-Technologies/smart_open/pull/421), [@petedannemann](https://github.com/petedannemann))
  - Implemented efficient readline for ByteBuffer (PR [#426](https://github.com/RaRe-Technologies/smart_open/pull/426), [@mpenkov](https://github.com/mpenkov))
  - Fix WebHDFS read method (PR [#433](https://github.com/RaRe-Technologies/smart_open/pull/433), [@mpenkov](https://github.com/mpenkov))
  - Make S3 uploads more robust (PR [#434](https://github.com/RaRe-Technologies/smart_open/pull/434), [@mpenkov](https://github.com/mpenkov))
  - Add pathlib monkeypatch with replacement of `pathlib.Path.open` (PR [#436](https://github.com/RaRe-Technologies/smart_open/pull/436), [@menshikh-iv](https://github.com/menshikh-iv))
  - Fix error when calling str() or repr() on GCS SeekableBufferedInputBase (PR [#442](https://github.com/RaRe-Technologies/smart_open/pull/442), [@robcowie](https://github.com/robcowie))
  - Move optional dependencies to extras (PR [#454](https://github.com/RaRe-Technologies/smart_open/pull/454), [@Amertz08](https://github.com/Amertz08))
  - Correctly handle GCS paths that contain '?' char  (PR [#460](https://github.com/RaRe-Technologies/smart_open/pull/460), [@chakruperitus](https://github.com/chakruperitus))
  - Make our doctools submodule more robust (PR [#467](https://github.com/RaRe-Technologies/smart_open/pull/467), [@mpenkov](https://github.com/mpenkov))

Starting with this release, you will have to run:

    pip install smart_open[gcs] to use the GCS transport.

In the future, all extra dependencies will be optional.  If you want to continue installing all of them, use:

	pip install smart_open[all]

See the README.rst for details.

# 1.10.0, 16 Mar 2020

  - Various webhdfs improvements (PR [#383](https://github.com/RaRe-Technologies/smart_open/pull/383), [@mrk-its](https://github.com/mrk-its))
  - Fixes "the connection was closed by the remote peer" error (PR [#389](https://github.com/RaRe-Technologies/smart_open/pull/389), [@Gapex](https://github.com/Gapex))
  - allow use of S3 single part uploads (PR [#400](https://github.com/RaRe-Technologies/smart_open/pull/400), [@adrpar](https://github.com/adrpar))
  - Add test data in package via MANIFEST.in (PR [#401](https://github.com/RaRe-Technologies/smart_open/pull/401), [@jayvdb](https://github.com/jayvdb))
  - Google Cloud Storage (GCS) (PR [#404](https://github.com/RaRe-Technologies/smart_open/pull/404), [@petedannemann](https://github.com/petedannemann))
  - Implement to_boto3 function for S3 I/O. (PR [#405](https://github.com/RaRe-Technologies/smart_open/pull/405), [@mpenkov](https://github.com/mpenkov))
  - enable smart_open to operate without docstrings (PR [#406](https://github.com/RaRe-Technologies/smart_open/pull/406), [@mpenkov](https://github.com/mpenkov))
  - Implement object_kwargs parameter (PR [#411](https://github.com/RaRe-Technologies/smart_open/pull/411), [@mpenkov](https://github.com/mpenkov))
  - Remove dependency on old boto library (PR [#413](https://github.com/RaRe-Technologies/smart_open/pull/413), [@mpenkov](https://github.com/mpenkov))
  - implemented efficient readline for ByteBuffer (PR [#426](https://github.com/RaRe-Technologies/smart_open/pull/426), [@mpenkov](https://github.com/mpenkov))
  - improve buffering efficiency (PR [#427](https://github.com/RaRe-Technologies/smart_open/pull/427), [@mpenkov](https://github.com/mpenkov))
  - fix WebHDFS read method (PR [#433](https://github.com/RaRe-Technologies/smart_open/pull/433), [@mpenkov](https://github.com/mpenkov))
  - Make S3 uploads more robust (PR [#434](https://github.com/RaRe-Technologies/smart_open/pull/434), [@mpenkov](https://github.com/mpenkov))

# 1.9.0, 3 Nov 2019

  - Add version_id transport parameter for fetching a specific S3 object version (PR [#325](https://github.com/RaRe-Technologies/smart_open/pull/325), [@interpolatio](https://github.com/interpolatio))
  - Document passthrough use case (PR [#333](https://github.com/RaRe-Technologies/smart_open/pull/333), [@mpenkov](https://github.com/mpenkov))
  - Support seeking over HTTP and HTTPS (PR [#339](https://github.com/RaRe-Technologies/smart_open/pull/339), [@interpolatio](https://github.com/interpolatio))
  - Add support for rt, rt+, wt, wt+, at, at+ methods (PR [#342](https://github.com/RaRe-Technologies/smart_open/pull/342), [@interpolatio](https://github.com/interpolatio))
  - Change VERSION to version.py (PR [#349](https://github.com/RaRe-Technologies/smart_open/pull/349), [@mpenkov](https://github.com/mpenkov))
  - Adding howto guides (PR [#355](https://github.com/RaRe-Technologies/smart_open/pull/355), [@mpenkov](https://github.com/mpenkov))
  - smart_open/s3: Initial implementations of str and repr (PR [#359](https://github.com/RaRe-Technologies/smart_open/pull/359), [@ZlatSic](https://github.com/ZlatSic))
  - Support writing any bytes-like object to S3. (PR [#361](https://github.com/RaRe-Technologies/smart_open/pull/361), [@gilbsgilbs](https://github.com/gilbsgilbs))

# 1.8.4, 2 Jun 2019

  - Don't use s3 bucket_head to check for bucket existence (PR [#315](https://github.com/RaRe-Technologies/smart_open/pull/315), [@caboteria](https://github.com/caboteria))
  - Dont list buckets in s3 tests (PR [#318](https://github.com/RaRe-Technologies/smart_open/pull/318), [@caboteria](https://github.com/caboteria))
  - Use warnings.warn instead of logger.warning (PR [#321](https://github.com/RaRe-Technologies/smart_open/pull/321), [@mpenkov](https://github.com/mpenkov))
  - Optimize reading from S3 (PR [#322](https://github.com/RaRe-Technologies/smart_open/pull/322), [@mpenkov](https://github.com/mpenkov))

# 1.8.3, 26 April 2019

  - Improve S3 read performance by not copying buffer (PR [#284](https://github.com/RaRe-Technologies/smart_open/pull/284), [@aperiodic](https://github.com/aperiodic))
  - accept bytearray and memoryview as input to write in s3 submodule (PR [#293](https://github.com/RaRe-Technologies/smart_open/pull/293), [@bmizhen-exos](https://github.com/bmizhen-exos))
  - Fix two S3 bugs (PR [#307](https://github.com/RaRe-Technologies/smart_open/pull/307), [@mpenkov](https://github.com/mpenkov))
  - Minor fixes: bz2file dependency, paramiko warning handling (PR [#309](https://github.com/RaRe-Technologies/smart_open/pull/309), [@mpenkov](https://github.com/mpenkov))
  - improve unit tests (PR [#310](https://github.com/RaRe-Technologies/smart_open/pull/310), [@mpenkov](https://github.com/mpenkov))

# 1.8.2, 17 April 2019

  - Removed dependency on lzma (PR [#262](https://github.com/RaRe-Technologies/smart_open/pull/282), [@tdhopper](https://github.com/tdhopper))
  - backward compatibility fixes (PR [#294](https://github.com/RaRe-Technologies/smart_open/pull/294), [@mpenkov](https://github.com/mpenkov))
  - Minor fixes (PR [#291](https://github.com/RaRe-Technologies/smart_open/pull/291), [@mpenkov](https://github.com/mpenkov))
  - Fix #289: the smart_open package now correctly exposes a `__version__` attribute
  - Fix #285: handle edge case with question marks in an S3 URL

This release rolls back support for transparently decompressing .xz files,
previously introduced in 1.8.1.  This is a useful feature, but it requires a
tricky dependency.  It's still possible to handle .xz files with relatively
little effort. Please see the
[README.rst](https://github.com/RaRe-Technologies/smart_open/blob/master/README.rst#supported-compression-formats)
file for details.

# 1.8.1, 6 April 2019

  - Added support for .xz / lzma (PR [#262](https://github.com/RaRe-Technologies/smart_open/pull/262), [@vmarkovtsev](https://github.com/vmarkovtsev))
  - Added streaming HTTP support (PR [#236](https://github.com/RaRe-Technologies/smart_open/pull/236), [@handsomezebra](https://github.com/handsomezebra))
  - Fix handling of "+" mode, refactor tests (PR [#263](https://github.com/RaRe-Technologies/smart_open/pull/263), [@vmarkovtsev](https://github.com/vmarkovtsev))
  - Added support for SSH/SCP/SFTP (PR [#58](https://github.com/RaRe-Technologies/smart_open/pull/58), [@val314159](https://github.com/val314159) & [@mpenkov](https://github.com/mpenkov))
  - Added new feature: compressor registry (PR [#266](https://github.com/RaRe-Technologies/smart_open/pull/266), [@mpenkov](https://github.com/mpenkov))
  - Implemented new `smart_open.open` function (PR [#268](https://github.com/RaRe-Technologies/smart_open/pull/268), [@mpenkov](https://github.com/mpenkov))

## smart_open.open

This new function replaces `smart_open.smart_open`, which is now deprecated.
Main differences:

- ignore_extension → ignore_ext
- new `transport_params` dict parameter to contain keyword parameters for the transport layer (S3, HTTPS, HDFS, etc).

Main advantages of the new function:

- Simpler interface for the user, less parameters
- Greater API flexibility: adding additional keyword arguments will no longer require updating the top-level interface
- Better documentation for keyword parameters (previously, they were documented via examples only)

The old `smart_open.smart_open` function is deprecated, but continues to work as previously.


# 1.8.0, 17th January 2019

  - Add `python3.7` support (PR [#240](https://github.com/RaRe-Technologies/smart_open/pull/240), [@menshikh-iv](https://github.com/menshikh-iv))
  - Add `http/https` schema correctly (PR [#242](https://github.com/RaRe-Technologies/smart_open/pull/242), [@gliv](https://github.com/gliv))
  - Fix url parsing for `S3` (PR [#235](https://github.com/RaRe-Technologies/smart_open/pull/235), [@rileypeterson](https://github.com/rileypeterson))
  - Clean up `_parse_uri_s3x`, resolve edge cases (PR [#237](https://github.com/RaRe-Technologies/smart_open/pull/237), [@mpenkov](https://github.com/mpenkov))
  - Handle leading slash in local path edge case (PR [#238](https://github.com/RaRe-Technologies/smart_open/pull/238), [@mpenkov](https://github.com/mpenkov))
  - Roll back README changes (PR [#239](https://github.com/RaRe-Technologies/smart_open/pull/239), [@mpenkov](https://github.com/mpenkov))
  - Add example how to work with Digital Ocean spaces and boto profile (PR [#248](https://github.com/RaRe-Technologies/smart_open/pull/248), [@navado](https://github.com/@navado) & [@mpenkov](https://github.com/mpenkov))
  - Fix boto fail to load gce plugin (PR [#255](https://github.com/RaRe-Technologies/smart_open/pull/255), [@menshikh-iv](https://github.com/menshikh-iv))
  - Drop deprecated `sudo` from travis config (PR [#256](https://github.com/RaRe-Technologies/smart_open/pull/256), [@cclauss](https://github.com/cclauss))
  - Raise `ValueError` if s3 key does not exist (PR [#245](https://github.com/RaRe-Technologies/smart_open/pull/245), [@adrpar](https://github.com/adrpar))
  - Ensure `_list_bucket` uses continuation token for subsequent pages (PR [#246](https://github.com/RaRe-Technologies/smart_open/pull/246), [@tcsavage](https://github.com/tcsavage))

# 1.7.1, 18th September 2018

  - Unpin boto/botocore for regular installation. Fix #227 (PR [#232](https://github.com/RaRe-Technologies/smart_open/pull/232), [@menshikh-iv](https://github.com/menshikh-iv))

# 1.7.0, 18th September 2018

  - Drop support for `python3.3` and `python3.4` & workaround for broken `moto` (PR [#225](https://github.com/RaRe-Technologies/smart_open/pull/225), [@menshikh-iv](https://github.com/menshikh-iv))
  - Add `s3a://` support for `S3`. Fix #210 (PR [#229](https://github.com/RaRe-Technologies/smart_open/pull/229), [@mpenkov](https://github.com/mpenkov))
  - Allow use `@` in object (key) names for `S3`. Fix #94 (PRs [#204](https://github.com/RaRe-Technologies/smart_open/pull/204) & [#224](https://github.com/RaRe-Technologies/smart_open/pull/224), [@dkasyanov](https://github.com/dkasyanov) & [@mpenkov](https://github.com/mpenkov))
  - Make `close` idempotent & add dummy `flush` for `S3` (PR [#212](https://github.com/RaRe-Technologies/smart_open/pull/212), [@mpenkov](https://github.com/mpenkov))
  - Use built-in `open` whenever possible. Fix #207 (PR [#208](https://github.com/RaRe-Technologies/smart_open/pull/208), [@mpenkov](https://github.com/mpenkov))
  - Fix undefined name `uri` in `smart_open_lib.py`. Fix #213 (PR [#214](https://github.com/RaRe-Technologies/smart_open/pull/214), [@cclauss](https://github.com/cclauss))
  - Fix new unittests from [#212](https://github.com/RaRe-Technologies/smart_open/pull/212) (PR [#219](https://github.com/RaRe-Technologies/smart_open/pull/219), [@mpenkov](https://github.com/mpenkov))
  - Reorganize README & make examples py2/py3 compatible (PR [#211](https://github.com/RaRe-Technologies/smart_open/pull/211), [@piskvorky](https://github.com/piskvorky))

# 1.6.0, 29th June 2018

  - Migrate to `boto3`. Fix #43 (PR [#164](https://github.com/RaRe-Technologies/smart_open/pull/164), [@mpenkov](https://github.com/mpenkov))
  - Refactoring smart_open to share compression and encoding functionality (PR [#185](https://github.com/RaRe-Technologies/smart_open/pull/185), [@mpenkov](https://github.com/mpenkov))
  - Drop `python2.6` compatibility. Fix #156 (PR [#192](https://github.com/RaRe-Technologies/smart_open/pull/192), [@mpenkov](https://github.com/mpenkov))
  - Accept a custom `boto3.Session` instance (support STS AssumeRole). Fix #130, #149, #199 (PR [#201](https://github.com/RaRe-Technologies/smart_open/pull/201), [@eschwartz](https://github.com/eschwartz))
  - Accept `multipart_upload` parameters (supports ServerSideEncryption) for `S3`. Fix (PR [#202](https://github.com/RaRe-Technologies/smart_open/pull/202), [@eschwartz](https://github.com/eschwartz))
  - Add support for `pathlib.Path`. Fix #170 (PR [#175](https://github.com/RaRe-Technologies/smart_open/pull/175), [@clintval](https://github.com/clintval))
  - Fix performance regression using local file-system. Fix #184 (PR [#190](https://github.com/RaRe-Technologies/smart_open/pull/190), [@mpenkov](https://github.com/mpenkov))
  - Replace `ParsedUri` class with functions, cleanup internal argument parsing (PR [#191](https://github.com/RaRe-Technologies/smart_open/pull/191), [@mpenkov](https://github.com/mpenkov))
  - Handle edge case (read 0 bytes) in read function. Fix #171 (PR [#193](https://github.com/RaRe-Technologies/smart_open/pull/193), [@mpenkov](https://github.com/mpenkov))
  - Fix bug with changing `f._current_pos` when call `f.readline()` (PR [#182](https://github.com/RaRe-Technologies/smart_open/pull/182), [@inksink](https://github.com/inksink))
  - Сlose the old body explicitly after `seek` for `S3`. Fix #187 (PR [#188](https://github.com/RaRe-Technologies/smart_open/pull/188), [@inksink](https://github.com/inksink))

# 1.5.7, 18th March 2018

  - Fix author/maintainer fields in `setup.py`, avoid bug from `setuptools==39.0.0` and add workaround for `botocore` and `python==3.3`. Fix #176 (PR [#178](https://github.com/RaRe-Technologies/smart_open/pull/178) & [#177](https://github.com/RaRe-Technologies/smart_open/pull/177), [@menshikh-iv](https://github.com/menshikh-iv) & [@baldwindc](https://github.com/baldwindc))

# 1.5.6, 28th December 2017

  - Improve S3 read performance. Fix #152 (PR [#157](https://github.com/RaRe-Technologies/smart_open/pull/157), [@mpenkov](https://github.com/mpenkov))
  - Add integration testing + benchmark with real S3. Partial fix #151, #156 (PR [#158](https://github.com/RaRe-Technologies/smart_open/pull/158), [@menshikh-iv](https://github.com/menshikh-iv) & [@mpenkov](https://github.com/mpenkov))
  - Disable integration testing if secure vars isn't defined (PR [#157](https://github.com/RaRe-Technologies/smart_open/pull/158), [@menshikh-iv](https://github.com/menshikh-iv))

# 1.5.5, 6th December 2017

  - Fix problems from 1.5.4 release. Fix #153, #154 , partial fix #152 (PR [#155](https://github.com/RaRe-Technologies/smart_open/pull/155), [@mpenkov](https://github.com/mpenkov))

# 1.5.4, 30th November 2017

  - Add naitive .gz support for HDFS (PR [#128](https://github.com/RaRe-Technologies/smart_open/pull/128), [@yupbank](https://github.com/yupbank))
  - Drop python2.6 support + fix style (PR [#137](https://github.com/RaRe-Technologies/smart_open/pull/137), [@menshikh-iv](https://github.com/menshikh-iv))
  - Create separate compression-specific layer. Fix [#91](https://github.com/RaRe-Technologies/smart_open/issues/91) (PR [#131](https://github.com/RaRe-Technologies/smart_open/pull/131), [@mpenkov](https://github.com/mpenkov))
  - Fix ResourceWarnings + replace deprecated assertEquals (PR [#140](https://github.com/RaRe-Technologies/smart_open/pull/140), [@horpto](https://github.com/horpto))
  - Add encoding parameter to smart_open. Fix [#142](https://github.com/RaRe-Technologies/smart_open/issues/142) (PR [#143](https://github.com/RaRe-Technologies/smart_open/pull/143), [@mpenkov](https://github.com/mpenkov))
  - Add encoding tests for readers. Fix [#145](https://github.com/RaRe-Technologies/smart_open/issues/145), partial fix [#146](https://github.com/RaRe-Technologies/smart_open/issues/146) (PR [#147](https://github.com/RaRe-Technologies/smart_open/pull/147), [@mpenkov](https://github.com/mpenkov))
  - Fix file mode for updating case (PR [#150](https://github.com/RaRe-Technologies/smart_open/pull/150), [@menshikh-iv](https://github.com/menshikh-iv))

# 1.5.3, 18th May 2017

  - Remove GET parameters from url. Fix #120 (PR #121, @mcrowson)

# 1.5.2, 12th Apr 2017

  - Enable compressed formats over http. Avoid filehandle leak. Fix #109 and #110. (PR #112, @robottwo )
  - Make possible to change number of retries (PR #102, @shaform)	

# 1.5.1, 16th Mar 2017

  - Bugfix for compressed formats (PR #110, @tmylk)

# 1.5.0, 14th Mar 2017

  - HTTP/HTTPS read support w/ Kerberos (PR #107, @robottwo)

# 1.4.0, 13th Feb 2017

  - HdfsOpenWrite implementation similar to read (PR #106, @skibaa)  
  - Support custom S3 server host, port, ssl. (PR #101, @robottwo)
  - Add retry around `s3_iter_bucket_process_key` to address S3 Read Timeout errors. (PR #96, @bbbco)  
  - Include tests data in sdist + install them. (PR #105, @cournape)
  
# 1.3.5, 5th October 2016

# - Add MANIFEST.in required for conda-forge recip (PR #90, @tmylk)
  - Fix #92. Allow hash in filename (PR #93, @tmylk)

# 1.3.4, 26th August 2016

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

# 1.3.3, 16th May 2016

  - Accept an instance of boto.s3.key.Key to smart_open (PR #38, @asieira)
  - Allow passing `encrypt_key` and other parameters to `initiate_multipart_upload` (PR #63, @asieira)
  - Allow passing boto `host` and `profile_name` to smart_open (PR #71 #68, @robcowie)
  - Write an empty key to S3 even if nothing is written to S3OpenWrite (PR #61, @petedmarsh)
  - Support `LC_ALL=C` environment variable setup (PR #40, @nikicc)
  - Python 3.5 support

# 1.3.2, 3rd January 2016

  - Bug fix release to enable 'wb+' file mode (PR #50)


# 1.3.1, 18th December 2015

  - Disable multiprocessing if unavailable. Allows to run on Google Compute Engine. (PR #41, @nikicc)
  - Httpretty updated to allow LC_ALL=C locale config. (PR #39, @jsphpl)
  - Accept an instance of boto.s3.key.Key (PR #38, @asieira)


# 1.3.0, 19th September 2015

  - WebHDFS read/write (PR #29, @ziky90)
  - re-upload last S3 chunk in failed upload (PR #20, @andreycizov)
  - return the entire key in s3_iter_bucket instead of only the key name (PR #22, @salilb)
  - pass optional keywords on S3 write (PR #30, @val314159)
  - smart_open a no-op if passed a file-like object with a read attribute (PR #32, @gojomo)
  - various improvements to testing (PR #30, @val314159)


# 1.1.0, 1st February 2015

  - support for multistream bzip files (PR #9, @pombredanne)
  - introduce this CHANGELOG
