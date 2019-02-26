This directory contains integration tests for smart_open.
To run the tests, you need read/write access to an S3 bucket.
Also, you need to install py.test and its benchmarks addon:

    pip install -r requirements.txt

Then, to run the tests, run:

    SO_S3_URL=s3://bucket/smart_open_test py.test integration-tests/test_s3.py

You may use any key name instead of "smart_open_test".
It does not have to be an existing key.
**The tests will remove the key prior to each test, so be sure the key doesn't contain anything important.**

The tests will take several minutes to complete.
Each test will run several times to obtain summary statistics such as min, max, mean and median.
This allows us to detect regressions in performance.
Here is some example output (you need a wide screen to get the best of it):

```
(smartopen)sergeyich:smart_open misha$ SMART_OPEN_S3_URL=s3://bucket/smart_open_test py.test integration-tests/test_s3.py
=============================================== test session starts ================================================
platform darwin -- Python 3.6.3, pytest-3.3.0, py-1.5.2, pluggy-0.6.0
benchmark: 3.1.1 (defaults: timer=time.perf_counter disable_gc=False min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=100000)
rootdir: /Users/misha/git/smart_open, inifile:
plugins: benchmark-3.1.1
collected 6 items

integration-tests/test_s3.py ......                                                                          [100%]


--------------------------------------------------------------------------------------- benchmark: 6 tests --------------------------------------------------------------------------------------
Name (time in s)                     Min                Max               Mean             StdDev             Median                IQR            Outliers     OPS            Rounds  Iterations
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_s3_readwrite_text            2.7593 (1.0)       3.4935 (1.0)       3.2203 (1.0)       0.3064 (1.0)       3.3202 (1.04)      0.4730 (1.0)           1;0  0.3105 (1.0)           5           1
test_s3_readwrite_text_gzip       3.0242 (1.10)      4.6782 (1.34)      3.7079 (1.15)      0.8531 (2.78)      3.2001 (1.0)       1.5850 (3.35)          2;0  0.2697 (0.87)          5           1
test_s3_readwrite_binary          3.0549 (1.11)      3.9062 (1.12)      3.5399 (1.10)      0.3516 (1.15)      3.4721 (1.09)      0.5532 (1.17)          2;0  0.2825 (0.91)          5           1
test_s3_performance_gz            3.1885 (1.16)      5.2845 (1.51)      3.9298 (1.22)      0.8197 (2.68)      3.6974 (1.16)      0.9693 (2.05)          1;0  0.2545 (0.82)          5           1
test_s3_readwrite_binary_gzip     3.3756 (1.22)      5.0423 (1.44)      4.1763 (1.30)      0.6381 (2.08)      4.0722 (1.27)      0.9209 (1.95)          2;0  0.2394 (0.77)          5           1
test_s3_performance               7.6758 (2.78)     29.5266 (8.45)     18.8346 (5.85)     10.3003 (33.62)    21.1854 (6.62)     19.6234 (41.49)         3;0  0.0531 (0.17)          5           1
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
============================================ 6 passed in 285.14 seconds ============================================
```
