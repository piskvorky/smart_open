#!/bin/bash

set -e
set -x


benchmark(){
  if [[ "$TRAVIS_SECURE_ENV_VARS" = "true" && "$RUN_BENCHMARKS" = "true" ]]; then
      SO_S3_URL="$SO_S3_URL"/`python -c "from uuid import uuid4;print(uuid4())"`;
      COMMIT_HASH=`git rev-parse HEAD`;

      pytest integration-tests/test_s3.py --benchmark-save="$COMMIT_HASH";

      aws s3 cp .benchmarks/*/*.json "$SO_S3_RESULT_URL";
      aws s3 rm --recursive $SO_S3_URL;
  fi
}

integration(){
  pytest integration-tests/test_http.py integration-tests/test_207.py
  if [[ "$TRAVIS_SECURE_ENV_VARS" = "true" ]]; then
    pytest integration-tests/test_s3_ported.py;
  fi
}

"$@"
