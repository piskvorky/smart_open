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
  else
    echo "[WARNING] Skip 'benchmark' testing"
  fi
}

integration(){
  pytest integration-tests/test_http.py integration-tests/test_207.py

  if [[ "$TRAVIS_SECURE_ENV_VARS" = "true" ]]; then
    pytest integration-tests/test_s3_ported.py;
  else
    echo "[WARNING] Skip 'integration' testing"
  fi
}


enable_moto_server(){
  moto_server -p5000 2>/dev/null&
}

disable_moto_server(){
  lsof -i tcp:5000 | tail -n1 | cut -f2 -d" " | xargs kill -9 || true
}

"$@"
