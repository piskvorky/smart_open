#!/bin/bash

set -e
set -x


is_travis_secure_vars_available(){
  if [[ "${TRAVIS_SECURE_ENV_VARS}" == "true" ]]; then
    return 0
  else
    echo "[WARNING] TRAVIS_SECURE_ENV_VARS=${TRAVIS_SECURE_ENV_VARS} (but should be true)"
    return 1
  fi
}

benchmark(){
  if ! is_travis_secure_vars_available; then
    return 0
  fi

  SO_S3_URL="${SO_S3_URL}"/`python -c "from uuid import uuid4;print(uuid4())"`;
  COMMIT_HASH=`git rev-parse HEAD`;

  pytest integration-tests/test_s3.py --benchmark-save="${COMMIT_HASH}";

  aws s3 cp .benchmarks/*/*.json "${SO_S3_RESULT_URL}";
  aws s3 rm --recursive "${SO_S3_URL}";
}

integration(){
  pytest integration-tests/test_http.py integration-tests/test_207.py
  if ! is_travis_secure_vars_available; then
    return 0
  fi

  pytest integration-tests/test_s3_ported.py;
}

doctest(){
  if ! is_travis_secure_vars_available; then
    return 0
  fi

  python -m doctest README.rst -v;
}


enable_moto_server(){
  moto_server -p5000 2>/dev/null&
}

disable_moto_server(){
  lsof -i tcp:5000 | tail -n1 | cut -f2 -d" " | xargs kill -9
}

"$@"
