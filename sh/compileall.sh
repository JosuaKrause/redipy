#!/usr/bin/env bash

set -ex

cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}" )/../" &> /dev/null

PYTHON="${PYTHON:-python}"

./sh/findpy.sh \
    | xargs --no-run-if-empty ${PYTHON} -m compileall -q -j 0
