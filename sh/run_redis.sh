#!/usr/bin/env bash

set -ex

cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}" )/../" &> /dev/null

PYTHON="${PYTHON:-python}"
PORT="${PORT:-6380}"
# CFG=$(realpath "redis.main.conf")
CFG=
REDIS_PATH=userdata/test/
mkdir -p "${REDIS_PATH}"

cd "${REDIS_PATH}" && redis-server "${CFG}" --port "${PORT}"
