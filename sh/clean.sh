#!/usr/bin/env bash

set -ex

cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}" )/../" &> /dev/null

rm -r test-results/ || true
rm -r coverage/ || true
rm -r plugins/test/ || true
rm -r build/ || true
rm -r dist/ || true

find . -type d \( \
        -path './venv' -o \
        -path './.*' -o \
        -path './userdata' \
        \) -prune -o \( \
        -type d \
        -name '__pycache__' \
        \) \
    | grep -vF './venv' \
    | grep -vF './.' \
    | grep -vF './userdata' \
    | xargs --no-run-if-empty rm -r

rm -r src/redipy.egg-info || echo "no files to delete"

if command -v redis-cli &> /dev/null; then
    redis-cli -p 6380 \
        "EVAL" \
        "for _,k in ipairs(redis.call('keys', KEYS[1])) do redis.call('del', k) end" \
        1 \
        'test:salt:*' \
        || echo "no redis server active"
fi
