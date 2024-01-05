#!/usr/bin/env bash
# Copyright 2024 Josua Krause
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
