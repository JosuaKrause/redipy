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

PYTHON="${PYTHON:-python}"
PORT="${PORT:-6380}"
# CFG=$(realpath "redis.main.conf")
CFG=
REDIS_PATH=userdata/test/
mkdir -p "${REDIS_PATH}"

cd "${REDIS_PATH}" && redis-server "${CFG}" --port "${PORT}"
