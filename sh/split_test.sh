#!/usr/bin/env bash
#
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
#
set -ex

cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}" )/../" &> /dev/null

MAKE="${MAKE:-make}"
PYTHON="${PYTHON:-python}"

rm "test-results/results-*.xml" || true
OUT=$("${PYTHON}" -m test split --filepath test-results/results.xml --total-nodes "${TOTAL_NODES}" --node-id "${CUR_NODE_IX}")
IFS=',' read -a FILE_INFO <<< "$OUT"
echo "previous timings: ${FILE_INFO[0]}"
FILES=$(echo "${OUT}" | sed -E 's/^[^,]*,//')
echo "selected tests: ${FILES}"
rm -r "test-results/" || true
RESULT_FNAME="results-${PYTHON_NAME}-${CUR_NODE_IX}.xml" "${MAKE}" pytest FILE="${FILES}"
tail -v -n +1 test-results/*.xml
