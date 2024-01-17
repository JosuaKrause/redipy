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
RESULT_FNAME="${RESULT_FNAME:-results.xml}"
IFS=',' read -a FILE_INFO <<< "$1"
FILES=("${FILE_INFO[@]}")
export USER_FILEPATH=./userdata

coverage erase

${MAKE} clean

${MAKE} compileall

run_test() {
    ${PYTHON} -m pytest \
        -xvv --full-trace \
        --junitxml="test-results/parts/result${2}.xml" \
        --cov --cov-append \
        $1
}
export -f run_test

if ! [ -z "${FILES}" ]; then
    IDX=0
    echo "${FILES[@]}"
    for CUR_TEST in "${FILES[@]}"; do
        run_test $CUR_TEST $IDX
        IDX=$((IDX+1))
    done
else
    IDX=0
    for CUR in $(find 'test' -type d \( \
            -path 'test/data' -o \
            -path 'test/__pycache__' \
            \) -prune -o \( \
            -name '*.py' -and \
            -name 'test_*' \
            \) | \
            grep -E '.*\.py' | \
            sort -sf); do
        run_test ${CUR} $IDX
        IDX=$((IDX+1))
    done
fi
${PYTHON} -m test merge --dir test-results --out-fname ${RESULT_FNAME}
rm -r "test-results/parts/" || true

coverage xml -o coverage/reports/xml_report.xml
coverage html -d coverage/reports/html_report
