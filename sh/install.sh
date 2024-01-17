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

PYTHON="${PYTHON:-python}"
which ${PYTHON} > /dev/null
if [ $? -ne 0 ]; then
    PYTHON=python
fi

MAJOR=$(${PYTHON} -c 'import sys; print(sys.version_info.major)')
MINOR=$(${PYTHON} -c 'import sys; print(sys.version_info.minor)')
echo "${PYTHON} v${MAJOR}.${MINOR}"
if [ ${MAJOR} -eq 3 ] && [ ${MINOR} -lt 11 ] || [ ${MAJOR} -lt 3 ]; then
    echo "${PYTHON} version must at least be 3.11" >&2
    exit 1
fi

${PYTHON} -m pip install --progress-bar off --upgrade pip
${PYTHON} -m pip install --progress-bar off --upgrade -r requirements.txt
${PYTHON} -m pip install --progress-bar off --upgrade -r requirements.dev.txt
