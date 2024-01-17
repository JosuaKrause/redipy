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
VERSION=$(${MAKE} -s version)

git fetch --tags
if git show-ref --tags "v${VERSION}" --quiet; then
    echo "version ${VERSION} already exists"
    exit 1
fi

FILE_WHL="dist/redipy-${VERSION}-py3-none-any.whl"
FILE_SRC="dist/redipy-${VERSION}.tar.gz"

if [ ! -f "${FILE_WHL}" ] || [ ! -f "${FILE_SRC}" ]; then
    ${MAKE} pack
fi

${PYTHON} -m twine upload --repository pypi "${FILE_WHL}" "${FILE_SRC}"
git tag "v${VERSION}"
git push origin "v${VERSION}"
echo "successfully deployed ${VERSION}"
