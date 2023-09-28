#!/usr/bin/env bash

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
