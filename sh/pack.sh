#!/usr/bin/env bash

set -ex

cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}" )/../" &> /dev/null

PYTHON="${PYTHON:-python}"

${PYTHON} -m pip install --progress-bar off --upgrade build twine
${PYTHON} -m build
