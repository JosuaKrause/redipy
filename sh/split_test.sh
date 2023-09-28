#!/usr/bin/env bash

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
