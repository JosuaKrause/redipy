#!/usr/bin/env bash

set -ex

cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}" )/../" &> /dev/null

PYTHON="${PYTHON:-python}"

cat "requirements.txt" "requirements.dev.txt" | sed -E 's/[>=~]=.+//' | sort -sf | diff -U 1 "requirements.noversion.txt" -
