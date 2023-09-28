#!/usr/bin/env bash

set -ex

cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}" )/../" &> /dev/null

if output=$(git status --porcelain) && ! [ -z "$output" ]; then
    echo "working copy is not clean" >&2
    exit 1
fi

if ! git diff --exit-code 2>&1 >/dev/null && git diff --cached --exit-code 2>&1 >/dev/null ; then
    echo "working copy is not clean" >&2
    exit 2
fi

if ! git diff-index --quiet HEAD -- ; then
    echo "there are uncommitted files" >&2
    exit 3
fi

if [ -z $(git ls-files --other --exclude-standard --directory) ]; then
    echo "there are untracked files"
    exit 4
fi
