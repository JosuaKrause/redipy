#!/usr/bin/env bash

set -ex

cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}" )/../" &> /dev/null

if [ $(git rev-parse --abbrev-ref HEAD) = "main" ]; then
    echo "not on main"
    exit 1
fi
