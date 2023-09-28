#!/usr/bin/env bash

set -e

cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}" )/../" &> /dev/null

USAGE="Usage: $0 [--tag] [--next]"

usage() {
    echo $USAGE
    echo "-h: print help"
    echo "--tag: reads the version from the closest tag instead of pyproject"
    echo "--next: compute the next version"
    exit 1
}

ARG_TAG=
ARG_NEXT=

while [ $# -gt 0 ]; do
    case "$1" in
        "")
            ;;
        -h)
            usage ;;
        --tag)
            ARG_TAG=1
            ;;
        --next)
            ARG_NEXT=1
            ;;
        *)
            usage ;;
    esac
    shift
done

MAKE="${MAKE:-make}"
PYTHON="${PYTHON:-python}"

if [ -z "${ARG_TAG}" ]; then
    TOML_CHK="import tomllib;print(tomllib.load(open('pyproject.toml', 'rb'))['project']['version'])"
    CUR_VERSION=$(echo "${TOML_CHK}" | ${PYTHON} 2>/dev/null)
else
    CUR_VERSION=$(git describe --tags --abbrev=0)
fi

if [ -z "${ARG_NEXT}" ]; then
    echo "${CUR_VERSION}"
    exit 0
fi

PREFIX="v"
if [ -z "${ARG_TAG}" ]; then
    CUR_VERSION="v${CUR_VERSION}"
    PREFIX=
fi

# version must match either of:
# v<MAJOR_VERSION>.<MINOR_VERSION>.<PATCH_VERSION>rc<RC_VERSION>
# v<MAJOR_VERSION>.<MINOR_VERSION>.<PATCH_VERSION>

MAJOR_VERSION=$(echo "${CUR_VERSION}" | awk -F'rc' '{print $1}' | awk -F'v' '{print $2}' | awk -F'.' '{print $1}')
MINOR_VERSION=$(echo "${CUR_VERSION}" | awk -F'rc' '{print $1}' | awk -F'v' '{print $2}' | awk -F'.' '{print $2}')
PATCH_VERSION=$(echo "${CUR_VERSION}" | awk -F'rc' '{print $1}' | awk -F'v' '{print $2}' | awk -F'.' '{print $3}')
RC_VERSION=$(echo "${CUR_VERSION}" | awk -F'rc' '{print $2}')

# next version on minor version only
MINOR_VERSION=$((MINOR_VERSION + 1))
PATCH_VERSION=0
RC_VERSION=0

if [ -n $RC_VERSION -a $RC_VERSION -ne 0 ]
then
    echo "${PREFIX}${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}rc${RC_VERSION}"
else
    echo "${PREFIX}${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}"
fi
