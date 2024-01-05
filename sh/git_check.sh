#!/usr/bin/env bash
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
