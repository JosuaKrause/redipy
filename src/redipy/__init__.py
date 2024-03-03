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
"""Redipy is a backend agnostic redis interface. It supports an in process
memory redis backend and a redis server connection backend. Redis functions can
be created using functionality of the `redipy.script` module. New functions or
redis functionality can be added with the help of the `redipy.plugin` module.

The most common symbols of redipy are reexported at the top level for easy
access."""
from typing import Any

import redipy.plugin  # pylint: disable=unused-import  # noqa
import redipy.script  # pylint: disable=unused-import  # noqa
from redipy.api import (
    PipelineAPI,
    RedisAPI,
    RedisClientAPI,
    RSetMode,
    RSM_ALWAYS,
    RSM_EXISTS,
    RSM_MISSING,
)
from redipy.backend.backend import ExecFunction
from redipy.backend.runtime import Runtime
from redipy.main import Redis
from redipy.memory.rt import LocalRuntime
from redipy.redis.conn import RedisConfig, RedisConnection, RedisFactory


PACKAGE_VERSION: str | None = None


def _get_version() -> str:
    # pylint: disable=import-outside-toplevel
    global PACKAGE_VERSION  # pylint: disable=global-statement

    if PACKAGE_VERSION is None:
        try:
            from importlib.metadata import PackageNotFoundError, version

            PACKAGE_VERSION = version("redipy")
        except PackageNotFoundError:
            try:
                import os
                import tomllib

                pyproject_fname = os.path.join(
                    os.path.dirname(__file__), "../../pyproject.toml")
                if (os.path.exists(pyproject_fname)
                        and os.path.isfile(pyproject_fname)):
                    with open(pyproject_fname, "rb") as fin:
                        pyproject = tomllib.load(fin)
                    if pyproject["project"]["name"] == "redipy":
                        PACKAGE_VERSION = f"{pyproject['project']['version']}*"
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        if PACKAGE_VERSION is None:
            PACKAGE_VERSION = "unknown"
    return PACKAGE_VERSION


def __getattr__(name: str) -> Any:
    if name in ("version", "__version__"):
        return _get_version()
    raise AttributeError(f"No attribute {name} in module {__name__}.")


__all__ = [
    "ExecFunction",
    "LocalRuntime",
    "PipelineAPI",
    "plugin",
    "Redis",
    "RedisAPI",
    "RedisClientAPI",
    "RedisConfig",
    "RedisConnection",
    "RedisFactory",
    "RSetMode",
    "RSM_ALWAYS",
    "RSM_EXISTS",
    "RSM_MISSING",
    "Runtime",
    "script",
]
