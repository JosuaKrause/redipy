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
