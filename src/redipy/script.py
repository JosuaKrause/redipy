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
"""This module offers functionality to create Redis functions. The `FnContext`
class is the base of all scripts and after defining the script it can be
registered via the `register_script` method of `redipy.Redis`.

This module re-exports various scripting functionality for convenience. The
module exists as unified module for all scripting related symbols.
"""
from redipy.backend.backend import ExecFunction
from redipy.graph.expr import JSONType
from redipy.symbolic.expr import Constant, Strs
from redipy.symbolic.fun import (
    CallFn,
    FindFn,
    FromJSON,
    LogFn,
    LogLevel,
    RedisFn,
    ToIntStr,
    ToJSON,
    ToNum,
    ToStr,
    TypeStr,
)
from redipy.symbolic.rhash import RedisHash
from redipy.symbolic.rlist import RedisList
from redipy.symbolic.rvar import RedisVar
from redipy.symbolic.rzset import RedisSortedSet
from redipy.symbolic.seq import FnContext


__all__ = [
    "CallFn",
    "Constant",
    "ExecFunction",
    "FindFn",
    "FnContext",
    "FromJSON",
    "JSONType",
    "LogFn",
    "LogLevel",
    "RedisFn",
    "RedisHash",
    "RedisList",
    "RedisSortedSet",
    "RedisVar",
    "Strs",
    "ToIntStr",
    "ToJSON",
    "ToNum",
    "ToStr",
    "TypeStr",
]
