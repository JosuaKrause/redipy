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
