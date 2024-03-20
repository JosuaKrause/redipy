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
"""Implements redis functionality for the memory backend."""
from typing import cast

from redipy.api import RSetMode, RSM_ALWAYS, RSM_EXISTS, RSM_MISSING
from redipy.graph.expr import JSONType
from redipy.memory.state import Machine
from redipy.plugin import ArgcSpec, LocalRedisFunction


class RExistsFn(LocalRedisFunction):
    """Implements the exists function."""
    @staticmethod
    def name() -> str:
        return "exists"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.exists(key)


class RDelFn(LocalRedisFunction):
    """Implements the del function."""
    @staticmethod
    def name() -> str:
        return "del"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.delete(key)


class RTypeFn(LocalRedisFunction):
    """Implements the type function."""
    @staticmethod
    def name() -> str:
        return "type"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.key_type(key)


class RSetFn(LocalRedisFunction):
    """Implements the set function."""
    @staticmethod
    def name() -> str:
        return "set"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
            "at_least": True,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        value = f"{args[0]}"
        args = args[1:]
        mode: RSetMode = RSM_ALWAYS
        return_previous = False
        expire_in = None
        keep_ttl = False
        pos = 0
        while pos < len(args):
            arg = f"{args[pos]}".upper()
            if arg == "XX":
                mode = RSM_EXISTS
            elif arg == "NX":
                mode = RSM_MISSING
            elif arg == "GET":
                return_previous = True
            elif arg == "PX":
                pos += 1
                expire_in = float(cast(float, args[pos])) / 1000.0
            elif arg == "KEEPTTL":
                keep_ttl = True
            pos += 1
        return sm.set_value(
            key,
            value,
            mode=mode,
            return_previous=return_previous,
            expire_in=expire_in,
            keep_ttl=keep_ttl)


class RGetFn(LocalRedisFunction):
    """Implements the get function."""
    @staticmethod
    def name() -> str:
        return "get"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.get_value(key)


class RIncrByFn(LocalRedisFunction):
    """Implements the incrby and incrbyfloat functions."""
    @staticmethod
    def name() -> str:
        return "incrby"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.incrby(key, float(cast(float, args[0])))


class RLPushFn(LocalRedisFunction):
    """Implements lpush."""
    @staticmethod
    def name() -> str:
        return "lpush"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
            "at_least": True,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.lpush(key, *(f"{arg}" for arg in args))


class RRPushFn(LocalRedisFunction):
    """Implements rpush."""
    @staticmethod
    def name() -> str:
        return "rpush"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
            "at_least": True,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.rpush(key, *(f"{arg}" for arg in args))


class RLPopFn(LocalRedisFunction):
    """Implements lpop."""
    @staticmethod
    def name() -> str:
        return "lpop"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
            "at_most": 1,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.lpop(
            key, None if len(args) < 1 else int(cast(int, args[0])))


class RRPopFn(LocalRedisFunction):
    """Implements rpop."""
    @staticmethod
    def name() -> str:
        return "rpop"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
            "at_most": 1,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.rpop(
            key, None if len(args) < 1 else int(cast(int, args[0])))


class RLRangeFn(LocalRedisFunction):
    """Implements the lrange function."""
    @staticmethod
    def name() -> str:
        return "lrange"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 2,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.lrange(key, int(cast(int, args[0])), int(cast(int, args[1])))


class RLLenFn(LocalRedisFunction):
    """Implements the llen function."""
    @staticmethod
    def name() -> str:
        return "llen"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.llen(key)


class RZAddFn(LocalRedisFunction):
    """Implements the zadd function."""
    @staticmethod
    def name() -> str:
        return "zadd"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 2,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.zadd(key, {f"{args[1]}": float(cast(float, args[0]))})


class RZPopMaxFn(LocalRedisFunction):
    """Implements zpopmax."""
    @staticmethod
    def name() -> str:
        return "zpopmax"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
            "at_most": 1,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return cast(list | None, sm.zpop_max(
            key, 1 if len(args) < 1 else int(cast(int, args[0]))))


class RZPopMinFn(LocalRedisFunction):
    """Implements zpopmin."""
    @staticmethod
    def name() -> str:
        return "zpopmin"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
            "at_most": 1,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return cast(list | None, sm.zpop_min(
            key, 1 if len(args) < 1 else int(cast(int, args[0]))))


class RZRangeFn(LocalRedisFunction):
    """Implements the zrange function."""
    @staticmethod
    def name() -> str:
        return "zrange"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 2,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.zrange(key, int(cast(int, args[0])), int(cast(int, args[1])))


class RZCardFn(LocalRedisFunction):
    """Implements the zcard function."""
    @staticmethod
    def name() -> str:
        return "zcard"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.zcard(key)


class RHSetFn(LocalRedisFunction):
    """Implements the hset function."""
    @staticmethod
    def name() -> str:
        return "hset"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 2,
            "at_least": True,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        mapping = {}
        ix = 0
        while ix < len(args):
            field = f"{args[ix]}"
            ix += 1
            if ix >= len(args):
                raise ValueError(f"unbalanced field value pairs: {args}")
            value = f"{args[ix]}"
            ix += 1
            mapping[field] = value
        return sm.hset(key, mapping)


class RHDelFn(LocalRedisFunction):
    """Implements the hdel function."""
    @staticmethod
    def name() -> str:
        return "hdel"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
            "at_least": True,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.hdel(key, *(f"{arg}" for arg in args))


class RHGetFn(LocalRedisFunction):
    """Implements the hget function."""
    @staticmethod
    def name() -> str:
        return "hget"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.hget(key, f"{args[0]}")


class RHMGetFn(LocalRedisFunction):
    """Implements the hmget function."""
    @staticmethod
    def name() -> str:
        return "hmget"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
            "at_least": True,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.hmget(key, *(f"{arg}" for arg in args))


class RHIncrByFn(LocalRedisFunction):
    """Implements the hincrby and hincrbyfloat functions."""
    @staticmethod
    def name() -> str:
        return "hincrby"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 2,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.hincrby(key, f"{args[0]}", cast(float, args[1]))


class RHKeysFn(LocalRedisFunction):
    """Implements the hkeys function."""
    @staticmethod
    def name() -> str:
        return "hkeys"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.hkeys(key)


class RHValsFn(LocalRedisFunction):
    """Implements the hvals function."""
    @staticmethod
    def name() -> str:
        return "hvals"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.hvals(key)


class RHGetAllFn(LocalRedisFunction):
    """Implements the hgetall function."""
    @staticmethod
    def name() -> str:
        return "hgetall"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.hgetall(key)


class RSAdd(LocalRedisFunction):
    """Implements the sadd function."""
    @staticmethod
    def name() -> str:
        return "sadd"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
            "at_least": True,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.sadd(key, *(f"{arg}" for arg in args))


class RSRem(LocalRedisFunction):
    """Implements the srem function."""
    @staticmethod
    def name() -> str:
        return "srem"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
            "at_least": True,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.srem(key, *(f"{arg}" for arg in args))


class RSIsMember(LocalRedisFunction):
    """Implements the sismember function."""
    @staticmethod
    def name() -> str:
        return "sismember"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.sismember(key, f"{args[0]}")


class RSMembers(LocalRedisFunction):
    """Implements the smembers function."""
    @staticmethod
    def name() -> str:
        return "smembers"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sorted(sm.smembers(key))


class RSCard(LocalRedisFunction):
    """Implements the scard function."""
    @staticmethod
    def name() -> str:
        return "scard"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 0,
        }

    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        return sm.scard(key)
