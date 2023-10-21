from typing import cast

from redipy.api import RSetMode, RSM_ALWAYS, RSM_EXISTS, RSM_MISSING
from redipy.graph.expr import JSONType
from redipy.memory.state import Machine
from redipy.plugin import ArgcSpec, LocalRedisFunction


class RSetFn(LocalRedisFunction):
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
        return sm.set(
            key,
            value,
            mode=mode,
            return_previous=return_previous,
            expire_in=expire_in,
            keep_ttl=keep_ttl)


class RGetFn(LocalRedisFunction):
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
        return sm.get(key)


class RIncrBy(LocalRedisFunction):
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
        return sm.incrby(key, cast(float, args[0]))


class RLPushFn(LocalRedisFunction):
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


class RLLenFn(LocalRedisFunction):
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


class RZCard(LocalRedisFunction):
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


class RExists(LocalRedisFunction):
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


class RDel(LocalRedisFunction):
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


class RHSet(LocalRedisFunction):
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
            key = f"{args[ix]}"
            ix += 1
            if ix >= len(args):
                raise ValueError(f"unbalanced key value pairs: {args}")
            value = f"{args[ix]}"
            ix += 1
            mapping[key] = value
        return sm.hset(key, mapping)


class RHDel(LocalRedisFunction):
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


class RHGet(LocalRedisFunction):
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


class RHMGet(LocalRedisFunction):
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


class RHIncrBy(LocalRedisFunction):
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


class RHKeys(LocalRedisFunction):
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


class RHVals(LocalRedisFunction):
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


class RHGetAll(LocalRedisFunction):
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
