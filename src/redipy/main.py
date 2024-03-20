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
"""This module contains the main class for accessing redis. The Redis class
can be instantiated with different backends."""
import contextlib
import datetime
from collections.abc import Callable, Iterator
from typing import Literal, overload

from redipy.api import (
    KeyType,
    PipelineAPI,
    RedisClientAPI,
    REX_ALWAYS,
    RExpireMode,
    RSetMode,
    RSM_ALWAYS,
)
from redipy.backend.backend import ExecFunction
from redipy.backend.runtime import Runtime
from redipy.graph.seq import SequenceObj
from redipy.memory.rt import LocalRuntime
from redipy.redis.conn import RedisConfig, RedisConnection, RedisFactory
from redipy.symbolic.seq import FnContext


class Redis(RedisClientAPI):
    """
    This class is a wrapper around different runtime backends. Use this class
    to instantiate a redipy runtime.
    """
    def __init__(
            self,
            backend: Literal["memory", "redis", "custom", "infer"] = "infer",
            *,
            cfg: RedisConfig | None = None,
            host: str | None = None,
            port: int | None = None,
            passwd: str | None = None,
            redis_module: str | None = None,
            prefix: str | None = None,
            path: str | None = None,
            is_caching_enabled: bool = True,
            redis_factory: RedisFactory | None = None,
            rt: Runtime | None = None,
            lua_code_hook: Callable[[list[str]], None] | None = None,
            compile_hook: Callable[[SequenceObj], None] | None = None,
            verbose_lua_test: bool = False,
            ) -> None:
        """
        Creates a redis API. You can choose which backend runtime to use or
        you can provide your own. If called without arguments a memory runtime
        is initialized.

        Args:
            backend (Literal["memory", "redis", "custom", "infer"], optional):
            Explicitly states which backend to use. If omitted the backend is
            inferred from the rest of the arguments. Defaults to "infer".

            cfg (RedisConfig | None, optional): An object containing redis
            connection parameters. Only used for "redis" backend.
            Defaults to None.

            host (str | None, optional): The redis host. Only used for "redis"
            backend. Defaults to None.

            port (int | None, optional): The redis port. Only used for "redis"
            backend. Defaults to None.

            passwd (str | None, optional): The redis password. Only used for
            "redis" backend. Defaults to None.

            redis_module (str | None, optional): A prefix that is added to all
            keys in the redis backend. Only used for "redis" backend. Defaults
            to None.

            prefix (str | None, optional): A prefix that is added to all keys
            in the redis backend. This preceeds the redis_module. Only used for
            "redis" backend. Defaults to None.

            path (str | None, optional): The path to the redis configuration
            file and rdb file. The value is effectively ignored but can be
            accessed via the redis runtime. This can be useful when you want to
            control the redis server via python. Only used for "redis" backend.
            Defaults to None.

            is_caching_enabled (bool, optional): Whether redis connections are
            cached between command calls. Only used for "redis" backend.
            Defaults to True.

            redis_factory (RedisFactory | None, optional): An optional factory
            method to create the redis connection object. Only used for "redis"
            backend. Defaults to None.

            rt (Runtime | None, optional): The backend runtime object.
            Only used for "custom" backend. Defaults to None.

            lua_code_hook (Callable[[list[str]], None] | None, optional):
            A debugging hook that is called when registering a script with the
            generated lua code. Only used for "redis" backend.
            Defaults to None.

            compile_hook (Callable[[SequenceObj], None] | None, optional):
            A debugging hook that is called when registering a script with the
            internal execution graph as argument. This is available on every
            backend. Defaults to None.

            verbose_lua_test (bool, optional): A debugging feature that prints
            the lua code to standard out every time a script is registered.
            This also applies to manually registered scripts (as opposed to
            automatically generated lua scripts). Defaults to False.

        Raises:
            ValueError: If the wrong arguments are provided for a given
            backend. Note, that superfluous arguments are not detected. Only
            if a required argument is missing this exception is raised.
        """
        if backend == "infer":
            redis_cfgs = [cfg, host, port, passwd, prefix, path]
            if rt is not None:
                backend = "custom"
            elif any(val is not None for val in redis_cfgs):
                backend = "redis"
            else:
                backend = "memory"
        if backend == "custom":
            if rt is None:
                raise ValueError("rt must not be None for custom backend")
        elif backend == "redis" or (backend == "infer" and any(cfg)):
            if cfg is None:
                cfg = {
                    "host": "localhost" if host is None else host,
                    "port": 6379 if port is None else port,
                    "passwd": "" if passwd is None else passwd,
                    "prefix": "" if prefix is None else prefix,
                    "path": "." if path is None else path,
                }
            rrt = RedisConnection(
                "" if redis_module is None else redis_module,
                cfg=cfg,
                redis_factory=redis_factory,
                is_caching_enabled=is_caching_enabled,
                verbose_test=verbose_lua_test)
            if lua_code_hook is not None:
                rrt.set_code_hook(lua_code_hook)
            rt = rrt
        elif backend == "memory":
            rt = LocalRuntime()
        else:
            raise ValueError(f"unknown backend {backend}")
        self._rt: Runtime = rt
        rt.set_compile_hook(compile_hook)

    def get_runtime(self) -> Runtime:
        """
        Returns the associated runtime.

        Returns:
            Runtime: The current runtime.
        """
        return self._rt

    def maybe_get_redis_runtime(self) -> RedisConnection | None:
        """
        Returns the redis runtime if this is a redis connection.

        Returns:
            RedisConnection | None: The redis runtime if available.
            None otherwise.
        """
        if not isinstance(self._rt, RedisConnection):
            return None
        return self._rt

    def maybe_get_memory_runtime(self) -> LocalRuntime | None:
        """
        Returns the memory runtime if this is a memory connection.

        Returns:
            LocalRuntime | None: The memory runtime if available.
            None otherwise.
        """
        if not isinstance(self._rt, LocalRuntime):
            return None
        return self._rt

    def get_redis_runtime(self) -> RedisConnection:
        """
        Returns the redis runtime.

        Raises:
            ValueError: If this is not a redis connection.

        Returns:
            RedisConnection: The redis runtime.
        """
        res = self.maybe_get_redis_runtime()
        if res is None:
            raise ValueError("not a redis runtime")
        return res

    def get_memory_runtime(self) -> LocalRuntime:
        """
        Returns the memory runtime.

        Raises:
            ValueError: If this is not a memory connection.

        Returns:
            LocalRuntime: The memory runtime.
        """
        res = self.maybe_get_memory_runtime()
        if res is None:
            raise ValueError("not a memory runtime")
        return res

    def register_script(self, ctx: FnContext) -> ExecFunction:
        return self._rt.register_script(ctx)

    @contextlib.contextmanager
    def pipeline(self) -> Iterator[PipelineAPI]:
        with self._rt.pipeline() as pipe:
            yield pipe

    def exists(self, *keys: str) -> int:
        return self._rt.exists(*keys)

    def delete(self, *keys: str) -> int:
        return self._rt.delete(*keys)

    def key_type(self, key: str) -> KeyType | None:
        return self._rt.key_type(key)

    def scan(
            self,
            cursor: int,
            *,
            match: str | None = None,
            count: int | None = None,
            filter_type: KeyType | None = None) -> tuple[int, list[str]]:
        return self._rt.scan(
            cursor, match=match, count=count, filter_type=filter_type)

    def keys_block(
            self,
            *,
            match: str | None = None,
            filter_type: KeyType | None = None) -> list[str]:
        return self._rt.keys_block(match=match, filter_type=filter_type)

    def flushall(self) -> None:
        return self._rt.flushall()

    @overload
    def set_value(
            self,
            key: str,
            value: str,
            *,
            mode: RSetMode,
            return_previous: Literal[True],
            expire_timestamp: datetime.datetime | None,
            expire_in: float | None,
            keep_ttl: bool) -> str | None:
        ...

    @overload
    def set_value(
            self,
            key: str,
            value: str,
            *,
            mode: RSetMode,
            return_previous: Literal[False],
            expire_timestamp: datetime.datetime | None,
            expire_in: float | None,
            keep_ttl: bool) -> bool | None:
        ...

    @overload
    def set_value(
            self,
            key: str,
            value: str,
            *,
            mode: RSetMode = RSM_ALWAYS,
            return_previous: bool = False,
            expire_timestamp: datetime.datetime | None = None,
            expire_in: float | None = None,
            keep_ttl: bool = False) -> str | bool | None:
        ...

    def set_value(
            self,
            key: str,
            value: str,
            *,
            mode: RSetMode = RSM_ALWAYS,
            return_previous: bool = False,
            expire_timestamp: datetime.datetime | None = None,
            expire_in: float | None = None,
            keep_ttl: bool = False) -> str | bool | None:
        return self._rt.set_value(
            key,
            value,
            mode=mode,
            return_previous=return_previous,
            expire_timestamp=expire_timestamp,
            expire_in=expire_in,
            keep_ttl=keep_ttl)

    def get_value(self, key: str) -> str | None:
        return self._rt.get_value(key)

    def expire(
            self,
            key: str,
            *,
            mode: RExpireMode = REX_ALWAYS,
            expire_timestamp: datetime.datetime | None = None,
            expire_in: float | None = None) -> bool:
        return self._rt.expire(
            key,
            mode=mode,
            expire_timestamp=expire_timestamp,
            expire_in=expire_in)

    def ttl(self, key: str) -> float | None:
        return self._rt.ttl(key)

    def incrby(self, key: str, inc: float | int) -> float:
        return self._rt.incrby(key, inc)

    def lpush(self, key: str, *values: str) -> int:
        return self._rt.lpush(key, *values)

    def rpush(self, key: str, *values: str) -> int:
        return self._rt.rpush(key, *values)

    @overload
    def lpop(
            self,
            key: str,
            count: None = None) -> str | None:
        ...

    @overload
    def lpop(  # pylint: disable=signature-differs
            self,
            key: str,
            count: int) -> list[str] | None:
        ...

    def lpop(
            self,
            key: str,
            count: int | None = None) -> str | list[str] | None:
        return self._rt.lpop(key, count)

    @overload
    def rpop(
            self,
            key: str,
            count: None = None) -> str | None:
        ...

    @overload
    def rpop(  # pylint: disable=signature-differs
            self,
            key: str,
            count: int) -> list[str] | None:
        ...

    def rpop(
            self,
            key: str,
            count: int | None = None) -> str | list[str] | None:
        return self._rt.rpop(key, count)

    def lrange(self, key: str, start: int, stop: int) -> list[str]:
        return self._rt.lrange(key, start, stop)

    def llen(self, key: str) -> int:
        return self._rt.llen(key)

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        return self._rt.zadd(key, mapping)

    def zpop_max(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        return self._rt.zpop_max(key, count)

    def zpop_min(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        return self._rt.zpop_min(key, count)

    def zrange(self, key: str, start: int, stop: int) -> list[str]:
        return self._rt.zrange(key, start, stop)

    def zcard(self, key: str) -> int:
        return self._rt.zcard(key)

    def hset(self, key: str, mapping: dict[str, str]) -> int:
        return self._rt.hset(key, mapping)

    def hdel(self, key: str, *fields: str) -> int:
        return self._rt.hdel(key, *fields)

    def hget(self, key: str, field: str) -> str | None:
        return self._rt.hget(key, field)

    def hmget(self, key: str, *fields: str) -> dict[str, str | None]:
        return self._rt.hmget(key, *fields)

    def hincrby(self, key: str, field: str, inc: float | int) -> float:
        return self._rt.hincrby(key, field, inc)

    def hkeys(self, key: str) -> list[str]:
        return self._rt.hkeys(key)

    def hvals(self, key: str) -> list[str]:
        return self._rt.hvals(key)

    def hgetall(self, key: str) -> dict[str, str]:
        return self._rt.hgetall(key)

    def sadd(self, key: str, *values: str) -> int:
        return self._rt.sadd(key, *values)

    def srem(self, key: str, *values: str) -> int:
        return self._rt.srem(key, *values)

    def sismember(self, key: str, value: str) -> bool:
        return self._rt.sismember(key, value)

    def scard(self, key: str) -> int:
        return self._rt.scard(key)

    def smembers(self, key: str) -> set[str]:
        return self._rt.smembers(key)
