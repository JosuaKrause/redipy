import contextlib
import datetime
from collections.abc import Iterator
from typing import Literal, overload

from redipy.api import PipelineAPI, RedisClientAPI, RSetMode, RSM_ALWAYS
from redipy.backend.backend import ExecFunction
from redipy.backend.runtime import Runtime
from redipy.memory.rt import LocalRuntime
from redipy.redis.conn import RedisConfig, RedisConnection, RedisFactory
from redipy.symbolic.seq import FnContext


class Redis(RedisClientAPI):
    def __init__(
            self,
            backend: Literal["memory", "redis", "custom"],
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
            ) -> None:
        if backend == "custom":
            if rt is None:
                raise ValueError("rt must not be None for custom backend")
        elif backend == "memory":
            rt = LocalRuntime()
        elif backend == "redis":
            if cfg is None:
                cfg = {
                    "host": "localhost" if host is None else host,
                    "port": 6379 if port is None else port,
                    "passwd": "" if passwd is None else passwd,
                    "prefix": "" if prefix is None else prefix,
                    "path": "." if path is None else path,
                }
            rt = RedisConnection(
                "" if redis_module is None else redis_module,
                cfg=cfg,
                redis_factory=redis_factory,
                is_caching_enabled=is_caching_enabled)
        else:
            raise ValueError(f"unknown backend {backend}")
        self._rt: Runtime = rt

    def register_script(self, ctx: FnContext) -> ExecFunction:
        return self._rt.register_script(ctx)

    @contextlib.contextmanager
    def pipeline(self) -> Iterator[PipelineAPI]:
        with self._rt.pipeline() as pipe:
            yield pipe

    @overload
    def set(
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
    def set(
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
    def set(
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

    def set(
            self,
            key: str,
            value: str,
            *,
            mode: RSetMode = RSM_ALWAYS,
            return_previous: bool = False,
            expire_timestamp: datetime.datetime | None = None,
            expire_in: float | None = None,
            keep_ttl: bool = False) -> str | bool | None:
        return self._rt.set(
            key,
            value,
            mode=mode,
            return_previous=return_previous,
            expire_timestamp=expire_timestamp,
            expire_in=expire_in,
            keep_ttl=keep_ttl)

    def get(self, key: str) -> str | None:
        return self._rt.get(key)

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

    def zcard(self, key: str) -> int:
        return self._rt.zcard(key)

    def delete(self, *keys: str) -> int:
        return self._rt.delete(*keys)

    def hset(self, key: str, mapping: dict[str, str]) -> int:
        return self._rt.hset(key, mapping)

    def hdel(self, key: str, *fields: str) -> int:
        return self._rt.hdel(key, *fields)

    def hget(self, key: str, field: str) -> str | None:
        return self._rt.hget(key, field)

    def hmget(self, key: str, *fields: str) -> dict[str, str | None]:
        return self._rt.hmget(key, *fields)

    def hincrby(self, key: str, field: str, inc: float) -> float:
        return self._rt.hincrby(key, field, inc)

    def hkeys(self, key: str) -> list[str]:
        return self._rt.hkeys(key)

    def hvals(self, key: str) -> list[str]:
        return self._rt.hvals(key)

    def hgetall(self, key: str) -> dict[str, str]:
        return self._rt.hgetall(key)
