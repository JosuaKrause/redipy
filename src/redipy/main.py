import contextlib
from collections.abc import Iterator
from typing import Literal, overload

from redipy.api import PipelineAPI, RedisClientAPI
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

    def set(self, key: str, value: str) -> str:
        return self._rt.set(key, value)

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
