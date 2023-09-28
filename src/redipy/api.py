from typing import overload

from redipy.backend.backend import ExecFunction
from redipy.symbolic.seq import FnContext


class RedisAPI:
    def register_script(self, ctx: FnContext) -> ExecFunction:
        raise NotImplementedError()

    def set(self, key: str, value: str) -> str:
        raise NotImplementedError()

    def get(self, key: str) -> str | None:
        raise NotImplementedError()

    def lpush(self, key: str, *values: str) -> int:
        raise NotImplementedError()

    def rpush(self, key: str, *values: str) -> int:
        raise NotImplementedError()

    @overload
    def lpop(
            self,
            key: str,
            count: None = None) -> str | None:
        ...

    @overload
    def lpop(
            self,
            key: str,
            count: int) -> list[str] | None:
        ...

    def lpop(
            self,
            key: str,
            count: int | None = None) -> str | list[str] | None:
        raise NotImplementedError()

    @overload
    def rpop(
            self,
            key: str,
            count: None = None) -> str | None:
        ...

    @overload
    def rpop(
            self,
            key: str,
            count: int) -> list[str] | None:
        ...

    def rpop(
            self,
            key: str,
            count: int | None = None) -> str | list[str] | None:
        raise NotImplementedError()

    def llen(self, key: str) -> int:
        raise NotImplementedError()

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        raise NotImplementedError()

    def zpop_max(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        raise NotImplementedError()

    def zpop_min(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        raise NotImplementedError()

    def zcard(self, key: str) -> int:
        raise NotImplementedError()
