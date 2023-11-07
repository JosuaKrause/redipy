import contextlib
import datetime
from collections.abc import Iterator
from typing import Literal, overload

from redipy.backend.backend import ExecFunction
from redipy.symbolic.seq import FnContext


RSetMode = Literal[
    "always",
    "if_missing",  # NX
    "if_exists",  # XX
]
"""The conditions on when to set a value for the set command."""
RSM_ALWAYS: RSetMode = "always"
"""The value will always be set."""
RSM_MISSING: RSetMode = "if_missing"
"""The value will only be set when the key was missing.
This is equivalent to the NX flag."""
RSM_EXISTS: RSetMode = "if_exists"
"""The value will only be set when the key did exist.
This is equivalent to the XX flag."""


class PipelineAPI:
    # FIXME: maybe make pipelines just another program
    def execute(self) -> list:
        """
        Executes the pipeline and returns the result values of each command.

        Returns:
            list: The result values of each command.
        """
        raise NotImplementedError()

    def set(
            self,
            key: str,
            value: str,
            *,
            mode: RSetMode = RSM_ALWAYS,
            return_previous: bool = False,
            expire_timestamp: datetime.datetime | None = None,
            expire_in: float | None = None,
            keep_ttl: bool = False) -> None:
        """
        The redis SET command (https://redis.io/commands/set/).

        Args:
            key (str): The key.
            value (str): The value.
            mode (RSetMode, optional): Under which condition to set the value
            valid values are RSM_ALWAYS, RSM_MISSING, and RSM_EXISTS.
            RSM_MISSING is the equivalent of setting the NX flag. RSM_EXISTS is
            the equivalent of the XX flag. Defaults to RSM_ALWAYS.
            return_previous (bool, optional): Whether to return the previous
            value associated with the key. Defaults to False.
            expire_timestamp (datetime.datetime | None, optional): A timestamp
            on when to expire the key. Defaults to None.
            expire_in (float | None, optional): A relative time in seconds on
            when to expire the key. Defaults to None.
            keep_ttl (bool, optional): Whether to keep previous expiration
            times. Defaults to False.
        """
        raise NotImplementedError()

    def get(self, key: str) -> None:
        raise NotImplementedError()

    def lpush(self, key: str, *values: str) -> None:
        raise NotImplementedError()

    def rpush(self, key: str, *values: str) -> None:
        raise NotImplementedError()

    def lpop(
            self,
            key: str,
            count: int | None = None) -> None:
        raise NotImplementedError()

    def rpop(
            self,
            key: str,
            count: int | None = None) -> None:
        raise NotImplementedError()

    def llen(self, key: str) -> None:
        raise NotImplementedError()

    def zadd(self, key: str, mapping: dict[str, float]) -> None:
        raise NotImplementedError()

    def zpop_max(
            self,
            key: str,
            count: int = 1,
            ) -> None:
        raise NotImplementedError()

    def zpop_min(
            self,
            key: str,
            count: int = 1,
            ) -> None:
        raise NotImplementedError()

    def zcard(self, key: str) -> None:
        raise NotImplementedError()

    def incrby(self, key: str, inc: float | int) -> None:
        raise NotImplementedError()

    def exists(self, *keys: str) -> None:
        raise NotImplementedError()

    def delete(self, *keys: str) -> None:
        raise NotImplementedError()

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        raise NotImplementedError()

    def hdel(self, key: str, *fields: str) -> None:
        raise NotImplementedError()

    def hget(self, key: str, field: str) -> None:
        raise NotImplementedError()

    def hmget(self, key: str, *fields: str) -> None:
        raise NotImplementedError()

    def hincrby(self, key: str, field: str, inc: float | int) -> None:
        raise NotImplementedError()

    def hkeys(self, key: str) -> None:
        raise NotImplementedError()

    def hvals(self, key: str) -> None:
        raise NotImplementedError()

    def hgetall(self, key: str) -> None:
        raise NotImplementedError()


class RedisAPI:
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
        """
        The redis SET command (https://redis.io/commands/set/).

        Args:
            key (str): The key.
            value (str): The value.
            mode (RSetMode, optional): Under which condition to set the value
            valid values are RSM_ALWAYS, RSM_MISSING, and RSM_EXISTS.
            RSM_MISSING is the equivalent of setting the NX flag. RSM_EXISTS is
            the equivalent of the XX flag. Defaults to RSM_ALWAYS.
            return_previous (bool, optional): Whether to return the previous
            value associated with the key. Defaults to False.
            expire_timestamp (datetime.datetime | None, optional): A timestamp
            on when to expire the key. Defaults to None.
            expire_in (float | None, optional): A relative time in seconds on
            when to expire the key. Defaults to None.
            keep_ttl (bool, optional): Whether to keep previous expiration
            times. Defaults to False.

        Returns:
            str | bool | None: The return value depends on the return_previous
            argument.
        """
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

    def incrby(self, key: str, inc: float | int) -> float:
        raise NotImplementedError()

    def exists(self, *keys: str) -> int:
        raise NotImplementedError()

    def delete(self, *keys: str) -> int:
        raise NotImplementedError()

    def hset(self, key: str, mapping: dict[str, str]) -> int:
        raise NotImplementedError()

    def hdel(self, key: str, *fields: str) -> int:
        raise NotImplementedError()

    def hget(self, key: str, field: str) -> str | None:
        raise NotImplementedError()

    def hmget(self, key: str, *fields: str) -> dict[str, str | None]:
        raise NotImplementedError()

    def hincrby(self, key: str, field: str, inc: float | int) -> float:
        raise NotImplementedError()

    def hkeys(self, key: str) -> list[str]:
        raise NotImplementedError()

    def hvals(self, key: str) -> list[str]:
        raise NotImplementedError()

    def hgetall(self, key: str) -> dict[str, str]:
        raise NotImplementedError()


class RedisClientAPI(RedisAPI):
    @contextlib.contextmanager
    def pipeline(self) -> Iterator[PipelineAPI]:
        raise NotImplementedError()

    def register_script(self, ctx: FnContext) -> ExecFunction:
        raise NotImplementedError()
