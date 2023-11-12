"""This module defines the basic redis API. All redis functions appear once
in RedisAPI and once in PipelineAPI. Additional functionality is added via
RedisClientAPI."""
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
    """Redis API as pipeline. All methods return None and you have to call
    execute to retrieve the results of the pipeline commands."""
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
        Sets a value for a given key. The value can be scheduled to expire.

        See also the redis documentation: https://redis.io/commands/set/

        The pipeline value depends on the return_previous argument.

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
        """
        Retrieves the value for the given key.

        See also the redis documentation: https://redis.io/commands/get/

        The pipeline value is the value or None if the key does not exists or
        the value has expired.

        Args:
            key (str): The key.
        """
        raise NotImplementedError()

    def lpush(self, key: str, *values: str) -> None:
        """
        Pushes values to the left side of the list associated with the key.

        See also the redis documentation: https://redis.io/commands/lpush/

        The pipeline value is the length of the list after the push.

        Args:
            key (str): The key.

            *values (str): The values to push.
        """
        raise NotImplementedError()

    def rpush(self, key: str, *values: str) -> None:
        """
        Pushes values to the right side of the list associated with the key.

        See also the redis documentation: https://redis.io/commands/rpush/

        The pipeline value is the length of the list after the push.

        Args:
            key (str): The key.

            *values (str): The values to push.
        """
        raise NotImplementedError()

    def lpop(
            self,
            key: str,
            count: int | None = None) -> None:
        """
        Pops a number of values from the left side of the list associated with
        the key.

        See also the redis documentation: https://redis.io/commands/lpop/

        The pipeline value is None if the key doesn't exist. If a count
        is set a list with values in pop order is set as pipeline value (even
        if it is set to one). If count is not set (default or None) the single
        value that got popped is set as pipeline value.

        Args:
            key (str): The key.

            count (int | None, optional): The number values to pop.
            Defaults to a single value.
        """
        raise NotImplementedError()

    def rpop(
            self,
            key: str,
            count: int | None = None) -> None:
        """
        Pops a number of values from the right side of the list associated with
        the key.

        See also the redis documentation: https://redis.io/commands/rpop/

        The pipeline value is None if the key doesn't exist. If a count
        is set a list with values in pop order is set as pipeline value (even
        if it is set to one). If count is not set (default or None) the single
        value that got popped is set as pipeline value.

        Args:
            key (str): The key.

            count (int | None, optional): The number values to pop.
            Defaults to a single value.
        """
        raise NotImplementedError()

    def llen(self, key: str) -> None:
        """
        Computes the length of the list associated with the key.

        See also the redis documentation: https://redis.io/commands/llen/

        The length of the list is set as pipeline value.

        Args:
            key (str): The key.
        """
        raise NotImplementedError()

    def zadd(self, key: str, mapping: dict[str, float]) -> None:
        """
        Adds elements to the sorted set associated with the key.

        See also the redis documentation: https://redis.io/commands/zadd/

        NOTE: not all setting modes are implemented yet.

        The number of new members is set as pipeline value.

        Args:
            key (str): The key.
            mapping (dict[str, float]): A dictionary with values and scores.
        """
        raise NotImplementedError()

    def zpop_max(
            self,
            key: str,
            count: int = 1,
            ) -> None:
        """
        Pops a number of members of the sorted set associated with the given
        key with the highest scores.

        See also the redis documentation: https://redis.io/commands/zpopmax/

        The members with their associated scores in pop order is set as
        pipeline value.

        Args:
            key (str): The key.

            count (int, optional): The number of members to remove.
            Defaults to 1.
        """
        raise NotImplementedError()

    def zpop_min(
            self,
            key: str,
            count: int = 1,
            ) -> None:
        """
        Pops a number of members of the sorted set associated with the given
        key with the lowest scores.

        See also the redis documentation: https://redis.io/commands/zpopmin/

        The members with their associated scores in pop order is set as
        pipeline value.

        Args:
            key (str): The key.

            count (int, optional): The number of members to remove.
            Defaults to 1.
        """
        raise NotImplementedError()

    def zcard(self, key: str) -> None:
        """
        Computes the cardinality of the sorted set associated with the given
        key.

        See also the redis documentation: https://redis.io/commands/zcard/

        The number of members in the set is set as pipeline value.

        Args:
            key (str): The key.
        """
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
    """The redis API."""
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
        Sets a value for a given key. The value can be scheduled to expire.

        See also the redis documentation: https://redis.io/commands/set/

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
        """
        Retrieves the value for the given key.

        See also the redis documentation: https://redis.io/commands/get/

        Args:
            key (str): The key.

        Returns:
            str | None: The value or None if the key does not exists or the
            value has expired.
        """
        raise NotImplementedError()

    def lpush(self, key: str, *values: str) -> int:
        """
        Pushes values to the left side of the list associated with the key.

        See also the redis documentation: https://redis.io/commands/lpush/

        Args:
            key (str): The key.

            *values (str): The values to push.

        Returns:
            int: The length of the list after the push.
        """
        raise NotImplementedError()

    def rpush(self, key: str, *values: str) -> int:
        """
        Pushes values to the right side of the list associated with the key.

        See also the redis documentation: https://redis.io/commands/rpush/

        Args:
            key (str): The key.

            *values (str): The values to push.

        Returns:
            int: The length of the list after the push.
        """
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
        """
        Pops a number of values from the left side of the list associated with
        the key.

        See also the redis documentation: https://redis.io/commands/lpop/

        Args:
            key (str): The key.

            count (int | None, optional): The number values to pop.
            Defaults to a single value.

        Returns:
            str | list[str] | None: None if the key doesn't exist. If a count
            is set a list with values in pop order is returned (even if it is
            set to one). If count is not set (default or None) the single value
            that got popped is returned.
        """
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
        """
        Pops a number of values from the right side of the list associated with
        the key.

        See also the redis documentation: https://redis.io/commands/rpop/

        Args:
            key (str): The key.

            count (int | None, optional): The number values to pop.
            Defaults to a single value.

        Returns:
            str | list[str] | None: None if the key doesn't exist. If a count
            is set a list with values in pop order is returned (even if it is
            set to one). If count is not set (default or None) the single value
            that got popped is returned.
        """
        raise NotImplementedError()

    def llen(self, key: str) -> int:
        """
        Computes the length of the list associated with the key.

        See also the redis documentation: https://redis.io/commands/llen/

        Args:
            key (str): The key.

        Returns:
            int: The length of the list.
        """
        raise NotImplementedError()

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        """
        Adds elements to the sorted set associated with the key.

        See also the redis documentation: https://redis.io/commands/zadd/

        NOTE: not all setting modes are implemented yet.

        Args:
            key (str): The key.
            mapping (dict[str, float]): A dictionary with values and scores.

        Returns:
            int: The number of new members.
        """
        raise NotImplementedError()

    def zpop_max(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        """
        Pops a number of members of the sorted set associated with the given
        key with the highest scores.

        See also the redis documentation: https://redis.io/commands/zpopmax/

        Args:
            key (str): The key.

            count (int, optional): The number of members to remove.
            Defaults to 1.

        Returns:
            list[tuple[str, float]]: The members with their associated scores
            in pop order.
        """
        raise NotImplementedError()

    def zpop_min(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        """
        Pops a number of members of the sorted set associated with the given
        key with the lowest scores.

        See also the redis documentation: https://redis.io/commands/zpopmin/

        Args:
            key (str): The key.

            count (int, optional): The number of members to remove.
            Defaults to 1.

        Returns:
            list[tuple[str, float]]: The members with their associated scores
            in pop order.
        """
        raise NotImplementedError()

    def zcard(self, key: str) -> int:
        """
        Computes the cardinality of the sorted set associated with the given
        key.

        See also the redis documentation: https://redis.io/commands/zcard/

        Args:
            key (str): The key.

        Returns:
            int: The number of members in the set.
        """
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
    """This class enriches the redis API with pipeline and script
    functionality."""
    @contextlib.contextmanager
    def pipeline(self) -> Iterator[PipelineAPI]:
        """
        Starts a redis pipeline. When leaving the resource block the pipeline
        is executed automatically and the results are discarded. If you need
        the results call execute on the pipeline object.

        Yields:
            Iterator[PipelineAPI]: The pipeline.
        """
        raise NotImplementedError()

    def register_script(self, ctx: FnContext) -> ExecFunction:
        """
        Registers a script that can be executed in this redis runtime.

        Args:
            ctx (FnContext): The script to register.

        Returns:
            ExecFunction: A python that can be called to execute the script.
        """
        raise NotImplementedError()
