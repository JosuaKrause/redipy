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
"""This module defines the basic redis API. All redis functions appear once
in RedisAPI and once in PipelineAPI. Additional functionality is added via
RedisClientAPI."""
import contextlib
import datetime
from collections.abc import Iterable, Iterator
from typing import cast, get_args, Literal, overload

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


RExpireMode = Literal[
    "always",
    "if_persist",  # NX
    "if_expire",  # XX
    "if_later",  # GT
    "if_earlier",  # LT
]
"""The conditions on when to expire a value for the expire command."""
REX_ALWAYS: RExpireMode = "always"
"""The expiration will always be set."""
REX_PERSIST: RExpireMode = "if_persist"
"""The expiration will be set only if there is no previous expiration.
This is equivalent to the NX flag."""
REX_EXPIRE: RExpireMode = "if_expire"
"""The expiration will be set only if there is a previous expiration.
This is equivalent to the XX flag."""
REX_LATER: RExpireMode = "if_later"
"""The expiration will be set only if it is later than the previous expiration.
This is equivalent to the GT flag."""
REX_EARLIER: RExpireMode = "if_earlier"
"""The expiration will be set only if it is earlier than the previous
expiration. This is equivalent to the LT flag."""


KeyType = Literal[
    "string",
    "list",
    "set",
    "zset",
    "hash",
    "stream",
]
"""The different key types."""


KEY_TYPES: set[KeyType] = set(get_args(KeyType))


@overload
def as_key_type(text: str) -> KeyType | None:
    ...


@overload
def as_key_type(text: None) -> None:
    ...


def as_key_type(text: str | None) -> KeyType | None:
    """
    Converts a string into a key type.

    Args:
        text (str): The string.

    Raises:
        ValueError: If the string does not represent a key type.

    Returns:
        KeyType: The key type or None if the input was None or the input was
            the string "none".
    """
    if text is None or text == "none":
        return None
    if text not in KEY_TYPES:
        raise ValueError(
            f"unknown key type: {text}. Only {KEY_TYPES} are supported.")
    return cast(KeyType, text)


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

    def exists(self, *keys: str) -> None:
        """
        Determines whether specified keys exist.

        See also the redis documentation: https://redis.io/commands/exists/

        The pipeline value is set to the number of keys that exist.

        Args:
            *keys (str): The keys.
        """
        raise NotImplementedError()

    def delete(self, *keys: str) -> None:
        """
        Deletes keys.

        See also the redis documentation: https://redis.io/commands/del/

        The pipeline value is set to the number of keys that got removed.

        Args:
            *keys (str): The keys.
        """
        raise NotImplementedError()

    def key_type(self, key: str) -> None:
        """
        The type of the given key if it exists.

        See also the redis documentation: https://redis.io/commands/type/

        The pipeline value is the key type or None if the key does not exist.

        Args:
            key (str): The key.
        """
        raise NotImplementedError()

    def scan(
            self,
            cursor: int,
            *,
            match: str | None = None,
            count: int | None = None,
            filter_type: KeyType | None = None) -> None:
        """
        Performs a singular scan operation on the keys of the database. Note,
        that this cannot be used to scan all keys in a pipeline as scan depends
        on the previous' scan cursor which is unobtainable within the same
        pipeline.

        See also the redis documentation: https://redis.io/commands/scan/

        A tuple of the new cursor and the current keys. If the new cursor is
        0 the iteration ends.

        Args:
            cursor (int): The cursor. This value is either 0 (for starting a
                scan) or a value returned previously by this function.
            match (str | None, optional): Filters the keys according to a redis
                match string. Defaults to None.
            count (int | None, optional): Estimate of expected returned keys
                in one call. The actual number returned might be different.
                Defaults to None.
            filter_type (KeyType | None, optional): Filters by the key type.
                Defaults to None.
        """
        raise NotImplementedError()

    def keys(
            self,
            *,
            match: str | None = None,
            filter_type: KeyType | None = None) -> None:
        """
        Retrieves all matching keys. This method can take very long to complete
        and block the database for a long time. It is best to avoid retrieving
        keys in a pipeline.

        See also the redis documentation: https://redis.io/commands/keys/

        The pipeline value is the list of unique matching keys.

        Args:
            match (str | None, optional): Filters the keys according to a redis
                match string. Defaults to None.
            filter_type (KeyType | None, optional): Filters by the key type.
                Defaults to None.
        """
        raise NotImplementedError()

    def set_value(
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
                RSM_MISSING is the equivalent of setting the NX flag.
                RSM_EXISTS is the equivalent of the XX flag. Defaults to
                RSM_ALWAYS.

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

    def get_value(self, key: str) -> None:
        """
        Retrieves the value for the given key.

        See also the redis documentation: https://redis.io/commands/get/

        The pipeline value is the value or None if the key does not exists or
        the value has expired.

        Args:
            key (str): The key.
        """
        raise NotImplementedError()

    def expire(
            self,
            key: str,
            *,
            mode: RExpireMode = REX_ALWAYS,
            expire_timestamp: datetime.datetime | None = None,
            expire_in: float | None = None) -> None:
        """
        Sets or removes the expiration time of a key. The expiration can either
        be a timestamp or a relative amount. If no time is set the expiration
        is removed and the key persists.

        See also the redis documentation:
        https://redis.io/commands/expire/
        and https://redis.io/commands/expireat/
        and https://redis.io/commands/expiretime/
        and https://redis.io/commands/persist/
        and https://redis.io/commands/pexpire/
        and https://redis.io/commands/pexpireat/
        and https://redis.io/commands/pexpiretime/

        The pipeline value is set to whether a change in expiration occurred.

        Args:
            key (str): The key.

            mode (RExpireMode, optional): The expiration mode. Defaults to
                REX_ALWAYS. The mode is ignored if all expirations are None.

            expire_timestamp (datetime.datetime | None, optional): A timestamp
                on when to expire the key. Defaults to None.

            expire_in (float | None, optional): A relative time in seconds on
                when to expire the key. Defaults to None.
        """
        raise NotImplementedError()

    def ttl(self, key: str) -> None:
        """
        Retrieves the time-to-live for a key.

        See also the redis documentation:
        https://redis.io/commands/ttl/ and https://redis.io/commands/pttl/

        The pipeline value is set to the time to live in seconds with
        millisecond precision. If the key is persistent the time is `<= 0.0`.
        If the key does not exist None is returned.

        Args:
            key (str): The key.
        """
        raise NotImplementedError()

    def incrby(self, key: str, inc: float | int) -> None:
        """
        Updates the value associated with the given key by a relative amount.
        The value is interpreted as number. If the value doesn't exist zero is
        used as starting point.

        See also the redis documentation:
        https://redis.io/commands/incrby/
        https://redis.io/commands/incrbyfloat/

        The pipeline value is set to the new value as float.
        If the value cannot be interpreted as float while executing the
        pipeline a ValueError exception is raised.

        Args:
            key (str): The key.

            inc (float | int): The relative change.
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

    def lrange(self, key: str, start: int, stop: int) -> None:
        """
        Returns a number of values from the list specified by the given range.
        Negative numbers are interpreted as index from the back of the list.
        Out of range indices are ignored, potentially returning an empty list.

        See also the redis documentation: https://redis.io/commands/lrange/

        The pipeline value is the resulting elements.

        Args:
            key (str): The key.

            start (int): The start index.

            stop (int): The stop index (inclusive).
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

    def zrange(self, key: str, start: int, stop: int) -> None:
        """
        Returns a number of values from the sorted set specified by the given
        range. As of now the indices are based on the order of the set.
        Negative numbers are interpreted as index from the back of the set.
        Out of range indices are ignored, potentially returning an empty set.

        See also the redis documentation: https://redis.io/commands/zrange/

        NOTE: not all modes are implemented yet.

        The members names are set as pipeline value.

        Args:
            key (str): The key.

            start (int): The start index.

            stop (int): The stop index (inclusive).
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

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        """
        Sets a mapping for the given hash.

        See also the redis documentation: https://redis.io/commands/hset/

        The pipeline value is set to the number of fields added.

        Args:
            key (str): The key.

            mapping (dict[str, str]): The field value pairs to be set.
        """
        raise NotImplementedError()

    def hdel(self, key: str, *fields: str) -> None:
        """
        Deletes fields from the given hash.

        See also the redis documentation: https://redis.io/commands/hdel/

        The pipeline value is set to the number of fields that got deleted
        (excluding fields that did not exist).

        Args:
            key (str): The key.

            *fields (str): The fields to delete.
        """
        raise NotImplementedError()

    def hget(self, key: str, field: str) -> None:
        """
        Retrieves the value associated with a field of a hash.

        See also the redis documentation: https://redis.io/commands/hget/

        The pipeline value is set to the value of the field or None if the
        field doesn't exist.

        Args:
            key (str): The key.

            field (str): The field.
        """
        raise NotImplementedError()

    def hmget(self, key: str, *fields: str) -> None:
        """
        Retrieves the values associated with given fields of a hash.

        See also the redis documentation: https://redis.io/commands/hmget/

        The pipeline value is set to a dictionary with fields mapping to their
        values. If a field doesn't exist in the hash the value is returned
        as None.

        Args:
            key (str): The key.

            *fields (str): The fields to retrieve.
        """
        raise NotImplementedError()

    def hincrby(self, key: str, field: str, inc: float | int) -> None:
        """
        Interprets a field value of a hash as number and updates the value.

        See also the redis documentation:
        https://redis.io/commands/hincrby/
        https://redis.io/commands/hincrbyfloat/

        The pipeline value is set to the new value of the field.

        Args:
            key (str): The key.

            field (str): The field to interpret as number.

            inc (float | int): The relative numerical change.
        """
        raise NotImplementedError()

    def hkeys(self, key: str) -> None:
        """
        Retrieves the fields of a hash.

        See also the redis documentation: https://redis.io/commands/hkeys/

        The pipeline value is set to a list of all fields of the given hash.

        Args:
            key (str): The key.
        """
        raise NotImplementedError()

    def hvals(self, key: str) -> None:
        """
        Retrieves the values of a hash.

        See also the redis documentation: https://redis.io/commands/hvals/

        The pipeline value is set to a list of all values of the given hash.

        Args:
            key (str): The key.
        """
        raise NotImplementedError()

    def hgetall(self, key: str) -> None:
        """
        Retrieves all fields and values of a hash.

        See also the redis documentation: https://redis.io/commands/hgetall/

        The pipeline value is set to a dictionary with fields mapping to their
        values.

        Args:
            key (str): The key.
        """
        raise NotImplementedError()

    def sadd(self, key: str, *values: str) -> None:
        """
        Add values to the set of the given key.

        See also the redis documentation: https://redis.io/commands/sadd/

        The pipeline value is the number of new items in the set.

        Args:
            key (str): The key.
            *values (str): The values to add.
        """
        raise NotImplementedError()

    def srem(self, key: str, *values: str) -> None:
        """
        Remove values from the set of the given key.

        See also the redis documentation: https://redis.io/commands/srem/

        The pipeline value is the number of existing items removed from the
        set.

        Args:
            key (str): The key.
            *values (str): The values to remove.
        """
        raise NotImplementedError()

    def sismember(self, key: str, value: str) -> None:
        """
        Tests whether the given value is present in the set of the given key.

        See also the redis documentation: https://redis.io/commands/sismember/

        The pipeline value is a boolean. True, if the set contains the value.

        Args:
            key (str): The key.
            value (str): The value.
        """
        raise NotImplementedError()

    def scard(self, key: str) -> None:
        """
        Computes the size of the set given by the key.

        See also the redis documentation: https://redis.io/commands/scard/

        The pipeline value is the number of elements in the set.

        Args:
            key (str): The key.
        """
        raise NotImplementedError()

    def smembers(self, key: str) -> None:
        """
        Returns all elements of the set given by the key.

        See also the redis documentation: https://redis.io/commands/smembers/

        The pipeline value is a set of all elements of the set.

        Args:
            key (str): The key.
        """
        raise NotImplementedError()


class RedisAPI:
    """The redis API."""
    def exists(self, *keys: str) -> int:
        """
        Determines whether specified keys exist.

        See also the redis documentation: https://redis.io/commands/exists/

        Args:
            *keys (str): The keys.

        Returns:
            int: The number of keys that exist.
        """
        raise NotImplementedError()

    def delete(self, *keys: str) -> int:
        """
        Deletes keys.

        See also the redis documentation: https://redis.io/commands/del/

        Args:
            *keys (str): The keys.

        Returns:
            int: The number of keys that got removed.
        """
        raise NotImplementedError()

    def key_type(self, key: str) -> KeyType | None:
        """
        The type of the given key if it exists.

        See also the redis documentation: https://redis.io/commands/type/

        Args:
            key (str): The key.

        Returns:
            KeyType | None: The key type or None if the key does not exist.
        """
        raise NotImplementedError()

    def scan(
            self,
            cursor: int,
            *,
            match: str | None = None,
            count: int | None = None,
            filter_type: KeyType | None = None) -> tuple[int, list[str]]:
        """
        Scans the keys of the database.

        See also the redis documentation: https://redis.io/commands/scan/

        Args:
            cursor (int): The cursor. This value is either 0 (for starting a
                scan) or a value returned previously by this function.
            match (str | None, optional): Filters the keys according to a redis
                match string. Defaults to None.
            count (int | None, optional): Estimate of expected returned keys
                in one call. The actual number returned might be different.
                Defaults to None.
            filter_type (KeyType | None, optional): Filters by the key type.
                Defaults to None.

        Returns:
            tuple[int, list[str]]: A tuple of the new cursor and the current
                keys. If the new cursor is 0 the iteration ends.
        """
        raise NotImplementedError()

    def iter_keys(
            self,
            *,
            match: str | None = None,
            filter_type: KeyType | None = None) -> Iterable[str]:
        """
        Iterates matching keys. This is a more streamlined interface to scan.

        See also the redis documentation: https://redis.io/commands/scan/

        Args:
            match (str | None, optional): Filters the keys according to a redis
                match string. Defaults to None.
            filter_type (KeyType | None, optional): Filters by the key type.
                Defaults to None.

        Yields:
            str: The keys of this query. Duplicate keys might get returned.
        """
        cursor = 0
        count = 10
        while True:
            cursor, keys = self.scan(
                cursor,
                match=match,
                count=count,
                filter_type=filter_type)
            yield from keys
            if cursor == 0:
                break
            count = int(min(1000, count * 2))

    def keys_block(
            self,
            *,
            match: str | None = None,
            filter_type: KeyType | None = None) -> list[str]:
        """
        Retrieves all matching keys. This method blocks the full database until
        all keys are returned. In most cases it is better to use the
        non-blocking function instead.

        See also the redis documentation: https://redis.io/commands/keys/

        Args:
            match (str | None, optional): Filters the keys according to a redis
                match string. Defaults to None.
            filter_type (KeyType | None, optional): Filters by the key type.
                Defaults to None.

        Returns:
            list[str]: The list of unique matching keys.
        """
        raise NotImplementedError()

    def keys(
            self,
            *,
            match: str | None = None,
            filter_type: KeyType | None = None,
            block: bool = True) -> set[str]:
        """
        Retrieves all matching keys.

        See also the redis documentation: https://redis.io/commands/scan/ and
            https://redis.io/commands/keys/

        Args:
            match (str | None, optional): Filters the keys according to a redis
                match string. Defaults to None.
            filter_type (KeyType | None, optional): Filters by the key type.
                Defaults to None.
            block (bool, optional): Whether to block the full database while
                retrieving the matching keys. Defaults to True.

        Returns:
            set[str]: The set of unique matching keys.
        """
        if block:
            return set(self.keys_block(match=match, filter_type=filter_type))
        return set(self.iter_keys(match=match, filter_type=filter_type))

    def flushall(self) -> None:
        """
        Flushes all keys in the database. Whether the operation is asynchronous
        is up to the implementation.

        See also the redis documentation: https://redis.io/commands/flushall/
        """
        # FIXME: add `block: bool` argument
        raise NotImplementedError()

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
        """
        Sets a value for a given key. The value can be scheduled to expire.

        See also the redis documentation: https://redis.io/commands/set/

        Args:
            key (str): The key.

            value (str): The value.

            mode (RSetMode, optional): Under which condition to set the value
                valid values are RSM_ALWAYS, RSM_MISSING, and RSM_EXISTS.
                RSM_MISSING is the equivalent of setting the NX flag.
                RSM_EXISTS is the equivalent of the XX flag. Defaults to
                RSM_ALWAYS.

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

    def get_value(self, key: str) -> str | None:
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

    def expire(
            self,
            key: str,
            *,
            mode: RExpireMode = REX_ALWAYS,
            expire_timestamp: datetime.datetime | None = None,
            expire_in: float | None = None) -> bool:
        """
        Sets or removes the expiration time of a key. The expiration can either
        be a timestamp or a relative amount. If no time is set the expiration
        is removed and the key persists.

        See also the redis documentation:
        https://redis.io/commands/expire/
        and https://redis.io/commands/expireat/
        and https://redis.io/commands/expiretime/
        and https://redis.io/commands/persist/
        and https://redis.io/commands/pexpire/
        and https://redis.io/commands/pexpireat/
        and https://redis.io/commands/pexpiretime/

        Args:
            key (str): The key.

            mode (RExpireMode, optional): The expiration mode. Defaults to
                REX_ALWAYS. The mode is ignored if all expirations are None.

            expire_timestamp (datetime.datetime | None, optional): A timestamp
                on when to expire the key. Defaults to None.

            expire_in (float | None, optional): A relative time in seconds on
                when to expire the key. Defaults to None.

        Returns:
            bool: Whether a change in expiration occurred.
        """
        raise NotImplementedError()

    def ttl(self, key: str) -> float | None:
        """
        Retrieves the time-to-live for a key.

        See also the redis documentation:
        https://redis.io/commands/ttl/ and https://redis.io/commands/pttl/

        Args:
            key (str): The key.

        Returns:
            float | None: The time to live in seconds with millisecond
                precision. If the key is persistent the time is `<= 0.0`.
                If the key does not exist None is returned.
        """
        raise NotImplementedError()

    def incrby(self, key: str, inc: float | int) -> float:
        """
        Updates the value associated with the given key by a relative amount.
        The value is interpreted as number. If the value doesn't exist zero is
        used as starting point.

        See also the redis documentation:
        https://redis.io/commands/incrby/
        https://redis.io/commands/incrbyfloat/

        Args:
            key (str): The key.

            inc (float | int): The relative change.

        Raises:
            ValueError: If the value cannot be interpreted as float.

        Returns:
            float: The new value as float.
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

    def lrange(self, key: str, start: int, stop: int) -> list[str]:
        """
        Returns a number of values from the list specified by the given range.
        Negative numbers are interpreted as index from the back of the list.
        Out of range indices are ignored, potentially returning an empty list.

        See also the redis documentation: https://redis.io/commands/lrange/

        Args:
            key (str): The key.

            start (int): The start index.

            stop (int): The stop index (inclusive).

        Returns:
            list[str]: The elements.
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

    def zrange(self, key: str, start: int, stop: int) -> list[str]:
        """
        Returns a number of values from the sorted set specified by the given
        range. As of now the indices are based on the order of the set.
        Negative numbers are interpreted as index from the back of the set.
        Out of range indices are ignored, potentially returning an empty set.

        See also the redis documentation: https://redis.io/commands/zrange/

        NOTE: not all modes are implemented yet.

        Args:
            key (str): The key.

            start (int): The start index.

            stop (int): The stop index (inclusive).

        Returns:
            list[str]: The members names.
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

    def hset(self, key: str, mapping: dict[str, str]) -> int:
        """
        Sets a mapping for the given hash.

        See also the redis documentation: https://redis.io/commands/hset/

        Args:
            key (str): The key.

            mapping (dict[str, str]): The field value pairs to be set.

        Returns:
            int: The number of fields added.
        """
        raise NotImplementedError()

    def hdel(self, key: str, *fields: str) -> int:
        """
        Deletes fields from the given hash.

        See also the redis documentation: https://redis.io/commands/hdel/

        Args:
            key (str): The key.

            *fields (str): The fields to delete.

        Returns:
            int: The number of fields that got deleted (excluding fields that
            did not exist).
        """
        raise NotImplementedError()

    def hget(self, key: str, field: str) -> str | None:
        """
        Retrieves the value associated with a field of a hash.

        See also the redis documentation: https://redis.io/commands/hget/

        Args:
            key (str): The key.

            field (str): The field.

        Returns:
            str | None: The value of the field or None if the field doesn't
            exist.
        """
        raise NotImplementedError()

    def hmget(self, key: str, *fields: str) -> dict[str, str | None]:
        """
        Retrieves the values associated with given fields of a hash.

        See also the redis documentation: https://redis.io/commands/hmget/

        Args:
            key (str): The key.

            *fields (str): The fields to retrieve.

        Returns:
            dict[str, str | None]: A dictionary with fields mapping to their
            values. If a field doesn't exist in the hash the value is returned
            as None.
        """
        raise NotImplementedError()

    def hincrby(self, key: str, field: str, inc: float | int) -> float:
        """
        Interprets a field value of a hash as number and updates the value.

        See also the redis documentation:
        https://redis.io/commands/hincrby/
        https://redis.io/commands/hincrbyfloat/

        Args:
            key (str): The key.

            field (str): The field to interpret as number.

            inc (float | int): The relative numerical change.

        Returns:
            float: The new value of the field.
        """
        raise NotImplementedError()

    def hkeys(self, key: str) -> list[str]:
        """
        Retrieves the fields of a hash.

        See also the redis documentation: https://redis.io/commands/hkeys/

        Args:
            key (str): The key.

        Returns:
            list[str]: All fields of the given hash.
        """
        raise NotImplementedError()

    def hvals(self, key: str) -> list[str]:
        """
        Retrieves the values of a hash.

        See also the redis documentation: https://redis.io/commands/hvals/

        Args:
            key (str): The key.

        Returns:
            list[str]: All values of the given hash.
        """
        raise NotImplementedError()

    def hgetall(self, key: str) -> dict[str, str]:
        """
        Retrieves all fields and values of a hash.

        See also the redis documentation: https://redis.io/commands/hgetall/

        Args:
            key (str): The key.

        Returns:
            dict[str, str]: A dictionary with fields mapping to their values.
        """
        raise NotImplementedError()

    def sadd(self, key: str, *values: str) -> int:
        """
        Add values to the set of the given key.

        See also the redis documentation: https://redis.io/commands/sadd/

        Args:
            key (str): The key.
            *values (str): The values to add.

        Returns:
            int: The number of new items in the set.
        """
        raise NotImplementedError()

    def srem(self, key: str, *values: str) -> int:
        """
        Remove values from the set of the given key.

        See also the redis documentation: https://redis.io/commands/srem/

        Args:
            key (str): The key.
            *values (str): The values to remove.

        Returns:
            int: The number of existing items removed from the set.
        """
        raise NotImplementedError()

    def sismember(self, key: str, value: str) -> bool:
        """
        Tests whether the given value is present in the set of the given key.

        See also the redis documentation: https://redis.io/commands/sismember/

        Args:
            key (str): The key.
            value (str): The value.

        Returns:
            bool: True, if the set contains the value.
        """
        raise NotImplementedError()

    def scard(self, key: str) -> int:
        """
        Computes the size of the set given by the key.

        See also the redis documentation: https://redis.io/commands/scard/

        Args:
            key (str): The key.

        Returns:
            int: The number of elements in the set.
        """
        raise NotImplementedError()

    def smembers(self, key: str) -> set[str]:
        """
        Returns all elements of the set given by the key.

        See also the redis documentation: https://redis.io/commands/smembers/

        Args:
            key (str): The key.

        Returns:
            set[str]: All elements of the set.
        """
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
            PipelineAPI: The pipeline.
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
