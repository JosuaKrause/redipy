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
"""A hash based cache. Consumers will be notified as soon as a value is
available if any computation is already ongoing. The cache should be used on
a memory limited redis backend."""
from collections.abc import Callable
from typing import Generic, TypeVar

from redipy.api import RedisClientAPI, RSM_MISSING


K = TypeVar('K')
V = TypeVar('V')


class RCache(Generic[K, V]):  # pylint: disable=too-few-public-methods
    """A redis based cache with in-flight computation detection."""
    def __init__(
            self,
            rt: RedisClientAPI,
            *,
            prefix: str,
            hasher: Callable[[K], str],
            compute: Callable[[K], V],
            value_store: Callable[[V], str],
            value_read: Callable[[str], V]) -> None:
        """
        Creates a redis based cache.

        Args:
            rt (RedisClientAPI): The redis client.

            prefix (str): The key prefix.

            hasher (Callable[[K], str]): Function to create a hash for a given
                key.

            compute (Callable[[K], V]): Compute the actual value for a given
                key.

            value_store (Callable[[V], str]): Convert the value into a string
                for storing. The string must never be empty.

            value_read (Callable[[str], V]): Convert a string into a value for
                retrieving.
        """
        self._rt = rt
        self._prefix = prefix
        self._hasher = hasher
        self._compute = compute
        self._value_store = value_store
        self._value_read = value_read

    def _redis_key(self, hash_str: str) -> str:
        prefix = self._prefix
        if prefix:
            prefix = f"{prefix}:"
        return f"{prefix}{hash_str}"

    def get_value(self, key: K, *, timeout: float = 300.0) -> V:
        """
        Retrieves the value of the given key. If the value is not already
        cached it is computed. If a value is being computed elsewhere at the
        moment the function call blocks until the computation is complete.
        During this, if timeout is reached, the computation will start again.

        Args:
            key (K): The key.

            timeout (float, optional): Timeout before starting to compute the
                value ourself if another computation was already in-flight
                in seconds. Defaults to 300.0.

        Raises:
            ValueError: If the value string is the empty string after
                converting the result.

        Returns:
            V: The result value.
        """
        rt = self._rt
        hash_str = self._hasher(key)
        rkey = self._redis_key(hash_str)
        value = rt.get_value(rkey)
        if value is not None:
            if value:
                return self._value_read(value)
            # someone else is computing the value
            value = rt.wait_for(rkey, lambda: rt.get_value(rkey), timeout)
            if value:
                return self._value_read(value)
        # we have to compute the value
        rt.set_value(rkey, "", mode=RSM_MISSING)
        res = self._compute(key)
        res_str = self._value_store(res)
        if not res_str:
            raise ValueError(f"compute for {key=} with {res=} mapping to ''")
        rt.set_value(rkey, res_str)
        rt.publish(rkey, hash_str)
        return res
