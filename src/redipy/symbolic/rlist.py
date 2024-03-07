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
"""Access to redis lists."""
from redipy.symbolic.expr import Expr, MixedType
from redipy.symbolic.fun import RedisObj


class RedisList(RedisObj):
    """A redis list."""
    def lpush(self, *values: MixedType) -> Expr:
        """
        Pushes values to the left end of the list.

        Args:
            *values (MixedType): The values to push.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("lpush", *values)

    def rpush(self, *values: MixedType) -> Expr:
        """
        Pushes values to the right end of the list.

        Args:
            *values (MixedType): The values to push.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("rpush", *values)

    def lpop(
            self, count: MixedType = None, *, no_adjust: bool = False) -> Expr:
        """
        Pops values from the left end of the list.

        Args:
            count (MixedType, optional): The number of values to pop.
            Defaults to one (None) returning a singleton.

            no_adjust (bool, optional): Whether to prevent patching the
            function call. This should not be neccessary. Defaults to False.

        Returns:
            Expr: The expression.
        """
        if count is None:
            return self.redis_fn("lpop", no_adjust=no_adjust)
        return self.redis_fn("lpop", count, no_adjust=no_adjust)

    def rpop(
            self, count: MixedType = None, *, no_adjust: bool = False) -> Expr:
        """
        Pops values from the right end of the list.

        Args:
            count (MixedType, optional): The number of values to pop.
            Defaults to one (None) returning a singleton.

            no_adjust (bool, optional): Whether to prevent patching the
            function call. This should not be neccessary. Defaults to False.

        Returns:
            Expr: The expression.
        """
        if count is None:
            return self.redis_fn("rpop", no_adjust=no_adjust)
        return self.redis_fn("rpop", count, no_adjust=no_adjust)

    def lrange(self, start: MixedType, stop: MixedType) -> Expr:
        """
        Returns a number of values from the list specified by the given range.
        Negative numbers are interpreted as index from the back of the list.
        Out of range indices are ignored, potentially returning an empty list.

        Args:
            start (MixedType): The start index.

            stop (MixedType): The stop index (inclusive).

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("lrange", start, stop)

    def llen(self) -> Expr:
        """
        The length of the list.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("llen")
