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
"""Access to redis sorted sets."""
from redipy.symbolic.expr import Expr, MixedType
from redipy.symbolic.fun import RedisObj


class RedisSortedSet(RedisObj):
    """A redis sorted set."""
    def add(self, score: MixedType, value: MixedType) -> Expr:
        """
        Adds an element to the sorted set.

        Args:
            score (MixedType): The score.
            value (MixedType): The value.

        Returns:
            Expr: The expression.
        """
        # FIXME add all arguments
        return self.redis_fn("zadd", score, value)

    def pop_max(self, count: MixedType = None) -> Expr:
        """
        Pops maximum values from the sorted set.

        Args:
            count (MixedType, optional): The number of values to pop. Defaults
            to one (None).

        Returns:
            Expr: The expression.
        """
        if count is None:
            return self.redis_fn("zpopmax")
        return self.redis_fn("zpopmax", count)

    def pop_min(self, count: MixedType = None) -> Expr:
        """
        Pops minimum values from the sorted set.

        Args:
            count (MixedType, optional): The number of values to pop. Defaults
            to one (None).

        Returns:
            Expr: The expression.
        """
        if count is None:
            return self.redis_fn("zpopmin")
        return self.redis_fn("zpopmin", count)

    def range(self, start: MixedType, stop: MixedType) -> Expr:
        """
        Returns a range of member names.

        Args:
            start (MixedType): The start index.

            stop (MixedType): The stop index (inclusive).

        Returns:
            Expr: The expression.
        """
        # FIXME add all arguments
        return self.redis_fn("zrange", start, stop)

    def card(self) -> Expr:
        """
        Computes the cardinality of the sorted set.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("zcard")
