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
"""Access to redis sets."""
from redipy.symbolic.expr import Expr, MixedType
from redipy.symbolic.fun import RedisObj


class RedisSet(RedisObj):
    """A redis set."""
    def add(self, value: MixedType) -> Expr:
        """
        Adds an element to the set.

        Args:
            value (MixedType): The value.

        Returns:
            Expr: The expression.
        """
        # FIXME add all arguments
        return self.redis_fn("sadd", value)

    def remove(self, value: MixedType) -> Expr:
        """
        Removes an element from the set.

        Args:
            value (MixedType): The value.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("srem", value)

    def has(self, value: MixedType) -> Expr:
        """
        Whether the set contains the given value.

        Args:
            value (MixedType): The value.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("sismember", value)

    def members(self) -> Expr:
        """
        Returns a list of set members.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("smembers")

    def card(self) -> Expr:
        """
        Computes the cardinality of the set.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("scard")
