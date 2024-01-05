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
"""Access to redis hash types."""
from redipy.symbolic.expr import Expr, MixedType
from redipy.symbolic.fun import RedisObj


class RedisHash(RedisObj):
    """A redis hash."""
    def hset(self, mapping: dict[MixedType, MixedType]) -> Expr:
        """
        Sets the fields to the provided values.

        Args:
            mapping (dict[MixedType, MixedType]): Mapping of field names and
            values.

        Returns:
            Expr: The expression.
        """
        args = []
        for key, value in mapping.items():
            args.append(key)
            args.append(value)
        return self.redis_fn("hset", *args)

    def hdel(self, *fields: MixedType) -> Expr:
        """
        Deletes fields.

        Args:
            *fields (MixedType): The fields to remove.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("hdel", *fields)

    def hget(self, field: MixedType) -> Expr:
        """
        Returns the value of a field.

        Args:
            field (MixedType): The field.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("hget", field)

    def hmget(self, *fields: MixedType) -> Expr:
        """
        Get multiple fields.

        Args:
            *fields (MixedType): The fields to remove.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("hmget", *fields)

    def hincrby(self, field: MixedType, inc: MixedType) -> Expr:
        """
        Updates the numerical value of a field.

        Args:
            field (MixedType): The field.
            inc (MixedType): The relative change.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("hincrby", field, inc)

    def hkeys(self) -> Expr:
        """
        Returns the keys of the hash.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("hkeys")

    def hvals(self) -> Expr:
        """
        Returns the values of the hash.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("hvals")

    def hgetall(self) -> Expr:
        """
        Returns the full hash.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("hgetall")
