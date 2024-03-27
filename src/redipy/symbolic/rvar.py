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
"""Access to redis variables."""
from redipy.api import RSetMode, RSM_ALWAYS, RSM_EXISTS, RSM_MISSING
from redipy.symbolic.expr import Expr, MixedType
from redipy.symbolic.fun import RedisObj


class RedisVar(RedisObj):
    """A redis variable."""
    def set_value(
            self,
            value: MixedType,
            *,
            mode: RSetMode = RSM_ALWAYS,
            return_previous: bool = False,
            expire_in: float | None = None,
            keep_ttl: bool = False) -> Expr:
        """
        Sets the value.

        Args:
            value (MixedType): The value.

            mode (RSetMode, optional): The condition to set the value. Defaults
            to RSM_ALWAYS.

            return_previous (bool, optional): Whether to return the previous
            value. Defaults to False.

            expire_in (float | None, optional): Expires the value in seconds.
            Defaults to None.

            keep_ttl (bool, optional): Preserve the time to live. Defaults to
            False.

        Returns:
            Expr: The expression.
        """
        args: list[MixedType] = []
        if mode == RSM_EXISTS:
            args.append("XX")
        elif mode == RSM_MISSING:
            args.append("NX")
        if return_previous:
            args.append("GET")
        if expire_in is not None:
            args.append("PX")
            expire_milli = int(expire_in * 1000.0)
            args.append(expire_milli)
        elif keep_ttl:
            args.append("KEEPTTL")
        return self.redis_fn("set", value, *args)

    def get_value(
            self,
            *,
            default: MixedType = None,
            no_adjust: bool = False) -> Expr:
        """
        Returns the value.

        Args:
            default (MixedType, optional): The default value to return if
            the key does not exist.

            no_adjust (bool, optional): Whether to prevent patching the
            function call. This should not be neccessary. Defaults to False.

        Returns:
            Expr: The expression.
        """
        if default is not None:
            return self.redis_fn("get", no_adjust=True).or_(default)
        return self.redis_fn("get", no_adjust=no_adjust)

    def incrby(self, inc: MixedType) -> Expr:
        """
        Updates the numeric value by a given amount.

        Args:
            inc (MixedType): The relative amount.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("incrby", inc)
