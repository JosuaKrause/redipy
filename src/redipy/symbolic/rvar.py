"""Access to redis variables."""
from redipy.api import RSetMode, RSM_ALWAYS, RSM_EXISTS, RSM_MISSING
from redipy.symbolic.expr import Expr, MixedType
from redipy.symbolic.fun import RedisObj


class RedisVar(RedisObj):
    """A redis variable."""
    def set(
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

    def get(self, *, no_adjust: bool = False) -> Expr:
        """
        Returns the value.

        Args:
            no_adjust (bool, optional): Whether to prevent patching the
            function call. This should not be neccessary. Defaults to False.

        Returns:
            Expr: The expression.
        """
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
