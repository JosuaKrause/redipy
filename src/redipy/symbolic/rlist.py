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
