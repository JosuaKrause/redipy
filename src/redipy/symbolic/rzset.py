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
        # TODO add all arguments
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

    def card(self) -> Expr:
        """
        Computes the cardinality of the sorted set.

        Returns:
            Expr: The expression.
        """
        return self.redis_fn("zcard")

    # FIXME implement ZRANGE
