"""Access to redis sorted sets."""
from redipy.symbolic.expr import Expr, MixedType
from redipy.symbolic.fun import RedisObj


class RedisSortedSet(RedisObj):
    """A redis sorted set."""
    def add(self, score: MixedType, value: MixedType) -> Expr:
        # TODO add all arguments
        return self.redis_fn("zadd", score, value)

    def pop_max(self, count: MixedType = None) -> Expr:
        if count is None:
            return self.redis_fn("zpopmax")
        return self.redis_fn("zpopmax", count)

    def pop_min(self, count: MixedType = None) -> Expr:
        if count is None:
            return self.redis_fn("zpopmin")
        return self.redis_fn("zpopmin", count)

    def card(self) -> Expr:
        return self.redis_fn("zcard")

    # FIXME implement ZRANGE
