"""Access to redis lists."""
from redipy.symbolic.expr import Expr, MixedType
from redipy.symbolic.fun import RedisObj


class RedisList(RedisObj):
    """A redis list."""
    def lpush(self, *values: MixedType) -> Expr:
        return self.redis_fn("lpush", *values)

    def rpush(self, *values: MixedType) -> Expr:
        return self.redis_fn("rpush", *values)

    def lpop(
            self, count: MixedType = None, *, no_adjust: bool = False) -> Expr:
        if count is None:
            return self.redis_fn("lpop", no_adjust=no_adjust)
        return self.redis_fn("lpop", count, no_adjust=no_adjust)

    def rpop(
            self, count: MixedType = None, *, no_adjust: bool = False) -> Expr:
        if count is None:
            return self.redis_fn("rpop", no_adjust=no_adjust)
        return self.redis_fn("rpop", count, no_adjust=no_adjust)

    def llen(self) -> Expr:
        return self.redis_fn("llen")

    # TODO implement lrange
