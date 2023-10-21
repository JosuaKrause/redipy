from redipy.symbolic.expr import Expr, MixedType
from redipy.symbolic.fun import RedisObj


class RedisHash(RedisObj):
    def hset(self, mapping: dict[MixedType, MixedType]) -> Expr:
        args = []
        for key, value in mapping.items():
            args.append(key)
            args.append(value)
        return self.redis_fn("hset", self.key(), *args)

    def hdel(self, *fields: MixedType) -> Expr:
        return self.redis_fn("hdel", *fields)

    def hget(self, field: MixedType) -> Expr:
        return self.redis_fn("hget", field)

    def hmget(self, *fields: MixedType) -> Expr:
        return self.redis_fn("hmget", *fields)

    def hincrby(self, field: MixedType, inc: MixedType) -> Expr:
        return self.redis_fn("hincrby", field, inc)

    def hkeys(self) -> Expr:
        return self.redis_fn("hkeys")

    def hvals(self) -> Expr:
        return self.redis_fn("hvals")

    def hgetall(self) -> Expr:
        return self.redis_fn("hgetall")
