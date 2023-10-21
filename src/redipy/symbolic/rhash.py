from redipy.symbolic.expr import Expr, MixedType
from redipy.symbolic.fun import KeyVariable, RedisFn


class RedisHash:
    def __init__(self, key: KeyVariable) -> None:
        self._key = key

    def hset(self, mapping: dict[MixedType, MixedType]) -> Expr:
        args = []
        for key, value in mapping.items():
            args.append(key)
            args.append(value)
        return RedisFn("hset", self._key, *args)

    def hdel(self, *fields: MixedType) -> Expr:
        return RedisFn("hdel", self._key, *fields)

    def hget(self, field: str) -> Expr:
        return RedisFn("hget", self._key, field)

    def hmget(self, *fields: MixedType) -> Expr:
        return RedisFn("hmget", self._key, *fields)

    def hincrby(self, field: MixedType, inc: MixedType) -> Expr:
        return RedisFn("hincrby", self._key, field, inc)

    def hkeys(self) -> Expr:
        return RedisFn("hkeys", self._key)

    def hvals(self) -> Expr:
        return RedisFn("hvals", self._key)

    def hgetall(self) -> Expr:
        return RedisFn("hgetall", self._key)
