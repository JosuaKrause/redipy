from redipy.symbolic.expr import Expr, MixedType
from redipy.symbolic.fun import KeyVariable, RedisFn


class RedisList:
    def __init__(self, key: KeyVariable) -> None:
        self._key = key

    def lpush(self, *values: MixedType) -> Expr:
        return RedisFn("lpush", self._key, *values)

    def rpush(self, *values: MixedType) -> Expr:
        return RedisFn("rpush", self._key, *values)

    def lpop(
            self, count: MixedType = None, *, no_adjust: bool = False) -> Expr:
        if count is None:
            return RedisFn("lpop", self._key, no_adjust=no_adjust)
        return RedisFn("lpop", self._key, count, no_adjust=no_adjust)

    def rpop(
            self, count: MixedType = None, *, no_adjust: bool = False) -> Expr:
        if count is None:
            return RedisFn("rpop", self._key, no_adjust=no_adjust)
        return RedisFn("rpop", self._key, count, no_adjust=no_adjust)

    def llen(self) -> Expr:
        return RedisFn("llen", self._key)
