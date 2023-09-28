from redipy.symbolic.expr import Expr, MixedType
from redipy.symbolic.fun import KeyVariable, RedisFn


class RedisSortedSet:
    def __init__(self, key: KeyVariable) -> None:
        self._key = key

    def add(self, score: MixedType, value: MixedType) -> Expr:
        # TODO add all arguments
        return RedisFn("zadd", self._key, score, value)

    def pop_max(self, count: MixedType = None) -> Expr:
        if count is None:
            return RedisFn("zpopmax", self._key)
        return RedisFn("zpopmax", self._key, count)

    def pop_min(self, count: MixedType = None) -> Expr:
        if count is None:
            return RedisFn("zpopmin", self._key)
        return RedisFn("zpopmin", self._key, count)

    def card(self) -> Expr:
        return RedisFn("zcard", self._key)
