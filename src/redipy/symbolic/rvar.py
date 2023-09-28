from redipy.symbolic.expr import Expr, MixedType
from redipy.symbolic.fun import KeyVariable, RedisFn


class RedisVar:
    def __init__(self, key: KeyVariable) -> None:
        self._key = key

    def set(self, value: MixedType) -> Expr:
        # TODO add all arguments
        return RedisFn("set", self._key, value)

    def get(self, *, no_adjust: bool = False) -> Expr:
        return RedisFn("get", self._key, no_adjust=no_adjust)
