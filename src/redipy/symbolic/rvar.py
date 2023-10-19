from redipy.api import RSetMode, RSM_ALWAYS, RSM_EXISTS, RSM_MISSING
from redipy.symbolic.expr import Expr, MixedType
from redipy.symbolic.fun import KeyVariable, RedisFn


class RedisVar:
    def __init__(self, key: KeyVariable) -> None:
        self._key = key

    def set(
            self,
            value: MixedType,
            *,
            mode: RSetMode = RSM_ALWAYS,
            return_previous: bool = False,
            expire_in: float | None = None,
            keep_ttl: bool = False) -> Expr:
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
        return RedisFn("set", self._key, value, *args)

    def get(self, *, no_adjust: bool = False) -> Expr:
        return RedisFn("get", self._key, no_adjust=no_adjust)
