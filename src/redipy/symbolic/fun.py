from typing import Literal

from redipy.graph.expr import ExprObj, KeyObj, RefIdObj
from redipy.symbolic.core import Variable
from redipy.symbolic.expr import (
    Constant,
    Expr,
    ExprHelper,
    lit_helper,
    MixedType,
)


class KeyVariable(Variable):
    def __init__(self, name: str) -> None:
        super().__init__()
        self._name = name

    def get_declare_rhs(self) -> Expr:
        return ExprHelper(
            lambda: {
                "kind": "load_key_arg",
                "index": self.get_index(),
            })

    def prefix(self) -> str:
        return "key"

    def get_key_ref(self) -> KeyObj:
        return {
            "kind": "key",
            "name": f"{self.prefix()}_{self.get_index()}",
            "readable": self._name,
        }

    def get_ref(self) -> RefIdObj:
        return self.get_key_ref()


class CallFn(Expr):
    def __init__(
            self,
            fname: str,
            *args: MixedType,
            no_adjust: bool = False) -> None:
        self._fname = fname
        self._args: list[Expr] = [lit_helper(arg) for arg in args]
        self._no_adjust = no_adjust

    def compile(self) -> ExprObj:
        return {
            "kind": "call",
            "name": self._fname,
            "args": [arg.compile() for arg in self._args],
            "no_adjust": self._no_adjust,
        }


class FindFn(CallFn):
    def __init__(
            self,
            haystack: MixedType,
            needle: MixedType,
            start_ix: MixedType = None) -> None:
        super().__init__(
            "string.find",
            haystack,
            needle,
            *([] if start_ix is None else [start_ix]))


class FromJSON(CallFn):
    def __init__(self, arg: MixedType) -> None:
        super().__init__("cjson.decode", arg)


class ToJSON(CallFn):
    def __init__(self, arg: MixedType) -> None:
        super().__init__("cjson.encode", arg)


class ToNum(CallFn):
    def __init__(self, arg: MixedType) -> None:
        super().__init__("tonumber", arg)


class ToStr(CallFn):
    def __init__(self, arg: MixedType) -> None:
        super().__init__("tostr", arg)


class RedisFn(CallFn):
    def __init__(
            self,
            redis_fn: str,
            key: KeyVariable,
            *args: MixedType,
            no_adjust: bool = False) -> None:
        super().__init__(
            "redis.call", redis_fn, key, *args, no_adjust=no_adjust)


LogLevel = Literal["debug", "verbose", "notice", "warning"]


class LogFn(CallFn):
    def __init__(
            self,
            level: LogLevel,
            message: MixedType) -> None:
        super().__init__("redis.log", self.convert_level(level), message)

    @staticmethod
    def convert_level(
            level: LogLevel) -> Expr:
        level_map: dict[LogLevel, str] = {
            "debug": "redis.LOG_DEBUG",
            "verbose": "redis.LOG_VERBOSE",
            "notice": "redis.LOG_NOTICE",
            "warning": "redis.LOG_WARNING",
        }
        raw = level_map.get(level)
        if raw is None:
            raise ValueError(f"could not determine debug level: {level}")
        return Constant(raw)
