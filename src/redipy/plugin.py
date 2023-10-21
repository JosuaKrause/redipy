import importlib
from typing import NotRequired, TypedDict, TypeVar

from redipy.graph.expr import ExprObj
from redipy.memory.state import Machine
from redipy.symbolic.expr import JSONType


ArgcSpec = TypedDict('ArgcSpec', {
    "count": int,
    "at_least": NotRequired[bool],
    "at_most": NotRequired[int],
})


class NamedFunction:  # pylint: disable=too-few-public-methods
    @staticmethod
    def name() -> str:
        raise NotImplementedError()


class GeneralFunction(NamedFunction):
    @staticmethod
    def argc() -> ArgcSpec:
        raise NotImplementedError()


class LocalRedisFunction(GeneralFunction):
    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        raise NotImplementedError()


class LocalGeneralFunction(GeneralFunction):
    @staticmethod
    def call(args: list[JSONType]) -> JSONType:
        raise NotImplementedError()


class LuaRedisFunction(GeneralFunction):
    @staticmethod
    def call(args: list[JSONType]) -> ExprObj:
        raise NotImplementedError()

    @staticmethod
    def patch(expr: ExprObj, *, is_expr_stmt: bool) -> ExprObj:
        raise NotImplementedError()


class LuaGeneralFunction(GeneralFunction):
    @staticmethod
    def call(args: list[JSONType]) -> ExprObj:
        raise NotImplementedError()

    @staticmethod
    def patch(expr: ExprObj, *, is_expr_stmt: bool) -> ExprObj:
        raise NotImplementedError()


class HelperFunction(NamedFunction):
    @staticmethod
    def args() -> str:
        raise NotImplementedError()

    @staticmethod
    def body() -> str:
        raise NotImplementedError()


T = TypeVar('T', bound=NamedFunction)


def add_plugin(
        module: str,
        target: dict[str, T],
        clazz: type[T],
        disallowed: set[str] | None = None) -> None:
    if disallowed is None:
        disallowed = set()
    mod = importlib.import_module(module)
    candidates = [
        cls
        for cls in mod.__dict__.values()
        if isinstance(cls, type)
        and cls.__module__ == module
        and issubclass(cls, clazz)
    ]
    for rfun in candidates:
        rname = rfun.name()
        if rname in disallowed:
            raise RuntimeError(f"redis function name {rname} is not allowed")
        if rname in target:
            raise RuntimeError(
                f"duplicate redis function definition: {rname}")
        target[rname] = rfun()
