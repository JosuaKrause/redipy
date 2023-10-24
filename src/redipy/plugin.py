import importlib
from typing import NotRequired, TypedDict, TypeVar

from redipy.graph.expr import CallObj, ExprObj, JSONType
from redipy.memory.state import Machine


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


HELPER_PKG = "redipy"


class LuaPatch:
    def __init__(self, name: str) -> None:
        self._name = name

    def name(self) -> str:
        return self._name

    @staticmethod
    def names() -> set[str]:
        raise NotImplementedError()

    def helper_pkg(self) -> str:
        return HELPER_PKG


class LuaRedisPatch(LuaPatch):
    def patch(
            self,
            expr: CallObj,
            args: list[ExprObj],
            *,
            is_expr_stmt: bool) -> ExprObj:
        raise NotImplementedError()


class LuaGeneralPatch(LuaPatch):
    def patch(self, expr: CallObj, *, is_expr_stmt: bool) -> ExprObj:
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
            raise RuntimeError(
                f"function name {rname} is not allowed")
        if rname in target:
            raise RuntimeError(
                f"duplicate function definition: {rname}")
        target[rname] = rfun()


U = TypeVar('U', bound=LuaPatch)


def add_patch_plugin(
        module: str,
        target: dict[str, U],
        clazz: type[U],
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
    for pfun in candidates:
        for pname in pfun.names():
            if pname in disallowed:
                raise RuntimeError(
                    f"patch name {pname} is not allowed")
            if pname in target:
                raise RuntimeError(
                    f"duplicate patch definition: {pname}")
            target[pname] = pfun(pname)
