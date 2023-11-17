"""General functions for scripts."""
from typing import Literal

from redipy.graph.expr import ExprObj
from redipy.symbolic.expr import Constant, Expr, lit_helper, MixedType


class CallFn(Expr):
    """Calls a function. The name of the function must be known at compile
    time."""
    def __init__(
            self,
            fname: str,
            *args: MixedType,
            no_adjust: bool = False) -> None:
        """
        Creates a function call.

        Args:
            fname (str): The name of the function.

            *args (MixedType): The arguments of the function.

            no_adjust (bool, optional): Whether to disallow patching the
            function call. In most cases this should be False.
            Defaults to False.
        """
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
    """Finds a substring inside a string."""
    def __init__(
            self,
            haystack: MixedType,
            needle: MixedType,
            start_ix: MixedType = None) -> None:
        """
        Finds a substring inside a string.

        Args:
            haystack (MixedType): The string to search in.

            needle (MixedType): The string to search for.

            start_ix (MixedType, optional): The starting index of the search.
            Defaults to None.
        """
        super().__init__(
            "string.find",
            haystack,
            needle,
            *([] if start_ix is None else [start_ix]))


class FromJSON(CallFn):
    """Decodes a string into a JSON object."""
    def __init__(self, arg: MixedType) -> None:
        super().__init__("cjson.decode", arg)


class ToJSON(CallFn):
    """Encodes a JSON object into a string."""
    def __init__(self, arg: MixedType) -> None:
        super().__init__("cjson.encode", arg)


class ToNum(CallFn):
    """Converts the expression into a number."""
    def __init__(self, arg: MixedType) -> None:
        super().__init__("tonumber", arg)


class ToIntStr(CallFn):
    """Converts a number into an integer string representation."""
    def __init__(self, arg: MixedType) -> None:
        super().__init__("asintstr", arg)


class ToStr(CallFn):
    """Converts the expression into a string. If you need an integer string for
    a number use `ToIntStr` instead."""
    def __init__(self, arg: MixedType) -> None:
        super().__init__("tostring", arg)


class TypeStr(CallFn):
    """Converts the expression into the name of its type."""
    def __init__(self, arg: MixedType) -> None:
        super().__init__("type", arg)


class RedisFn(CallFn):
    """Calls a redis function. The name of the redis function must be known
    at compile time."""
    def __init__(
            self,
            redis_fn: str,
            key: MixedType,
            *args: MixedType,
            no_adjust: bool = False) -> None:
        """
        Creates a function call.

        Args:
            redis_fn (str): The name of the redis function.

            key (MixedType): The key.

            *args (MixedType): The arguments of the redis function.

            no_adjust (bool, optional): Whether to disallow patching the
            function call. In most cases this should be False.
            Defaults to False.
        """
        super().__init__(
            "redis.call", redis_fn, key, *args, no_adjust=no_adjust)


class RedisObj:
    """An object to operate redis functions on a key. Subclassing this class
    is the most common way of exposing redis functionality."""
    def __init__(self, key: MixedType) -> None:
        """
        Creates a redis object for the given key.

        Args:
            key (MixedType): The key.
        """
        self._key = lit_helper(key)

    def key(self) -> Expr:
        """
        Returns the key expression.

        Returns:
            Expr: The key expression.
        """
        return self._key

    def redis_fn(
            self,
            name: str,
            *args: MixedType,
            no_adjust: bool = False) -> Expr:
        """
        Calls a redis function with the given name, key, and arguments.

        Args:
            name (str): The name of the redis function.

            *args (MixedType): The remaining arguments of the redis function.

            no_adjust (bool, optional): Whether to disallow patching the
            function call. In most cases this should be False.
            Defaults to False.

        Returns:
            Expr: The function call.
        """
        return RedisFn(name, self.key(), *args, no_adjust=no_adjust)

    def exists(self) -> Expr:
        """
        Computes whether the key exists calling the `EXISTS` redis function.

        Returns:
            Expr: The expression for the redis function call.
        """
        return self.redis_fn("exists")

    def delete(self) -> Expr:
        """
        Deletes the key calling the `DEL` redis function.

        Returns:
            Expr: The expression for the redis function call.
        """
        return self.redis_fn("del")


LogLevel = Literal["debug", "verbose", "notice", "warning"]
"""Logging level constants."""


class LogFn(CallFn):
    """Logs a string to the default output depending on the logging level."""
    def __init__(
            self,
            level: LogLevel,
            message: MixedType) -> None:
        """
        Logs a string to the default output depending on the logging level.

        Args:
            level (LogLevel): The logging level.

            message (MixedType): The string.
        """
        super().__init__("redis.log", self.convert_level(level), message)

    @staticmethod
    def convert_level(
            level: LogLevel) -> Expr:
        """
        Converts a logging level to constants.

        Args:
            level (LogLevel): The logging level.

        Raises:
            ValueError: If the logging level is invalid.

        Returns:
            Expr: The corresponding constant.
        """
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
