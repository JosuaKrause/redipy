# Copyright 2024 Josua Krause
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Base classes for plugin functionality. Subclass the appropriate base class
and register it in the appropriate backend's `add_..._plugin` method."""
import importlib
from typing import NotRequired, TypedDict, TypeVar

from redipy.graph.expr import CallObj, ExprObj, JSONType
from redipy.memory.state import Machine


ArgcSpec = TypedDict('ArgcSpec', {
    "count": int,
    "at_least": NotRequired[bool],
    "at_most": NotRequired[int],
})
"""Specification of number of function arguments."""


class NamedFunction:  # pylint: disable=too-few-public-methods
    """Base class for named functions."""
    @staticmethod
    def name() -> str:
        """
        The name of the function.

        Returns:
            str: The name.
        """
        raise NotImplementedError()


class GeneralFunction(NamedFunction):
    """Base class for general functions."""
    @staticmethod
    def argc() -> ArgcSpec:
        """
        The specification of the number of arguments of the function.

        Returns:
            ArgcSpec: The specification of arguments for the function.
        """
        raise NotImplementedError()


class LocalRedisFunction(GeneralFunction):
    """A redis function for the memory backend."""
    @staticmethod
    def call(sm: Machine, key: str, args: list[JSONType]) -> JSONType:
        """
        Executes the redis function for the memory backend.

        Args:
            sm (Machine): The memory redis state.

            key (str): The key.

            args (list[JSONType]): The rest of the arguments.

        Returns:
            JSONType: The result of the redis function.
        """
        raise NotImplementedError()


class LocalGeneralFunction(GeneralFunction):
    """A general script function for the memory backend."""
    @staticmethod
    def call(args: list[JSONType]) -> JSONType:
        """
        Executes a general function for the memory backend.

        Args:
            args (list[JSONType]): The arguments.

        Returns:
            JSONType: The result of the general function.
        """
        raise NotImplementedError()


HELPER_PKG = "redipy"
"""The name of the helper package in lua."""


class LuaPatch:
    """Base class for lua patches."""
    def __init__(self, name: str) -> None:
        self._name = name

    def name(self) -> str:
        """
        The name of the patched function.

        Returns:
            str: One of the values of names.
        """
        return self._name

    @staticmethod
    def names() -> set[str]:
        """
        All names the patch is applied to.

        Returns:
            set[str]: All function names the patch is applied to.
        """
        raise NotImplementedError()

    def helper_pkg(self) -> str:
        """
        The name of the lua helper package.

        Returns:
            str: The lua helper package.
        """
        return HELPER_PKG


class LuaRedisPatch(LuaPatch):
    """Patches a lua redis function call."""
    def patch(
            self,
            name: str,
            expr: CallObj,
            args: list[ExprObj],
            *,
            is_expr_stmt: bool) -> ExprObj:
        """
        Applies the patch on the expression graph for the given redis call.

        Args:
            name (str): The redis function name.

            expr (CallObj): The function call.

            args (list[ExprObj]): The arguments of the function call.

            is_expr_stmt (bool): Whether the call is a lua statement. Most
            patches should not be applied in this context as lua doesn't allow
            expressions as statements (whereas function calls can be both).
            In most cases add the following at the beginning of the patch
            implementation:
            ```
            if is_expr_stmt:
                return expr
            ```

        Returns:
            ExprObj: The patched expression.
        """
        raise NotImplementedError()


class LuaGeneralPatch(LuaPatch):
    """Patches a general lua function call."""
    def patch(self, expr: CallObj, *, is_expr_stmt: bool) -> ExprObj:
        """
        Applies the patch on the expression graph for the given function call.

        Args:
            expr (CallObj): The function call.

            is_expr_stmt (bool): Whether the call is a lua statement. Most
            patches should not be applied in this context as lua doesn't allow
            expressions as statements (whereas function calls can be both).
            In most cases add the following at the beginning of the patch
            implementation:
            ```
            if is_expr_stmt:
                return expr
            ```

        Returns:
            ExprObj: The patched expression.
        """
        raise NotImplementedError()


class HelperFunction(NamedFunction):
    """Defines a lua helper function."""
    @staticmethod
    def args() -> str:
        """
        The literal argument line.

        Returns:
            str: The argument line as string (the content between parentheses).
        """
        raise NotImplementedError()

    @staticmethod
    def body() -> str:
        """
        The literal function body.

        Returns:
            str: The function body as multi-line string. Use 4 space
            indentation. The content will be deindented and the indentation
            will be adjusted to lua's 2 spaces when loading the function.
        """
        raise NotImplementedError()


T = TypeVar('T', bound=NamedFunction)


def add_plugin(
        module: str,
        target: dict[str, T],
        clazz: type[T],
        disallowed: set[str] | None = None) -> None:
    """
    Adds eligible elements of the given module as plugins to the target.

    Args:
        module (str): The module to read the plugins from.

        target (dict[str, T]): The destination to store the plugins.

        clazz (type[T]): Only elements of this class are loaded as plugins.

        disallowed (set[str] | None, optional): A set of invalid names.
        If a plugin attempts to use one of those names an error is raised.
        Defaults to None.

    Raises:
        RuntimeError: If a plugin cannot be loaded.
    """
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
    """
    Adds eligible elements of the given module as plugins to the target.
    The elements must be lua patches.

    Args:
        module (str): The module to read the plugins from.

        target (dict[str, U]): The destination to store the plugins.

        clazz (type[U]): Only elements of this class are loaded as plugins.

        disallowed (set[str] | None, optional): A set of invalid names.
        If a plugin attempts to use one of those names an error is raised.
        Defaults to None.

    Raises:
        RuntimeError: If a plugin cannot be loaded.
    """
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
