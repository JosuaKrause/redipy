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
"""This module provides core functionality for the symbolic scripting
engine."""
from collections.abc import Callable

from redipy.graph.cmd import AssignmentObj, CommandObj
from redipy.graph.expr import ArgObj, ExprObj, KeyObj, RefIdObj, VarObj
from redipy.symbolic.expr import Expr, ExprHelper, lit_helper, MixedType


class Compilable:  # pylint: disable=too-few-public-methods
    """Can be compiled to a CommandObj statement."""
    def compile(self) -> CommandObj:
        """
        Compiles to a statement.

        Returns:
            CommandObj: The CommandObj statement.
        """
        raise NotImplementedError()


class CmdHelper(Compilable):  # pylint: disable=too-few-public-methods
    """A wrapper to allow compiling anonymously."""
    def __init__(self, stmt_fn: Callable[[], CommandObj]) -> None:
        """
        Creates a command helper.

        Args:
            stmt_fn (Callable[[], CommandObj]): The compiling function.
        """
        self._stmt_fn = stmt_fn

    def compile(self) -> CommandObj:
        return self._stmt_fn()


class Variable(Expr):
    """A reference to a variable. Various methods to interact with the variable
    are available."""
    def __init__(self) -> None:
        self._index: int | None = None

    def len_(self) -> 'Expr':
        """
        Computes the length of the array assuming that the variable points to
        one.

        Returns:
            Expr: The expression to compute the length.
        """
        return ArrayLen(self)

    def __getitem__(self, other: 'MixedType') -> 'Expr':
        """
        Accesses an element of the array assuming that the variable points to
        one.

        Args:
            other (MixedType): The index.

        Returns:
            Expr: The expression to access an index.
        """
        return ArrayAt(self, other)

    def get_key(self, key: 'MixedType') -> 'Expr':
        """
        Accesses an element of the dictionary assuming that the variable points
        to one.

        Args:
            key (MixedType): The key.

        Returns:
            Expr: The expression to access a key.
        """
        return DictKey(self, key)

    def set_index(self, index: int) -> None:
        """
        Sets the internal number of this variable.

        Args:
            index (int): The index of this variable.
        """
        self._index = index

    def get_index(self) -> int:
        """
        Returns the internal number of this variable.

        Returns:
            int: The index of this variable.
        """
        assert self._index is not None
        return self._index

    def prefix(self) -> str:
        """
        The name prefix of this variable.

        Returns:
            str: The name prefix.
        """
        raise NotImplementedError()

    def get_declare(self) -> AssignmentObj:
        """
        Creates a statement that declares the variable. Do not call this method
        directly.

        Returns:
            AssignmentObj: The assignment statement.
        """
        return {
            "kind": "declare",
            "assign": self.get_ref(),
            "value": self.get_declare_rhs().compile(),
        }

    def get_declare_rhs(self) -> Expr:
        """
        Returns the expression containing the initial value of the variable.

        Returns:
            Expr: The initial value expression.
        """
        raise NotImplementedError()

    def get_ref(self) -> RefIdObj:
        """
        Get an expression to reference the value of the variable.

        Returns:
            RefIdObj: The reference expression.
        """
        raise NotImplementedError()

    def compile(self) -> RefIdObj:
        return self.get_ref()

    def assign(self, val: MixedType) -> CmdHelper:
        """
        Assigns a value to the variable.

        Args:
            val (MixedType): The value expression.

        Returns:
            CmdHelper: The statement to assign the value.
        """
        expr = lit_helper(val)
        return CmdHelper(
            lambda: {
                "kind": "assign",
                "assign": self.get_ref(),
                "value": expr.compile(),
            })

    def set_at(self, index: MixedType, val: MixedType) -> CmdHelper:
        """
        Sets the value at a given index of the array assuming the variable
        points to one.

        Args:
            index (MixedType): The index.
            val (MixedType): The value to assign.

        Returns:
            CmdHelper: The assignment statement.
        """
        ix = lit_helper(index)
        expr = lit_helper(val)
        return CmdHelper(
            lambda: {
                "kind": "assign_at",
                "assign": self.get_ref(),
                "index": ix.compile(),
                "value": expr.compile(),
            })

    def set_key(self, key: MixedType, val: MixedType) -> CmdHelper:
        """
        Sets the value for a given key of the dictionary assuming the variable
        points to one.

        Args:
            key (MixedType): The key.
            val (MixedType): The value to assign.

        Returns:
            CmdHelper: The assignment statement.
        """
        kval = lit_helper(key)
        expr = lit_helper(val)
        return CmdHelper(
            lambda: {
                "kind": "assign_key",
                "assign": self.get_ref(),
                "key": kval.compile(),
                "value": expr.compile(),
            })


class KeyVariable(Variable):
    """A key argument variable."""
    def __init__(self, name: str) -> None:
        """
        A key argument variable. Do not call this directly.

        Args:
            name (str): The name of the key argument.
        """
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
        """
        Returns a reference to the key.

        Returns:
            KeyObj: The key reference.
        """
        return {
            "kind": "key",
            "name": f"{self.prefix()}_{self.get_index()}",
            "readable": self._name,
        }

    def get_ref(self) -> RefIdObj:
        return self.get_key_ref()


class JSONArg(Variable):
    """An argument to the script. This can contain any value that can be
    converted to JSON. Any value is converted to JSON for transport to the
    function and automatically converted back inside the script."""
    def __init__(self, name: str) -> None:
        """
        Creates a JSON argument. Do not call this directly.

        Args:
            name (str): The name of the argument.
        """
        super().__init__()
        self._name = name

    def get_declare_rhs(self) -> Expr:
        return ExprHelper(
            lambda: {
                "kind": "load_json_arg",
                "index": self.get_index(),
            })

    def prefix(self) -> str:
        return "arg"

    def get_arg_ref(self) -> ArgObj:
        """
        Get a reference to the argument.

        Returns:
            ArgObj: The reference expression.
        """
        return {
            "kind": "arg",
            "name": f"{self.prefix()}_{self.get_index()}",
            "readable": self._name,
        }

    def get_ref(self) -> RefIdObj:
        return self.get_arg_ref()


class LocalVariable(Variable):
    """A local variable."""
    def __init__(self, init: MixedType) -> None:
        """
        Creates a local variable. Do not call this directly.

        Args:
            init (MixedType): The initial value of the variable.
        """
        super().__init__()
        self._init = lit_helper(init)

    def get_declare_rhs(self) -> Expr:
        return self._init

    def prefix(self) -> str:
        return "var"

    def get_var_ref(self) -> VarObj:
        """
        Get a reference to the variable.

        Returns:
            VarObj: The reference expression.
        """
        return {
            "kind": "var",
            "name": f"{self.prefix()}_{self.get_index()}",
        }

    def get_ref(self) -> RefIdObj:
        return self.get_var_ref()


class ArrayAt(Expr):
    """Accesses an array at a given index."""
    def __init__(self, array: Variable, index: MixedType) -> None:
        """
        Accesses an array at the given index.

        Args:
            array (Variable): The array.
            index (MixedType): The index.
        """
        super().__init__()
        self._array = array
        self._index = lit_helper(index)

    def compile(self) -> ExprObj:
        return {
            "kind": "array_at",
            "arr": self._array.get_ref(),
            "index": self._index.compile(),
        }


class DictKey(Expr):
    """Accesses a dictionary at a given key."""
    def __init__(self, obj: Variable, key: MixedType) -> None:
        """
        Accesses a dictionary at a given key.

        Args:
            obj (Variable): The dictionary.
            key (MixedType): The key.
        """
        super().__init__()
        self._obj = obj
        self._key = lit_helper(key)

    def compile(self) -> ExprObj:
        return {
            "kind": "dict_key",
            "obj": self._obj.get_ref(),
            "key": self._key.compile(),
        }


class ArrayLen(Expr):
    """Computes the length of an array."""
    def __init__(self, array: Variable) -> None:
        """
        Computes the length of an array.

        Args:
            array (Variable): The array.
        """
        super().__init__()
        self._array = array

    def compile(self) -> ExprObj:
        return {
            "kind": "array_len",
            "var": self._array.get_ref(),
        }
