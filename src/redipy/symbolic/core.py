"""This module provides core functionality for the symbolic scripting
engine."""
from collections.abc import Callable

from redipy.graph.cmd import AssignmentObj, CommandObj
from redipy.graph.expr import ArgObj, ExprObj, RefIdObj, VarObj
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


class JSONArg(Variable):
    """An argument to the script. This can contain any value that can be
    converted to JSON. Any value is converted to JSON for transport to the
    function and automatically converted back inside the script."""
    def __init__(self, name: str) -> None:
        """
        Creates a JSON argument.

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
        return {
            "kind": "arg",
            "name": f"{self.prefix()}_{self.get_index()}",
            "readable": self._name,
        }

    def get_ref(self) -> RefIdObj:
        return self.get_arg_ref()


class LocalVariable(Variable):
    def __init__(self, init: MixedType) -> None:
        super().__init__()
        self._init = lit_helper(init)

    def get_declare_rhs(self) -> Expr:
        return self._init

    def prefix(self) -> str:
        return "var"

    def get_var_ref(self) -> VarObj:
        return {
            "kind": "var",
            "name": f"{self.prefix()}_{self.get_index()}",
        }

    def get_ref(self) -> RefIdObj:
        return self.get_var_ref()


class ArrayAt(Expr):
    def __init__(self, array: Variable, index: MixedType) -> None:
        super().__init__()
        self._array = array
        self._index = lit_helper(index)

    def compile(self) -> ExprObj:
        return {
            "kind": "array_at",
            "var": self._array.get_ref(),
            "index": self._index.compile(),
        }


class ArrayLen(Expr):
    def __init__(self, array: Variable) -> None:
        super().__init__()
        self._array = array

    def compile(self) -> ExprObj:
        return {
            "kind": "array_len",
            "var": self._array.get_ref(),
        }
