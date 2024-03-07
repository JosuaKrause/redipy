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
"""Provides core functionality of expressions."""
from collections.abc import Callable

from redipy.graph.expr import ExprObj, ValueType


class Expr:  # pylint: disable=too-few-public-methods
    """The base class for all expressions."""
    def compile(self) -> ExprObj:
        """
        Compiles the expression into an execution graph expression object.

        Returns:
            ExprObj: The expression object.
        """
        raise NotImplementedError()

    def __add__(self, other: 'MixedType') -> 'Expr':
        """
        Adds two expressions.

        Args:
            other (MixedType): The other expression.

        Returns:
            Expr: The resulting expression.
        """
        return AddOp(self, other)

    def __sub__(self, other: 'MixedType') -> 'Expr':
        """
        Subtracts two expressions.

        Args:
            other (MixedType): The other expression.

        Returns:
            Expr: The resulting expression.
        """
        return SubOp(self, other)

    def eq_(self, other: 'MixedType') -> 'Expr':
        """
        Compares two expressions for equality.

        Args:
            other (MixedType): The other expression.

        Returns:
            Expr: The resulting expression.
        """
        return EqOp(self, other)

    def ne_(self, other: 'MixedType') -> 'Expr':
        """
        Compares two expressions for inequality.

        Args:
            other (MixedType): The other expression.

        Returns:
            Expr: The resulting expression.
        """
        return NeOp(self, other)

    def lt_(self, other: 'MixedType') -> 'Expr':
        """
        Compares two expressions returning whether this expression is less than
        the other expression.

        Args:
            other (MixedType): The other expression.

        Returns:
            Expr: The resulting expression.
        """
        return LtOp(self, other)

    def le_(self, other: 'MixedType') -> 'Expr':
        """
        Compares two expressions returning whether this expression is less or
        equal to the other expression.

        Args:
            other (MixedType): The other expression.

        Returns:
            Expr: The resulting expression.
        """
        return LeOp(self, other)

    def gt_(self, other: 'MixedType') -> 'Expr':
        """
        Compares two expressions returning whether this expression is greater
        than the other expression.

        Args:
            other (MixedType): The other expression.

        Returns:
            Expr: The resulting expression.
        """
        return GtOp(self, other)

    def ge_(self, other: 'MixedType') -> 'Expr':
        """
        Compares two expressions returning whether this expression is greater
        or equal to the other expression.

        Args:
            other (MixedType): The other expression.

        Returns:
            Expr: The resulting expression.
        """
        return GeOp(self, other)

    def not_(self) -> 'Expr':
        """
        Negates the current expression.

        Returns:
            Expr: The negated expression.
        """
        return NotOp(self)

    def or_(self, other: 'MixedType') -> 'Expr':
        """
        Logically ORs this expression with the other expression.

        Args:
            other (MixedType): The other expression.

        Returns:
            Expr: The resulting expression.
        """
        return OrOp(self, other)

    def and_(self, other: 'MixedType') -> 'Expr':
        """
        Logically ANDs this expression with the other expression.

        Args:
            other (MixedType): The other expression.

        Returns:
            Expr: The resulting expression.
        """
        return AndOp(self, other)


class ExprHelper(Expr):  # pylint: disable=too-few-public-methods
    """Provides a wrapper to use a function to get an expression."""
    def __init__(self, expr_fn: Callable[[], ExprObj]) -> None:
        """
        Creates a wrapper to use a function to get an expression.

        Args:
            expr_fn (Callable[[], ExprObj]): Function that returns an
            expression.
        """
        super().__init__()
        self._expr_fn = expr_fn

    def compile(self) -> ExprObj:
        return self._expr_fn()


class Constant(Expr):
    """Accesses a named constant."""
    def __init__(self, raw: str) -> None:
        """
        Accesses a named constant.

        Args:
            raw (str): The name of the constant.
        """
        super().__init__()
        self._raw = raw

    def compile(self) -> ExprObj:
        return {
            "kind": "constant",
            "raw": self._raw,
        }


LiteralType = str | int | float | bool | list | dict | None
"""Literal values that transparently get converted to expressions."""
MixedType = LiteralType | Expr
"""An expression or literal."""


class Strs(Expr):
    """Concatenates a sequence of values."""
    def __init__(self, *values: MixedType) -> None:
        """
        Concatenates a sequence of values.

        Args:
            *values (MixedType): The values to concatenate. Should be strings.
        """
        super().__init__()
        self._values = [lit_helper(val) for val in values]

    def compile(self) -> ExprObj:
        return {
            "kind": "concat",
            "strings": [val.compile() for val in self._values],
        }


class LiteralOp(Expr):
    """Expression for literal values."""
    def __init__(self, value: LiteralType) -> None:
        """
        Expression for literal values.

        Args:
            value (LiteralType): The literal value.
        """
        super().__init__()
        self._value = value
        self._type = self.compute_type(value)

    @staticmethod
    def compute_type(value: LiteralType) -> ValueType:
        """
        Determines the type of the literal.

        Args:
            value (LiteralType): The literal.

        Raises:
            ValueError: If the type can not be determined.

        Returns:
            ValueType: The type of the literal.
        """
        if value is None:
            return "none"
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        if isinstance(value, str):
            return "str"
        if isinstance(value, list):
            return "list"
        if isinstance(value, dict):
            return "dict"
        raise ValueError(f"unknown type for: {value}")

    def compile(self) -> ExprObj:
        return {
            "kind": "val",
            "value": self._value,
            "type": self._type,
        }


def lit_helper(value: MixedType) -> Expr:
    """
    Converts a mixed type into an expression. If the mixed type is a literal
    it will be wrapped in an expression.

    Args:
        value (MixedType): An expression or literal.

    Returns:
        Expr: The expression.
    """
    if isinstance(value, Expr):
        return value
    return LiteralOp(value)


class NotOp(Expr):
    """Negates an expression."""
    def __init__(self, expr: MixedType) -> None:
        """
        Negates an expression. Do not call directly. Use the expression method
        instead.

        Args:
            expr (MixedType): The expression to negate.
        """
        super().__init__()
        self._expr = lit_helper(expr)

    def compile(self) -> ExprObj:
        return {
            "kind": "unary",
            "op": "not",
            "arg": self._expr.compile(),
        }


class Op(Expr):
    """Combines two expressions using an operation."""
    def __init__(self, lhs: MixedType, rhs: MixedType) -> None:
        """
        Combines two expressions using an operation. Do not call directly.
        Use the corresponding expression method instead.

        Args:
            lhs (MixedType): The left expression.

            rhs (MixedType): The right expression.
        """
        super().__init__()
        self._lhs = lit_helper(lhs)
        self._rhs = lit_helper(rhs)

    def get_left(self) -> Expr:
        """
        Returns the left expression.

        Returns:
            Expr: The left expression.
        """
        return self._lhs

    def get_right(self) -> Expr:
        """
        Returns the right expression.

        Returns:
            Expr: The right expression.
        """
        return self._rhs

    def compile(self) -> ExprObj:
        raise NotImplementedError()


class AndOp(Op):
    """Computes the logical AND of both expressions."""
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "and",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class OrOp(Op):
    """Computes the logical OR of both expressions."""
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "or",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class AddOp(Op):
    """Adds two expressions."""
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "add",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class SubOp(Op):
    """Subtracts two expressions."""
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "sub",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class LtOp(Op):
    """Computes whether the left hand side is less than the right hand side."""
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "lt",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class LeOp(Op):
    """Computes whether the left hand side is less or equal to the right hand
    side."""
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "le",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class GtOp(Op):
    """Computes whether the left hand side is greater than the right hand
    side."""
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "gt",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class GeOp(Op):
    """Computes whether the left hand side is greater or equal to the right
    hand side."""
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "ge",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class EqOp(Op):
    """Computes whether two expressions evaluate to equal values."""
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "eq",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class NeOp(Op):
    """Computes whether two expressions evaluate to unequal values."""
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "ne",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }
