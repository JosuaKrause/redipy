from collections.abc import Callable

from redipy.graph.expr import ExprObj, ValueType


class Expr:  # pylint: disable=too-few-public-methods
    def compile(self) -> ExprObj:
        raise NotImplementedError()

    def __add__(self, other: 'MixedType') -> 'Expr':
        return AddOp(self, other)

    def __sub__(self, other: 'MixedType') -> 'Expr':
        return SubOp(self, other)

    def eq_(self, other: 'MixedType') -> 'Expr':
        return EqOp(self, other)

    def ne_(self, other: 'MixedType') -> 'Expr':
        return NeOp(self, other)

    def lt_(self, other: 'MixedType') -> 'Expr':
        return LtOp(self, other)

    def le_(self, other: 'MixedType') -> 'Expr':
        return LeOp(self, other)

    def gt_(self, other: 'MixedType') -> 'Expr':
        return GtOp(self, other)

    def ge_(self, other: 'MixedType') -> 'Expr':
        return GeOp(self, other)

    def not_(self) -> 'Expr':
        return NotOp(self)

    def or_(self, other: 'MixedType') -> 'Expr':
        return OrOp(self, other)

    def and_(self, other: 'MixedType') -> 'Expr':
        return AndOp(self, other)


class ExprHelper(Expr):  # pylint: disable=too-few-public-methods
    def __init__(self, expr_fn: Callable[[], ExprObj]) -> None:
        super().__init__()
        self._expr_fn = expr_fn

    def compile(self) -> ExprObj:
        return self._expr_fn()


class Constant(Expr):
    def __init__(self, raw: str) -> None:
        super().__init__()
        self._raw = raw

    def compile(self) -> ExprObj:
        return {
            "kind": "constant",
            "raw": self._raw,
        }


LiteralType = str | int | float | bool | list | None
MixedType = LiteralType | Expr


class Strs(Expr):
    def __init__(self, *values: MixedType) -> None:
        super().__init__()
        self._values = [lit_helper(val) for val in values]

    def compile(self) -> ExprObj:
        return {
            "kind": "concat",
            "strings": [val.compile() for val in self._values],
        }


class LiteralOp(Expr):
    def __init__(self, value: LiteralType) -> None:
        super().__init__()
        self._value = value
        self._type = self.compute_type(value)

    @staticmethod
    def compute_type(value: LiteralType) -> ValueType:
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
        raise ValueError(f"unknown type for: {value}")

    def compile(self) -> ExprObj:
        return {
            "kind": "val",
            "value": self._value,
            "type": self._type,
        }


def lit_helper(value: MixedType) -> Expr:
    if isinstance(value, Expr):
        return value
    return LiteralOp(value)


class NotOp(Expr):
    def __init__(self, expr: MixedType) -> None:
        super().__init__()
        self._expr = lit_helper(expr)

    def compile(self) -> ExprObj:
        return {
            "kind": "unary",
            "op": "not",
            "arg": self._expr.compile(),
        }


class Op(Expr):
    def __init__(self, lhs: MixedType, rhs: MixedType) -> None:
        super().__init__()
        self._lhs = lit_helper(lhs)
        self._rhs = lit_helper(rhs)

    def get_left(self) -> Expr:
        return self._lhs

    def get_right(self) -> Expr:
        return self._rhs

    def compile(self) -> ExprObj:
        raise NotImplementedError()


class AndOp(Op):
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "and",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class OrOp(Op):
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "or",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class AddOp(Op):
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "add",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class SubOp(Op):
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "sub",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class LtOp(Op):
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "lt",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class LeOp(Op):
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "le",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class GtOp(Op):
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "gt",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class GeOp(Op):
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "ge",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class EqOp(Op):
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "eq",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }


class NeOp(Op):
    def compile(self) -> ExprObj:
        return {
            "kind": "binary",
            "op": "ne",
            "left": self.get_left().compile(),
            "right": self.get_right().compile(),
        }
