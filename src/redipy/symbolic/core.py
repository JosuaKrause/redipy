from collections.abc import Callable

from redipy.graph.cmd import AssignmentObj, CommandObj
from redipy.graph.expr import ArgObj, ExprObj, RefIdObj, VarObj
from redipy.symbolic.expr import Expr, ExprHelper, lit_helper, MixedType


class Compilable:  # pylint: disable=too-few-public-methods
    def compile(self) -> CommandObj:
        raise NotImplementedError()


class CmdHelper(Compilable):  # pylint: disable=too-few-public-methods
    def __init__(self, stmt_fn: Callable[[], CommandObj]) -> None:
        self._stmt_fn = stmt_fn

    def compile(self) -> CommandObj:
        return self._stmt_fn()


class Variable(Expr):
    def __init__(self) -> None:
        self._index: int | None = None

    def len_(self) -> 'Expr':
        return ArrayLen(self)

    def __getitem__(self, other: 'MixedType') -> 'Expr':
        return ArrayAt(self, other)

    def set_index(self, index: int) -> None:
        self._index = index

    def get_index(self) -> int:
        assert self._index is not None
        return self._index

    def prefix(self) -> str:
        raise NotImplementedError()

    def get_declare(self) -> AssignmentObj:
        return {
            "kind": "declare",
            "assign": self.get_ref(),
            "value": self.get_declare_rhs().compile(),
        }

    def get_declare_rhs(self) -> Expr:
        raise NotImplementedError()

    def get_ref(self) -> RefIdObj:
        raise NotImplementedError()

    def compile(self) -> RefIdObj:
        return self.get_ref()

    def assign(self, val: MixedType) -> CmdHelper:
        expr = lit_helper(val)
        return CmdHelper(
            lambda: {
                "kind": "assign",
                "assign": self.get_ref(),
                "value": expr.compile(),
            })

    def set_at(self, index: MixedType, val: MixedType) -> CmdHelper:
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
    def __init__(self, name: str) -> None:
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
