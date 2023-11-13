from redipy.graph.cmd import CommandObj, StmtObj
from redipy.graph.expr import IndexObj, RefIdObj, VarObj
from redipy.graph.seq import SequenceObj
from redipy.symbolic.core import (
    CmdHelper,
    Compilable,
    JSONArg,
    KeyVariable,
    LocalVariable,
    Variable,
)
from redipy.symbolic.expr import Expr, lit_helper, MixedType


class Sequence:
    def __init__(self, ctx: 'FnContext') -> None:
        self._ctx = ctx
        self._seq: list[Compilable] = []

    def add(
            self,
            term: Compilable | Expr) -> None:
        if isinstance(term, Compilable):
            self._seq.append(term)
        else:
            stmt: StmtObj = {
                "kind": "stmt",
                "expr": term.compile(),
            }
            self._seq.append(CmdHelper(lambda: stmt))

    def is_empty(self) -> bool:
        return not self._seq

    def get_cmds(self) -> list[CommandObj]:
        return [stmt.compile() for stmt in self._seq]

    def for_(self, array: Expr) -> tuple['Sequence', Variable, Variable]:
        loop = ForLoop(self._ctx, array)
        self.add(loop)
        return loop.get_loop(), loop.get_index(), loop.get_value()

    def if_(self, condition: MixedType) -> tuple['Sequence', 'Sequence']:
        branch = Branch(self._ctx, condition)
        self.add(branch)
        return branch.get_success(), branch.get_failure()

    def while_(self, condition: MixedType) -> 'Sequence':
        loop = WhileLoop(self._ctx, condition)
        self.add(loop)
        return loop.get_loop()


class Seq(Sequence):
    def compile(self) -> SequenceObj:
        return {
            "kind": "seq",
            "cmds": self.get_cmds(),
        }


class FnContext(Sequence):
    def __init__(self) -> None:
        super().__init__(self)
        self._args: list[tuple[str, JSONArg]] = []
        self._keys: list[tuple[str, KeyVariable]] = []
        self._anames: set[str] = set()
        self._knames: set[str] = set()
        self._locals: list[LocalVariable] = []
        self._loops: int = 0

    def add_arg(self, name: str) -> JSONArg:
        if name in self._anames:
            raise ValueError(f"ambiguous arg name: {name}")
        arg = JSONArg(name)
        arg.set_index(len(self._args))
        self._args.append((name, arg))
        self._anames.add(name)
        self.add(CmdHelper(arg.get_declare))
        return arg

    def add_key(self, name: str) -> KeyVariable:
        if name in self._knames:
            raise ValueError(f"ambiguous key name: {name}")
        key = KeyVariable(name)
        key.set_index(len(self._keys))
        self._keys.append((name, key))
        self._knames.add(name)
        self.add(CmdHelper(key.get_declare))
        return key

    def add_local(self, init: MixedType) -> LocalVariable:
        local = LocalVariable(init)
        local.set_index(len(self._locals))
        self._locals.append(local)
        self.add(CmdHelper(local.get_declare))
        return local

    def add_loop(self) -> int:
        loop = self._loops
        self._loops += 1
        return loop

    def set_return_value(self, value: MixedType) -> None:
        if value is None:
            self.add(CmdHelper(lambda: {
                "kind": "return",
                "value": None,
            }))
            return
        expr = lit_helper(value)
        self.add(CmdHelper(lambda: {
            "kind": "return",
            "value": expr.compile(),
        }))

    def compile(self) -> SequenceObj:
        return {
            "kind": "script",
            "cmds": self.get_cmds(),
            "argv": [arg for arg, _ in self._args],
            "keyv": [key for key, _ in self._keys],
        }


class Branch(Compilable):
    def __init__(self, ctx: FnContext, condition: MixedType) -> None:
        self._condition = lit_helper(condition)
        self._success = Seq(ctx)
        self._failure = Seq(ctx)

    def get_success(self) -> Sequence:
        return self._success

    def get_failure(self) -> Sequence:
        return self._failure

    def compile(self) -> CommandObj:
        return {
            "kind": "branch",
            "condition": self._condition.compile(),
            "then": self._success.compile(),
            "else": self._failure.compile(),
        }


class IndexVariable(Variable):
    def get_declare_rhs(self) -> Expr:
        raise RuntimeError("must be used in for loop")

    def prefix(self) -> str:
        return "ix"

    def get_index_ref(self) -> IndexObj:
        return {
            "kind": "index",
            "name": f"{self.prefix()}_{self.get_index()}",
        }

    def get_ref(self) -> RefIdObj:
        return self.get_index_ref()


class ValueVariable(Variable):
    def get_declare_rhs(self) -> Expr:
        raise RuntimeError("must be used in for loop")

    def prefix(self) -> str:
        return "val"

    def get_var_ref(self) -> VarObj:
        return {
            "kind": "var",
            "name": f"{self.prefix()}_{self.get_index()}",
        }

    def get_ref(self) -> RefIdObj:
        return self.get_var_ref()


class ForLoop(Compilable):
    def __init__(self, ctx: FnContext, array: Expr) -> None:
        loop_ix = ctx.add_loop()
        self._ix = IndexVariable()
        self._ix.set_index(loop_ix)
        self._val = ValueVariable()
        self._val.set_index(loop_ix)
        self._loop = Seq(ctx)
        self._array = array

    def get_index(self) -> Variable:
        return self._ix

    def get_value(self) -> Variable:
        return self._val

    def get_loop(self) -> Sequence:
        return self._loop

    def compile(self) -> CommandObj:
        return {
            "kind": "for",
            "array": self._array.compile(),
            "index": self._ix.get_index_ref(),
            "value": self._val.get_var_ref(),
            "body": self._loop.compile(),
        }


class WhileLoop(Compilable):
    def __init__(self, ctx: FnContext, condition: MixedType) -> None:
        self._condition = lit_helper(condition)
        self._loop = Seq(ctx)

    def get_loop(self) -> Sequence:
        return self._loop

    def compile(self) -> CommandObj:
        return {
            "kind": "while",
            "condition": self._condition.compile(),
            "body": self._loop.compile(),
        }
