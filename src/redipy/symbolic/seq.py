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
"""Provides core functionality of sequences."""
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
    """A sequence groups together statements."""
    def __init__(self, ctx: 'FnContext') -> None:
        """
        Creates a sequence. You probably shouldn't instantiate it yourself.

        Args:
            ctx (FnContext): The base script context.
        """
        self._ctx = ctx
        self._seq: list[Compilable] = []

    def compile(self) -> SequenceObj:
        """
        Compiles the sequence..

        Returns:
            SequenceObj: The SequenceObj sequence.
        """
        raise NotImplementedError()

    def add(
            self,
            term: Compilable | Expr) -> None:
        """
        Adds a new statement to the sequence.

        Args:
            term (Compilable | Expr): The statement. Expressions will
            automatically be converted appropriately.
        """
        if isinstance(term, Compilable):
            self._seq.append(term)
        else:
            stmt: StmtObj = {
                "kind": "stmt",
                "expr": term.compile(),
            }
            self._seq.append(CmdHelper(lambda: stmt))

    def is_empty(self) -> bool:
        """
        Whether the sequence is empty.

        Returns:
            bool: If True, the sequence has no elements in it.
        """
        return not self._seq

    def get_cmds(self) -> list[CommandObj]:
        """
        Returns all statements in this sequence.

        Returns:
            list[CommandObj]: The commands.
        """
        return [stmt.compile() for stmt in self._seq]

    def for_(self, array: Expr) -> tuple['Sequence', Variable, Variable]:
        """
        Creates a for loop.

        Returns:
            tuple[Sequence, Variable, Variable]: The first element is the
            sequence of the body of the for loop. The second element is the
            item index variable and the third is the item value variable.
        """
        loop = ForLoop(self._ctx, array)
        self.add(loop)
        return loop.get_loop(), loop.get_index(), loop.get_value()

    def if_(self, condition: MixedType) -> tuple['Sequence', 'Sequence']:
        """
        Creates an if branch.

        Args:
            condition (MixedType): The condition to continue the loop.

        Returns:
            tuple[Sequence, Sequence]: The first sequence is the body of the
            successful branch. The second sequence is the body of the
            unsuccessful branch.
        """
        branch = Branch(self._ctx, condition)
        self.add(branch)
        return branch.get_success(), branch.get_failure()

    def while_(self, condition: MixedType) -> 'Sequence':
        """
        Creates a while loop.

        Args:
            condition (MixedType): The condition.

        Returns:
            Sequence: The body of the loop. It is executed until `condition` is
            False.
        """
        loop = WhileLoop(self._ctx, condition)
        self.add(loop)
        return loop.get_loop()


class Seq(Sequence):
    """A standard sequence that does not add a stack frame."""
    def compile(self) -> SequenceObj:
        return {
            "kind": "seq",
            "cmds": self.get_cmds(),
        }


class FnContext(Sequence):
    """The base context for a script. Arguments, statements, and the return
    value are set through this class."""
    def __init__(self) -> None:
        """The base context for a script. Arguments, statements, and the return
        value are set through this class."""
        super().__init__(self)
        self._args: list[tuple[str, JSONArg]] = []
        self._keys: list[tuple[str, KeyVariable]] = []
        self._anames: set[str] = set()
        self._knames: set[str] = set()
        self._locals: list[LocalVariable] = []
        self._loops: int = 0

    def add_arg(self, name: str) -> JSONArg:
        """
        Adds a value / JSON argument with the given name.

        Args:
            name (str): The name.

        Raises:
            ValueError: If the name already exists.

        Returns:
            JSONArg: The argument reference.
        """
        if name in self._anames:
            raise ValueError(f"ambiguous arg name: {name}")
        arg = JSONArg(name)
        arg.set_index(len(self._args))
        self._args.append((name, arg))
        self._anames.add(name)
        self.add(CmdHelper(arg.get_declare))
        return arg

    def add_key(self, name: str) -> KeyVariable:
        """
        Adds a key argument with the given name.

        Args:
            name (str): The name.

        Raises:
            ValueError: If the name already exists.

        Returns:
            KeyVariable: The argument reference.
        """
        if name in self._knames:
            raise ValueError(f"ambiguous key name: {name}")
        key = KeyVariable(name)
        key.set_index(len(self._keys))
        self._keys.append((name, key))
        self._knames.add(name)
        self.add(CmdHelper(key.get_declare))
        return key

    def add_local(self, init: MixedType) -> LocalVariable:
        """
        Adds a local variable.

        Args:
            init (MixedType): The initial value of the variable.

        Returns:
            LocalVariable: The reference to the variable.
        """
        local = LocalVariable(init)
        local.set_index(len(self._locals))
        self._locals.append(local)
        self.add(CmdHelper(local.get_declare))
        return local

    def add_loop(self) -> int:
        """
        Adds a loop. For internal use only.

        Returns:
            int: The loop number.
        """
        loop = self._loops
        self._loops += 1
        return loop

    def set_return_value(self, value: MixedType) -> None:
        """
        Sets the return value of the script.

        Args:
            value (MixedType): The return value.
        """
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
    """An if / else block."""
    def __init__(self, ctx: FnContext, condition: MixedType) -> None:
        """
        Creates a branch.

        Args:
            ctx (FnContext): The script context.
            condition (MixedType): The branch condition.
        """
        self._condition = lit_helper(condition)
        self._success = Seq(ctx)
        self._failure = Seq(ctx)

    def get_success(self) -> Sequence:
        """
        The success branch.

        Returns:
            Sequence: The command sequence if the condition is successful.
        """
        return self._success

    def get_failure(self) -> Sequence:
        """
        The failure branch.

        Returns:
            Sequence: The command sequence if the condition failed.
        """
        return self._failure

    def compile(self) -> CommandObj:
        return {
            "kind": "branch",
            "condition": self._condition.compile(),
            "then": self._success.compile(),
            "else": self._failure.compile(),
        }


class IndexVariable(Variable):
    """A variable referencing the array index in a for loop."""
    def get_declare_rhs(self) -> Expr:
        raise RuntimeError("must be used in for loop")

    def prefix(self) -> str:
        return "ix"

    def get_index_ref(self) -> IndexObj:
        """
        Returns a reference to the index.

        Returns:
            IndexObj: The index reference.
        """
        return {
            "kind": "index",
            "name": f"{self.prefix()}_{self.get_index()}",
        }

    def get_ref(self) -> RefIdObj:
        return self.get_index_ref()


class ValueVariable(Variable):
    """A variable referencing the array element value in a for loop."""
    def get_declare_rhs(self) -> Expr:
        raise RuntimeError("must be used in for loop")

    def prefix(self) -> str:
        return "val"

    def get_var_ref(self) -> VarObj:
        """
        Returns a reference to the element value.

        Returns:
            VarObj: The element value reference.
        """
        return {
            "kind": "var",
            "name": f"{self.prefix()}_{self.get_index()}",
        }

    def get_ref(self) -> RefIdObj:
        return self.get_var_ref()


class ForLoop(Compilable):
    """A for loop over an array."""
    def __init__(self, ctx: FnContext, array: Expr) -> None:
        """
        Creates a for loop over the contents of an array.

        Args:
            ctx (FnContext): The script context.
            array (Expr): The array.
        """
        loop_ix = ctx.add_loop()
        self._ix = IndexVariable()
        self._ix.set_index(loop_ix)
        self._val = ValueVariable()
        self._val.set_index(loop_ix)
        self._loop = Seq(ctx)
        self._array = array

    def get_index(self) -> Variable:
        """
        The index variable.

        Returns:
            Variable: The index variable.
        """
        return self._ix

    def get_value(self) -> Variable:
        """
        The element value variable.

        Returns:
            Variable: The element value variable.
        """
        return self._val

    def get_loop(self) -> Sequence:
        """
        The loop block.

        Returns:
            Sequence: The loop block.
        """
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
    """A while loop."""
    def __init__(self, ctx: FnContext, condition: MixedType) -> None:
        """
        Creates a loop the repeats while a condition is met.

        Args:
            ctx (FnContext): The script context.
            condition (MixedType): The condition.
        """
        self._condition = lit_helper(condition)
        self._loop = Seq(ctx)

    def get_loop(self) -> Sequence:
        """The loop body."""
        return self._loop

    def compile(self) -> CommandObj:
        return {
            "kind": "while",
            "condition": self._condition.compile(),
            "body": self._loop.compile(),
        }
