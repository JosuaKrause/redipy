from typing import Generic, Protocol, TYPE_CHECKING, TypeVar

from redipy.graph.cmd import CommandObj
from redipy.graph.expr import ExprObj
from redipy.graph.seq import SequenceObj
from redipy.symbolic.expr import JSONType


if TYPE_CHECKING:
    from redipy.backend.runtime import Runtime


T = TypeVar('T')
ET = TypeVar('ET')
CC = TypeVar('CC')
EC = TypeVar('EC')
R = TypeVar('R', bound='Runtime')


class ExecFunction(Protocol):  # pylint: disable=too-few-public-methods
    def __call__(
            self,
            keys: dict[str, str],
            args: dict[str, JSONType]) -> JSONType:
        raise NotImplementedError()


class Backend(Generic[T, ET, CC, EC, R]):
    def translate(self, seq: SequenceObj) -> T:
        ctx = self.create_command_context()
        res = self.compile_sequence(ctx, seq)
        return self.finish(ctx, res)

    def create_command_context(self) -> CC:
        raise NotImplementedError()

    def finish(self, ctx: CC, script: T) -> T:
        raise NotImplementedError()

    def compile_sequence(self, ctx: CC, seq: SequenceObj) -> T:
        raise NotImplementedError()

    def compile_command(self, ctx: CC, cmd: CommandObj) -> T:
        raise NotImplementedError()

    def compile_expr(self, ctx: EC, expr: ExprObj) -> ET:
        raise NotImplementedError()

    def create_executable(
            self,
            code: T,
            runtime: R) -> ExecFunction:
        raise NotImplementedError()
