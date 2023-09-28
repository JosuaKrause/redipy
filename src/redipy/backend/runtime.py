import contextlib
import threading
from collections.abc import Callable, Iterator
from typing import Any, Generic, Self, TypeVar

from redipy.api import RedisAPI
from redipy.backend.backend import Backend, ExecFunction
from redipy.graph.seq import SequenceObj
from redipy.symbolic.seq import FnContext


T = TypeVar('T')


class Runtime(Generic[T], RedisAPI):
    def __init__(self) -> None:
        self._backend: Backend[T, Any, Any, Any, Self] | None = None
        self._compile_hook: Callable[[SequenceObj], None] | None = None
        self._code_hook: Callable[[T], None] | None = None
        self._lock = threading.RLock()

    @contextlib.contextmanager
    def lock(self) -> Iterator[None]:
        with self._lock:
            yield

    def set_compile_hook(self, hook: Callable[[SequenceObj], None]) -> None:
        self._compile_hook = hook

    def set_code_hook(self, hook: Callable[[T], None]) -> None:
        self._code_hook = hook

    def register_script(self, ctx: FnContext) -> ExecFunction:
        compiled = ctx.compile()
        if self._compile_hook is not None:
            self._compile_hook(compiled)

        backend = self.get_backend()
        code = backend.translate(compiled)
        if self._code_hook is not None:
            self._code_hook(code)
        return backend.create_executable(code, self)

    def get_backend(self) -> Backend[T, Any, Any, Any, Self]:
        if self._backend is None:
            with self.lock():
                if self._backend is None:
                    self._backend = self.create_backend()
        return self._backend

    @classmethod
    def create_backend(cls) -> Backend[T, Any, Any, Any, Self]:
        raise NotImplementedError()
