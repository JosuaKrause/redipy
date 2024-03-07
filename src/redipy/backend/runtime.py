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
"""This module defines the base runtime for the different backends."""
import contextlib
import threading
from collections.abc import Callable, Iterator
from typing import Any, Generic, Self, TypeVar

from redipy.api import RedisClientAPI
from redipy.backend.backend import Backend, ExecFunction
from redipy.graph.seq import SequenceObj
from redipy.symbolic.seq import FnContext


T = TypeVar('T')


class Runtime(Generic[T], RedisClientAPI):
    """
    The base class for the different backends.
    """
    def __init__(self) -> None:
        self._backend: Backend[T, Any, Any, Any, Self] | None = None
        self._compile_hook: Callable[[SequenceObj], None] | None = None
        self._code_hook: Callable[[T], None] | None = None
        self._lock = threading.RLock()

    @contextlib.contextmanager
    def lock(self) -> Iterator[None]:
        """
        Enters this runtime's exclusive area. The lock is used to safely create
        a script backend.

        Yields:
            None: Inside the block the lock is held.
        """
        with self._lock:
            yield

    def set_compile_hook(
            self, hook: Callable[[SequenceObj], None] | None) -> None:
        """
        Sets the compile hook that is called after compiling a script.

        Args:
            hook (Callable[[SequenceObj], None] | None): A function that takes
            the root sequence object as argument.
        """
        self._compile_hook = hook

    def set_code_hook(self, hook: Callable[[T], None] | None) -> None:
        """
        Sets the code hook that is called after compiling a script.

        Args:
            hook (Callable[[T], None] | None): A function that takes the
            final internal representation of the script as argument.
        """
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
        """
        Returns the script backend. Every runtime has exactly one script
        backend. The backend is lazily generated using create_backend.

        Returns:
            Backend[T, Any, Any, Any, Self]: _description_
        """
        if self._backend is None:
            with self.lock():
                if self._backend is None:
                    self._backend = self.create_backend()
                else:
                    pass  # pragma: no cover
        return self._backend

    @classmethod
    def create_backend(cls) -> Backend[T, Any, Any, Any, Self]:
        """
        Creates the script backend. Do not call this function directly.
        Call get_backend instead.

        Returns:
            Backend[T, Any, Any, Any, Self]: The new script backend.
        """
        raise NotImplementedError()
