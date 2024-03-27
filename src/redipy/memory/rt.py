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
"""The runtime for the memory backend."""
import contextlib
import datetime
import time
from collections.abc import Callable, Iterator
from typing import Any, Literal, overload, TypeVar

from redipy.api import (
    KeyType,
    PipelineAPI,
    REX_ALWAYS,
    RExpireMode,
    RSetMode,
    RSM_ALWAYS,
)
from redipy.backend.runtime import Runtime
from redipy.graph.expr import JSONType
from redipy.memory.local import Cmd, LocalBackend
from redipy.memory.state import Machine, State
from redipy.plugin import add_plugin, LocalGeneralFunction, LocalRedisFunction
from redipy.util import now


T = TypeVar('T')
C = TypeVar('C', bound=Callable)


CONST: dict[str, JSONType] = {
    "redis.LOG_DEBUG": "DEBUG",
    "redis.LOG_VERBOSE": "VERBOSE",
    "redis.LOG_NOTICE": "NOTICE",
    "redis.LOG_WARNING": "WARNING",
}
"""Log severity levels."""


class LocalRuntime(Runtime[Cmd]):
    """The runtime of the memory backend."""
    def __init__(self) -> None:
        super().__init__()
        self._sm = Machine(State())
        self._rfuns: dict[str, LocalRedisFunction] = {}
        self._gfuns: dict[str, LocalGeneralFunction] = {}
        self.add_redis_function_plugin("redipy.memory.rfun")
        self.add_general_function_plugin("redipy.memory.gfun")

    def add_redis_function_plugin(self, module: str) -> None:
        """Adds all redis functions (LocalRedisFunction) defined in the
        given module.

        Args:
            module (str): The module name.
        """
        add_plugin(module, self._rfuns, LocalRedisFunction)

    def add_general_function_plugin(self, module: str) -> None:
        """Adds all general functions (LocalGeneralFunction) defined in the
        given module

        Args:
            module (str): The module name.
        """
        add_plugin(
            module,
            self._gfuns,
            LocalGeneralFunction,
            disallowed={"redis.call"})

    def get_machine(self) -> Machine:
        """
        Returns the internal memory state.

        Returns:
            Machine: The memory state.
        """
        return self._sm

    @classmethod
    def create_backend(cls) -> LocalBackend:
        return LocalBackend()

    @contextlib.contextmanager
    def pipeline(self) -> Iterator[PipelineAPI]:

        def exec_call(execute: Callable[[], list]) -> list:
            with self.lock():
                now_mono = time.monotonic()
                self._sm.set_mono((now_mono, now()))
                res = execute()
                state = pipe.get_state()
                self._sm.get_state().apply(state, now_mono)
                state.reset()
                self._sm.set_mono(None)
                return res

        pipe = LocalPipeline(self, self._sm.get_state(), exec_call)
        yield pipe
        if pipe.has_pending():
            pipe.execute()

    @staticmethod
    def require_argc(
            args: list[JSONType],
            count: int,
            *,
            at_least: bool = False,
            at_most: int | None = None) -> None:
        """
        Confirms that the passed arguments conform to the argument
        specification.

        Args:
            args (list[JSONType]): The arguments to check.

            count (int): The expected number of arguments.

            at_least (bool, optional): If the number of arguments is a minimum.
            Defaults to False.

            at_most (int | None, optional): Defines an upper bound for the
            number of arguments. This argument does not define a minimum
            number of arguments (if at_least is set to True and at_most is
            set to a value the minimum number of arguments is 0).
            Defaults to None.

        Raises:
            ValueError: If the arguments don't conform the specification.
        """
        argc = len(args)
        if argc == count:
            return
        if at_most is not None and argc <= at_most:
            return
        if at_least and argc > count:
            return
        at_most_str = "" if at_most is None else f"up to {at_most} arguments "
        raise ValueError(
            "incorrect number of arguments need "
            f"{'at least' if at_least else 'exactly'} {count} "
            f"{at_most_str}got {argc}")

    def redis_fn(
            self,
            sm: Machine,
            name: str,
            args: list[JSONType]) -> JSONType:
        """
        Calls a redis function. For internal use only.

        Args:
            sm (Machine): The state of the memory runtime.

            name (str): The redis function name.

            args (list[JSONType]): The argument list.

        Raises:
            ValueError: If the function cannot be called.

        Returns:
            JSONType: The result of the function call.
        """
        key = f"{args[0]}"
        rfun = self._rfuns.get(name)
        if rfun is None:
            raise ValueError(f"unknown redis function {name}")
        argc = rfun.argc()
        count = argc["count"] + 1
        at_least = argc.get("at_least", False)
        at_most = argc.get("at_most")
        if at_most is not None:
            at_most += 1
        self.require_argc(args, count, at_least=at_least, at_most=at_most)
        return rfun.call(sm, key, args[1:])

    def call_fn(
            self, sm: Machine, name: str, args: list[JSONType]) -> JSONType:
        """
        Calls a function. For internal use only.

        Args:
            sm (Machine): The state of the memory runtime.

            name (str): The function name.

            args (list[JSONType]): The arguments.

        Raises:
            ValueError: If the function cannot be called.

        Returns:
            JSONType: The result of the function call.
        """
        if name == "redis.call":
            self.require_argc(args, 2, at_least=True)
            return self.redis_fn(sm, f"{args[0]}", args[1:])
        gfun = self._gfuns.get(name)
        if gfun is None:
            raise ValueError(f"unknown function {name}")
        argc = gfun.argc()
        count = argc["count"]
        at_least = argc.get("at_least", False)
        at_most = argc.get("at_most")
        self.require_argc(args, count, at_least=at_least, at_most=at_most)
        return gfun.call(args)

    def get_constant(self, raw: str) -> JSONType:
        """
        Returns a constant value. For internal use only.

        Args:
            raw (str): The name of the constant.

        Returns:
            JSONType: The value.
        """
        return CONST[raw]

    def exists(self, *keys: str) -> int:
        with self.lock():
            return self._sm.exists(*keys)

    def delete(self, *keys: str) -> int:
        with self.lock():
            return self._sm.delete(*keys)

    def key_type(self, key: str) -> KeyType | None:
        with self.lock():
            return self._sm.key_type(key)

    def scan(
            self,
            cursor: int,
            *,
            match: str | None = None,
            count: int | None = None,
            filter_type: KeyType | None = None) -> tuple[int, list[str]]:
        with self.lock():
            return self._sm.scan(
                cursor,
                match=match,
                count=count,
                filter_type=filter_type)

    def keys_block(
            self,
            *,
            match: str | None = None,
            filter_type: KeyType | None = None) -> list[str]:
        with self.lock():
            return self._sm.keys_block(match=match, filter_type=filter_type)

    def flushall(self) -> None:
        with self.lock():
            return self._sm.flushall()

    @overload
    def set_value(
            self,
            key: str,
            value: str,
            *,
            mode: RSetMode,
            return_previous: Literal[True],
            expire_timestamp: datetime.datetime | None,
            expire_in: float | None,
            keep_ttl: bool) -> str | None:
        ...

    @overload
    def set_value(
            self,
            key: str,
            value: str,
            *,
            mode: RSetMode,
            return_previous: Literal[False],
            expire_timestamp: datetime.datetime | None,
            expire_in: float | None,
            keep_ttl: bool) -> bool | None:
        ...

    @overload
    def set_value(
            self,
            key: str,
            value: str,
            *,
            mode: RSetMode = RSM_ALWAYS,
            return_previous: bool = False,
            expire_timestamp: datetime.datetime | None = None,
            expire_in: float | None = None,
            keep_ttl: bool = False) -> str | bool | None:
        ...

    def set_value(
            self,
            key: str,
            value: str,
            *,
            mode: RSetMode = RSM_ALWAYS,
            return_previous: bool = False,
            expire_timestamp: datetime.datetime | None = None,
            expire_in: float | None = None,
            keep_ttl: bool = False) -> str | bool | None:
        with self.lock():
            return self._sm.set_value(
                key,
                value,
                mode=mode,
                return_previous=return_previous,
                expire_timestamp=expire_timestamp,
                expire_in=expire_in,
                keep_ttl=keep_ttl)

    def get_value(self, key: str) -> str | None:
        with self.lock():
            return self._sm.get_value(key)

    def expire(
            self,
            key: str,
            *,
            mode: RExpireMode = REX_ALWAYS,
            expire_timestamp: datetime.datetime | None = None,
            expire_in: float | None = None) -> bool:
        return self._sm.expire(
            key,
            mode=mode,
            expire_timestamp=expire_timestamp,
            expire_in=expire_in)

    def ttl(self, key: str) -> float | None:
        return self._sm.ttl(key)

    def incrby(self, key: str, inc: float | int) -> float:
        with self.lock():
            return self._sm.incrby(key, inc)

    def lpush(self, key: str, *values: str) -> int:
        with self.lock():
            return self._sm.lpush(key, *values)

    def rpush(self, key: str, *values: str) -> int:
        with self.lock():
            return self._sm.rpush(key, *values)

    @overload
    def lpop(
            self,
            key: str,
            count: None = None) -> str | None:
        ...

    @overload
    def lpop(  # pylint: disable=signature-differs
            self,
            key: str,
            count: int) -> list[str] | None:
        ...

    def lpop(
            self,
            key: str,
            count: int | None = None) -> str | list[str] | None:
        with self.lock():
            return self._sm.lpop(key, count)

    @overload
    def rpop(
            self,
            key: str,
            count: None = None) -> str | None:
        ...

    @overload
    def rpop(  # pylint: disable=signature-differs
            self,
            key: str,
            count: int) -> list[str] | None:
        ...

    def rpop(
            self,
            key: str,
            count: int | None = None) -> str | list[str] | None:
        with self.lock():
            return self._sm.rpop(key, count)

    def lrange(self, key: str, start: int, stop: int) -> list[str]:
        with self.lock():
            return self._sm.lrange(key, start, stop)

    def llen(self, key: str) -> int:
        with self.lock():
            return self._sm.llen(key)

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        with self.lock():
            return self._sm.zadd(key, mapping)

    def zpop_max(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        with self.lock():
            return self._sm.zpop_max(key, count)

    def zpop_min(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        with self.lock():
            return self._sm.zpop_min(key, count)

    def zrange(self, key: str, start: int, stop: int) -> list[str]:
        with self.lock():
            return self._sm.zrange(key, start, stop)

    def zcard(self, key: str) -> int:
        with self.lock():
            return self._sm.zcard(key)

    def hset(self, key: str, mapping: dict[str, str]) -> int:
        with self.lock():
            return self._sm.hset(key, mapping)

    def hdel(self, key: str, *fields: str) -> int:
        with self.lock():
            return self._sm.hdel(key, *fields)

    def hget(self, key: str, field: str) -> str | None:
        with self.lock():
            return self._sm.hget(key, field)

    def hmget(self, key: str, *fields: str) -> dict[str, str | None]:
        with self.lock():
            return self._sm.hmget(key, *fields)

    def hincrby(self, key: str, field: str, inc: float | int) -> float:
        with self.lock():
            return self._sm.hincrby(key, field, inc)

    def hkeys(self, key: str) -> list[str]:
        with self.lock():
            return self._sm.hkeys(key)

    def hvals(self, key: str) -> list[str]:
        with self.lock():
            return self._sm.hvals(key)

    def hgetall(self, key: str) -> dict[str, str]:
        with self.lock():
            return self._sm.hgetall(key)

    def sadd(self, key: str, *values: str) -> int:
        with self.lock():
            return self._sm.sadd(key, *values)

    def srem(self, key: str, *values: str) -> int:
        with self.lock():
            return self._sm.srem(key, *values)

    def sismember(self, key: str, value: str) -> bool:
        with self.lock():
            return self._sm.sismember(key, value)

    def scard(self, key: str) -> int:
        with self.lock():
            return self._sm.scard(key)

    def smembers(self, key: str) -> set[str]:
        with self.lock():
            return self._sm.smembers(key)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}[{self._sm.get_state()}]"

    def __repr__(self) -> str:
        return self.__str__()


class LocalPipeline(PipelineAPI):
    """A pipeline for the memory backend runtime."""
    def __init__(
            self,
            rt: LocalRuntime,
            parent: State,
            exec_call: Callable[[Callable[[], list]], list]) -> None:
        """
        Creates a new pipeline. Do not manually create a pipeline. Use the
        `pipeline` function of the runtime instead.

        Args:
            rt (LocalRuntime): The memory backend runtime.

            parent (State): The memory state.

            exec_call (Callable[[Callable[[], list]], list]): Function that is
            called with the results of the pipeline when calling execute. This
            can be used to finalize the results before returning them.
        """
        super().__init__()
        self._rt = rt
        self._sm = Machine(State(parent))
        self._exec_call = exec_call
        self._cmd_queue: list[Callable[[], Any]] = []

    def get_runtime_tuple(self) -> tuple[LocalRuntime, Machine]:
        """
        Returns the access and state of the runtime.

        Returns:
            tuple[LocalRuntime, Machine]: The runtime object and the internal
            memory state of the pipeline.
        """
        return (self._rt, self._sm)

    def get_state(self) -> State:
        """
        Returns the raw access to the memory of the pipeline.

        Returns:
            State: The pipeline memory state.
        """
        return self._sm.get_state()

    def has_pending(self) -> bool:
        """
        Whether there are commands in the pipeline that have not been executed
        yet.

        Returns:
            bool: True if there are unexecuted commands in the pipeline.
        """
        return len(self._cmd_queue) > 0

    def execute(self) -> list:
        cmds = self._cmd_queue
        self._cmd_queue = []

        def executor() -> list:
            return [cmd() for cmd in cmds]

        return self._exec_call(executor)

    def add_cmd(self, cb: Callable[[], Any]) -> None:
        """
        Adds a command to the pipeline.

        Args:
            cb (Callable[[], Any]): The command.
        """
        self._cmd_queue.append(cb)

    def exists(self, *keys: str) -> None:
        self.add_cmd(lambda: self._sm.exists(*keys))

    def delete(self, *keys: str) -> None:
        self.add_cmd(lambda: self._sm.delete(*keys))

    def key_type(self, key: str) -> None:
        self.add_cmd(lambda: self._sm.key_type(key))

    def scan(
            self,
            cursor: int,
            *,
            match: str | None = None,
            count: int | None = None,
            filter_type: KeyType | None = None) -> None:
        self.add_cmd(lambda: self._sm.scan(
            cursor, match=match, count=count, filter_type=filter_type))

    def keys(
            self,
            *,
            match: str | None = None,
            filter_type: KeyType | None = None) -> None:
        self.add_cmd(lambda: sorted(self._sm.keys(
            match=match, filter_type=filter_type)))

    def set_value(
            self,
            key: str,
            value: str,
            *,
            mode: RSetMode = RSM_ALWAYS,
            return_previous: bool = False,
            expire_timestamp: datetime.datetime | None = None,
            expire_in: float | None = None,
            keep_ttl: bool = False) -> None:
        self.add_cmd(lambda: self._sm.set_value(
            key,
            value,
            mode=mode,
            return_previous=return_previous,
            expire_timestamp=expire_timestamp,
            expire_in=expire_in,
            keep_ttl=keep_ttl))

    def get_value(self, key: str) -> None:
        self.add_cmd(lambda: self._sm.get_value(key))

    def expire(
            self,
            key: str,
            *,
            mode: RExpireMode = REX_ALWAYS,
            expire_timestamp: datetime.datetime | None = None,
            expire_in: float | None = None) -> None:
        self.add_cmd(lambda: self._sm.expire(
            key,
            mode=mode,
            expire_timestamp=expire_timestamp,
            expire_in=expire_in))

    def ttl(self, key: str) -> None:
        self.add_cmd(lambda: self._sm.ttl(key))

    def incrby(self, key: str, inc: float | int) -> None:
        self.add_cmd(lambda: self._sm.incrby(key, inc))

    def lpush(self, key: str, *values: str) -> None:
        self.add_cmd(lambda: self._sm.lpush(key, *values))

    def rpush(self, key: str, *values: str) -> None:
        self.add_cmd(lambda: self._sm.rpush(key, *values))

    def lpop(
            self,
            key: str,
            count: int | None = None) -> None:
        self.add_cmd(lambda: self._sm.lpop(key, count))

    def rpop(
            self,
            key: str,
            count: int | None = None) -> None:
        self.add_cmd(lambda: self._sm.rpop(key, count))

    def lrange(self, key: str, start: int, stop: int) -> None:
        self.add_cmd(lambda: self._sm.lrange(key, start, stop))

    def llen(self, key: str) -> None:
        self.add_cmd(lambda: self._sm.llen(key))

    def zadd(self, key: str, mapping: dict[str, float]) -> None:
        lmap = {
            name: float(num)
            for name, num in mapping.items()
        }
        self.add_cmd(lambda: self._sm.zadd(key, lmap))

    def zpop_max(
            self,
            key: str,
            count: int = 1,
            ) -> None:
        self.add_cmd(lambda: self._sm.zpop_max(key, count))

    def zpop_min(
            self,
            key: str,
            count: int = 1,
            ) -> None:
        self.add_cmd(lambda: self._sm.zpop_min(key, count))

    def zrange(self, key: str, start: int, stop: int) -> None:
        self.add_cmd(lambda: self._sm.zrange(key, start, stop))

    def zcard(self, key: str) -> None:
        self.add_cmd(lambda: self._sm.zcard(key))

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        self.add_cmd(lambda: self._sm.hset(key, mapping))

    def hdel(self, key: str, *fields: str) -> None:
        self.add_cmd(lambda: self._sm.hdel(key, *fields))

    def hget(self, key: str, field: str) -> None:
        self.add_cmd(lambda: self._sm.hget(key, field))

    def hmget(self, key: str, *fields: str) -> None:
        self.add_cmd(lambda: self._sm.hmget(key, *fields))

    def hincrby(self, key: str, field: str, inc: float | int) -> None:
        self.add_cmd(lambda: self._sm.hincrby(key, field, inc))

    def hkeys(self, key: str) -> None:
        self.add_cmd(lambda: self._sm.hkeys(key))

    def hvals(self, key: str) -> None:
        self.add_cmd(lambda: self._sm.hvals(key))

    def hgetall(self, key: str) -> None:
        self.add_cmd(lambda: self._sm.hgetall(key))

    def sadd(self, key: str, *values: str) -> None:
        self.add_cmd(lambda: self._sm.sadd(key, *values))

    def srem(self, key: str, *values: str) -> None:
        self.add_cmd(lambda: self._sm.srem(key, *values))

    def sismember(self, key: str, value: str) -> None:
        self.add_cmd(lambda: self._sm.sismember(key, value))

    def scard(self, key: str) -> None:
        self.add_cmd(lambda: self._sm.scard(key))

    def smembers(self, key: str) -> None:
        self.add_cmd(lambda: self._sm.smembers(key))
