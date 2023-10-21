import contextlib
import datetime
from collections.abc import Callable, Iterator
from typing import Any, Literal, overload, TypeVar

from redipy.api import PipelineAPI, RSetMode, RSM_ALWAYS
from redipy.backend.runtime import Runtime
from redipy.graph.expr import JSONType
from redipy.memory.local import Cmd, LocalBackend
from redipy.memory.state import Machine, State
from redipy.plugin import add_plugin, LocalGeneralFunction, LocalRedisFunction


T = TypeVar('T')
C = TypeVar('C', bound=Callable)


CONST: dict[str, JSONType] = {
    "redis.LOG_DEBUG": "DEBUG",
    "redis.LOG_VERBOSE": "VERBOSE",
    "redis.LOG_NOTICE": "NOTICE",
    "redis.LOG_WARNING": "WARNING",
}


class LocalRuntime(Runtime[Cmd]):
    def __init__(self) -> None:
        super().__init__()
        self._sm = Machine(State())
        self._rfuns: dict[str, LocalRedisFunction] = {}
        self._gfuns: dict[str, LocalGeneralFunction] = {}
        self.add_redis_function_plugin("redipy.memory.rfun")
        self.add_general_function_plugin("redipy.memory.gfun")

    def add_redis_function_plugin(self, module: str) -> None:
        add_plugin(module, self._rfuns, LocalRedisFunction)

    def add_general_function_plugin(self, module: str) -> None:
        add_plugin(
            module,
            self._gfuns,
            LocalGeneralFunction,
            disallowed={"redis.call"})

    @classmethod
    def create_backend(cls) -> LocalBackend:
        return LocalBackend()

    @contextlib.contextmanager
    def pipeline(self) -> Iterator[PipelineAPI]:

        def exec_call(execute: Callable[[], list]) -> list:
            with self.lock():
                res = execute()
                state = pipe.get_state()
                self._sm.get_state().apply(state)
                state.reset()
                return res

        pipe = LocalPipeline(
            self._sm.get_state(), exec_call)
        yield pipe
        if pipe.has_queue():
            raise ValueError(f"unexecuted commands in pipeline {pipe}")

    @staticmethod
    def require_argc(
            args: list[JSONType],
            count: int,
            *,
            at_least: bool = False,
            at_most: int | None = None) -> None:
        argc = len(args)
        if argc == count:
            return
        if at_most is not None and argc <= at_most:
            return
        if at_least and argc > count:
            return
        raise ValueError(
            "incorrect number of arguments need "
            f"{'at least' if at_least else 'exactly'} {count} got {argc}")

    def redis_fn(
            self, name: str, args: list[JSONType]) -> JSONType:
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
        return rfun.call(self._sm, key, args[1:])

    def call_fn(
            self, name: str, args: list[JSONType]) -> JSONType:
        if name == "redis.call":
            self.require_argc(args, 2, at_least=True)
            return self.redis_fn(f"{args[0]}", args[1:])
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
        return CONST[raw]

    @overload
    def set(
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
    def set(
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
    def set(
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

    def set(
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
            return self._sm.set(
                key,
                value,
                mode=mode,
                return_previous=return_previous,
                expire_timestamp=expire_timestamp,
                expire_in=expire_in,
                keep_ttl=keep_ttl)

    def get(self, key: str) -> str | None:
        with self.lock():
            return self._sm.get(key)

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

    def zcard(self, key: str) -> int:
        with self.lock():
            return self._sm.zcard(key)

    def incrby(self, key: str, inc: float | int) -> float:
        with self.lock():
            return self._sm.incrby(key, inc)

    def exists(self, *keys: str) -> int:
        with self.lock():
            return self._sm.exists(*keys)

    def delete(self, *keys: str) -> int:
        with self.lock():
            return self._sm.delete(*keys)

    def hset(self, key: str, mapping: dict[str, str]) -> int:
        with self.lock():
            return self.hset(key, mapping)

    def hdel(self, key: str, *fields: str) -> int:
        with self.lock():
            return self.hdel(key, *fields)

    def hget(self, key: str, field: str) -> str | None:
        with self.lock():
            return self.hget(key, field)

    def hmget(self, key: str, *fields: str) -> dict[str, str | None]:
        with self.lock():
            return self.hmget(key, *fields)

    def hincrby(self, key: str, field: str, inc: float | int) -> float:
        with self.lock():
            return self.hincrby(key, field, inc)

    def hkeys(self, key: str) -> list[str]:
        with self.lock():
            return self.hkeys(key)

    def hvals(self, key: str) -> list[str]:
        with self.lock():
            return self.hvals(key)

    def hgetall(self, key: str) -> dict[str, str]:
        with self.lock():
            return self.hgetall(key)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}[{self._sm.get_state()}]"

    def __repr__(self) -> str:
        return self.__str__()


class LocalPipeline(PipelineAPI):
    def __init__(
            self,
            parent: State,
            exec_call: Callable[[Callable[[], list]], list]) -> None:
        super().__init__()
        self._sm = Machine(State(parent))
        self._exec_call = exec_call
        self._cmd_queue: list[Callable[[], Any]] = []

    def get_state(self) -> State:
        return self._sm.get_state()

    def has_queue(self) -> bool:
        return len(self._cmd_queue) > 0

    def execute(self) -> list:
        cmds = self._cmd_queue
        self._cmd_queue = []

        def executor() -> list:
            return [cmd() for cmd in cmds]

        return self._exec_call(executor)

    def add_cmd(self, cb: Callable[[], Any]) -> None:
        self._cmd_queue.append(cb)

    def set(
            self,
            key: str,
            value: str,
            *,
            mode: RSetMode = RSM_ALWAYS,
            return_previous: bool = False,
            expire_timestamp: datetime.datetime | None = None,
            expire_in: float | None = None,
            keep_ttl: bool = False) -> None:
        self.add_cmd(lambda: self._sm.set(
            key,
            value,
            mode=mode,
            return_previous=return_previous,
            expire_timestamp=expire_timestamp,
            expire_in=expire_in,
            keep_ttl=keep_ttl))

    def get(self, key: str) -> None:
        self.add_cmd(lambda: self._sm.get(key))

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

    def zcard(self, key: str) -> None:
        self.add_cmd(lambda: self._sm.zcard(key))

    def incrby(self, key: str, inc: float | int) -> None:
        self.add_cmd(lambda: self._sm.incrby(key, inc))

    def exists(self, *keys: str) -> None:
        self.add_cmd(lambda: self._sm.exists(*keys))

    def delete(self, *keys: str) -> None:
        self.add_cmd(lambda: self._sm.delete(*keys))

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        self.add_cmd(lambda: self.hset(key, mapping))

    def hdel(self, key: str, *fields: str) -> None:
        self.add_cmd(lambda: self.hdel(key, *fields))

    def hget(self, key: str, field: str) -> None:
        self.add_cmd(lambda: self.hget(key, field))

    def hmget(self, key: str, *fields: str) -> None:
        self.add_cmd(lambda: self.hmget(key, *fields))

    def hincrby(self, key: str, field: str, inc: float | int) -> None:
        self.add_cmd(lambda: self.hincrby(key, field, inc))

    def hkeys(self, key: str) -> None:
        self.add_cmd(lambda: self.hkeys(key))

    def hvals(self, key: str) -> None:
        self.add_cmd(lambda: self.hvals(key))

    def hgetall(self, key: str) -> None:
        self.add_cmd(lambda: self.hgetall(key))
