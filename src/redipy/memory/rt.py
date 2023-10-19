import contextlib
import datetime
import json
from collections.abc import Callable, Iterator
from typing import Any, cast, Literal, overload, TypeVar

from redipy.api import (
    PipelineAPI,
    RSetMode,
    RSM_ALWAYS,
    RSM_EXISTS,
    RSM_MISSING,
)
from redipy.backend.runtime import Runtime
from redipy.memory.local import Cmd, LocalBackend
from redipy.memory.state import Machine, State
from redipy.symbolic.expr import JSONType


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

    @classmethod
    def create_backend(cls) -> LocalBackend:
        return LocalBackend()

    @contextlib.contextmanager
    def pipeline(self) -> Iterator[PipelineAPI]:
        with self.lock():
            pipe = LocalPipeline(self._sm.get_state())
            yield pipe
            if pipe.has_queue():
                raise ValueError(f"unexecuted commands in pipeline {pipe}")
            self._sm.get_state().apply(pipe.get_state())

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

    def _set(self, key: str, value: str, args: list[JSONType]) -> JSONType:
        mode: RSetMode = RSM_ALWAYS
        return_previous = False
        expire_in = None
        keep_ttl = False
        pos = 0
        while pos < len(args):
            arg = f"{args[pos]}".upper()
            if arg == "XX":
                mode = RSM_EXISTS
            elif arg == "NX":
                mode = RSM_MISSING
            elif arg == "GET":
                return_previous = True
            elif arg == "PX":
                pos += 1
                expire_in = float(cast(float, args[pos])) / 1000.0
            elif arg == "KEEPTTL":
                keep_ttl = True
            pos += 1
        return self.set(
            key,
            value,
            mode=mode,
            return_previous=return_previous,
            expire_in=expire_in,
            keep_ttl=keep_ttl)

    def redis_fn(
            self, name: str, args: list[JSONType]) -> JSONType:
        key = f"{args[0]}"
        if name == "set":
            self.require_argc(args, 2, at_least=True)
            return self._set(key, f"{args[1]}", args[2:])
        if name == "get":
            self.require_argc(args, 1)
            return self.get(key)
        if name == "lpush":
            self.require_argc(args, 2, at_least=True)
            return self.lpush(key, *(f"{arg}" for arg in args[1:]))
        if name == "rpush":
            self.require_argc(args, 2, at_least=True)
            return self.rpush(key, *(f"{arg}" for arg in args[1:]))
        if name == "lpop":
            self.require_argc(args, 1, at_most=2)
            return self.lpop(
                key, None if len(args) < 2 else int(cast(int, args[1])))
        if name == "rpop":
            self.require_argc(args, 1, at_most=2)
            return self.rpop(
                key, None if len(args) < 2 else int(cast(int, args[1])))
        if name == "llen":
            self.require_argc(args, 1)
            return self.llen(key)
        if name == "zadd":
            self.require_argc(args, 3)
            return self.zadd(key, {f"{args[2]}": float(cast(float, args[1]))})
        if name == "zpopmax":
            self.require_argc(args, 1, at_most=2)
            return cast(list | None, self.zpop_max(
                key, 1 if len(args) < 2 else int(cast(int, args[1]))))
        if name == "zpopmin":
            self.require_argc(args, 1, at_most=2)
            return cast(list | None, self.zpop_min(
                key, 1 if len(args) < 2 else int(cast(int, args[1]))))
        if name == "zcard":
            self.require_argc(args, 1)
            return self.zcard(key)
        raise ValueError(f"unknown redis function {name}")

    def call_fn(
            self, name: str, args: list[JSONType]) -> JSONType:
        if name == "redis.call":
            self.require_argc(args, 2, at_least=True)
            return self.redis_fn(f"{args[0]}", args[1:])
        if name == "string.find":
            self.require_argc(args, 2, at_most=3)
            found_ix = f"{args[0]}".find(
                f"{args[1]}",
                int(cast(int, args[2])) if len(args) > 2 else None)
            return None if found_ix < 0 else found_ix
        if name == "cjson.decode":
            self.require_argc(args, 1)
            return json.loads(f"{args[0]}")
        if name == "cjson.encode":
            self.require_argc(args, 1)
            return json.dumps(
                f"{args[0]}",
                sort_keys=True,
                indent=None,
                separators=(",", ":"))
        if name == "tonumber":
            self.require_argc(args, 1)
            val = cast(str, args[0])
            try:
                return int(val)
            except (ValueError, TypeError):
                return float(val)
        if name == "tostring":
            self.require_argc(args, 1)
            oval = args[0]
            if isinstance(oval, bool):
                return f"{oval}".lower()
            # TODO dict, list
            if oval is None:
                return "nil"
            return f"{oval}"
        if name == "type":
            self.require_argc(args, 1)
            tmap: dict[type, str] = {
                bool: "boolean",
                dict: "table",
                float: "number",
                int: "number",
                list: "table",
                str: "string",
                type(None): "nil",
            }
            return tmap[type(args[0])]
        if name == "redis.log":
            self.require_argc(args, 2)
            print(f"{args[0]}: {args[1]}")
            return None
        raise ValueError(f"unknown function {name}")

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

    def __str__(self) -> str:
        return f"{self.__class__.__name__}[{self._sm.get_state()}]"

    def __repr__(self) -> str:
        return self.__str__()


class LocalPipeline(PipelineAPI):
    def __init__(self, parent: State) -> None:
        super().__init__()
        self._sm = Machine(State(parent))
        self._cmd_queue: list[Callable[[], Any]] = []

    def get_state(self) -> State:
        return self._sm.get_state()

    def has_queue(self) -> bool:
        return len(self._cmd_queue) > 0

    def execute(self) -> list:
        cmds = self._cmd_queue
        self._cmd_queue = []
        return [cmd() for cmd in cmds]

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
