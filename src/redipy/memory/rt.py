import collections
import contextlib
import json
from collections.abc import Callable, Iterator
from typing import Any, cast, overload, TypeVar

from redipy.api import RedisAPI
from redipy.backend.backend import ExecFunction
from redipy.backend.runtime import Runtime
from redipy.memory.local import Cmd, LocalBackend
from redipy.symbolic.expr import JSONType
from redipy.symbolic.seq import FnContext


T = TypeVar('T')
C = TypeVar('C', bound=Callable)


CONST: dict[str, JSONType] = {
    "redis.LOG_DEBUG": "DEBUG",
    "redis.LOG_VERBOSE": "VERBOSE",
    "redis.LOG_NOTICE": "NOTICE",
    "redis.LOG_WARNING": "WARNING",
}


def pipey(fun: C) -> C:
    # FIXME find a way that doesn't mess up the types

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        res = fun(*args, **kwargs)
        pipeline = args[0]._pipeline  # pylint: disable=protected-access
        if pipeline is not None:
            pipeline.append(res)
        return res

    return wrapper  # type: ignore


class LocalRuntime(Runtime[Cmd]):
    def __init__(self) -> None:
        super().__init__()
        self._vals: dict[str, str] = {}
        self._queues: collections.defaultdict[str, collections.deque[str]] = \
            collections.defaultdict(collections.deque)
        # TODO: to be replaced by something more efficient later
        self._zorder: collections.defaultdict[str, list[str]] = \
            collections.defaultdict(list)
        self._zscores: collections.defaultdict[str, dict[str, float]] = \
            collections.defaultdict(dict)
        self._pipeline: list | None = None

    @classmethod
    def create_backend(cls) -> LocalBackend:
        return LocalBackend()

    def register_script(self, ctx: FnContext) -> ExecFunction:
        return pipey(super().register_script(ctx))

    @contextlib.contextmanager
    def pipeline(self) -> Iterator[RedisAPI]:
        with self.lock():
            # FIXME create snapshot to be able to rollback on error?
            try:
                self._pipeline = []
                yield self
            finally:
                self._pipeline = None

    def execute(self) -> list:  # FIXME properly move into own class
        pres = self._pipeline
        if pres is None:
            raise ValueError("not in active pipeline!")
        self._pipeline = []
        return pres

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
        if name == "set":
            self.require_argc(args, 2)
            return self.set(key, f"{args[1]}")
        if name == "setnx":
            # TODO move up and just redirect
            self.require_argc(args, 2)
            if self._vals.get(key) is not None:
                return 0
            self._vals[key] = f"{args[1]}"
            return 1
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
            self.require_argc(args, 2)
            return self.zadd(key, {f"{args[1]}": float(cast(float, args[2]))})
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
        raise ValueError(f"unknow redis function {name}")

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
            return f"{args[0]}"
        if name == "redis.log":
            self.require_argc(args, 2)
            print(f"{args[0]}: {args[1]}")
            return None
        raise ValueError(f"unknown function {name}")

    def get_constant(self, raw: str) -> JSONType:
        return CONST[raw]

    @pipey
    def set(self, key: str, value: str) -> str:
        with self.lock():
            # TODO implement rest of arguments https://redis.io/commands/set/
            self._vals[key] = value
            return "OK"

    @pipey
    def get(self, key: str) -> str | None:
        with self.lock():
            return self._vals.get(key)

    @pipey
    def lpush(self, key: str, *values: str) -> int:
        with self.lock():
            queue = self._queues[key]
            queue.extendleft(values)
            return len(queue)

    @pipey
    def rpush(self, key: str, *values: str) -> int:
        with self.lock():
            queue = self._queues[key]
            queue.extend(values)
            return len(queue)

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

    @pipey
    def lpop(
            self,
            key: str,
            count: int | None = None) -> str | list[str] | None:
        with self.lock():
            queue = self._queues[key]
            if not queue:
                return None
            if count is None:
                return queue.popleft()
            popc = count
            res = []
            while popc > 0 and queue:
                res.append(queue.popleft())
                popc -= 1
            return res if res else None

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

    @pipey
    def rpop(
            self,
            key: str,
            count: int | None = None) -> str | list[str] | None:
        with self.lock():
            queue = self._queues[key]
            if not queue:
                return None
            if count is None:
                return queue.pop()
            popc = count
            res = []
            while popc > 0 and queue:
                res.append(queue.pop())
                popc -= 1
            return res if res else None

    @pipey
    def llen(self, key: str) -> int:
        with self.lock():
            mqueue = self._queues.get(key)
            return 0 if mqueue is None else len(mqueue)

    @pipey
    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        with self.lock():
            count = 0
            zscores = self._zscores[key]
            zorder = self._zorder[key]
            for name, score in mapping.items():
                if name not in zscores:
                    zorder.append(name)
                    count += 1
                zscores[name] = score
            zorder.sort(key=lambda k: (zscores[k], k))
            return count

    @pipey
    def zpop_max(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        with self.lock():
            zscores = self._zscores[key]
            zorder = self._zorder[key]
            res = []
            remain = 1 if count is None else count
            while remain > 0 and zorder:
                name = zorder.pop()
                score = zscores.pop(name)
                res.append((name, score))
                remain -= 1
            return res

    @pipey
    def zpop_min(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        with self.lock():
            zscores = self._zscores[key]
            zorder = self._zorder[key]
            res = []
            remain = 1 if count is None else count
            while remain > 0 and zorder:
                name = zorder.pop(0)
                score = zscores.pop(name)
                res.append((name, score))
                remain -= 1
            return res

    @pipey
    def zcard(self, key: str) -> int:
        with self.lock():
            return len(self._zorder[key])

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}["
            f"vals={self._vals},"
            f"queues={dict(self._queues)},"
            f"zorder={dict(self._zorder)},"
            f"zscores={dict(self._zscores)}]")

    def __repr__(self) -> str:
        return self.__str__()
