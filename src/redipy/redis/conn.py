import contextlib
import threading
import uuid
from collections.abc import Callable, Iterable, Iterator
from contextlib import AbstractContextManager
from typing import Any, overload, Protocol, TypedDict

from redis import Redis
from redis.commands.core import Script
from redis.exceptions import ResponseError

from redipy.backend.runtime import Runtime
from redipy.redis.lua import LuaBackend
from redipy.util import is_test


RedisConfig = TypedDict('RedisConfig', {
    "host": str,
    "port": int,
    "passwd": str,
    "prefix": str,
    "path": str,
})


class RedisFactory(Protocol):  # pylint: disable=too-few-public-methods
    def __call__(self, *, cfg: RedisConfig) -> Redis:
        ...


class RedisFunctionBytes(Protocol):  # pylint: disable=too-few-public-methods
    def __call__(
            self,
            *,
            keys: list[str],
            args: list[Any],
            client: Redis | None) -> bytes:
        ...


CONCURRENT_MODULE_CONN: int = 17


class RedisWrapper:
    def __init__(
            self,
            *,
            cfg: RedisConfig,
            redis_factory: RedisFactory | None = None,
            is_caching_enabled: bool = True) -> None:
        self._redis_factory: RedisFactory = (
            self._create_connection
            if redis_factory is None else redis_factory)
        self._cfg = cfg
        self._is_caching_enabled = is_caching_enabled
        self._service_conn: list[Redis | None] = \
            [None] * CONCURRENT_MODULE_CONN
        self._lock = threading.RLock()

    @staticmethod
    def get_connection_index() -> int:
        return threading.get_ident() % CONCURRENT_MODULE_CONN

    @staticmethod
    def _create_connection(*, cfg: RedisConfig) -> Redis:
        return Redis(
            host=cfg["host"],
            port=cfg["port"],
            db=0,
            password=cfg["passwd"],
            retry_on_timeout=True,
            health_check_interval=45,
            client_name=f"rc-{uuid.uuid4().hex}")

    def _get_redis_cached_conn(self) -> Redis:
        if not self._is_caching_enabled:
            return self._redis_factory(cfg=self._cfg)

        ix = self.get_connection_index()
        res = self._service_conn[ix]
        if res is None:
            with self._lock:
                res = self._service_conn[ix]
                if res is None:
                    res = self._redis_factory(cfg=self._cfg)
                    self._service_conn[ix] = res
        return res

    @contextlib.contextmanager
    def get_connection(self) -> Iterator[Redis]:
        try:
            yield self._get_redis_cached_conn()
        except Exception:
            self.reset()
            raise

    def reset(self) -> None:
        with self._lock:
            for ix, _ in enumerate(self._service_conn):
                self._service_conn[ix] = None


class RedisConnection(Runtime[list[str]]):
    def __init__(
            self,
            redis_module: str,
            *,
            cfg: RedisConfig,
            redis_factory: RedisFactory | None = None,
            is_caching_enabled: bool = True) -> None:
        super().__init__()
        self._conn = RedisWrapper(
            cfg=cfg,
            redis_factory=redis_factory,
            is_caching_enabled=is_caching_enabled)
        prefix_str = f"{cfg['prefix']}:" if cfg["prefix"] else ""
        module = f"{prefix_str}{redis_module}".rstrip(":")
        self._module = f"{module}:" if module else ""

    @classmethod
    def create_backend(cls) -> LuaBackend:
        return LuaBackend()

    def get_connection(self) -> AbstractContextManager[Redis]:
        return self._conn.get_connection()

    def get_dynamic_script(self, code: str) -> RedisFunctionBytes:
        if is_test():
            print(
                "Compiled script:\n-- SCRIPT START --\n"
                f"{code.rstrip()}\n-- SCRIPT END --")
        compute = Script(None, code.encode("utf-8"))
        context = 3

        def get_error(err_msg: str) -> tuple[str, list[str]] | None:
            ustr = "user_script:"
            ix = err_msg.find(ustr)
            if ix < 0:
                return None
            eix = err_msg.find(":", ix + len(ustr))
            if eix < 0:
                return None
            num = int(err_msg[ix + len(ustr):eix])
            rel_line = num

            new_msg = f"{err_msg[:ix + len(ustr)]}{rel_line}{err_msg[eix:]}"
            ctx = [""] * context + code.splitlines()
            return new_msg, ctx[num - 1:num + 2 * context]

        @contextlib.contextmanager
        def get_client(client: Redis | None) -> Iterator[Redis]:
            try:
                if client is None:
                    with self.get_connection() as res:
                        yield res
                else:
                    yield client
            except ResponseError as e:
                handle_err(e)
                raise e

        def handle_err(exc: ResponseError) -> None:
            if exc.args:
                msg = exc.args[0]
                res = get_error(msg)
                if res is not None:
                    ctx = "\n".join((
                        f"{'>' if ix == context else ' '} {line}"
                        for (ix, line) in enumerate(res[1])))
                    exc.add_note(
                        f"{res[0].rstrip()}\nCode:\n{code}\n\nContext:\n{ctx}")

        def execute_bytes_result(
                *,
                keys: list[str],
                args: list[bytes | str | int],
                client: Redis | None) -> bytes:
            with get_client(client) as inner:
                return compute(keys=keys, args=args, client=inner)

        return execute_bytes_result

    def get_prefix(self) -> str:
        return self._module

    def with_prefix(self, key: str) -> str:
        return f"{self.get_prefix()}{key}"

    def get_pubsub_key(self, key: str) -> str:
        return f"{self.get_prefix()}ps:{key}"

    def wait_for(
            self,
            key: str,
            predicate: Callable[[], bool],
            granularity: float = 30.0) -> None:
        if predicate():
            return
        with self.get_connection() as conn:
            with conn.pubsub() as psub:
                psub.subscribe(self.get_pubsub_key(key))
                try:
                    while not predicate():
                        psub.get_message(
                            ignore_subscribe_messages=True,
                            timeout=granularity)
                        while psub.get_message() is not None:  # flushing queue
                            pass
                finally:
                    psub.unsubscribe()

    def notify_all(self, key: str) -> None:
        with self.get_connection() as conn:
            conn.publish(self.get_pubsub_key(key), "notify")

    def ping(self) -> None:
        with self.get_connection() as conn:
            conn.ping()

    def flush_all(self) -> None:
        with self.get_connection() as conn:
            conn.flushall()

    def keys_count(self, prefix: str) -> int:
        full_prefix = f"{prefix}*"
        vals: set[bytes] = set()
        cursor = 0
        count = 10
        with self.get_connection() as conn:
            while True:
                cursor, res = conn.scan(cursor, full_prefix, count)
                vals.update(res)
                if cursor == 0:
                    break
                if count < 4000:
                    count = min(4000, count * 2)
        return len(vals)

    def keys_str(
            self, prefix: str, postfix: str | None = None) -> Iterable[str]:
        full_prefix = f"{prefix}*{'' if postfix is None else postfix}"
        vals: set[bytes] = set()
        cursor = 0
        count = 10
        with self.get_connection() as conn:
            while True:
                cursor, res = conn.scan(cursor, full_prefix, count)
                vals.update(res)
                if cursor == 0:
                    break
                if count < 1000:
                    count = int(min(1000, count * 1.2))
        return (val.decode("utf-8") for val in vals)

    def prefix_exists(
            self, prefix: str, postfix: str | None = None) -> bool:
        full_prefix = f"{prefix}*{'' if postfix is None else postfix}"
        cursor = 0
        count = 10
        with self.get_connection() as conn:
            while True:
                cursor, res = conn.scan(cursor, full_prefix, count)
                if res:
                    return True
                if cursor == 0:
                    return False
                if count < 1000:
                    count = int(min(1000, count * 1.2))

    def set(self, key: str, value: str) -> str:
        with self.get_connection() as conn:
            conn.set(self.with_prefix(key), value)
            return "OK"

    def get(self, key: str) -> str | None:
        with self.get_connection() as conn:
            res = conn.get(self.with_prefix(key))
            if res is None:
                return None
            return res.decode("utf-8")

    def lpush(self, key: str, *values: str) -> int:
        with self.get_connection() as conn:
            return conn.lpush(self.with_prefix(key), *values)

    def rpush(self, key: str, *values: str) -> int:
        with self.get_connection() as conn:
            return conn.rpush(self.with_prefix(key), *values)

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
        with self.get_connection() as conn:
            res = conn.lpop(self.with_prefix(key), count)
            if res is None:
                return None
            if count is None:
                return res.decode("utf-8")
            return [val.decode("utf-8") for val in res]

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
        with self.get_connection() as conn:
            res = conn.rpop(self.with_prefix(key), count)
            if res is None:
                return None
            if count is None:
                return res.decode("utf-8")
            return [val.decode("utf-8") for val in res]

    def llen(self, key: str) -> int:
        with self.get_connection() as conn:
            return conn.llen(self.with_prefix(key))

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        with self.get_connection() as conn:
            return int(
                conn.zadd(self.with_prefix(key), mapping))  # type: ignore

    def zpop_max(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        with self.get_connection() as conn:
            res = conn.zpopmax(self.with_prefix(key), count)
            return [
                (name.decode("utf-8"), float(score))
                for name, score in res
            ]

    def zpop_min(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        with self.get_connection() as conn:
            res = conn.zpopmin(self.with_prefix(key), count)
            return [
                (name.decode("utf-8"), float(score))
                for name, score in res
            ]

    def zcard(self, key: str) -> int:
        with self.get_connection() as conn:
            return int(conn.zcard(self.with_prefix(key)))
