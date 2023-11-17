"""The runtime for the redis backend."""
import contextlib
import datetime
import threading
import uuid
from collections.abc import Callable, Iterable, Iterator
from typing import (
    Any,
    cast,
    Literal,
    NoReturn,
    NotRequired,
    overload,
    Protocol,
    TypedDict,
)

from redis import Redis
from redis.client import Pipeline
from redis.commands.core import Script
from redis.exceptions import ResponseError

from redipy.api import (
    PipelineAPI,
    RSetMode,
    RSM_ALWAYS,
    RSM_EXISTS,
    RSM_MISSING,
)
from redipy.backend.runtime import Runtime
from redipy.redis.lua import LuaBackend
from redipy.util import (
    is_test,
    normalize_values,
    now,
    time_diff,
    to_list_str,
    to_maybe_str,
)


RedisConfig = TypedDict('RedisConfig', {
    "host": str,
    "port": int,
    "passwd": str,
    "prefix": NotRequired[str],
    "path": NotRequired[str],
})


class RedisFactory(Protocol):  # pylint: disable=too-few-public-methods
    """
    Factory function for creating a redis connection from a redis
    configuration.
    """
    def __call__(self, *, cfg: RedisConfig) -> Redis:
        """
        Creates a redis connection from a redis configuration.

        Args:
            cfg (RedisConfig): The redis configuration.

        Returns:
            Redis: The redis connection.
        """
        raise NotImplementedError()


class RedisFunctionBytes(Protocol):  # pylint: disable=too-few-public-methods
    """A redis function as it is returned from the redis connection."""
    def __call__(
            self,
            *,
            keys: list[str],
            args: list[Any],
            client: Redis | None) -> bytes:
        """
        Executes the redis function.

        Args:
            keys (list[str]): A list of keys used in the
            script. It is common convention to pass keys used in the script via
            this parameter but it is not necessary to do so. Importantly, keys
            can also be generated inside the script.

            args (list[Any]): Arguments to the script.

            client (Redis | None): An optionally different
            redis connection environment. Defaults to None.

        Returns:
            bytes: The result of the script.
        """
        raise NotImplementedError()


CONCURRENT_MODULE_CONN: int = 17
"""The number of allowed concurrent redis connections across threads."""


class RedisWrapper:
    """Manages redis connections."""
    def __init__(
            self,
            *,
            cfg: RedisConfig,
            redis_factory: RedisFactory | None = None,
            is_caching_enabled: bool = True) -> None:
        """
        Creates a redis wrapper for managing redis connections.

        Args:
            cfg (RedisConfig): The redis connection configuration.

            redis_factory (RedisFactory | None, optional): The redis connection
            factory. Defaults to None.

            is_caching_enabled (bool, optional): Whether caching of connections
            is enabled. Defaults to True.
        """
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
        """
        The connection index for this thread.

        Returns:
            int: The connection index.
        """
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
        """
        Returns a redis connection.

        Yields:
            Redis: The redis connection.
        """
        success = False
        try:
            yield self._get_redis_cached_conn()
            success = True
        finally:
            if not success:
                self.reset()

    def reset(self) -> None:
        """
        Removes all cached connections.
        """
        with self._lock:
            for ix, _ in enumerate(self._service_conn):
                self._service_conn[ix] = None


class PipelineConnection(PipelineAPI):
    """A pipeline for the redis backend runtime."""
    def __init__(self, pipe: Pipeline, prefix: str) -> None:
        super().__init__()
        self._pipe = pipe
        self._fixes: list[Callable[[Any], Any]] = []
        self._prefix = prefix

    def get_pipeline(self) -> Pipeline:
        """
        Return the underlying redis pipeline.

        Returns:
            Pipeline: The redis pipeline.
        """
        return self._pipe

    def has_pending(self) -> bool:
        """
        Whether there are commands in the pipeline that have not been executed
        yet.

        Returns:
            bool: True if there are unexecuted commands in the pipeline.
        """
        return len(self._fixes) > 0

    def with_prefix(self, key: str) -> str:
        """
        Computes the actual key value of this redis instance.

        Args:
            key (str): The key.

        Returns:
            str: The actual key.
        """
        return f"{self._prefix}{key}"

    def add_fixup(self, fix: Callable[[Any], Any]) -> None:
        """
        Adds a fixup callback for the current pipeline command. Every pipeline
        command must have exactly one fixup callback (even if it is just an
        identity function).

        Args:
            fix (Callable[[Any], Any]): A callback to convert the pipeline
            command result into the correct output value.
        """
        self._fixes.append(fix)

    def execute(self) -> list:
        fixes = self._fixes
        self._fixes = []
        res = self._pipe.execute()
        assert len(res) == len(fixes)
        return [
            fixup(val)
            for val, fixup in zip(res, fixes)
        ]

    def exists(self, *keys: str) -> None:
        self._pipe.exists(*(
            self.with_prefix(key) for key in keys))
        self.add_fixup(int)

    def delete(self, *keys: str) -> None:
        self._pipe.delete(*(
            self.with_prefix(key) for key in keys))
        self.add_fixup(int)

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
        expire = None
        if expire_in is not None:
            expire = int(expire_in * 1000.0)
        elif expire_timestamp is not None:
            expire = int(time_diff(now(), expire_timestamp) * 1000.0)
        self._pipe.set(
            self.with_prefix(key),
            value,
            get=return_previous,
            nx=(mode == RSM_MISSING),
            xx=(mode == RSM_EXISTS),
            px=expire,
            keepttl=keep_ttl)
        if return_previous:
            self.add_fixup(to_maybe_str)
        else:
            self.add_fixup(bool)

    def get(self, key: str) -> None:
        self._pipe.get(self.with_prefix(key))
        self.add_fixup(to_maybe_str)

    def incrby(self, key: str, inc: float | int) -> None:
        self._pipe.incrbyfloat(self.with_prefix(key), inc)
        self.add_fixup(float)

    def lpush(self, key: str, *values: str) -> None:
        self._pipe.lpush(self.with_prefix(key), *values)
        self.add_fixup(int)

    def rpush(self, key: str, *values: str) -> None:
        self._pipe.rpush(self.with_prefix(key), *values)
        self.add_fixup(int)

    def lpop(
            self,
            key: str,
            count: int | None = None) -> None:
        self._pipe.lpop(self.with_prefix(key), count)
        if count is None:
            self.add_fixup(to_maybe_str)
        else:
            self.add_fixup(to_list_str)

    def rpop(
            self,
            key: str,
            count: int | None = None) -> None:
        self._pipe.rpop(self.with_prefix(key), count)
        if count is None:
            self.add_fixup(to_maybe_str)
        else:
            self.add_fixup(to_list_str)

    def lrange(self, key: str, start: int, stop: int) -> None:
        self._pipe.lrange(self.with_prefix(key), start, stop)
        self.add_fixup(to_list_str)

    def llen(self, key: str) -> None:
        self._pipe.llen(self.with_prefix(key))
        self.add_fixup(int)

    def zadd(self, key: str, mapping: dict[str, float]) -> None:
        self._pipe.zadd(self.with_prefix(key), mapping=mapping)  # type: ignore
        self.add_fixup(int)

    def zpop_max(
            self,
            key: str,
            count: int = 1,
            ) -> None:
        self._pipe.zpopmax(self.with_prefix(key), count)
        self.add_fixup(normalize_values)

    def zpop_min(
            self,
            key: str,
            count: int = 1,
            ) -> None:
        self._pipe.zpopmin(self.with_prefix(key), count)
        self.add_fixup(normalize_values)

    def zcard(self, key: str) -> None:
        self._pipe.zcard(self.with_prefix(key))
        self.add_fixup(int)

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        self._pipe.hset(self.with_prefix(key), mapping=mapping)  # type: ignore
        self.add_fixup(int)

    def hdel(self, key: str, *fields: str) -> None:
        self._pipe.hdel(self.with_prefix(key), *fields)
        self.add_fixup(int)

    def hget(self, key: str, field: str) -> None:
        self._pipe.hget(self.with_prefix(key), field)
        self.add_fixup(to_maybe_str)

    def hmget(self, key: str, *fields: str) -> None:
        self._pipe.hmget(self.with_prefix(key), *fields)
        self.add_fixup(lambda res: {
            field: to_maybe_str(val)
            for field, val in zip(fields, res)
        })

    def hincrby(self, key: str, field: str, inc: float | int) -> None:
        self._pipe.hincrbyfloat(self.with_prefix(key), field, inc)
        self.add_fixup(float)

    def hkeys(self, key: str) -> None:
        self._pipe.hkeys(self.with_prefix(key))
        self.add_fixup(to_list_str)

    def hvals(self, key: str) -> None:
        self._pipe.hvals(self.with_prefix(key))
        self.add_fixup(to_list_str)

    def hgetall(self, key: str) -> None:
        self._pipe.hgetall(self.with_prefix(key))
        self.add_fixup(lambda res: {
            to_maybe_str(field): to_maybe_str(val)
            for field, val in res
        })


class RedisConnection(Runtime[list[str]]):
    """The runtime of the redis backend."""
    def __init__(
            self,
            redis_module: str,
            *,
            cfg: RedisConfig,
            redis_factory: RedisFactory | None = None,
            is_caching_enabled: bool = True,
            verbose_test: bool = True) -> None:
        super().__init__()
        self._conn: RedisWrapper = RedisWrapper(
            cfg=cfg,
            redis_factory=redis_factory,
            is_caching_enabled=is_caching_enabled)
        prefix = cfg.get("prefix", "")
        prefix_str = f"{prefix}:" if prefix else ""
        module = f"{prefix_str}{redis_module}".rstrip(":")
        self._module = f"{module}:" if module else ""
        self._is_print_scripts = verbose_test

    def set_print_scripts(self, is_print_scripts: bool) -> None:
        """
        Whether to print all lua scripts to stdout before registering.

        Args:
            is_print_scripts (bool): Whether to print all lua scripts.
        """
        self._is_print_scripts = is_print_scripts

    @classmethod
    def create_backend(cls) -> LuaBackend:
        return LuaBackend()

    @contextlib.contextmanager
    def pipeline(self) -> Iterator[PipelineAPI]:
        with self.get_connection() as conn:
            with conn.pipeline() as pipe:
                pconn = PipelineConnection(pipe, self._module)
                yield pconn
                if pconn.has_pending():
                    pconn.execute()  # drain pending tasks

    @contextlib.contextmanager
    def get_connection(self) -> Iterator[Redis]:
        """
        Get a raw redis connection.

        Yields:
            Redis: The redis connection.
        """
        with self._conn.get_connection() as conn:
            yield conn

    def get_dynamic_script(self, code: str) -> RedisFunctionBytes:
        """
        Registers a lua script.

        Args:
            code (str): The lua code.

        Returns:
            RedisFunctionBytes: An callable script.
        """
        if is_test() and self._is_print_scripts:
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

        def handle_err(exc: ResponseError) -> NoReturn:
            info = None
            if exc.args:
                msg = exc.args[0]
                res = get_error(msg)
                if res is not None:
                    ctx = "\n".join((
                        f"{'>' if ix == context else ' '} {line}"
                        for (ix, line) in enumerate(res[1])))
                    info = f"\nCode:\n{code}\n\nContext:\n{ctx}"
            if info is None:
                info = f": {exc}"
            raise ValueError(f"Error while executing script{info}") from exc

        def execute_bytes_result(
                *,
                keys: list[str],
                args: list[bytes | str | int],
                client: Redis | None) -> bytes:
            with get_client(client) as inner:
                return compute(keys=keys, args=args, client=inner)

        return execute_bytes_result

    def get_prefix(self) -> str:
        """
        The key prefix is added to all keys to create a virtual namespace on
        redis.

        Returns:
            str: The key prefix.
        """
        return self._module

    def with_prefix(self, key: str) -> str:
        """
        Computes the actual key value of this redis instance.

        Args:
            key (str): The key.

        Returns:
            str: The actual key.
        """
        return f"{self.get_prefix()}{key}"

    def get_pubsub_key(self, key: str) -> str:
        """
        Computes the actual key value for a pubsub key.

        Args:
            key (str): The key.

        Returns:
            str: The actual pubsub key.
        """
        return f"{self.get_prefix()}ps:{key}"

    def wait_for(
            self,
            key: str,
            predicate: Callable[[], bool],
            granularity: float = 30.0) -> None:
        """
        Waits until the condition is met.

        Args:
            key (str): The key used for the pubsub channel.

            predicate (Callable[[], bool]): The condition.

            granularity (float, optional): Maximum wait between predicate
            checks in seconds. Defaults to 30.0.
        """
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
        """
        Notifies a pubsub channel.

        Args:
            key (str): The key used for the pubsub channel.
        """
        with self.get_connection() as conn:
            conn.publish(self.get_pubsub_key(key), "notify")

    def ping(self) -> None:
        """
        Pings the redis server (and checks whether the connection is live).
        """
        with self.get_connection() as conn:
            conn.ping()

    def flush_all(self) -> None:
        """
        Removes all keys from the redis database.
        """
        with self.get_connection() as conn:
            conn.flushall()

    def keys_count(self, prefix: str) -> int:
        """
        Counts the number of keys with the given prefix. This method operates
        on raw keys ignoring the `get_prefix()` prefix.

        Args:
            prefix (str): The prefix.

        Returns:
            int: The number of keys.
        """
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
        """
        Return all keys with the given prefix (and postfix). This method
        operates on raw keys ignoring the `get_prefix()` prefix.

        Args:
            prefix (str): The prefix.

            postfix (str | None, optional): The postfix. Defaults to None.

        Returns:
            Iterable[str]: The raw keys.
        """
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
        """
        Whether keys exist with the given prefix (and postfix). This method
        operates on raw keys ignoring the `get_prefix()` prefix.

        Args:
            prefix (str): The prefix.

            postfix (str | None, optional): The postfix. Defaults to None.

        Returns:
            bool: Whether there are any keys with the given prefix
            (and postfix).
        """
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

    def exists(self, *keys: str) -> int:
        with self.get_connection() as conn:
            return conn.exists(*(
                self.with_prefix(key) for key in keys))

    def delete(self, *keys: str) -> int:
        with self.get_connection() as conn:
            return conn.delete(*(
                self.with_prefix(key) for key in keys))

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
        with self.get_connection() as conn:
            expire = None
            if expire_in is not None:
                expire = int(expire_in * 1000.0)
            elif expire_timestamp is not None:
                expire = int(time_diff(now(), expire_timestamp) * 1000.0)
            res = conn.set(
                self.with_prefix(key),
                value,
                get=return_previous,
                nx=(mode == RSM_MISSING),
                xx=(mode == RSM_EXISTS),
                px=expire,
                keepttl=keep_ttl)
            if return_previous:
                if res is not None:
                    return cast(bytes, res).decode("utf-8")
            elif res is None:
                return False
            return res

    def get(self, key: str) -> str | None:
        with self.get_connection() as conn:
            return to_maybe_str(conn.get(self.with_prefix(key)))

    def incrby(self, key: str, inc: float | int) -> float:
        with self.get_connection() as conn:
            return conn.incrbyfloat(self.with_prefix(key), inc)

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
            if count is None:
                return to_maybe_str(res)
            return to_list_str(res)

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
            if count is None:
                return to_maybe_str(res)
            return to_list_str(res)

    def lrange(self, key: str, start: int, stop: int) -> list[str]:
        with self.get_connection() as conn:
            return to_list_str(conn.lrange(self.with_prefix(key), start, stop))

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

    def hset(self, key: str, mapping: dict[str, str]) -> int:
        with self.get_connection() as conn:
            return conn.hset(
                self.with_prefix(key),
                mapping=mapping)  # type: ignore

    def hdel(self, key: str, *fields: str) -> int:
        with self.get_connection() as conn:
            return conn.hdel(self.with_prefix(key), *fields)

    def hget(self, key: str, field: str) -> str | None:
        with self.get_connection() as conn:
            return to_maybe_str(conn.hget(self.with_prefix(key), field))

    def hmget(self, key: str, *fields: str) -> dict[str, str | None]:
        with self.get_connection() as conn:
            res = conn.hmget(self.with_prefix(key), *fields)
            return {
                field: to_maybe_str(val)
                for field, val in zip(fields, res)
            }

    def hincrby(self, key: str, field: str, inc: float | int) -> float:
        with self.get_connection() as conn:
            return conn.hincrbyfloat(self.with_prefix(key), field, inc)

    def hkeys(self, key: str) -> list[str]:
        with self.get_connection() as conn:
            return to_list_str(conn.hkeys(self.with_prefix(key)))

    def hvals(self, key: str) -> list[str]:
        with self.get_connection() as conn:
            return to_list_str(conn.hvals(self.with_prefix(key)))

    def hgetall(self, key: str) -> dict[str, str]:
        with self.get_connection() as conn:
            return {
                to_maybe_str(field): to_maybe_str(val)
                for field, val in conn.hgetall(self.with_prefix(key)).items()
            }
