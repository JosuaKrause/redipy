import collections
import datetime
import time
from collections.abc import Iterable
from typing import Literal, overload

from redipy.api import RedisAPI, RSetMode, RSM_ALWAYS, RSM_EXISTS, RSM_MISSING
from redipy.util import now, time_diff


def compute_expire(
        *,
        expire_timestamp: datetime.datetime | None,
        expire_in: float | None) -> float | None:
    if expire_timestamp is None:
        if expire_in is None:
            return None
        return time.monotonic() + expire_in
    if expire_in is not None:
        raise ValueError(
            f"cannot set timestamp {expire_timestamp} "
            f"and duration {expire_in} at the same time")
    return time.monotonic() + time_diff(now(), expire_timestamp)


KeyType = Literal[
    "value",
    "list",
    "hash",
    "zset",
]


class State:
    def __init__(self, parent: 'State | None' = None) -> None:
        super().__init__()
        self._parent = parent
        self._vals: dict[str, tuple[str, float | None]] = {}
        self._queues: dict[str, collections.deque[str]] = {}
        self._hashes: dict[str, dict[str, str]] = {}
        # TODO: to be replaced by something more efficient later
        self._zorder: dict[str, list[str]] = {}
        self._zscores: dict[str, dict[str, float]] = {}
        self._deletes: set[str] = set()

    def verify_key(self, key_type: KeyType, key: str) -> None:
        if key_type != "value" and key in self._vals:
            raise ValueError(f"key {key} already used as value")
        if key_type != "list" and key in self._queues:
            raise ValueError(f"key {key} already used as list")
        if key_type != "hash" and key in self._hashes:
            raise ValueError(f"key {key} already used as hash")
        if key_type != "zset" and key in self._zorder:
            raise ValueError(f"key {key} already used as sorted set")
        if self._parent is not None:
            self._parent.verify_key(key_type, key)

    def exists(self, key: str) -> bool:
        if key in self._vals:
            return True
        if key in self._queues:
            return True
        if key in self._hashes:
            return True
        if key in self._zorder:
            return True
        if self._parent is not None:
            return self._parent.exists(key)
        return False

    def delete(self, deletes: Iterable[str]) -> None:
        if self._parent is not None:
            self._deletes.update(deletes)
        for key in deletes:
            self._vals.pop(key, None)
            self._queues.pop(key, None)
            self._hashes.pop(key, None)
            self._zorder.pop(key, None)
            self._zscores.pop(key, None)

    def apply(self, other: 'State') -> None:
        self._vals.update(other.raw_vals())
        self._queues.update(other.raw_queues())
        self._hashes.update(other.raw_hashes())
        self._zorder.update(other.raw_zorder())
        self._zscores.update(other.raw_zscores())
        self._clean_vals()
        self.delete(other.raw_deletes())

    def reset(self) -> None:
        self._vals.clear()
        self._queues.clear()
        self._hashes.clear()
        self._zorder.clear()
        self._zscores.clear()
        self._deletes.clear()

    def _is_alive(self, value: tuple[str, float | None]) -> bool:
        _, expire = value
        if expire is None:
            return True
        now_mono = time.monotonic()
        if expire > now_mono:
            return True
        return False

    def _clean_vals(self) -> None:
        if self._parent is not None:
            return
        remove = set()
        for key, value in self._vals.items():
            if self._is_alive(value):
                continue
            remove.add(key)
        self.delete(remove)

    def raw_vals(self) -> dict[str, tuple[str, float | None]]:
        return self._vals

    def raw_queues(
            self) -> dict[str, collections.deque[str]]:
        return self._queues

    def raw_hashes(self) -> dict[str, dict[str, str]]:
        return self._hashes

    def raw_zorder(self) -> dict[str, list[str]]:
        return self._zorder

    def raw_zscores(self) -> dict[str, dict[str, float]]:
        return self._zscores

    def raw_deletes(self) -> set[str]:
        return self._deletes

    def set_value(self, key: str, value: str, expire: float | None) -> None:
        if key not in self._vals:
            self.verify_key("value", key)
        self._vals[key] = (value, expire)

    def get_value(self, key: str) -> tuple[str, float | None] | None:
        res = self._vals.get(key)
        if res is not None and not self._is_alive(res):
            self._clean_vals()
            return None
        if res is None and self._parent is not None:
            return self._parent.get_value(key)
        return res

    def has_value(self, key: str) -> bool:
        return self.get_value(key) is not None

    def get_queue(self, key: str) -> collections.deque[str]:
        res = self._queues.get(key)
        if res is None:
            self.verify_key("list", key)
            if self._parent is not None:
                res = collections.deque(self._parent.get_queue(key))
            else:
                res = collections.deque()
            self._queues[key] = res
        return res

    def queue_len(self, key: str) -> int:
        res = self._queues.get(key)
        if res is None:
            if self._parent is not None:
                return self._parent.queue_len(key)
            return 0
        return len(res)

    def get_hash(self, key: str) -> dict[str, str]:
        res = self._hashes.get(key)
        if res is None:
            self.verify_key("hash", key)
            if self._parent is not None:
                res = dict(self._parent.get_hash(key))
            else:
                res = {}
            self._hashes[key] = res
        return res

    def readonly_hash(self, key: str) -> dict[str, str] | None:
        res = self._hashes.get(key)
        if res is None:
            if self._parent is None:
                return None
            return self._parent.readonly_hash(key)
        return res

    def get_zorder(self, key: str) -> list[str]:
        res = self._zorder.get(key)
        if res is None:
            self.verify_key("zset", key)
            if self._parent is not None:
                res = list(self._parent.get_zorder(key))
            else:
                res = []
            self._zorder[key] = res
        return res

    def zorder_len(self, key: str) -> int:
        res = self._zorder.get(key)
        if res is None:
            if self._parent is not None:
                return self._parent.zorder_len(key)
            return 0
        return len(res)

    def get_zscores(self, key: str) -> dict[str, float]:
        res = self._zscores.get(key)
        if res is None:
            self.verify_key("zset", key)
            if self._parent is not None:
                res = dict(self._parent.get_zscores(key))
            else:
                res = {}
            self._zscores[key] = res
        return res

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}["
            f"vals={self._vals},"
            f"queues={dict(self._queues)},"
            f"hashes={dict(self._hashes)},"
            f"zorder={dict(self._zorder)},"
            f"zscores={dict(self._zscores)}]")

    def __repr__(self) -> str:
        return self.__str__()


class Machine(RedisAPI):
    def __init__(self, state: State) -> None:
        super().__init__()
        self._state = state

    def get_state(self) -> State:
        return self._state

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
        expire = compute_expire(
            expire_timestamp=expire_timestamp, expire_in=expire_in)
        prev_value = None
        prev_expire = None
        prev = self._state.get_value(key)
        if prev is not None:
            prev_value, prev_expire = prev
        if keep_ttl:
            expire = prev_expire
        do_set = False
        if mode == RSM_ALWAYS:
            do_set = True
        elif mode == RSM_EXISTS:
            do_set = prev is not None
        elif mode == RSM_MISSING:
            do_set = prev is None
        else:
            raise ValueError(f"unknown mode: {mode}")
        if do_set:
            self._state.set_value(key, value, expire)
        if return_previous:
            return prev_value
        return do_set

    def get(self, key: str) -> str | None:
        res = self._state.get_value(key)
        if res is None:
            return None
        value, _ = res
        return value

    def lpush(self, key: str, *values: str) -> int:
        queue = self._state.get_queue(key)
        queue.extendleft(values)
        return len(queue)

    def rpush(self, key: str, *values: str) -> int:
        queue = self._state.get_queue(key)
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

    def lpop(
            self,
            key: str,
            count: int | None = None) -> str | list[str] | None:
        queue = self._state.get_queue(key)
        if not queue:
            return None
        if count is None:
            rval = queue.popleft()
            if not queue:
                self.delete(key)
            return rval
        popc = count
        res = []
        while popc > 0 and queue:
            res.append(queue.popleft())
            popc -= 1
        if not queue:
            self.delete(key)
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

    def rpop(
            self,
            key: str,
            count: int | None = None) -> str | list[str] | None:
        queue = self._state.get_queue(key)
        if not queue:
            return None
        if count is None:
            rval = queue.pop()
            if not queue:
                self.delete(key)
            return rval
        popc = count
        res = []
        while popc > 0 and queue:
            res.append(queue.pop())
            popc -= 1
        if not queue:
            self.delete(key)
        return res if res else None

    def llen(self, key: str) -> int:
        return self._state.queue_len(key)

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        count = 0
        zscores = self._state.get_zscores(key)
        zorder = self._state.get_zorder(key)
        for name, score in mapping.items():
            if name not in zscores:
                zorder.append(name)
                count += 1
            zscores[name] = score
        zorder.sort(key=lambda k: (zscores[k], k))
        return count

    def zpop_max(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        zscores = self._state.get_zscores(key)
        zorder = self._state.get_zorder(key)
        res = []
        remain = 1 if count is None else count
        while remain > 0 and zorder:
            name = zorder.pop()
            score = zscores.pop(name)
            res.append((name, score))
            remain -= 1
        return res

    def zpop_min(
            self,
            key: str,
            count: int = 1,
            ) -> list[tuple[str, float]]:
        zscores = self._state.get_zscores(key)
        zorder = self._state.get_zorder(key)
        res = []
        remain = 1 if count is None else count
        while remain > 0 and zorder:
            name = zorder.pop(0)
            score = zscores.pop(name)
            res.append((name, score))
            remain -= 1
        return res

    def zcard(self, key: str) -> int:
        return self._state.zorder_len(key)

    def incrby(self, key: str, inc: float | int) -> float:
        res = self._state.get_value(key)
        if res is None:
            val = "0"
            expire = None
        else:
            val, expire = res
        num = float(val) + inc
        self._state.set_value(key, f"{num}", expire)
        return num

    def exists(self, *keys: str) -> int:
        res = 0
        for key in keys:
            if self._state.exists(key):
                res += 1
        return res

    def delete(self, *keys: str) -> int:
        res = self.exists(*keys)
        self._state.delete(keys)
        return res

    def hset(self, key: str, mapping: dict[str, str]) -> int:
        obj = self._state.get_hash(key)
        res = len(set(mapping.keys()).difference(obj.keys()))
        obj.update(mapping)
        return res

    def hdel(self, key: str, *fields: str) -> int:
        obj = self._state.get_hash(key)
        res = 0
        for field in fields:
            if obj.pop(field, None) is None:
                res += 1
        if not obj:
            self._state.delete({key})
        return res

    def hget(self, key: str, field: str) -> str | None:
        res = self._state.readonly_hash(key)
        if res is None:
            return None
        return res.get(field)

    def hmget(self, key: str, *fields: str) -> dict[str, str | None]:
        res = self._state.readonly_hash(key)
        if res is None:
            return {}
        return {
            field: res.get(field)
            for field in fields
        }

    def hincrby(self, key: str, field: str, inc: float | int) -> float:
        res = self._state.get_hash(key)
        num = float(res.get(field, 0)) + inc
        res[key] = f"{num}"
        return num

    def hkeys(self, key: str) -> list[str]:
        res = self._state.readonly_hash(key)
        if res is None:
            return []
        return list(res.keys())

    def hvals(self, key: str) -> list[str]:
        res = self._state.readonly_hash(key)
        if res is None:
            return []
        return list(res.values())

    def hgetall(self, key: str) -> dict[str, str]:
        res = self._state.readonly_hash(key)
        if res is None:
            return {}
        return dict(res)
