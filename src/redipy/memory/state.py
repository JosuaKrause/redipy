"""This module handles the internal state of the memory runtime."""
import collections
import datetime
import itertools
import time
from typing import Literal, overload

from redipy.api import RedisAPI, RSetMode, RSM_ALWAYS, RSM_EXISTS, RSM_MISSING
from redipy.util import now, time_diff, to_number_str


def compute_expire(
        *,
        expire_timestamp: datetime.datetime | None,
        expire_in: float | None) -> float | None:
    """
    Computes the monotonic time point when a key should expire.

    Args:
        expire_timestamp (datetime.datetime | None): An absolute timestamp.
        expire_in (float | None): A relative time difference in seconds.

    Raises:
        ValueError: If both expire_timestamp and expire_in are set.

    Returns:
        float | None: The monotonic time point.
    """
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
"""The different key types."""


class State:
    """
    The state holding the actual values of the memory runtime. A state can have
    a parent state which values it shadows. The parent can be updated by
    applying all changes.
    """
    def __init__(self, parent: 'State | None' = None) -> None:
        """
        Creates a new state.

        Args:
            parent (State | None, optional): The optional parent whose values
            this state shadows. Defaults to None.
        """
        super().__init__()
        self._parent = parent
        self._vals: dict[str, tuple[str, float | None]] = {}
        self._queues: dict[str, collections.deque[str]] = {}
        self._hashes: dict[str, dict[str, str]] = {}
        # TODO: to be replaced by something more efficient later
        self._zorder: dict[str, list[str]] = {}
        self._zscores: dict[str, dict[str, float]] = {}
        self._deletes: set[str] = set()

    def key_type(self, key: str) -> KeyType | None:
        """
        Computes the type of a given key.

        Args:
            key (str): The key.

        Returns:
            KeyType | None: The type of the key or None if it doesn't exist.
        """
        if key in self._deletes:
            return None
        if key in self._vals:
            return "value"
        if key in self._queues:
            return "list"
        if key in self._hashes:
            return "hash"
        if key in self._zorder:
            return "zset"
        if self._parent is not None:
            return self._parent.key_type(key)
        return None

    def verify_key(self, key_type: KeyType, key: str) -> None:
        """
        Ensures that the given key does not already exist with a different
        type.

        Args:
            key_type (KeyType): The expected key type.
            key (str): The key.

        Raises:
            ValueError: If the key already exists with a different type.
        """
        if key in self._deletes:
            return
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
        """
        Checks whether a given key exists.

        Args:
            key (str): The key.

        Returns:
            bool: Whether it exists as any type.
        """
        if key in self._deletes:
            return False
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

    def delete(self, deletes: set[str]) -> None:
        """
        Deletes keys.

        Args:
            deletes (set[str]): The keys to delete.
        """
        if self._parent is not None:
            self._deletes.update(deletes)
        for key in deletes:
            self._vals.pop(key, None)
            self._queues.pop(key, None)
            self._hashes.pop(key, None)
            self._zorder.pop(key, None)
            self._zscores.pop(key, None)

    def apply(self, other: 'State') -> None:
        """
        Applies the given state. All updates that are different in the other
        state will be updated in the current state and all deleted keys in
        the other state will delete corresponding keys in the current state.
        If a key in the other state has a different type than the key in the
        current state the type in the current state will be changed. This
        relies on the invariant that a key can only appear once in the other
        and once in the current state.

        Args:
            other (State): The state to be applied to the current state.
        """
        raw_vals = other.raw_vals()
        raw_queues = other.raw_queues()
        raw_hashes = other.raw_hashes()
        raw_zorder = other.raw_zorder()
        raw_zscores = other.raw_zscores()

        new_keys: set[str] = set()  # NOTE: new means new for the given type
        new_keys.update(set(raw_vals.keys()) - set(self._vals.keys()))
        new_keys.update(set(raw_queues.keys()) - set(self._queues.keys()))
        new_keys.update(set(raw_hashes.keys()) - set(self._hashes.keys()))
        new_keys.update(set(raw_zorder.keys()) - set(self._zorder.keys()))
        new_keys.update(set(raw_zscores.keys()) - set(self._zscores.keys()))
        # NOTE: deleting all new keys makes sure they don't exist as a
        # different type
        self.delete(new_keys)

        self._vals.update(raw_vals)
        self._queues.update(raw_queues)
        self._hashes.update(raw_hashes)
        self._zorder.update(raw_zorder)
        self._zscores.update(raw_zscores)
        self._clean_vals()
        self.delete(other.raw_deletes())

    def reset(self) -> None:
        """
        Completely resets the state.
        """
        self._vals.clear()
        self._queues.clear()
        self._hashes.clear()
        self._zorder.clear()
        self._zscores.clear()
        self._deletes.clear()

    def _prepare_key_for_write(self, key: str) -> None:
        """
        Prepares a key for writing by ensuring that it is not deleted.

        Args:
            key (str): The key.
        """
        self._deletes.discard(key)

    def _is_pending_delete(self, key: str) -> bool:
        """
        Whether the given key is marked as deleted. Note, this is not the case
        after _prepare_key_for_write.

        Args:
            key (str): The key.

        Returns:
            bool: Whether the given key is in the delete set.
        """
        return key in self._deletes

    def _is_alive(self, value: tuple[str, float | None]) -> bool:
        """
        Checks whether the given value has expired.

        Args:
            value (tuple[str, float  |  None]): Tuple of value and optional
            expiration monotonic time point.

        Returns:
            bool: Whether the value has expired.
        """
        _, expire = value
        if expire is None:
            return True
        now_mono = time.monotonic()
        if expire > now_mono:
            return True
        return False

    def _clean_vals(self) -> None:
        """
        Cleans up expired values by deleting them.
        """
        if self._parent is not None:
            return
        remove = set()
        for key, value in self._vals.items():
            if self._is_alive(value):
                continue
            remove.add(key)
        self.delete(remove)

    def raw_vals(self) -> dict[str, tuple[str, float | None]]:
        """
        Raw access to the value dictionary.

        Returns:
            dict[str, tuple[str, float | None]]: The actual value dictionary of
            this state.
        """
        return self._vals

    def raw_queues(
            self) -> dict[str, collections.deque[str]]:
        """
        Raw access to the queues dictionary.

        Returns:
            dict[str, collections.deque[str]]: The actual queue dictionary of
            this state.
        """
        return self._queues

    def raw_hashes(self) -> dict[str, dict[str, str]]:
        """
        Raw access to the hashes dictionary.

        Returns:
            dict[str, dict[str, str]]: The actual hashes dictionary of this
            state.
        """
        return self._hashes

    def raw_zorder(self) -> dict[str, list[str]]:
        """
        Raw access to the zorder dictionary.

        Returns:
            dict[str, list[str]]: The actual zorder dictionary of this state.
        """
        return self._zorder

    def raw_zscores(self) -> dict[str, dict[str, float]]:
        """
        Raw access to the zscores dictionary.

        Returns:
            dict[str, dict[str, float]]: The actual zscores dictionary of this
            state.
        """
        return self._zscores

    def raw_deletes(self) -> set[str]:
        """
        Raw access to the deletes set.

        Returns:
            set[str]: The actual deletes set of this state.
        """
        return self._deletes

    def set_value(self, key: str, value: str, expire: float | None) -> None:
        """
        Sets the value for the given key.

        Args:
            key (str): The key.
            value (str): The value.
            expire (float | None): When the value should expire if at all.
        """
        if key not in self._vals:
            self.verify_key("value", key)
        self._prepare_key_for_write(key)
        self._vals[key] = (value, expire)

    def get_value(self, key: str) -> tuple[str, float | None] | None:
        """
        Retrieves the value associated with the given key.

        Args:
            key (str): The key.

        Returns:
            tuple[str, float | None] | None: The value of the key and when it
            will expire.
        """
        res = self._vals.get(key)
        if res is not None and not self._is_alive(res):
            self._clean_vals()
            return None
        if (
                res is None
                and self._parent is not None
                and not self._is_pending_delete(key)):
            return self._parent.get_value(key)
        return res

    def has_value(self, key: str) -> bool:
        """
        Whether the given key is associated with a value. Note, this is
        different from whether the key exists.

        Args:
            key (str): The key.

        Returns:
            bool: If the key exists and its type is "value".
        """
        return self.get_value(key) is not None

    def get_queue(self, key: str) -> collections.deque[str]:
        """
        Returns a queue for writing. If the queue doesn't exist already it will
        be created.

        Args:
            key (str): The key.

        Returns:
            collections.deque[str]: The queue for writing.
        """
        res = self._queues.get(key)
        if res is None:
            self.verify_key("list", key)
            if self._parent is not None and not self._is_pending_delete(key):
                res = collections.deque(self._parent.get_queue(key))
            else:
                res = collections.deque()
            self._queues[key] = res
        self._prepare_key_for_write(key)
        return res

    def readonly_queue(self, key: str) -> collections.deque[str] | None:
        """
        Returns a queue for reading only. If the queue doesn't exist None is
        returned.

        Args:
            key (str): The key.

        Returns:
            collections.deque[str] | None: The readonly queue. Make sure to not
            modify it as it is not a copy. None if the queue doesn't exist.
        """
        res = self._queues.get(key)
        if res is None:
            if self._parent is None or self._is_pending_delete(key):
                return None
            return self._parent.readonly_queue(key)
        return res

    def queue_len(self, key: str) -> int:
        """
        Computes the length of a queue.

        Args:
            key (str): The key.

        Returns:
            int: The length of the queue.
        """
        res = self.readonly_queue(key)
        if res is None:
            return 0
        return len(res)

    def get_hash(self, key: str) -> dict[str, str]:
        """
        Returns a hash for writing. If the hash doesn't exist already it will
        be created.

        Args:
            key (str): The key.

        Returns:
            dict[str, str]: The hash for writing.
        """
        res = self._hashes.get(key)
        if res is None:
            self.verify_key("hash", key)
            if self._parent is not None and not self._is_pending_delete(key):
                res = dict(self._parent.get_hash(key))
            else:
                res = {}
            self._hashes[key] = res
        self._prepare_key_for_write(key)
        return res

    def readonly_hash(self, key: str) -> dict[str, str] | None:
        """
        Returns a hash for reading only. If the hash doesn't exist None is
        returned.

        Args:
            key (str): The key.

        Returns:
            dict[str, str] | None: The readonly hash. Make sure to not
            modify it as it is not a copy. None if the hash doesn't exist.
        """
        res = self._hashes.get(key)
        if res is None:
            if self._parent is None or self._is_pending_delete(key):
                return None
            return self._parent.readonly_hash(key)
        return res

    def get_zset(self, key: str) -> tuple[list[str], dict[str, float]]:
        """
        Returns a zset for writing. If the zset doesn't exist already it
        will be created.

        Args:
            key (str): The key.

        Returns:
            tuple[list[str], dict[str, float]]: The tuple of zorder and zscores
            for writing.
        """
        rorder = self._zorder.get(key)
        rscores = self._zscores.get(key)
        if rorder is None:
            self.verify_key("zset", key)
            if self._parent is not None and not self._is_pending_delete(key):
                porder, pscores = self._parent.get_zset(key)
                rorder = list(porder)
                rscores = dict(pscores)
            else:
                rorder = []
                rscores = {}
            self._zorder[key] = rorder
            self._zscores[key] = rscores
        self._prepare_key_for_write(key)
        assert rscores is not None
        return (rorder, rscores)

    def readonly_zorder(self, key: str) -> list[str] | None:
        """
        Returns a zorder for reading only. If the zorder doesn't exist None is
        returned.

        Args:
            key (str): The key.

        Returns:
            list[str] | None: The readonly zorder. Make sure to not
            modify it as it is not a copy. None if the zorder doesn't exist.
        """
        res = self._zorder.get(key)
        if res is None:
            if self._parent is None or self._is_pending_delete(key):
                return None
            return self._parent.readonly_zorder(key)
        return res

    def readonly_zscores(self, key: str) -> dict[str, float] | None:
        """
        Returns a zscores for reading only. If the zscores doesn't exist None
        is returned.

        Args:
            key (str): The key.

        Returns:
            dict[str, float] | None: The readonly zscores. Make sure to not
            modify it as it is not a copy. None if the zscores doesn't exist.
        """
        res = self._zscores.get(key)
        if res is None:
            if self._parent is None or self._is_pending_delete(key):
                return None
            return self._parent.readonly_zscores(key)
        return res

    def zorder_len(self, key: str) -> int:
        """
        Computes the length of a zorder.

        Args:
            key (str): The key.

        Returns:
            int: The length of the zorder.
        """
        res = self.readonly_zorder(key)
        if res is None:
            return 0
        return len(res)

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}["
            f"vals={self._vals},"
            f"queues={dict(self._queues)},"
            f"hashes={dict(self._hashes)},"
            f"zorder={dict(self._zorder)},"
            f"zscores={dict(self._zscores)},"
            f"deletes={self._deletes},"
            f"has_parent={self._parent is not None}]")

    def __repr__(self) -> str:
        return self.__str__()


class Machine(RedisAPI):
    """
    A Machine manages the state of a memory runtime and exposes the redis API.
    """
    def __init__(self, state: State) -> None:
        """
        Creates a Machine.

        Args:
            state (State): The associated state.
        """
        super().__init__()
        self._state = state

    def get_state(self) -> State:
        """
        Returns the managed state.

        Returns:
            State: The associated state.
        """
        return self._state

    def exists(self, *keys: str) -> int:
        res = 0
        for key in keys:
            if self._state.exists(key):
                res += 1
        return res

    def delete(self, *keys: str) -> int:
        res = self.exists(*keys)
        self._state.delete(set(keys))
        return res

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

    def incrby(self, key: str, inc: float | int) -> float:
        res = self._state.get_value(key)
        if res is None:
            val = "0"
            expire = None
        else:
            val, expire = res
        num = float(val) + inc
        self._state.set_value(key, to_number_str(num), expire)
        return num

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

    def lrange(self, key: str, start: int, stop: int) -> list[str]:
        queue = self._state.readonly_queue(key)
        if queue is None:
            return []
        if start >= len(queue):
            return []
        if start < 0:
            start = max(0, start + len(queue))
        if stop < 0:
            stop += len(queue)
            if stop < 0:
                return []
        stop += 1
        queue.rotate(-start)
        res = list(itertools.islice(queue, 0, stop - start, 1))
        queue.rotate(start)
        return res

    def llen(self, key: str) -> int:
        return self._state.queue_len(key)

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        count = 0
        zorder, zscores = self._state.get_zset(key)
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
        zorder, zscores = self._state.get_zset(key)
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
        zorder, zscores = self._state.get_zset(key)
        res = []
        remain = 1 if count is None else count
        while remain > 0 and zorder:
            name = zorder.pop(0)
            score = zscores.pop(name)
            res.append((name, score))
            remain -= 1
        return res

    def zrange(self, key: str, start: int, stop: int) -> list[str]:
        zorder = self._state.readonly_zorder(key)
        if zorder is None:
            return []
        astop: int | None = stop
        if astop == -1 or astop is None:  # NOTE: mypy workaround
            astop = None
        else:
            astop += 1
        return zorder[start:astop]

    def zcard(self, key: str) -> int:
        return self._state.zorder_len(key)

    def hset(self, key: str, mapping: dict[str, str]) -> int:
        obj = self._state.get_hash(key)
        res = len(set(mapping.keys()).difference(obj.keys()))
        obj.update(mapping)
        return res

    def hdel(self, key: str, *fields: str) -> int:
        obj = self._state.get_hash(key)
        res = 0
        for field in fields:
            if obj.pop(field, None) is not None:
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
        res[field] = to_number_str(num)
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

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}[state={self._state}]")

    def __repr__(self) -> str:
        return self.__str__()
