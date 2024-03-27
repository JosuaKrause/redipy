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
"""This module handles the internal state of the memory runtime."""
import bisect
import collections
import itertools
import time
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Literal, overload, TypeVar

from redipy.api import (
    KeyType,
    RedisAPI,
    REX_ALWAYS,
    REX_EARLIER,
    REX_EXPIRE,
    REX_LATER,
    REX_PERSIST,
    RExpireMode,
    RSetMode,
    RSM_ALWAYS,
    RSM_EXISTS,
    RSM_MISSING,
)
from redipy.util import convert_pattern, now, time_diff, to_number_str


T = TypeVar('T')


def compute_expire(
        now_mono: float,
        now_ts: datetime,
        *,
        expire_timestamp: datetime | None,
        expire_in: float | None) -> float | None:
    """
    Computes the monotonic time point when a key should expire.

    Args:
        now_mono (float): The current monotonic time.
        now_ts (datetime): The current date time.
        expire_timestamp (datetime | None): An absolute timestamp.
        expire_in (float | None): A relative time difference in seconds.

    Raises:
        ValueError: If both expire_timestamp and expire_in are set.

    Returns:
        float | None: The monotonic time point.
    """
    if expire_timestamp is None:
        if expire_in is None:
            return None
        return now_mono + expire_in
    if expire_in is not None:
        raise ValueError(
            f"cannot set timestamp {expire_timestamp} "
            f"and duration {expire_in} at the same time")
    return now_mono + time_diff(now_ts, expire_timestamp)


MIN_SCAN_LENGTH: int = 10
"""The internal minimum number of items returned by `scan`."""


KEYS_MAX_SCAN: int = 10000
"""The internal maximum length for `scan` used by `keys`."""


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
        self._expire: dict[str, float] = {}
        self._vals: dict[str, str] = {}
        self._queues: dict[str, collections.deque[str]] = {}
        self._hashes: dict[str, dict[str, str]] = {}
        self._sets: dict[str, set[str]] = {}
        # TODO: to be replaced by something more efficient later
        self._zorder: dict[str, list[str]] = {}
        self._zscores: dict[str, dict[str, float]] = {}
        self._deletes: set[str] = set()
        self._key_cache: list[str] | None = None
        self._delete_count: int = 0

    def has_parent(self) -> bool:
        """
        Whether the state has a parent.

        Returns:
            bool: True, if this state has a parent.
        """
        return self._parent is not None

    def key_type(self, key: str) -> KeyType | None:
        """
        Computes the type of a given key. The function does not check
        expiration.

        Args:
            key (str): The key.

        Returns:
            KeyType | None: The type of the key or None if it doesn't exist.
        """
        if self._is_pending_delete(key):
            return None
        if key in self._vals:
            return "string"
        if key in self._queues:
            return "list"
        if key in self._hashes:
            return "hash"
        if key in self._sets:
            return "set"
        if key in self._zorder:
            return "zset"
        if self._parent is not None:
            return self._parent.key_type(key)
        return None

    def verify_key(self, key_type: KeyType, key: str) -> None:
        """
        Ensures that the given key does not already exist with a different
        type. This does not check for expiration.

        Args:
            key_type (KeyType): The expected key type.
            key (str): The key.

        Raises:
            TypeError: If the key already exists with a different type.
        """
        if self._is_pending_delete(key):
            return
        if key_type != "string" and key in self._vals:
            raise TypeError(f"key {key} is a string")
        if key_type != "list" and key in self._queues:
            raise TypeError(f"key {key} is a list")
        if key_type != "hash" and key in self._hashes:
            raise TypeError(f"key {key} is a hash")
        if key_type != "set" and key in self._sets:
            raise TypeError(f"key {key} is a set")
        if key_type != "zset" and key in self._zorder:
            raise TypeError(f"key {key} is a zset")
        if self._parent is not None:
            self._parent.verify_key(key_type, key)

    def _flush_key_cache(self) -> None:
        self._key_cache = None

    def _populate_key_cache(self, now_mono: float) -> list[str]:
        """
        Populates the key cache.

        Args:
            now_mono (float): The current time.

        Returns:
            list[str]: A sorted list of all keys.
        """
        self.clean_vals(now_mono)
        pre = [
            *self._vals,
            *self._queues,
            *self._hashes,
            *self._sets,
            *self._zorder,
        ]
        if self._parent is None:
            key_cache = sorted(pre)
        else:
            # NOTE: no need to remove duplicates here as they are allowed
            # by the spec and are removed downstream which is much cheaper
            parent_cache = self._parent.get_key_cache(now_mono)
            key_cache = sorted(pre + parent_cache)
        self._key_cache = key_cache
        return key_cache

    def get_key_cache(self, now_mono: float) -> list[str]:
        """
        Retrieves the full key cache. If necessary populates the key cache.
        The key cache is a sorted list of all keys. It is used for scanning
        keys.

        Args:
            now_mono (float): The current time.

        Returns:
            list[str]: A sorted list of all keys.
        """
        res = self._key_cache
        if res is None:
            res = self._populate_key_cache(now_mono)
        return res

    def _start_ix(self, pattern_prefix: str, now_mono: float) -> int:
        """
        Computes the start index in the key cache for a given pattern prefix.

        Args:
            pattern_prefix (str): The pattern prefix is the longest prefix of
                the pattern that does not include any wildcards or ranges.
            now_mono (float): The current time.

        Returns:
            int: Where the key cache can be started to be scanned to
                immediately retrieve keys starting with at least the pattern
                prefix.
        """
        return bisect.bisect_left(self.get_key_cache(now_mono), pattern_prefix)

    def _scan(
            self,
            cursor: int,
            length: int,
            pattern_prefix: str | None,
            now_mono: float) -> tuple[int, list[str]]:
        """
        Internal scan. The result might include keys that will be filtered in
        the actual scan function.

        Args:
            cursor (int): The scan cursor. If the scan cursor is 0 the starting
                position in the key cache is used. If the scan cursor is not 0
                it is the adjusted index one after the last returned key. The
                index is adjusted by the total amount of keys deleted. During
                iteration, if a key is added to the left of the cursor index
                it will not be returned (which is allowed by the spec) and some
                previously returned keys might be returned again. If the key is
                added to the right it will eventually be returned as well. If
                a key is deleted the adjustment of the cursor is shifted left
                by one (i.e., `delete_count` is increased by one). Due to the
                shift we continue iterating as if nothing happened. Shifting
                the index to the left guarantees that we do not skip an
                existing key if the deleted key was to the left.
            length (int): The length of the returned result. To avoid expensive
                iterations with small return lengths this value is clipped to
                `MIN_SCAN_LENGTH` on the lower end.
            pattern_prefix (str | None): The pattern prefix is the longest
                prefix of the pattern that does not include any wildcards or
                ranges. If None, all keys are considered.
            now_mono (float): The current time.

        Returns:
            tuple[int, list[str]]: A tuple of the next cursor and the returned
                keys. If the iteration is complete the next cursor is 0.
        """
        delete_count = self._delete_count
        if cursor == 0 and pattern_prefix is not None:
            cursor = self._start_ix(pattern_prefix, now_mono)
        else:
            cursor = max(cursor - delete_count, 0)
        key_cache = self.get_key_cache(now_mono)
        end_ix = max(cursor + max(length, MIN_SCAN_LENGTH), 1)
        res = key_cache[cursor:end_ix]
        if end_ix >= len(key_cache):
            end_ix = 0
        else:
            end_ix += delete_count
        return end_ix, res

    def scan_keys(
            self,
            cursor: int,
            now_mono: float,
            *,
            count: int,
            match: str | None,
            filter_type: KeyType | None) -> tuple[int, list[str]]:
        """
        Scan through all keys.

        Args:
            cursor (int): The scan cursor. A value of 0 initiates the beginning
                of the scan. All other values are defined internally and should
                only be previous return values of scan.
            now_mono (float): The current time.
            count (int): The length hint of the returned values. The actual
                number of returned keys might differ.
            match (str | None): If not None a redis key pattern to filter
                keys.
            filter_type (KeyType | None): If not None filter by key type.

        Returns:
            tuple[int, list[str]]: A tuple of the next cursor and the returned
                keys. If the iteration is complete the next cursor is 0.
        """
        if match is not None:
            prefix, pat = convert_pattern(match)
        else:
            prefix = None
            pat = None

        def process_type(keys: Iterable[str]) -> Iterable[str]:
            if filter_type is None:
                return keys
            return (
                key
                for key in keys
                if self.key_type(key) == filter_type)

        def process_pat(keys: Iterable[str]) -> Iterable[str]:
            if pat is None:
                return keys
            return (
                key
                for key in keys
                if pat.match(key))

        def process(keys: list[str]) -> list[str]:
            if filter_type is None and pat is None:
                return keys
            return list(process_pat(process_type(keys)))

        next_cursor, keys = self._scan(cursor, count, prefix, now_mono)
        if prefix is not None and keys:
            last_key = keys[-1]
            last_key = last_key[:len(prefix)]
            if last_key > prefix:
                next_cursor = 0
        return next_cursor, process(keys)

    def get_all_keys(
            self,
            now_mono: float,
            *,
            match: str | None,
            filter_type: KeyType | None) -> Iterable[str]:
        """
        Completes a full scan through all keys. As this operation is performed
        fully inside a lock, no duplicate keys will be returned.

        Args:
            now_mono (float): The current time.
            match (str | None): If not None a redis key pattern to filter
                keys.
            filter_type (KeyType | None): If not None filter by key type.

        Yields:
            str: The key.
        """
        count = MIN_SCAN_LENGTH
        cursor = 0
        while True:
            cursor, keys = self.scan_keys(
                cursor,
                now_mono,
                count=count,
                match=match,
                filter_type=filter_type)
            yield from keys
            if cursor == 0:
                break
            count = min(count * 2, KEYS_MAX_SCAN)

    def exists(self, key: str) -> bool:
        """
        Checks whether a given key exists. This method does not account for
        expiration.

        Args:
            key (str): The key.

        Returns:
            bool: Whether it exists as any type.
        """
        if self._is_pending_delete(key):
            return False
        if key in self._vals:
            return True
        if key in self._queues:
            return True
        if key in self._hashes:
            return True
        if key in self._sets:
            return True
        if key in self._zorder:
            return True
        if self._parent is not None:
            return self._parent.exists(key)
        return False

    def copy_from_parent(self, key: str) -> None:
        """
        Copies the current value from the parent if the value of the key is not
        already present in the own dictionary.

        Args:
            key (str): The key.
        """
        if self._is_pending_delete(key):
            return
        parent = self._parent
        if parent is None:
            return
        parent.copy_from_parent(key)

        def copy_over(from_dict: dict[str, T], to_dict: dict[str, T]) -> None:
            if key in to_dict:
                return
            other = from_dict.get(key)
            if other is not None:
                to_dict[key] = other

        copy_over(parent.raw_vals(), self._vals)
        copy_over(parent.raw_queues(), self._queues)
        copy_over(parent.raw_hashes(), self._hashes)
        copy_over(parent.raw_sets(), self._sets)
        copy_over(parent.raw_zorder(), self._zorder)
        copy_over(parent.raw_zscores(), self._zscores)

    def delete(self, deletes: set[str]) -> None:
        """
        Deletes keys.

        Args:
            deletes (set[str]): The keys to delete.
        """
        has_delete = False
        if self._parent is not None:
            self._deletes.update(deletes)
            has_delete = True
        for key in deletes:
            if self._expire.pop(key, None) is not None:
                has_delete = True
            if self._vals.pop(key, None) is not None:
                has_delete = True
            if self._queues.pop(key, None) is not None:
                has_delete = True
            if self._hashes.pop(key, None) is not None:
                has_delete = True
            if self._sets.pop(key, None) is not None:
                has_delete = True
            if self._zorder.pop(key, None) is not None:
                has_delete = True
            if self._zscores.pop(key, None) is not None:
                has_delete = True
        if has_delete:
            self._flush_key_cache()
            self._delete_count += 1

    def apply(self, other: 'State', now_mono: float) -> None:
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
            now_mono (float): The current time.
        """
        raw_expire = other.raw_expirations()
        raw_vals = other.raw_vals()
        raw_queues = other.raw_queues()
        raw_hashes = other.raw_hashes()
        raw_sets = other.raw_sets()
        raw_zorder = other.raw_zorder()
        raw_zscores = other.raw_zscores()

        new_keys: set[str] = set()  # NOTE: new means new for the given type
        new_keys.update(set(raw_expire.keys()) - set(self._expire.keys()))
        new_keys.update(set(raw_vals.keys()) - set(self._vals.keys()))
        new_keys.update(set(raw_queues.keys()) - set(self._queues.keys()))
        new_keys.update(set(raw_hashes.keys()) - set(self._hashes.keys()))
        new_keys.update(set(raw_sets.keys()) - set(self._sets.keys()))
        new_keys.update(set(raw_zorder.keys()) - set(self._zorder.keys()))
        new_keys.update(set(raw_zscores.keys()) - set(self._zscores.keys()))
        # NOTE: deleting all new keys makes sure they don't exist as a
        # different type
        self.delete(new_keys)

        self._expire.update(raw_expire)
        self._vals.update(raw_vals)
        self._queues.update(raw_queues)
        self._hashes.update(raw_hashes)
        self._sets.update(raw_sets)
        self._zorder.update(raw_zorder)
        self._zscores.update(raw_zscores)
        self.clean_vals(now_mono)
        self.delete(other.raw_deletes())

        if new_keys:
            self._flush_key_cache()

    def reset(self) -> None:
        """
        Completely resets the state.
        """
        self._expire.clear()
        self._vals.clear()
        self._queues.clear()
        self._hashes.clear()
        self._sets.clear()
        self._zorder.clear()
        self._zscores.clear()
        self._deletes.clear()
        self._flush_key_cache()

    def _prepare_key_for_write(self, key: str, *, is_new: bool) -> None:
        """
        Prepares a key for writing by ensuring that it is not deleted.

        Args:
            key (str): The key.
            is_new (bool): Whether the key is new.
        """
        exists = self.exists(key)
        if not exists or is_new:
            self._flush_key_cache()
        if exists:
            return
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

    def is_alive(self, key: str, now_mono: float) -> bool:
        """
        Checks whether the given key has not expired.

        Args:
            key (str): The key.
            now_mono (float): The current time.

        Returns:
            bool: Whether the key is alive.
        """
        expire = self.get_expire(key)
        if expire is None:
            return True
        if expire > now_mono:
            return True
        return False

    def clean_vals(self, now_mono: float) -> None:
        """
        Cleans up expired values by deleting them.

        Args:
            now_mono (float): The current time.
        """
        if self._parent is not None:
            return
        remove = set()
        for key, value in self._vals.items():
            if self.is_alive(value, now_mono):
                continue
            remove.add(key)
        self.delete(remove)

    def raw_expirations(self) -> dict[str, float]:
        """
        Raw access to the expiration dictionary.

        Returns:
            dict[str, float]: The actual expiration dictionary of this state.
                Here persistent is represented as a value <= 0.
        """
        return self._expire

    def raw_vals(self) -> dict[str, str]:
        """
        Raw access to the value dictionary.

        Returns:
            dict[str, str]: The actual value dictionary of this state.
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

    def raw_sets(self) -> dict[str, set[str]]:
        """
        Raw access to the sets dictionary.

        Returns:
            dict[str, set[str]]: The actual sets dictionary of this state.
        """
        return self._sets

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

    def set_value(self, key: str, value: str, now_mono: float) -> None:
        """
        Sets the value for the given key.

        Args:
            key (str): The key.
            value (str): The value.
            now_mono (float): The current time.
        """
        if not self.is_alive(key, now_mono):
            self.clean_vals(now_mono)
        is_new = False
        if key not in self._vals:
            self.verify_key("string", key)
            is_new = True
        self._prepare_key_for_write(key, is_new=is_new)
        self._vals[key] = value

    def get_value(
            self,
            key: str,
            now_mono: float) -> str | None:
        """
        Retrieves the value associated with the given key.

        Args:
            key (str): The key.
            now_mono (float): The current time.

        Returns:
            tuple[str, float | None] | None: The value of the key and when it
                will expire.
        """
        res = self._vals.get(key)
        if res is not None and not self.is_alive(key, now_mono):
            self.clean_vals(now_mono)
            return None
        if res is None:
            self.verify_key("string", key)
            if self._parent is not None and not self._is_pending_delete(key):
                return self._parent.get_value(key, now_mono)
        return res

    def get_expire(self, key: str) -> float | None:
        """
        Retrieves the expiration of the given key.

        Args:
            key (str): The key.

        Returns:
            float | None: The raw expiration.
        """
        res = self._expire.get(key)
        if res is None:
            if self._parent is not None and not self._is_pending_delete(key):
                return self._parent.get_expire(key)
        if res is not None and res <= 0.0:
            res = None
        return res

    def expire(
            self,
            key: str,
            expire_fn: Callable[[float | None], float | None]) -> bool:
        """
        Conditionally sets the expiration for the given key.

        Args:
            key (str): The key.
            expire_fn (Callable[[float | None], float | None]): Function
                to determine the new expiration value. The argument is the
                previous expiration value.

        Returns:
            bool: Whether the expiration changed.
        """
        if not self.exists(key):
            return False
        prev_expire = self.get_expire(key)
        new_expire = expire_fn(prev_expire)
        if new_expire == prev_expire:
            return False
        self.copy_from_parent(key)
        self._expire[key] = -1.0 if new_expire is None else new_expire
        return True

    def ttl(self, key: str, now_mono: float) -> float | None:
        """
        Computes the time-to-live for the given key.

        Args:
            key (str): The key.
            now_mono (float): The current time.

        Returns:
            float | None: The time in seconds until the key expires. If the
                value is `<= 0.0` it means the key does not have an expiration.
                If the result is None the key does not exist.
        """
        expire = self.get_expire(key)
        if expire is None:
            if not self.exists(key):
                return None
            return -1.0
        res_expire = expire - now_mono
        if res_expire <= 0.0:
            self.clean_vals(now_mono)
            return None
        return res_expire

    def get_queue(self, key: str, now_mono: float) -> collections.deque[str]:
        """
        Returns a queue for writing. If the queue doesn't exist already it will
        be created.

        Args:
            key (str): The key.
            now_mono (float): The current time.

        Returns:
            collections.deque[str]: The queue for writing.
        """
        res = self._queues.get(key)
        if res is not None and not self.is_alive(key, now_mono):
            self.clean_vals(now_mono)
            res = None
        is_new = False
        if res is None:
            self.verify_key("list", key)
            if self._parent is not None and not self._is_pending_delete(key):
                res = collections.deque(self._parent.get_queue(key, now_mono))
            else:
                res = collections.deque()
            self._queues[key] = res
            is_new = True
        self._prepare_key_for_write(key, is_new=is_new)
        return res

    def readonly_queue(
            self, key: str, now_mono: float) -> collections.deque[str] | None:
        """
        Returns a queue for reading only. If the queue doesn't exist None is
        returned.

        Args:
            key (str): The key.
            now_mono (float): The current time.

        Returns:
            collections.deque[str] | None: The readonly queue. Make sure to not
                modify it as it is not a copy. None if the queue doesn't exist.
        """
        res = self._queues.get(key)
        if res is not None and not self.is_alive(key, now_mono):
            self.clean_vals(now_mono)
            return None
        if res is None:
            self.verify_key("list", key)
            if self._parent is None or self._is_pending_delete(key):
                return None
            return self._parent.readonly_queue(key, now_mono)
        return res

    def queue_len(self, key: str, now_mono: float) -> int:
        """
        Computes the length of a queue.

        Args:
            key (str): The key.
            now_mono (float): The current time.

        Returns:
            int: The length of the queue.
        """
        res = self.readonly_queue(key, now_mono)
        if res is None:
            self.verify_key("list", key)
            return 0
        return len(res)

    def get_hash(self, key: str, now_mono: float) -> dict[str, str]:
        """
        Returns a hash for writing. If the hash doesn't exist already it will
        be created.

        Args:
            key (str): The key.
            now_mono (float): The current time.

        Returns:
            dict[str, str]: The hash for writing.
        """
        res = self._hashes.get(key)
        if res is not None and not self.is_alive(key, now_mono):
            self.clean_vals(now_mono)
            res = None
        is_new = False
        if res is None:
            self.verify_key("hash", key)
            if self._parent is not None and not self._is_pending_delete(key):
                res = dict(self._parent.get_hash(key, now_mono))
            else:
                res = {}
            self._hashes[key] = res
            is_new = True
        self._prepare_key_for_write(key, is_new=is_new)
        return res

    def readonly_hash(
            self, key: str, now_mono: float) -> dict[str, str] | None:
        """
        Returns a hash for reading only. If the hash doesn't exist None is
        returned.

        Args:
            key (str): The key.
            now_mono (float): The current time.

        Returns:
            dict[str, str] | None: The readonly hash. Make sure to not
                modify it as it is not a copy. None if the hash doesn't exist.
        """
        res = self._hashes.get(key)
        if res is not None and not self.is_alive(key, now_mono):
            self.clean_vals(now_mono)
            return None
        if res is None:
            self.verify_key("hash", key)
            if self._parent is None or self._is_pending_delete(key):
                return None
            return self._parent.readonly_hash(key, now_mono)
        return res

    def get_set(self, key: str, now_mono: float) -> set[str]:
        """
        Returns a set for writing. If the set doesn't exist already it will
        be created.

        Args:
            key (str): The key.
            now_mono (float): The current time.

        Returns:
            set[str]: The set for writing.
        """
        res = self._sets.get(key)
        if res is not None and not self.is_alive(key, now_mono):
            self.clean_vals(now_mono)
            res = None
        is_new = False
        if res is None:
            self.verify_key("set", key)
            if self._parent is not None and not self._is_pending_delete(key):
                res = set(self._parent.get_set(key, now_mono))
            else:
                res = set()
            self._sets[key] = res
            is_new = True
        self._prepare_key_for_write(key, is_new=is_new)
        return res

    def readonly_set(self, key: str, now_mono: float) -> set[str] | None:
        """
        Returns a set for reading only. If the set doesn't exist None is
        returned.

        Args:
            key (str): The key.
            now_mono (float): The current time.

        Returns:
            set[str] | None: The readonly set. Make sure to not
            modify it as it is not a copy. None if the set doesn't exist.
        """
        res = self._sets.get(key)
        if res is not None and not self.is_alive(key, now_mono):
            self.clean_vals(now_mono)
            return None
        if res is None:
            self.verify_key("set", key)
            if self._parent is None or self._is_pending_delete(key):
                return None
            return self._parent.readonly_set(key, now_mono)
        return res

    def get_zset(
            self,
            key: str,
            now_mono: float) -> tuple[list[str], dict[str, float]]:
        """
        Returns a zset for writing. If the zset doesn't exist already it
        will be created.

        Args:
            key (str): The key.
            now_mono (float): The current time.

        Returns:
            tuple[list[str], dict[str, float]]: The tuple of zorder and zscores
            for writing.
        """
        rorder = self._zorder.get(key)
        rscores = self._zscores.get(key)
        if rorder is not None and not self.is_alive(key, now_mono):
            self.clean_vals(now_mono)
            rorder = None
            rscores = None
        is_new = False
        if rorder is None:
            self.verify_key("zset", key)
            if self._parent is not None and not self._is_pending_delete(key):
                porder, pscores = self._parent.get_zset(key, now_mono)
                rorder = list(porder)
                rscores = dict(pscores)
            else:
                rorder = []
                rscores = {}
            self._zorder[key] = rorder
            self._zscores[key] = rscores
            is_new = True
        self._prepare_key_for_write(key, is_new=is_new)
        assert rscores is not None
        return (rorder, rscores)

    def readonly_zorder(self, key: str, now_mono: float) -> list[str] | None:
        """
        Returns a zorder for reading only. If the zorder doesn't exist None is
        returned.

        Args:
            key (str): The key.
            now_mono (float): The current time.

        Returns:
            list[str] | None: The readonly zorder. Make sure to not
            modify it as it is not a copy. None if the zorder doesn't exist.
        """
        res = self._zorder.get(key)
        if res is not None and not self.is_alive(key, now_mono):
            self.clean_vals(now_mono)
            return None
        if res is None:
            self.verify_key("zset", key)
            if self._parent is None or self._is_pending_delete(key):
                return None
            return self._parent.readonly_zorder(key, now_mono)
        return res

    def readonly_zscores(
            self, key: str, now_mono: float) -> dict[str, float] | None:
        """
        Returns a zscores for reading only. If the zscores doesn't exist None
        is returned.

        Args:
            key (str): The key.
            now_mono (float): The current time.

        Returns:
            dict[str, float] | None: The readonly zscores. Make sure to not
            modify it as it is not a copy. None if the zscores doesn't exist.
        """
        res = self._zscores.get(key)
        if res is not None and not self.is_alive(key, now_mono):
            self.clean_vals(now_mono)
            return None
        if res is None:
            self.verify_key("zset", key)
            if self._parent is None or self._is_pending_delete(key):
                return None
            return self._parent.readonly_zscores(key, now_mono)
        return res

    def zorder_len(self, key: str, now_mono: float) -> int:
        """
        Computes the length of a zorder.

        Args:
            key (str): The key.
            now_mono (float): The current time.

        Returns:
            int: The length of the zorder.
        """
        res = self.readonly_zorder(key, now_mono)
        if res is None:
            self.verify_key("zset", key)
            return 0
        return len(res)

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}["
            f"expire={self._expire},"
            f"vals={self._vals},"
            f"queues={dict(self._queues)},"
            f"hashes={dict(self._hashes)},"
            f"sets={dict(self._sets)},"
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
        self._now_mono: tuple[float, datetime] | None = None

    def set_mono(self, now_mono: tuple[float, datetime] | None) -> None:
        """
        Sets the current time.

        Args:
            now_mono (tuple[float, datetime] | None): The current time for
                pipelines as monotonic time and datetime. Otherwise None.
        """
        self._now_mono = now_mono

    def get_mono(self) -> float:
        """
        Returns the current monotonic time.

        Returns:
            float: The current time or the time associated with this machine if
                it is a pipeline.
        """
        if self._now_mono is None:
            return time.monotonic()
        return self._now_mono[0]

    def get_ts(self) -> datetime:
        """
        Returns the current time.

        Returns:
            datetime: The current time or the time associated with this machine
                if it is a pipeline.
        """
        return now() if self._now_mono is None else self._now_mono[1]

    def get_state(self) -> State:
        """
        Returns the managed state.

        Returns:
            State: The associated state.
        """
        return self._state

    def exists(self, *keys: str) -> int:
        now_mono = self.get_mono()
        res = 0
        for key in keys:
            if self._state.exists(key) and self._state.is_alive(key, now_mono):
                res += 1
        return res

    def delete(self, *keys: str) -> int:
        res = self.exists(*keys)
        self._state.delete(set(keys))
        return res

    def key_type(self, key: str) -> KeyType | None:
        now_mono = self.get_mono()
        if not self._state.is_alive(key, now_mono):
            return None
        return self._state.key_type(key)

    def scan(
            self,
            cursor: int,
            *,
            match: str | None = None,
            count: int | None = None,
            filter_type: KeyType | None = None) -> tuple[int, list[str]]:
        now_mono = self.get_mono()
        return self._state.scan_keys(
            cursor,
            now_mono,
            count=10 if count is None else count,
            match=match,
            filter_type=filter_type)

    def keys_block(
            self,
            *,
            match: str | None = None,
            filter_type: KeyType | None = None) -> list[str]:
        now_mono = self.get_mono()
        # NOTE: get_all_keys cannot have duplicates
        return list(self._state.get_all_keys(
            now_mono, match=match, filter_type=filter_type))

    def flushall(self) -> None:
        # NOTE: this method cannot be used in a pipeline as the effect
        # is immediate!
        if self._state.has_parent():
            raise RuntimeError("cannot use flushall with a parent!")
        self._state.reset()

    @overload
    def set_value(
            self,
            key: str,
            value: str,
            *,
            mode: RSetMode,
            return_previous: Literal[True],
            expire_timestamp: datetime | None,
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
            expire_timestamp: datetime | None,
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
            expire_timestamp: datetime | None = None,
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
            expire_timestamp: datetime | None = None,
            expire_in: float | None = None,
            keep_ttl: bool = False) -> str | bool | None:
        state = self._state
        now_mono = self.get_mono()
        now_ts = self.get_ts()
        expire = compute_expire(
            now_mono,
            now_ts,
            expire_timestamp=expire_timestamp,
            expire_in=expire_in)
        prev_value = state.get_value(key, now_mono)
        do_set = False
        if mode == RSM_ALWAYS:
            do_set = True
        elif mode == RSM_EXISTS:
            do_set = prev_value is not None
        elif mode == RSM_MISSING:
            do_set = prev_value is None
        else:
            raise ValueError(f"unknown mode: {mode}")
        if do_set:
            state.set_value(key, value, now_mono)
            state.expire(
                key, lambda prev_expire: prev_expire if keep_ttl else expire)
        if return_previous:
            return prev_value
        return do_set

    def get_value(self, key: str) -> str | None:
        now_mono = self.get_mono()
        return self._state.get_value(key, now_mono)

    def expire(
            self,
            key: str,
            *,
            mode: RExpireMode = REX_ALWAYS,
            expire_timestamp: datetime | None = None,
            expire_in: float | None = None) -> bool:
        state = self._state
        now_mono = self.get_mono()
        now_ts = self.get_ts()
        expire = compute_expire(
            now_mono,
            now_ts,
            expire_timestamp=expire_timestamp,
            expire_in=expire_in)

        def choose_expire(prev_expire: float | None) -> float | None:
            if expire is None:
                return prev_expire if mode == REX_EARLIER else None
            if mode == REX_ALWAYS:
                return expire
            if mode == REX_PERSIST:
                return expire if prev_expire is None else prev_expire
            if mode == REX_EXPIRE:
                return None if prev_expire is None else expire
            if mode == REX_EARLIER:
                if prev_expire is None:
                    return expire
                return min(expire, prev_expire)
            if mode == REX_LATER:
                if prev_expire is None:
                    return None
                return max(expire, prev_expire)
            raise ValueError(f"unknown mode: {mode}")

        if not state.is_alive(key, now_mono) or not state.exists(key):
            return False
        return state.expire(key, choose_expire)

    def ttl(self, key: str) -> float | None:
        now_mono = self.get_mono()
        return self._state.ttl(key, now_mono)

    def incrby(self, key: str, inc: float | int) -> float:
        now_mono = self.get_mono()
        val = self._state.get_value(key, now_mono)
        if val is None:
            num = inc
        else:
            num = float(val) + inc
        self._state.set_value(key, to_number_str(num), now_mono)
        return num

    def lpush(self, key: str, *values: str) -> int:
        now_mono = self.get_mono()
        queue = self._state.get_queue(key, now_mono)
        queue.extendleft(values)
        return len(queue)

    def rpush(self, key: str, *values: str) -> int:
        now_mono = self.get_mono()
        queue = self._state.get_queue(key, now_mono)
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
        now_mono = self.get_mono()
        queue = self._state.get_queue(key, now_mono)
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
        now_mono = self.get_mono()
        queue = self._state.get_queue(key, now_mono)
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
        now_mono = self.get_mono()
        queue = self._state.readonly_queue(key, now_mono)
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
        now_mono = self.get_mono()
        return self._state.queue_len(key, now_mono)

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        now_mono = self.get_mono()
        count = 0
        zorder, zscores = self._state.get_zset(key, now_mono)
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
        now_mono = self.get_mono()
        zorder, zscores = self._state.get_zset(key, now_mono)
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
        now_mono = self.get_mono()
        zorder, zscores = self._state.get_zset(key, now_mono)
        res = []
        remain = 1 if count is None else count
        while remain > 0 and zorder:
            name = zorder.pop(0)
            score = zscores.pop(name)
            res.append((name, score))
            remain -= 1
        return res

    def zrange(self, key: str, start: int, stop: int) -> list[str]:
        now_mono = self.get_mono()
        zorder = self._state.readonly_zorder(key, now_mono)
        if zorder is None:
            return []
        astop: int | None = stop
        if astop == -1 or astop is None:  # NOTE: mypy workaround
            astop = None
        else:
            astop += 1
        return zorder[start:astop]

    def zcard(self, key: str) -> int:
        now_mono = self.get_mono()
        return self._state.zorder_len(key, now_mono)

    def hset(self, key: str, mapping: dict[str, str]) -> int:
        now_mono = self.get_mono()
        obj = self._state.get_hash(key, now_mono)
        res = len(set(mapping.keys()).difference(obj.keys()))
        obj.update(mapping)
        return res

    def hdel(self, key: str, *fields: str) -> int:
        now_mono = self.get_mono()
        obj = self._state.get_hash(key, now_mono)
        res = 0
        for field in fields:
            if obj.pop(field, None) is not None:
                res += 1
        if not obj:
            self._state.delete({key})
        return res

    def hget(self, key: str, field: str) -> str | None:
        now_mono = self.get_mono()
        res = self._state.readonly_hash(key, now_mono)
        if res is None:
            return None
        return res.get(field)

    def hmget(self, key: str, *fields: str) -> dict[str, str | None]:
        now_mono = self.get_mono()
        res = self._state.readonly_hash(key, now_mono)
        if res is None:
            return {}
        return {
            field: res.get(field)
            for field in fields
        }

    def hincrby(self, key: str, field: str, inc: float | int) -> float:
        now_mono = self.get_mono()
        res = self._state.get_hash(key, now_mono)
        num = float(res.get(field, 0)) + inc
        res[field] = to_number_str(num)
        return num

    def hkeys(self, key: str) -> list[str]:
        now_mono = self.get_mono()
        res = self._state.readonly_hash(key, now_mono)
        if res is None:
            return []
        return list(res.keys())

    def hvals(self, key: str) -> list[str]:
        now_mono = self.get_mono()
        res = self._state.readonly_hash(key, now_mono)
        if res is None:
            return []
        return list(res.values())

    def hgetall(self, key: str) -> dict[str, str]:
        now_mono = self.get_mono()
        res = self._state.readonly_hash(key, now_mono)
        if res is None:
            return {}
        return dict(res)

    def sadd(self, key: str, *values: str) -> int:
        now_mono = self.get_mono()
        obj = self._state.get_set(key, now_mono)
        before = len(obj)
        obj.update(values)
        return len(obj) - before

    def srem(self, key: str, *values: str) -> int:
        now_mono = self.get_mono()
        obj = self._state.get_set(key, now_mono)
        before = len(obj)
        obj.difference_update(set(values))
        if not obj:
            self._state.delete({key})
        return before - len(obj)

    def sismember(self, key: str, value: str) -> bool:
        now_mono = self.get_mono()
        obj = self._state.readonly_set(key, now_mono)
        if obj is None:
            return False
        return value in obj

    def scard(self, key: str) -> int:
        now_mono = self.get_mono()
        obj = self._state.readonly_set(key, now_mono)
        if obj is None:
            return 0
        return len(obj)

    def smembers(self, key: str) -> set[str]:
        now_mono = self.get_mono()
        obj = self._state.readonly_set(key, now_mono)
        if obj is None:
            return set()
        return set(obj)

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}[state={self._state}]")

    def __repr__(self) -> str:
        return self.__str__()
