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
"""Test keys operations."""


import re
from collections.abc import Callable, Iterable
from test.util import get_setup
from typing import Any, cast

import pytest

from redipy.api import KeyType, PipelineAPI
from redipy.backend.runtime import Runtime
from redipy.main import Redis
from redipy.util import convert_pattern


KEY_TYPE_REG: list[KeyType] = [
    "string",
    "list",
    "set",
    "zset",
    "hash",
]


KEY_TYPE_MODES: list[KeyType | None] = [
    "string",
    "list",
    "set",
    "zset",
    "hash",
    None,
]


DEFAULTS: dict[
        KeyType, Callable[[Runtime | PipelineAPI, str, int], object]] = {
    "string": lambda rt, key, ix: rt.set_value(key, f"v{ix}"),
    "list": lambda rt, key, ix: rt.rpush(key, f"v{ix}"),
    "set": lambda rt, key, ix: rt.sadd(key, f"v{ix}"),
    "zset": lambda rt, key, ix: rt.zadd(key, {f"v{ix}": ix}),
    "hash": lambda rt, key, ix: rt.hset(key, {"value": f"v{ix}"}),
}


CHECKS: dict[KeyType, Callable[[Runtime, str, int], bool]] = {
    "string": lambda rt, key, ix: rt.get_value(key) == f"v{ix}",
    "list": lambda rt, key, ix: set(rt.lrange(key, 0, -1)) == {f"v{ix}"},
    "set": lambda rt, key, ix: rt.smembers(key) == {f"v{ix}"},
    "zset": lambda rt, key, ix: rt.zrange(key, 0, -1) == [f"v{ix}"],
    "hash": lambda rt, key, ix: rt.hgetall(key) == {"value": f"v{ix}"},
}


PIPE_CHECKS: dict[KeyType, Callable[[PipelineAPI, str], None]] = {
    "string": lambda pipe, key: pipe.get_value(key),
    "list": lambda pipe, key: pipe.lrange(key, 0, -1),
    "set": lambda pipe, key: pipe.smembers(key),
    "zset": lambda pipe, key: pipe.zrange(key, 0, -1),
    "hash": lambda pipe, key: pipe.hgetall(key),
}


PIPE_EXPECTED: dict[KeyType, Callable[[Any, int], bool]] = {
    "string": lambda res, ix: res == f"v{ix}",
    "list": lambda res, ix: set(res) == {f"v{ix}"},
    "set": lambda res, ix: res == {f"v{ix}"},
    "zset": lambda res, ix: res == [f"v{ix}"],
    "hash": lambda res, ix: res == {"value": f"v{ix}"},
}


@pytest.mark.parametrize("types", KEY_TYPE_MODES)
@pytest.mark.parametrize("k_add", [None, 11])
@pytest.mark.parametrize("k_del", [None, 7])
@pytest.mark.parametrize("match", [None, "k1*", "k???"])
@pytest.mark.parametrize("count", [30, 200, 500, 1000, 2000, 10000, 100000])
@pytest.mark.parametrize("rt_lua", [False, True])
def test_scan(
        types: KeyType | None,
        k_add: int | None,
        k_del: int | None,
        match: str | None,
        count: int,
        rt_lua: bool) -> None:
    """
    Test scan command with concurrent writes and deletes.

    Args:
        types (KeyType | None): Which key types to use.
        k_add (int | None): Which keys to add late.
        k_del (int | None): Which keys to delete.
        match (str | None): The filter pattern.
        count (int): How many keys to generate.
        rt_lua (bool): Whether to use the redis or memory runtime.
    """
    rt = get_setup("test_keys", rt_lua)

    if count > 200 and (k_add is None or k_del is None):
        # NOTE: those tests wouldn't add anything interesting
        return
    if count > 2000 and (types is not None or match is None):
        # NOTE: we reduce test cases for large data
        return
    if rt_lua and count >= 10000:
        # NOTE: redis backend tests get really slow with many keys
        return

    all_keys: dict[str, KeyType] = {}
    keys: dict[str, KeyType] = {}
    maybe: dict[str, KeyType] = {}
    later: list[tuple[KeyType, str]] = []

    def gen(
            start: int,
            stop: int,
            step: int = 1) -> Iterable[tuple[KeyType, str]]:
        yield from ((
            KEY_TYPE_REG[ix % len(KEY_TYPE_REG)] if types is None else types,
            f"k{ix}",
        ) for ix in range(start, stop, step))

    def extract(key: str) -> int:
        return int(key[1:])

    for key_type, key in gen(0, count):
        ix = extract(key)
        if k_add is not None and ix % k_add == 0:
            later.append((key_type, key))
            continue
        DEFAULTS[key_type](rt, key, ix)
        keys[key] = key_type
        all_keys[key] = key_type
    assert len(keys) + len(later) == count
    total: set[str] = set()

    def cur_max() -> int:
        if not total:
            return -1
        return max((extract(key) for key in total))

    def cond_op(
            arr: Iterable[tuple[KeyType, str]],
            cond: Callable[[int], bool],
            *,
            is_add: bool) -> None:
        for cur_type, cur_key in arr:
            cur_ix = extract(cur_key)
            if not cond(cur_ix):
                continue
            maybe[cur_key] = cur_type
            if is_add:
                DEFAULTS[cur_type](rt, cur_key, cur_ix)
                # NOTE: we do not add to our reference since scan doesn't
                # guarantee those keys
                all_keys[cur_key] = cur_type
            else:
                exp_type = keys.pop(cur_key, None)
                if exp_type is None:
                    # NOTE: key might have never been added
                    rt.delete(cur_key)
                    assert rt.exists(cur_key) == 0
                    assert rt.key_type(cur_key) is None
                else:
                    assert rt.exists(cur_key) == 1
                    assert rt.key_type(cur_key) == exp_type
                    assert rt.delete(cur_key) == 1
    iters = 0
    cursor = 0
    scan_count = 50
    while True:
        cursor, partial = rt.scan(cursor, match=match, count=scan_count)
        total.update(partial)
        if cursor == 0:
            break
        iters += 1
        if iters == 3:
            cmax_add = cur_max()
            cond_op(later, lambda ix: ix < cmax_add, is_add=True)
        elif iters == 5 and k_del is not None:
            cmax_del = cur_max()
            cond_op((gen(0, cmax_del, k_del)), lambda _: True, is_add=False)
        elif iters == 7:
            cond_op(later, lambda _: True, is_add=True)
        elif iters == 11 and k_del is not None:
            cond_op(gen(0, count, k_del), lambda _: True, is_add=False)
        elif iters == 12:
            scan_count = 1000  # NOTE: return the rest a bit quicker

    pat: re.Pattern | None = None
    if match:
        _, pat = convert_pattern(match)
    for ref_key, ref_type in keys.items():
        if pat is not None and not pat.match(ref_key):
            assert ref_key not in total
            continue
        assert ref_key in total
        assert rt.exists(ref_key) > 0
        assert rt.key_type(ref_key) == ref_type
        assert CHECKS[ref_type](rt, ref_key, extract(ref_key))
        assert rt.delete(ref_key) == 1
    for m_key, m_type in maybe.items():
        if pat is not None and not pat.match(m_key):
            continue
        if rt.exists(m_key) == 0:
            continue
        assert rt.key_type(m_key) == m_type
        assert CHECKS[m_type](rt, m_key, extract(m_key))
        assert rt.delete(m_key) == 1
    for rem_key in total:
        if rt.exists(rem_key):
            rem_type = rt.key_type(rem_key)
            assert rem_type is not None
            assert CHECKS[rem_type](rt, rem_key, extract(rem_key))
            assert rt.delete(rem_key) == 1
    for final_key in rt.keys(block=True):
        assert final_key in all_keys
        final_type = all_keys[final_key]
        assert rt.exists(final_key) > 0
        assert rt.key_type(final_key) == final_type
        assert CHECKS[final_type](rt, final_key, extract(final_key))
        assert rt.delete(final_key) == 1


@pytest.mark.parametrize("types", KEY_TYPE_MODES)
@pytest.mark.parametrize("match", [None, "k1*", "k???"])
@pytest.mark.parametrize("block", [False, True])
@pytest.mark.parametrize("rt_lua", [False, True])
def test_keys(
        types: KeyType | None,
        match: str | None,
        block: bool,
        rt_lua: bool) -> None:
    """
    Test keys command.

    Args:
        types (KeyType | None): Which key types to use.
        match (str | None): The filter pattern.
        block (bool): Whether to use blocking operations.
        rt_lua (bool): Whether to use the redis or memory runtime.
    """
    rt = get_setup("test_keys", rt_lua)
    if rt_lua and types is not None:
        return  # FIXME: implement filter_type for redis connections

    rt_alt = get_setup("test_keys_alt", rt_lua)
    # NOTE: this value should never be returned by keys or scan
    rt_alt.set_value("foo", "clobber")

    def gen(
            start: int,
            stop: int,
            step: int = 1) -> Iterable[tuple[KeyType, str]]:
        yield from ((
            KEY_TYPE_REG[ix % len(KEY_TYPE_REG)] if types is None else types,
            f"k{ix}",
        ) for ix in range(start, stop, step))

    def extract(key: str) -> int:
        return int(key[1:])

    keys: dict[str, KeyType] = {}
    assert rt.keys(match=match, block=block, filter_type=types) == set()
    with rt.pipeline() as pipe:
        pipe.scan(0, match=match, filter_type=types)
        pipe.keys(match=match, filter_type=types)
        scan_res, keys_res = pipe.execute()
        # NOTE: scan_res[0] could be != 0 if other keys exist in the db
        assert scan_res[1] == []
        assert keys_res == []
        for key_type, key in gen(0, 100):
            ix = extract(key)
            DEFAULTS[key_type](pipe, key, ix)
            keys[key] = key_type
    assert rt.keys(block=block) == set(keys.keys())

    def for_type(filter_type: KeyType | None) -> None:
        total = rt.keys(block=block, match=match, filter_type=filter_type)
        pat: re.Pattern | None = None
        if match:
            _, pat = convert_pattern(match)

        def get_type_check(ref_type: str) -> Callable[[Any], bool]:
            return lambda res: res == ref_type

        def get_pipe_check(
                ref_key: str, ref_type: KeyType) -> Callable[[Any], bool]:
            expected = PIPE_EXPECTED[ref_type]
            ix = extract(ref_key)
            return lambda res: expected(res, ix)

        checks: list[tuple[Callable[[Any], bool], str]] = []
        with rt.pipeline() as pipe:
            for ref_key, ref_type in keys.items():
                if pat is not None and not pat.match(ref_key):
                    assert ref_key not in total
                    continue
                if filter_type is not None and ref_type != filter_type:
                    assert ref_key not in total
                    continue
                assert ref_key in total
                pipe.exists(ref_key)
                checks.append((lambda res: int(res) > 0, f"exists {ref_key}"))
                pipe.key_type(ref_key)
                checks.append((
                    get_type_check(ref_type),
                    f"type {ref_key} {ref_type}"))
                PIPE_CHECKS[ref_type](pipe, ref_key)
                checks.append((
                    get_pipe_check(ref_key, ref_type),
                    f"check {ref_key} {ref_type}"))
            results = pipe.execute()
        for cur_res, cur_check in zip(results, checks):
            check, check_name = cur_check
            assert check(cur_res), f"{check_name} result: {cur_res}"

    test_types: list[KeyType | None] = [None] if rt_lua else KEY_TYPE_MODES
    for test_type in test_types:
        for_type(test_type)

    with rt.pipeline() as pipe:
        pipe.keys()
        finals_keys: list[str] = cast(list, pipe.execute()[0])

    for final_key in finals_keys:
        assert final_key in keys
        final_type = keys[final_key]
        assert rt.exists(final_key) > 0
        assert rt.key_type(final_key) == final_type
        assert CHECKS[final_type](rt, final_key, extract(final_key))
        assert rt.delete(final_key) == 1

    assert rt.keys(match=match, block=block, filter_type=types) == set()
    with rt.pipeline() as pipe:
        pipe.scan(0, match=match, filter_type=types)
        pipe.keys(match=match, filter_type=types)
        scan_final, keys_final = pipe.execute()
        assert scan_final[1] == []
        assert keys_final == []
    cursor = scan_final[0]
    while cursor != 0:
        cursor, scan_arr = rt.scan(cursor, match=match, filter_type=types)
        assert scan_arr == []

    assert rt_alt.get_value("foo") == "clobber"


@pytest.mark.parametrize("rt_lua", [False, True])
def test_flushall(rt_lua: bool) -> None:
    """
    Test flushall command.

    Args:
        rt_lua (bool): Whether to use the redis or memory runtime.
    """
    rt = get_setup("test_flushall", rt_lua)

    rt_alt = Redis(rt=get_setup("test_flushall_alt", rt_lua))
    assert rt_alt.keys() == set()
    # NOTE: this value must not be removed
    rt_alt.set_value("foo", "stayingalive")

    assert rt.keys() == set()
    assert rt_alt.get_value("foo") == "stayingalive"
    rt.flushall()
    assert rt.keys() == set()
    assert rt_alt.get_value("foo") == "stayingalive"

    def gen(
            start: int,
            stop: int,
            step: int = 1) -> Iterable[tuple[KeyType, str]]:
        yield from ((
            KEY_TYPE_REG[ix % len(KEY_TYPE_REG)],
            f"k{ix}",
        ) for ix in range(start, stop, step))

    def extract(key: str) -> int:
        return int(key[1:])

    keys: dict[str, KeyType] = {}
    for key_type, key in gen(0, 100):
        ix = extract(key)
        DEFAULTS[key_type](rt, key, ix)
        keys[key] = key_type

    assert rt.keys() == set(keys.keys())
    assert rt_alt.get_value("foo") == "stayingalive"
    rt.flushall()
    assert rt.keys() == set()
    assert rt_alt.get_value("foo") == "stayingalive"
    assert rt_alt.key_type("foo") == "string"
    assert rt_alt.keys_block() == ["foo"]

    with rt_alt.pipeline() as pipe:
        pipe.delete("foo")
        pipe.key_type("foo")
        pipe.set_value("foo", "bar")
        assert pipe.execute() == [1, None, True]

    assert rt_alt.get_value("foo") == "bar"
    rt_alt.flushall()
    assert rt_alt.keys() == set()
    assert rt_alt.get_value("foo") is None
