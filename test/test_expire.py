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
"""Test expire operations."""


import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from test.util import get_setup
from typing import Any, TypeAlias

import pytest

from redipy.api import (
    KeyType,
    PipelineAPI,
    REX_ALWAYS,
    REX_EARLIER,
    REX_EXPIRE,
    REX_LATER,
    REX_PERSIST,
    RExpireMode,
)
from redipy.main import Redis
from redipy.util import now


Action: TypeAlias = tuple[Callable[[Any], bool], Callable[[Any], str]]


KEY_TYPE_REG: list[KeyType] = [
    "string",
    "list",
    "set",
    "zset",
    "hash",
]


DEFAULTS: dict[
        KeyType, Callable[[Redis | PipelineAPI, str, int], object]] = {
    "string": lambda rt, key, ix: rt.set_value(key, f"v{ix}"),
    "list": lambda rt, key, ix: rt.rpush(key, f"v{ix}"),
    "set": lambda rt, key, ix: rt.sadd(key, f"v{ix}"),
    "zset": lambda rt, key, ix: rt.zadd(key, {f"v{ix}": ix}),
    "hash": lambda rt, key, ix: rt.hset(key, {"value": f"v{ix}"}),
}


CHECKS: dict[KeyType, Callable[[Redis, str, int], bool]] = {
    "string": lambda rt, key, ix: rt.get_value(key) == f"v{ix}",
    "list": lambda rt, key, ix: set(rt.lrange(key, 0, -1)) == {f"v{ix}"},
    "set": lambda rt, key, ix: rt.smembers(key) == {f"v{ix}"},
    "zset": lambda rt, key, ix: rt.zrange(key, 0, -1) == [f"v{ix}"],
    "hash": lambda rt, key, ix: rt.hgetall(key) == {"value": f"v{ix}"},
}


MISSING: dict[KeyType, Callable[[Redis, str], bool]] = {
    "string": lambda rt, key: rt.get_value(key) is None,
    "list": lambda rt, key: rt.lrange(key, 0, -1) == [],
    "set": lambda rt, key: rt.smembers(key) == set(),
    "zset": lambda rt, key: rt.zrange(key, 0, -1) == [],
    "hash": lambda rt, key: rt.hgetall(key) == {},
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


PIPE_MISSING: dict[KeyType, Callable[[Any], bool]] = {
    "string": lambda res: res is None,
    "list": lambda res: res == [],
    "set": lambda res: res == set(),
    "zset": lambda res: res == [],
    "hash": lambda res: res == {},
}


@pytest.mark.parametrize("types", KEY_TYPE_REG)
@pytest.mark.parametrize("is_pipe", [False, True])
@pytest.mark.parametrize("rt_lua", [False, True])
def test_expire(
        types: KeyType,
        is_pipe: bool,
        rt_lua: bool) -> None:
    """
    Test expire command.

    Args:
        types (KeyType): Which key types to use.
        is_pipe (bool): Whether the operations are performed in a pipeline.
        rt_lua (bool): Whether to use the redis or memory runtime.
    """
    redis = Redis(rt=get_setup("test_expire", rt_lua))
    print(f"is_pipe={is_pipe} rt_lua={rt_lua} types={types}")
    if rt_lua and is_pipe:
        # FIXME: figure out why pipelines don't work correctly with redis
        return

    @contextmanager
    def block() -> Iterator[tuple[Redis | PipelineAPI, list[Action]]]:
        actions: list[Action] = []
        if is_pipe:
            with redis.pipeline() as pipe:
                yield pipe, actions
                res = pipe.execute()
                assert len(res) == len(actions)
                for val, (call, msg) in zip(res, actions):
                    assert call(val), msg(val)
        else:
            yield redis, actions
            assert len(actions) == 0

    def create(
            rt: Redis | PipelineAPI,
            actions: list[Action],
            key: str,
            ix: int) -> None:
        DEFAULTS[types](rt, key, ix)
        if is_pipe:
            actions.append((lambda _: True, lambda _: "?"))

    def expire(
            rt: Redis | PipelineAPI,
            actions: list[Action],
            key: str,
            *,
            expect: bool,
            mode: RExpireMode = REX_ALWAYS,
            expire_in: float | None = None,
            expire_timestamp: datetime | None = None) -> None:
        print(
            f"expire key {key} with {mode} in {expire_in or expire_timestamp}")
        if isinstance(rt, PipelineAPI):
            rt.expire(
                key,
                mode=mode,
                expire_in=expire_in,
                expire_timestamp=expire_timestamp)
            actions.append((
                lambda val: val == expect,
                lambda val: f"val={val} == expect={expect}"))
        else:
            assert expect == rt.expire(
                key,
                mode=mode,
                expire_in=expire_in,
                expire_timestamp=expire_timestamp)

    def ttl(
            rt: Redis | PipelineAPI,
            actions: list[Action],
            key: str,
            *,
            expect: bool | None) -> None:
        estr = None if expect is None else ("> 0.0" if expect else "<= 0.0")

        def check_ttl(res: float | None) -> bool:
            if expect is None:
                return res is None
            if res is None:
                return False
            if expect:
                return res > 0.0
            return res <= 0.0

        if isinstance(rt, PipelineAPI):
            rt.ttl(key)
            actions.append((
                check_ttl,
                lambda val: f"ttl of key {key} is {val} expected {estr}"))
        else:
            assert check_ttl(rt.ttl(key))

    def check(
            rt: Redis | PipelineAPI,
            actions: list[Action],
            key: str,
            ix: int | None) -> None:
        if ix is None:
            if isinstance(rt, PipelineAPI):
                PIPE_CHECKS[types](rt, key)
                actions.append(
                    (PIPE_MISSING[types], lambda val: f"{key} missing {val}"))
                rt.exists(key)
                actions.append(
                    (lambda val: val == 0, lambda val: f"exists {val} != 0"))
            else:
                assert MISSING[types](rt, key)
                assert rt.exists(key) == 0
            return
        if isinstance(rt, PipelineAPI):
            PIPE_CHECKS[types](rt, key)
            actions.append((
                lambda val: PIPE_EXPECTED[types](val, ix),
                lambda val: f"expected value with {key} and {ix} got {val}"))
        else:
            assert CHECKS[types](rt, key, ix)

    with block() as (rt, actions):
        create(rt, actions, "a", 1)
        create(rt, actions, "b", 2)
        create(rt, actions, "c", 3)
        create(rt, actions, "d", 4)
        create(rt, actions, "e", 5)
        create(rt, actions, "f", 6)
        create(rt, actions, "g", 7)
        create(rt, actions, "h", 8)
        create(rt, actions, "i", 9)
        create(rt, actions, "j", 10)
        create(rt, actions, "k", 11)

    expire_timestamp = now() + timedelta(seconds=0.3 if is_pipe else 0.1)
    with block() as (rt, actions):
        expire(rt, actions, "a", expect=False, mode=REX_EXPIRE, expire_in=0.1)
        expire(rt, actions, "b", expect=True, mode=REX_ALWAYS, expire_in=0.1)
        expire(
            rt,
            actions,
            "c",
            expect=True,
            mode=REX_PERSIST,
            expire_timestamp=expire_timestamp)
        expire(
            rt,
            actions,
            "d",
            expect=True,
            mode=REX_EARLIER,
            expire_timestamp=expire_timestamp)
        expire(
            rt,
            actions,
            "e",
            expect=False,
            mode=REX_LATER,
            expire_timestamp=expire_timestamp)
        expire(rt, actions, "f", expect=True, expire_in=0.3)
        expire(rt, actions, "g", expect=True, expire_in=0.3)
        expire(rt, actions, "h", expect=True, expire_in=1.0)
        expire(rt, actions, "i", expect=True, expire_in=0.3)
        expire(rt, actions, "j", expect=True, expire_in=1.0)
        expire(rt, actions, "k", expect=True, expire_in=0.5)

        check(rt, actions, "a", 1)
        check(rt, actions, "b", 2)
        check(rt, actions, "c", 3)
        check(rt, actions, "d", 4)
        check(rt, actions, "e", 5)
        check(rt, actions, "f", 6)
        check(rt, actions, "g", 7)
        check(rt, actions, "h", 8)
        check(rt, actions, "i", 9)
        check(rt, actions, "j", 10)
        check(rt, actions, "k", 11)
        if is_pipe:
            time.sleep(0.2)  # should execute before the pipe is executed

    with block() as (rt, actions):
        ttl(rt, actions, "a", expect=False)
        ttl(rt, actions, "b", expect=True)
        ttl(rt, actions, "c", expect=True)
        ttl(rt, actions, "d", expect=True)
        ttl(rt, actions, "e", expect=False)
        ttl(rt, actions, "f", expect=True)
        ttl(rt, actions, "g", expect=True)
        ttl(rt, actions, "h", expect=True)
        ttl(rt, actions, "i", expect=True)
        ttl(rt, actions, "j", expect=True)
        ttl(rt, actions, "k", expect=True)

    time.sleep(0.2)

    with block() as (rt, actions):
        check(rt, actions, "a", 1)
        check(rt, actions, "b", None)
        check(rt, actions, "c", None)
        check(rt, actions, "d", None)
        check(rt, actions, "e", 5)
        check(rt, actions, "f", 6)
        check(rt, actions, "g", 7)
        check(rt, actions, "h", 8)
        check(rt, actions, "i", 9)
        check(rt, actions, "j", 10)
        check(rt, actions, "k", 11)

        ttl(rt, actions, "a", expect=False)
        ttl(rt, actions, "b", expect=None)
        ttl(rt, actions, "c", expect=None)
        ttl(rt, actions, "d", expect=None)
        ttl(rt, actions, "e", expect=False)
        ttl(rt, actions, "f", expect=True)
        ttl(rt, actions, "g", expect=True)
        ttl(rt, actions, "h", expect=True)
        ttl(rt, actions, "i", expect=True)
        ttl(rt, actions, "j", expect=True)
        ttl(rt, actions, "k", expect=True)

    with block() as (rt, actions):
        expire(rt, actions, "a", expect=True, expire_in=0.1)
        expire(rt, actions, "b", expect=False, expire_in=0.1)
        expire(rt, actions, "e", expect=True, expire_in=0.0)
        expire(rt, actions, "f", expect=True, mode=REX_EXPIRE, expire_in=0.3)
        expire(rt, actions, "g", expect=True, mode=REX_LATER, expire_in=0.3)
        expire(rt, actions, "h", expect=True, mode=REX_EARLIER, expire_in=0.1)
        expire(rt, actions, "i", expect=True)
        expire(rt, actions, "j", expect=True, mode=REX_LATER)
        expire(rt, actions, "k", expect=False, mode=REX_EARLIER)

        check(rt, actions, "a", 1)
        check(rt, actions, "b", None)
        check(rt, actions, "c", None)
        check(rt, actions, "d", None)
        check(rt, actions, "e", None)
        check(rt, actions, "f", 6)
        check(rt, actions, "g", 7)
        check(rt, actions, "h", 8)
        check(rt, actions, "i", 9)
        check(rt, actions, "j", 10)
        check(rt, actions, "k", 11)

        ttl(rt, actions, "a", expect=True)
        ttl(rt, actions, "b", expect=None)
        ttl(rt, actions, "c", expect=None)
        ttl(rt, actions, "d", expect=None)
        ttl(rt, actions, "e", expect=None)
        ttl(rt, actions, "f", expect=True)
        ttl(rt, actions, "g", expect=True)
        ttl(rt, actions, "h", expect=True)
        ttl(rt, actions, "i", expect=False)
        ttl(rt, actions, "j", expect=False)
        ttl(rt, actions, "k", expect=True)

    time.sleep(0.2)

    with block() as (rt, actions):
        check(rt, actions, "a", None)
        check(rt, actions, "b", None)
        check(rt, actions, "c", None)
        check(rt, actions, "d", None)
        check(rt, actions, "e", None)
        check(rt, actions, "f", 6)
        check(rt, actions, "g", 7)
        check(rt, actions, "h", None)
        check(rt, actions, "i", 9)
        check(rt, actions, "j", 10)
        check(rt, actions, "k", 11)

        ttl(rt, actions, "a", expect=None)
        ttl(rt, actions, "b", expect=None)
        ttl(rt, actions, "c", expect=None)
        ttl(rt, actions, "d", expect=None)
        ttl(rt, actions, "e", expect=None)
        ttl(rt, actions, "f", expect=True)
        ttl(rt, actions, "g", expect=True)
        ttl(rt, actions, "h", expect=None)
        ttl(rt, actions, "i", expect=False)
        ttl(rt, actions, "j", expect=False)
        ttl(rt, actions, "k", expect=True)

    time.sleep(0.2)

    with block() as (rt, actions):
        check(rt, actions, "a", None)
        check(rt, actions, "b", None)
        check(rt, actions, "c", None)
        check(rt, actions, "d", None)
        check(rt, actions, "e", None)
        check(rt, actions, "f", None)
        check(rt, actions, "g", None)
        check(rt, actions, "h", None)
        check(rt, actions, "i", 9)
        check(rt, actions, "j", 10)
        check(rt, actions, "k", None)

        ttl(rt, actions, "a", expect=None)
        ttl(rt, actions, "b", expect=None)
        ttl(rt, actions, "c", expect=None)
        ttl(rt, actions, "d", expect=None)
        ttl(rt, actions, "e", expect=None)
        ttl(rt, actions, "f", expect=None)
        ttl(rt, actions, "g", expect=None)
        ttl(rt, actions, "h", expect=None)
        ttl(rt, actions, "i", expect=False)
        ttl(rt, actions, "j", expect=False)
        ttl(rt, actions, "k", expect=None)

    # FIXME: test calling expire from a script
