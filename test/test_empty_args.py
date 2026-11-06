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
"""Tests that commands requiring arguments reject empty calls like redis."""
from collections.abc import Callable
from test.util import get_setup
from typing import Any

import pytest
from redis.exceptions import DataError, ResponseError

from redipy.api import RedisAPI
from redipy.main import Redis


def _wrong_args(cmd: str) -> tuple[type[Exception], str]:
    return ResponseError, f"wrong number of arguments for '{cmd}' command"


EMPTY_CALLS: list[tuple[
        str,
        Callable[[RedisAPI], Any],
        tuple[type[Exception], str]]] = [
    ("delete", lambda rt: rt.delete(), _wrong_args("del")),
    ("exists", lambda rt: rt.exists(), _wrong_args("exists")),
    ("hdel", lambda rt: rt.hdel("k"), _wrong_args("hdel")),
    ("sadd", lambda rt: rt.sadd("k"), _wrong_args("sadd")),
    ("srem", lambda rt: rt.srem("k"), _wrong_args("srem")),
    ("lpush", lambda rt: rt.lpush("k"), _wrong_args("lpush")),
    ("rpush", lambda rt: rt.rpush("k"), _wrong_args("rpush")),
    (
        "zadd",
        lambda rt: rt.zadd("k", {}),
        (DataError, "ZADD requires at least one element/score pair")),
    (
        "hset",
        lambda rt: rt.hset("k", {}),
        (DataError, "'hset' with no key value pairs")),
    (
        "hmget",
        lambda rt: rt.hmget("k"),
        (TypeError, "missing 1 required positional argument")),
]


@pytest.mark.parametrize("rt_lua", [False, True])
@pytest.mark.parametrize(
    "name,call,expected", EMPTY_CALLS, ids=[c[0] for c in EMPTY_CALLS])
def test_empty_args(
        rt_lua: bool,
        name: str,
        call: Callable[[RedisAPI], Any],
        expected: tuple[type[Exception], str]) -> None:
    """
    Tests that calling a command without its required arguments raises the
    same error on both backends.

    Args:
        rt_lua (bool): Whether to use the redis or memory runtime.

        name (str): The test name.

        call (Callable[[RedisAPI], Any]): Executes the command.

        expected (tuple[type[Exception], str]): The error type and message.
    """
    rt = get_setup(f"test_empty_args_{name}", rt_lua)
    redis = Redis(rt=rt)
    exc_type, msg = expected

    with pytest.raises(exc_type, match=msg):
        call(redis)

    with pytest.raises(exc_type, match=msg):
        with redis.pipeline() as pipe:
            call(pipe)  # type: ignore
            pipe.execute()

    # a failed call must not have created anything
    assert redis.exists("k") == 0


@pytest.mark.parametrize("rt_lua", [False, True])
def test_nonempty_args(rt_lua: bool) -> None:
    """
    Tests that valid minimal calls still work.

    Args:
        rt_lua (bool): Whether to use the redis or memory runtime.
    """
    rt = get_setup("test_nonempty_args", rt_lua)
    redis = Redis(rt=rt)
    redis.set_value("a", "1")
    assert redis.exists("a") == 1
    assert redis.exists("a", "a") == 2
    assert redis.delete("a", "missing") == 1
    assert redis.lpush("l", "x") == 1
    assert redis.sadd("s", "x") == 1
    assert redis.hset("h", {"f": "v"}) == 1
    assert redis.hmget("h", "f") == {"f": "v"}
    assert redis.zadd("z", {"x": 1.0}) == 1
