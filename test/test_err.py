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
"""Tests various error cases."""

from collections.abc import Callable
from test.util import get_setup
from typing import Any

import pytest

from redipy.api import as_key_type, KeyType, PipelineAPI, RedisAPI
from redipy.main import Redis


KEY_TYPE_REG: list[KeyType] = [
    "string",
    "list",
    "set",
    "zset",
    "hash",
]


DEFAULTS: dict[
        KeyType, Callable[[RedisAPI | PipelineAPI, str, int], object]] = {
    "string": lambda rt, key, ix: rt.set_value(key, f"v{ix}"),
    "list": lambda rt, key, ix: rt.rpush(key, f"v{ix}"),
    "set": lambda rt, key, ix: rt.sadd(key, f"v{ix}"),
    "zset": lambda rt, key, ix: rt.zadd(key, {f"v{ix}": ix}),
    "hash": lambda rt, key, ix: rt.hset(key, {"value": f"v{ix}"}),
}


CHECKS: dict[KeyType, Callable[[RedisAPI, str], Any]] = {
    "string": lambda rt, key: rt.get_value(key),
    "list": lambda rt, key: rt.lrange(key, 0, -1),
    "set": lambda rt, key: rt.smembers(key),
    "zset": lambda rt, key: rt.zrange(key, 0, -1),
    "hash": lambda rt, key: rt.hgetall(key),
}


PIPE_CHECKS: dict[KeyType, Callable[[PipelineAPI, str], None]] = {
    "string": lambda pipe, key: pipe.get_value(key),
    "list": lambda pipe, key: pipe.lrange(key, 0, -1),
    "set": lambda pipe, key: pipe.smembers(key),
    "zset": lambda pipe, key: pipe.zrange(key, 0, -1),
    "hash": lambda pipe, key: pipe.hgetall(key),
}


EXPECTED: dict[KeyType, Callable[[Any, int], bool]] = {
    "string": lambda res, ix: res == f"v{ix}",
    "list": lambda res, ix: set(res) == {f"v{ix}"},
    "set": lambda res, ix: res == {f"v{ix}"},
    "zset": lambda res, ix: res == [f"v{ix}"],
    "hash": lambda res, ix: res == {"value": f"v{ix}"},
}


def test_errors() -> None:
    """Tests various error or edge cases."""
    with pytest.raises(ValueError, match="unknown key type: foo"):
        as_key_type("foo")
    assert as_key_type("none") is None

    with pytest.raises(ValueError, match="unknown backend foo"):
        Redis(backend="foo")  # type: ignore

    with pytest.raises(
            ValueError, match="rt must not be None for custom backend"):
        Redis(backend="custom")

    rt_redis = Redis("custom", rt=get_setup("test_err", rt_lua=True))
    assert rt_redis.maybe_get_redis_runtime() is not None
    assert rt_redis.get_redis_runtime() is not None
    assert rt_redis.maybe_get_memory_runtime() is None
    with pytest.raises(ValueError, match="not a memory runtime"):
        rt_redis.get_memory_runtime()

    rt_mem = Redis("custom", rt=get_setup("test_err", rt_lua=False))
    assert rt_mem.maybe_get_redis_runtime() is None
    with pytest.raises(ValueError, match="not a redis runtime"):
        assert rt_mem.get_redis_runtime() is not None
    assert rt_mem.maybe_get_memory_runtime() is not None
    assert rt_mem.get_memory_runtime() is not None


@pytest.mark.parametrize("rt_lua", [False, True])
def test_rt_errors(rt_lua: bool) -> None:
    """
    Test runtime errors.

    Args:
        rt_lua (bool): Whether to use the redis or memory runtime.
    """
    rt = get_setup("test_rt_errors", rt_lua)
    redis = Redis(rt=rt)

    assert redis.scan(0) == (0, [])

    for ix, key_type in enumerate(KEY_TYPE_REG):
        key = f"k{ix}"
        DEFAULTS[key_type](redis, key, ix)

    for key_type_left in KEY_TYPE_REG:
        for ix, key_type_right in enumerate(KEY_TYPE_REG):
            key = f"k{ix}"
            print(
                f"key: {key} "
                f"expected: {key_type_left} actual: {key_type_right}")
            if key_type_left == key_type_right:
                val = CHECKS[key_type_left](redis, key)
                assert EXPECTED[key_type_left](val, ix)
            else:
                with pytest.raises(
                        TypeError,
                        match=r"key.*(ha|i)s a"):
                    res = CHECKS[key_type_left](redis, key)
                    print(
                        f"got: {res} "
                        f"correct: {EXPECTED[key_type_left](res, ix)}")
