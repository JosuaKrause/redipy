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
"""Tests redis functionality via the main API class."""
from collections.abc import Callable
from test.util import get_test_config
from typing import cast

import pytest

from redipy.api import PipelineAPI
from redipy.graph.expr import JSONType
from redipy.main import Redis
from redipy.symbolic.core import KeyVariable
from redipy.symbolic.expr import Expr
from redipy.symbolic.rhash import RedisHash
from redipy.symbolic.rlist import RedisList
from redipy.symbolic.rvar import RedisVar
from redipy.symbolic.rzset import RedisSortedSet
from redipy.symbolic.seq import FnContext
from redipy.util import code_fmt, lua_fmt


@pytest.mark.parametrize("rt_lua", [False, True])
def test_api(rt_lua: bool) -> None:
    """
    Tests redis functionality via the main API class.

    Args:
        rt_lua (bool): Whether to use the redis or memory runtime.
    """
    # pylint: disable=unnecessary-lambda
    code_name = None
    lua_script = None

    def set_lua_script(name: str | None, lscript: str | None) -> None:
        nonlocal code_name
        nonlocal lua_script

        code_name = name
        lua_script = lscript

    def code_hook(code: list[str]) -> None:
        nonlocal code_name
        nonlocal lua_script

        if lua_script is None:
            return
        filtered_code = []
        in_helpers = False
        for line in code:
            if line == "-- HELPERS START --":
                in_helpers = True
                continue
            if line == "-- HELPERS END --":
                in_helpers = False
                continue
            if in_helpers:
                continue
            filtered_code.append(line)
        code_str = code_fmt(filtered_code)
        lscript = lua_fmt(lua_script)
        success = False
        try:
            assert code_str == lscript
            success = True
        finally:
            if not success:
                print(f"script name: {code_name}")

    redis = Redis(
        cfg=get_test_config() if rt_lua else None,
        lua_code_hook=code_hook)

    def check(
            name: str,
            *,
            setup: Callable[[str], JSONType],
            normal: Callable[[str], JSONType],
            setup_pipe: Callable[[PipelineAPI, str], None],
            pipeline: Callable[[PipelineAPI, str], None],
            lua: Callable[[FnContext, KeyVariable], Expr],
            code: str,
            teardown: Callable[[str], JSONType],
            output_setup: JSONType,
            output: JSONType,
            output_teardown: JSONType,
            lua_patch: Callable[[JSONType], JSONType] | None = None) -> None:
        print(f"testing {name}")
        key = "foo"

        def lua_patch_id(res: JSONType) -> JSONType:
            return res

        if lua_patch is None:
            lua_patch = lua_patch_id

        assert setup(key) == output_setup
        result = normal(key)
        assert result == output
        assert teardown(key) == output_teardown

        with redis.pipeline() as pipe:
            assert setup_pipe(pipe, key) is None
            assert pipeline(pipe, key) is None
            setup_result, result = pipe.execute()
        assert setup_result == output_setup
        assert result == output
        assert teardown(key) == output_teardown

        ctx = FnContext()
        key_var = ctx.add_key("key")
        lcl = ctx.add_local(lua(ctx, key_var))
        ctx.set_return_value(lcl)

        lua_code = lua_fmt(f"""
            --[[ KEYV
            key
            ]]
            --[[ ARGV
            ]]
            local key_0 = (KEYS[1])  -- key
            local var_0 = {code}
            return cjson.encode(var_0)
        """)
        set_lua_script(name, lua_code)
        fun = redis.register_script(ctx)

        assert setup(key) == output_setup
        result = lua_patch(fun(keys={"key": key}, args={}))
        assert result == output
        assert teardown(key) == output_teardown

        assert setup(key) == output_setup
        result = lua_patch(fun(keys={"key": key}, args={}, client=redis))
        assert result == output
        assert teardown(key) == output_teardown

        assert setup(key) == output_setup
        result = lua_patch(
            fun(keys={"key": key}, args={}, client=redis.get_runtime()))
        assert result == output
        assert teardown(key) == output_teardown

        with redis.pipeline() as pipe:
            assert setup_pipe(pipe, key) is None
            assert fun(keys={"key": key}, args={}, client=pipe) is None
            setup_result, result = pipe.execute()
        assert setup_result == output_setup
        assert lua_patch(result) == output
        assert teardown(key) == output_teardown

    check(
        "exists",
        setup=lambda key: redis.set_value(key, "a"),
        normal=lambda key: redis.exists(key),
        setup_pipe=lambda pipe, key: pipe.set_value(key, "a"),
        pipeline=lambda pipe, key: pipe.exists(key),
        lua=lambda ctx, key: RedisVar(key).exists(),
        code="redis.call(\"exists\", key_0)",
        teardown=lambda key: [redis.get_value(key), redis.delete(key)],
        output_setup=True,
        output=1,
        output_teardown=["a", 1])

    check(
        "incrby",
        setup=lambda key: redis.set_value(key, "0.25"),
        normal=lambda key: redis.incrby(key, 0.5),
        setup_pipe=lambda pipe, key: pipe.set_value(key, "0.25"),
        pipeline=lambda pipe, key: pipe.incrby(key, 0.5),
        lua=lambda ctx, key: RedisVar(key).incrby(0.5),
        code="tonumber(redis.call(\"incrbyfloat\", key_0, 0.5))",
        teardown=lambda key: [redis.get_value(key), redis.delete(key)],
        output_setup=True,
        output=0.75,
        output_teardown=["0.75", 1])

    check(
        "lpop_0",
        setup=lambda key: redis.lpush(key, "a"),
        normal=lambda key: redis.lpop(key),
        setup_pipe=lambda pipe, key: pipe.lpush(key, "a"),
        pipeline=lambda pipe, key: pipe.lpop(key),
        lua=lambda ctx, key: RedisList(key).lpop(),
        code="(redis.call(\"lpop\", key_0) or nil)",
        # use exists here and llen below
        teardown=lambda key: redis.exists(key),
        output_setup=1,
        output="a",
        output_teardown=0)

    check(
        "lpop_1",
        setup=lambda key: redis.lpush(key, "a", "b", "c"),
        normal=lambda key: redis.lpop(key, 2),
        setup_pipe=lambda pipe, key: pipe.lpush(key, "a", "b", "c"),
        pipeline=lambda pipe, key: pipe.lpop(key, 2),
        lua=lambda ctx, key: RedisList(key).lpop(2),
        code="redis.call(\"lpop\", key_0, 2)",
        teardown=lambda key: [
            redis.llen(key), redis.delete(key), redis.exists(key)],
        output_setup=3,
        output=["c", "b"],
        output_teardown=[1, 1, 0])

    check(
        "rpop_0",
        setup=lambda key: redis.rpush(key, "a"),
        normal=lambda key: redis.rpop(key),
        setup_pipe=lambda pipe, key: pipe.rpush(key, "a"),
        pipeline=lambda pipe, key: pipe.rpop(key),
        lua=lambda ctx, key: RedisList(key).rpop(),
        code="(redis.call(\"rpop\", key_0) or nil)",
        teardown=lambda key: redis.exists(key),
        output_setup=1,
        output="a",
        output_teardown=0)

    check(
        "rpop_1",
        setup=lambda key: redis.rpush(key, "a", "b", "c"),
        normal=lambda key: redis.rpop(key, 2),
        setup_pipe=lambda pipe, key: pipe.rpush(key, "a", "b", "c"),
        pipeline=lambda pipe, key: pipe.rpop(key, 2),
        lua=lambda ctx, key: RedisList(key).rpop(2),
        code="redis.call(\"rpop\", key_0, 2)",
        teardown=lambda key: [
            redis.llen(key), redis.delete(key), redis.exists(key)],
        output_setup=3,
        output=["c", "b"],
        output_teardown=[1, 1, 0])

    check(
        "zpopmax_0",
        setup=lambda key: redis.zadd(key, {"a": 0.25, "b": 0.5, "c": 0.75}),
        normal=lambda key: redis.zpop_max(key, 2),
        setup_pipe=lambda pipe, key: pipe.zadd(
            key, {"a": 0.25, "b": 0.5, "c": 0.75}),
        pipeline=lambda pipe, key: pipe.zpop_max(key, 2),
        lua=lambda ctx, key: RedisSortedSet(key).pop_max(2),
        code="redipy.pairlist_scores(redis.call(\"zpopmax\", key_0, 2))",
        lua_patch=lambda res: [tuple(elem) for elem in cast(list, res)],
        teardown=lambda key: [
            redis.zcard(key), redis.delete(key), redis.zcard(key)],
        output_setup=3,
        output=[("c", 0.75), ("b", 0.5)],
        output_teardown=[1, True, 0])

    check(
        "zpopmax_1",
        setup=lambda key: redis.zadd(key, {"a": 0.25, "b": 0.5, "c": 0.75}),
        normal=lambda key: redis.zpop_max(key),
        setup_pipe=lambda pipe, key: pipe.zadd(
            key, {"a": 0.25, "b": 0.5, "c": 0.75}),
        pipeline=lambda pipe, key: pipe.zpop_max(key),
        lua=lambda ctx, key: RedisSortedSet(key).pop_max(),
        code="redipy.pairlist_scores(redis.call(\"zpopmax\", key_0))",
        lua_patch=lambda res: [tuple(elem) for elem in cast(list, res)],
        teardown=lambda key: [
            redis.zcard(key), redis.delete(key), redis.zcard(key)],
        output_setup=3,
        output=[("c", 0.75)],
        output_teardown=[2, True, 0])

    check(
        "zpopmin_0",
        setup=lambda key: redis.zadd(key, {"a": 0.25, "b": 0.5, "c": 0.75}),
        normal=lambda key: redis.zpop_min(key, 2),
        setup_pipe=lambda pipe, key: pipe.zadd(
            key, {"a": 0.25, "b": 0.5, "c": 0.75}),
        pipeline=lambda pipe, key: pipe.zpop_min(key, 2),
        lua=lambda ctx, key: RedisSortedSet(key).pop_min(2),
        code="redipy.pairlist_scores(redis.call(\"zpopmin\", key_0, 2))",
        lua_patch=lambda res: [tuple(elem) for elem in cast(list, res)],
        teardown=lambda key: [
            redis.zcard(key), redis.delete(key), redis.zcard(key)],
        output_setup=3,
        output=[("a", 0.25), ("b", 0.5)],
        output_teardown=[1, True, 0])

    check(
        "zpopmin_1",
        setup=lambda key: redis.zadd(key, {"a": 0.25, "b": 0.5, "c": 0.75}),
        normal=lambda key: redis.zpop_min(key),
        setup_pipe=lambda pipe, key: pipe.zadd(
            key, {"a": 0.25, "b": 0.5, "c": 0.75}),
        pipeline=lambda pipe, key: pipe.zpop_min(key),
        lua=lambda ctx, key: RedisSortedSet(key).pop_min(),
        code="redipy.pairlist_scores(redis.call(\"zpopmin\", key_0))",
        lua_patch=lambda res: [tuple(elem) for elem in cast(list, res)],
        teardown=lambda key: [
            redis.zcard(key), redis.delete(key), redis.zcard(key)],
        output_setup=3,
        output=[("a", 0.25)],
        output_teardown=[2, True, 0])

    check(
        "zrange_0",
        setup=lambda key: redis.zadd(key, {"a": 0.25, "b": 0.5, "c": 0.75}),
        normal=lambda key: redis.zrange(key, 1, 2),
        setup_pipe=lambda pipe, key: pipe.zadd(
            key, {"a": 0.25, "b": 0.5, "c": 0.75}),
        pipeline=lambda pipe, key: pipe.zrange(key, 1, 2),
        lua=lambda ctx, key: RedisSortedSet(key).range(1, 2),
        code="redis.call(\"zrange\", key_0, 1, 2)",
        teardown=lambda key: [redis.delete(key), redis.zcard(key)],
        output_setup=3,
        output=["b", "c"],
        output_teardown=[True, 0])

    check(
        "zrange_1",
        setup=lambda key: redis.zadd(key, {"a": 0.25, "b": 0.5, "c": 0.75}),
        normal=lambda key: redis.zrange(key, 0, -2),
        setup_pipe=lambda pipe, key: pipe.zadd(
            key, {"a": 0.25, "b": 0.5, "c": 0.75}),
        pipeline=lambda pipe, key: pipe.zrange(key, 0, -2),
        lua=lambda ctx, key: RedisSortedSet(key).range(0, -2),
        code="redis.call(\"zrange\", key_0, 0, -2)",
        teardown=lambda key: [redis.delete(key), redis.zcard(key)],
        output_setup=3,
        output=["a", "b"],
        output_teardown=[True, 0])

    check(
        "hget",
        setup=lambda key: redis.hset(key, {"a": "0", "b": "1", "c": "2"}),
        normal=lambda key: redis.hget(key, "b"),
        setup_pipe=lambda pipe, key: pipe.hset(
            key, {"a": "0", "b": "1", "c": "2"}),
        pipeline=lambda pipe, key: pipe.hget(key, "b"),
        lua=lambda ctx, key: RedisHash(key).hget("b"),
        code="(redis.call(\"hget\", key_0, \"b\") or nil)",
        teardown=lambda key: [
            redis.exists(key),
            redis.hdel(key, "a"),
            redis.exists(key),
            redis.hdel(key, "b", "c"),
            redis.exists(key),
        ],
        output_setup=3,
        output="1",
        output_teardown=[1, 1, 1, 2, 0])

    check(
        "hmget",
        setup=lambda key: redis.hset(key, {"d": "3", "e": "4", "f": "5"}),
        normal=lambda key: redis.hmget(key, "b", "c", "d", "e"),
        setup_pipe=lambda pipe, key: pipe.hset(
            key, {"d": "3", "e": "4", "f": "5"}),
        pipeline=lambda pipe, key: pipe.hmget(key, "b", "c", "d", "e"),
        lua=lambda ctx, key: RedisHash(key).hmget("b", "c", "d", "e"),
        code=(
            "redipy.keyval_dict(redis.call(\"hmget\", "
            "key_0, \"b\", \"c\", \"d\", \"e\"), \"b\", \"c\", \"d\", \"e\")"
        ),
        teardown=lambda key: [
            redis.exists(key),
            redis.delete(key),
            redis.hgetall(key),
        ],
        output_setup=3,
        output={"b": None, "c": None, "d": "3", "e": "4"},
        output_teardown=[1, 1, {}])

    check(
        "hincrby",
        setup=lambda key: redis.hset(key, {"d": "3", "e": "4", "f": "5"}),
        normal=lambda key: redis.hincrby(key, "e", 2),
        setup_pipe=lambda pipe, key: pipe.hset(
            key, {"d": "3", "e": "4", "f": "5"}),
        pipeline=lambda pipe, key: pipe.hincrby(key, "e", 2),
        lua=lambda ctx, key: RedisHash(key).hincrby("e", 2),
        code="tonumber(redis.call(\"hincrbyfloat\", key_0, \"e\", 2))",
        teardown=lambda key: [
            redis.hgetall(key),
            redis.delete(key),
            redis.exists(key),
        ],
        output_setup=3,
        output=6,
        output_teardown=[{"d": "3", "e": "6", "f": "5"}, 1, 0])

    check(
        "hdel",
        setup=lambda key: redis.hset(key, {"d": "3", "e": "4", "f": "5"}),
        normal=lambda key: redis.hdel(key, "c", "d", "e"),
        setup_pipe=lambda pipe, key: pipe.hset(
            key, {"d": "3", "e": "4", "f": "5"}),
        pipeline=lambda pipe, key: pipe.hdel(key, "c", "d", "e"),
        lua=lambda ctx, key: RedisHash(key).hdel("c", "d", "e"),
        code="redis.call(\"hdel\", key_0, \"c\", \"d\", \"e\")",
        teardown=lambda key: [redis.hgetall(key), redis.delete(key)],
        output_setup=3,
        output=2,
        output_teardown=[{"f": "5"}, 1])

    check(
        "hkeys",
        setup=lambda key: redis.hset(key, {"d": "3", "e": "4", "f": "5"}),
        normal=lambda key: redis.hkeys(key),
        setup_pipe=lambda pipe, key: pipe.hset(
            key, {"d": "3", "e": "4", "f": "5"}),
        pipeline=lambda pipe, key: pipe.hkeys(key),
        lua=lambda ctx, key: RedisHash(key).hkeys(),
        code="redis.call(\"hkeys\", key_0)",
        teardown=lambda key: redis.delete(key),
        output_setup=3,
        output=["d", "e", "f"],
        output_teardown=1)

    check(
        "hvals",
        setup=lambda key: redis.hset(key, {"d": "3", "e": "4", "f": "5"}),
        normal=lambda key: redis.hvals(key),
        setup_pipe=lambda pipe, key: pipe.hset(
            key, {"d": "3", "e": "4", "f": "5"}),
        pipeline=lambda pipe, key: pipe.hvals(key),
        lua=lambda ctx, key: RedisHash(key).hvals(),
        code="redis.call(\"hvals\", key_0)",
        teardown=lambda key: redis.delete(key),
        output_setup=3,
        output=["3", "4", "5"],
        output_teardown=1)

    check(
        "hgetall",
        setup=lambda key: redis.hset(key, {"d": "3", "e": "4", "f": "5"}),
        normal=lambda key: redis.hgetall(key),
        setup_pipe=lambda pipe, key: pipe.hset(
            key, {"d": "3", "e": "4", "f": "5"}),
        pipeline=lambda pipe, key: pipe.hgetall(key),
        lua=lambda ctx, key: RedisHash(key).hgetall(),
        code="redipy.pairlist_dict(redis.call(\"hgetall\", key_0))",
        teardown=lambda key: redis.delete(key),
        output_setup=3,
        output={"d": "3", "e": "4", "f": "5"},
        output_teardown=1)

    check(
        "lrange_0",
        setup=lambda key: redis.rpush(key, "a", "b", "c"),
        normal=lambda key: redis.lrange(key, 0, 0),
        setup_pipe=lambda pipe, key: pipe.rpush(key, "a", "b", "c"),
        pipeline=lambda pipe, key: pipe.lrange(key, 0, 0),
        lua=lambda ctx, key: RedisList(key).lrange(0, 0),
        code="redis.call(\"lrange\", key_0, 0, 0)",
        teardown=lambda key: redis.delete(key),
        output_setup=3,
        output=["a"],
        output_teardown=1)

    check(
        "lrange_1",
        setup=lambda key: redis.rpush(key, "a", "b", "c"),
        normal=lambda key: redis.lrange(key, -3, 2),
        setup_pipe=lambda pipe, key: pipe.rpush(key, "a", "b", "c"),
        pipeline=lambda pipe, key: pipe.lrange(key, -3, 2),
        lua=lambda ctx, key: RedisList(key).lrange(-3, 2),
        code="redis.call(\"lrange\", key_0, -3, 2)",
        teardown=lambda key: redis.delete(key),
        output_setup=3,
        output=["a", "b", "c"],
        output_teardown=1)

    check(
        "lrange_2",
        setup=lambda key: redis.rpush(key, "a", "b", "c", "d", "e"),
        normal=lambda key: redis.lrange(key, 1, -2),
        setup_pipe=lambda pipe, key: pipe.rpush(key, "a", "b", "c", "d", "e"),
        pipeline=lambda pipe, key: pipe.lrange(key, 1, -2),
        lua=lambda ctx, key: RedisList(key).lrange(1, -2),
        code="redis.call(\"lrange\", key_0, 1, -2)",
        teardown=lambda key: redis.delete(key),
        output_setup=5,
        output=["b", "c", "d"],
        output_teardown=1)

    check(
        "lrange_3",
        setup=lambda key: redis.rpush(key, "a", "b", "c", "d", "e"),
        normal=lambda key: redis.lrange(key, -100, 100),
        setup_pipe=lambda pipe, key: pipe.rpush(key, "a", "b", "c", "d", "e"),
        pipeline=lambda pipe, key: pipe.lrange(key, -100, 100),
        lua=lambda ctx, key: RedisList(key).lrange(-100, 100),
        code="redis.call(\"lrange\", key_0, -100, 100)",
        teardown=lambda key: redis.delete(key),
        output_setup=5,
        output=["a", "b", "c", "d", "e"],
        output_teardown=1)
