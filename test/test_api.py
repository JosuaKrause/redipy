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
        assert teardown(key) == output_teardown
        assert result == output

        with redis.pipeline() as pipe:
            assert setup_pipe(pipe, key) is None
            assert pipeline(pipe, key) is None
            setup_result, result = pipe.execute()
        assert teardown(key) == output_teardown
        assert setup_result == output_setup
        assert result == output

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
        assert teardown(key) == output_teardown
        assert result == output

        assert setup(key) == output_setup
        result = lua_patch(fun(keys={"key": key}, args={}, client=redis))
        assert teardown(key) == output_teardown
        assert result == output

        assert setup(key) == output_setup
        result = lua_patch(
            fun(keys={"key": key}, args={}, client=redis.get_runtime()))
        assert teardown(key) == output_teardown
        assert result == output

        with redis.pipeline() as pipe:
            assert setup_pipe(pipe, key) is None
            assert fun(keys={"key": key}, args={}, client=pipe) is None
            setup_result, result = pipe.execute()
        assert teardown(key) == output_teardown
        assert setup_result == output_setup
        assert lua_patch(result) == output

    check(
        "exists",
        setup=lambda key: redis.set(key, "a"),
        normal=lambda key: redis.exists(key),
        setup_pipe=lambda pipe, key: pipe.set(key, "a"),
        pipeline=lambda pipe, key: pipe.exists(key),
        lua=lambda ctx, key: RedisVar(key).exists(),
        code="redis.call(\"exists\", key_0)",
        teardown=lambda key: [redis.get(key), redis.delete(key)],
        output_setup=True,
        output=1,
        output_teardown=["a", 1])

    check(
        "lpop",
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
        "rpop",
        setup=lambda key: redis.rpush(key, "a"),
        normal=lambda key: redis.rpop(key),
        setup_pipe=lambda pipe, key: pipe.rpush(key, "a"),
        pipeline=lambda pipe, key: pipe.rpop(key),
        lua=lambda ctx, key: RedisList(key).rpop(),
        code="(redis.call(\"rpop\", key_0) or nil)",
        teardown=lambda key: redis.llen(key),
        output_setup=1,
        output="a",
        output_teardown=0)

    check(
        "zpopmax",
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
        "zpopmin",
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
