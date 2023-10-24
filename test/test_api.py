from collections.abc import Callable
from test.util import get_test_config

import pytest

from redipy.api import PipelineAPI
from redipy.graph.expr import JSONType
from redipy.main import Redis
from redipy.symbolic.expr import Expr
from redipy.symbolic.fun import KeyVariable
from redipy.symbolic.rlist import RedisList
from redipy.symbolic.rvar import RedisVar
from redipy.symbolic.seq import FnContext
from redipy.util import code_fmt, lua_fmt


@pytest.mark.parametrize("rt_lua", [False, True])
def test_api(rt_lua: bool) -> None:
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
        code_str = code_fmt(code)
        lscript = lua_fmt(lua_script)
        success = False
        try:
            assert code_str == lscript
            success = True
        finally:
            if not success:
                print(f"script name: {code_name}")

    redis = Redis(
        "redis" if rt_lua else "memory",
        cfg=get_test_config() if rt_lua else None,
        lua_code_hook=code_hook)

    def check(
            name: str,
            setup: Callable[[str], JSONType],
            normal: Callable[[str], JSONType],
            pipeline: Callable[[PipelineAPI, str], None],
            lua: Callable[[FnContext, KeyVariable], Expr],
            code: str,
            teardown: Callable[[str], JSONType],
            output_setup: JSONType,
            output: JSONType,
            output_teardown: JSONType) -> None:
        print(f"testing {name}")
        key = "foo"

        assert setup(key) == output_setup
        result = normal(key)
        assert teardown(key) == output_teardown
        assert result == output

        assert setup(key) == output_setup
        with redis.pipeline() as pipe:
            pipeline(pipe, key)
            result, = pipe.execute()
        assert teardown(key) == output_teardown
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
        result = fun(keys={"key": key}, args={})
        assert teardown(key) == output_teardown
        assert result == output

    check(
        "exists",
        lambda key: redis.set(key, "a"),
        lambda key: redis.exists(key),
        lambda pipe, key: pipe.exists(key),
        lambda ctx, key: RedisVar(key).exists(),
        "redis.call(\"exists\", key_0)",
        lambda key: [redis.get(key), redis.delete(key)],
        True,
        1,
        ["a", 1])

    check(
        "lpop",
        lambda key: redis.lpush(key, "a"),
        lambda key: redis.lpop(key),
        lambda pipe, key: pipe.lpop(key),
        lambda ctx, key: RedisList(key).lpop(),
        "(redis.call(\"lpop\", key_0) or nil)",
        lambda key: redis.exists(key),
        1,
        "a",
        0)

    check(
        "rpop",
        lambda key: redis.rpush(key, "a"),
        lambda key: redis.rpop(key),
        lambda pipe, key: pipe.rpop(key),
        lambda ctx, key: RedisList(key).rpop(),
        "(redis.call(\"rpop\", key_0) or nil)",
        lambda key: redis.exists(key),
        1,
        "a",
        0)
