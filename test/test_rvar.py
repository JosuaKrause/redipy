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
"""Tests redis values."""
import json
import time
from collections.abc import Callable
from test.util import get_setup, get_test_config

import pytest

from redipy.api import RSM_EXISTS, RSM_MISSING
from redipy.graph.expr import JSONType
from redipy.memory.local import LocalBackend
from redipy.memory.rt import LocalRuntime
from redipy.redis.conn import RedisConnection
from redipy.redis.lua import LuaBackend
from redipy.symbolic.expr import Expr
from redipy.symbolic.fun import ToNum, ToStr, TypeStr
from redipy.symbolic.rvar import RedisVar
from redipy.symbolic.seq import FnContext
from redipy.util import code_fmt, lua_fmt


LUA_SCRIPT = lua_fmt("""
--[[ KEYV
k
]]
--[[ ARGV
a
]]
local arg_0 = cjson.decode(ARGV[1])  -- a
local key_0 = (KEYS[1])  -- k
if (tonumber((redis.call("get", key_0) or 0)) <= arg_0) then
    redis.call("set", key_0, arg_0)
end
local var_0 = (redis.call("get", key_0) or nil)
if (var_0 ~= nil) then
    var_0 = tonumber(var_0)
end
return cjson.encode(var_0)
""")


RUN_TESTS: list[tuple[str, JSONType, JSONType]] = [
    ("foo", 1, 1),
    ("foo", 3, 3),
    ("foo", 2, 3),
    ("bar", 5, 5),
    ("bar", 2, 5),
    ("foo", 4, 4),
    ("bar", 5, 5),
    ("foo", 8, 8),
    ("foo", 9, 9),
    ("neg", -1, None),
    ("neg", 0, 0),
    ("neg", 1, 1),
    ("neg", 0, 1),
    ("bar", 2, 5),
    ("pos", 3, 5),
    ("bar", 7, 7),
    ("pos", 6, 6),
]


def test_rvar() -> None:
    """Tests redis values with an additional explicit script."""
    ctx = FnContext()
    a = ctx.add_arg("a")
    k = RedisVar(ctx.add_key("k"))
    b_then, _ = ctx.if_(ToNum(k.get_value(no_adjust=True).or_(0)).le_(a))
    b_then.add(k.set_value(a))
    res = ctx.add_local(k.get_value())
    r_then, _ = ctx.if_(res.ne_(None))
    r_then.add(res.assign(ToNum(res)))
    ctx.set_return_value(res)

    compiled = ctx.compile()
    print(json.dumps(compiled, indent=2, sort_keys=True))

    lcl = LocalBackend()
    lcl_code = lcl.translate(compiled)
    lrt = LocalRuntime()
    run_lcl = lcl.create_executable(lcl_code, lrt)

    lrt.set_value("pos", "5")
    for (k_in, a_in, expect_out) in RUN_TESTS:
        is_out = run_lcl(keys={"k": k_in}, args={"a": a_in})
        assert is_out == expect_out
    assert lrt.get_value("foo") == "9"
    assert lrt.get_value("bar") == "7"
    assert lrt.get_value("neg") == "1"
    assert lrt.get_value("pos") == "6"

    lua = LuaBackend()
    lua_code = lua.translate(compiled)
    assert code_fmt(lua_code) == LUA_SCRIPT

    rrt = RedisConnection("test_rvar", cfg=get_test_config())
    run_redis = lua.create_executable(lua_code, rrt)

    rrt.set_value("pos", "5")
    for (k_in, a_in, expect_out) in RUN_TESTS:
        is_out = run_redis(keys={"k": k_in}, args={"a": a_in})
        assert is_out == expect_out
    assert rrt.get_value("foo") == "9"
    assert rrt.get_value("bar") == "7"
    assert rrt.get_value("neg") == "1"
    assert rrt.get_value("pos") == "6"


@pytest.mark.parametrize("rt_lua", [False, True])
def test_set_ext_args(rt_lua: bool) -> None:
    """
    Tests key expiration.

    Args:
        rt_lua (bool): Whether to use the redis or memory runtime.
    """
    rt = get_setup(
        "test_set_ext_args", rt_lua, no_compile_hook=True)

    def fun_check(
            key: str,
            name: str,
            expr: Callable[[RedisVar, Expr], Expr],
            type_str: str,
            value_str: str,
            lua_snippet: str) -> None:
        lua_script = f"""
            --[[ KEYV
            in
            ]]
            --[[ ARGV
            in
            ]]
            local arg_0 = cjson.decode(ARGV[1])  -- in
            local key_0 = (KEYS[1])  -- in
            local var_0 = {lua_snippet}
            local var_1 = cjson.decode("[]")
            var_1[#var_1 + 1] = ""
            var_1[#var_1 + 1] = ""
            var_1[0 + 1] = type(var_0)
            var_1[1 + 1] = tostring(var_0)
            return cjson.encode(var_1)
        """

        def code_hook(code: list[str]) -> None:
            code_str = code_fmt(code)
            assert code_str == lua_fmt(lua_script)
            print(code_str)

        if rt_lua:
            rt.set_code_hook(code_hook)

        ctx = FnContext()
        arg_in = ctx.add_arg("in")
        rvar = RedisVar(ctx.add_key("in"))
        res_val = ctx.add_local(expr(rvar, arg_in))
        res_arr = ctx.add_local([])
        ctx.add(res_arr.set_at(res_arr.len_(), ""))
        ctx.add(res_arr.set_at(res_arr.len_(), ""))
        ctx.add(res_arr.set_at(0, TypeStr(res_val)))
        ctx.add(res_arr.set_at(1, ToStr(res_val)))
        ctx.set_return_value(res_arr)

        exec_fun = rt.register_script(ctx)
        res = exec_fun(keys={"in": key}, args={"in": name})
        assert isinstance(res, list)
        assert len(res) == 2
        assert res[0] == type_str
        assert res[1] == value_str

    assert rt.get_value("bar") is None
    assert rt.set_value("bar", "a") is True
    assert rt.get_value("bar") == "a"
    assert rt.set_value("bar", "c", mode=RSM_MISSING) is False
    assert rt.get_value("bar") == "a"
    assert rt.set_value("bar", "b", return_previous=True, expire_in=0.1) == "a"
    assert rt.get_value("bar") == "b"
    assert rt.set_value("bar", "c", mode=RSM_MISSING, keep_ttl=True) is False
    assert rt.get_value("bar") == "b"
    assert rt.set_value("bar", "e", mode=RSM_EXISTS, keep_ttl=True) is True
    assert rt.get_value("bar") == "e"

    with rt.pipeline() as pipe:
        pipe.get_value("baz")  # 0
        pipe.set_value("baz", "-", mode=RSM_EXISTS)  # 1
        pipe.get_value("baz")  # 2
        pipe.set_value("baz", "a")  # 3
        pipe.get_value("baz")  # 4
        pipe.set_value("baz", "b", mode=RSM_EXISTS)  # 5
        pipe.get_value("baz")  # 6
        pipe.set_value("baz", "c", mode=RSM_MISSING)  # 7
        pipe.get_value("baz")  # 8
        pipe.set_value("baz", "d", return_previous=True, expire_in=0.1)  # 9
        pipe.get_value("baz")  # 10
        pipe_res = pipe.execute()
    assert pipe_res[0] is None
    assert pipe_res[1] is False
    assert pipe_res[2] is None
    assert pipe_res[3] is True
    assert pipe_res[4] == "a"
    assert pipe_res[5] is True
    assert pipe_res[6] == "b"
    assert pipe_res[7] is False
    assert pipe_res[8] == "b"
    assert pipe_res[9] == "b"
    assert pipe_res[10] == "d"

    assert rt.get_value("foo") is None
    fun_check(
        "foo",
        "a",
        lambda rvar, name: rvar.set_value(name),
        "boolean",
        "true",
        "(redis.call(\"set\", key_0, arg_0) ~= false)")
    assert rt.get_value("foo") == "a"
    fun_check(
        "foo",
        "c",
        lambda rvar, name: rvar.set_value(name, mode=RSM_MISSING),
        "boolean",
        "false",
        "(redis.call(\"set\", key_0, arg_0, \"NX\") ~= false)")
    assert rt.get_value("foo") == "a"
    fun_check(
        "foo",
        "b",
        lambda rvar, name: rvar.set_value(
            name, return_previous=True, expire_in=0.1),
        "string",
        "a",
        "redis.call(\"set\", key_0, arg_0, \"GET\", \"PX\", 100)")
    assert rt.get_value("foo") == "b"
    fun_check(
        "foo",
        "c",
        lambda rvar, name: rvar.set_value(
            name, mode=RSM_MISSING, keep_ttl=True),
        "boolean",
        "false",
        "(redis.call(\"set\", key_0, arg_0, \"NX\", \"KEEPTTL\") ~= false)")
    assert rt.get_value("foo") == "b"
    fun_check(
        "foo",
        "e",
        lambda rvar, name: rvar.set_value(
            name, mode=RSM_EXISTS, keep_ttl=True),
        "boolean",
        "true",
        "(redis.call(\"set\", key_0, arg_0, \"XX\", \"KEEPTTL\") ~= false)")
    assert rt.get_value("foo") == "e"

    time.sleep(0.1)

    assert rt.set_value("bar", "d", mode=RSM_MISSING) is True
    assert rt.get_value("bar") == "d"

    assert rt.get_value("baz") is None

    fun_check(
        "foo",
        "d",
        lambda rvar, name: rvar.set_value(name, mode=RSM_MISSING),
        "boolean",
        "true",
        "(redis.call(\"set\", key_0, arg_0, \"NX\") ~= false)")
    assert rt.get_value("foo") == "d"
