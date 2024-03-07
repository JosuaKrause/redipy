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
"""Test of basic script functionality."""
import json
from test.util import get_test_config

from redipy.graph.expr import JSONType
from redipy.memory.local import LocalBackend
from redipy.memory.rt import LocalRuntime
from redipy.redis.conn import RedisConnection
from redipy.redis.lua import LuaBackend
from redipy.symbolic.seq import FnContext
from redipy.util import code_fmt, lua_fmt


LUA_SCRIPT = lua_fmt("""
--[[ KEYV
]]
--[[ ARGV
a
b
]]
local arg_0 = cjson.decode(ARGV[1])  -- a
local arg_1 = cjson.decode(ARGV[2])  -- b
local var_0 = 5
local var_1 = 0.0
if ((arg_0 + arg_1) >= 10) then
    var_0 = (arg_0 - arg_1)
    var_1 = 2.5
else
    var_1 = 7.5
end
return cjson.encode((var_0 + var_1))
""")


RUN_TESTS: list[tuple[JSONType, JSONType, JSONType]] = [
    (2, 4, 12.5),
    (3, 7, -1.5),
    (13, 2, 13.5),
]


def test_simple() -> None:
    """Test of basic script functionality."""
    ctx = FnContext()
    a = ctx.add_arg("a")
    b = ctx.add_arg("b")
    c = ctx.add_local(5)
    d = ctx.add_local(0.0)
    b_then, b_else = ctx.if_((a + b).ge_(10))
    b_then.add(c.assign(a - b))
    b_then.add(d.assign(2.5))
    b_else.add(d.assign(7.5))
    ctx.set_return_value(c + d)

    compiled = ctx.compile()
    print(json.dumps(compiled, indent=2, sort_keys=True))

    lcl = LocalBackend()
    lcl_code = lcl.translate(compiled)
    lrt = LocalRuntime()
    run_lcl = lcl.create_executable(lcl_code, lrt)

    for (a_in, b_in, out) in RUN_TESTS:
        res = run_lcl(keys={}, args={"a": a_in, "b": b_in})
        assert res == out

    lua = LuaBackend()
    lua_code = lua.translate(compiled)
    assert code_fmt(lua_code) == LUA_SCRIPT

    conn = RedisConnection("test_simple", cfg=get_test_config())
    run_redis = lua.create_executable(lua_code, conn)

    for (a_in, b_in, out) in RUN_TESTS:
        res = run_redis(keys={}, args={"a": a_in, "b": b_in})
        assert res == out
