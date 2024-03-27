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
"""Tests redis lists."""
from test.util import get_setup, run_code

import pytest

from redipy.backend.backend import ExecFunction
from redipy.symbolic.fun import ToNum
from redipy.symbolic.rlist import RedisList
from redipy.symbolic.seq import FnContext
from redipy.util import lua_fmt


LUA_SCRIPT = lua_fmt("""
--[[ KEYV
inp
left
right
]]
--[[ ARGV
cmp
]]
local arg_0 = cjson.decode(ARGV[1])  -- cmp
local key_0 = (KEYS[1])  -- inp
local key_1 = (KEYS[2])  -- left
local key_2 = (KEYS[3])  -- right
local var_0 = (redis.call("lpop", key_0) or nil)
while (var_0 ~= nil) do
    if (tonumber(var_0) < arg_0) then
        redis.call("lpush", key_1, var_0)
    else
        redis.call("rpush", key_2, var_0)
    end
    var_0 = (redis.call("lpop", key_0) or nil)
end
if (redis.call("llen", key_1) > redis.call("llen", key_2)) then
    redis.call("lpush", key_2, (redis.call("rpop", key_1) or nil))
end
""")


RUN_TESTS: list[tuple[tuple[str, str, str, int], tuple[int, int]]] = [
    # a: 5, 2, 3, 7, 9, 8, 8, 1, 4
    (("a", "b", "c", 5), (4, 5)),
    # a:
    # b: 4, 1, 3, 2
    # c: 5, 7, 9, 8, 8
    # d: 1, 2, 3, 4, 5, 6, 7, 8, 9
    (("d", "b", "e", 7), (9, 4)),
    # d:
    # b: 6, 5, 4, 3, 2, 1, 4, 1, 3
    # e: 2, 7, 8, 9
    (("b", "a", "d", 3), (3, 6)),
    # b:
    # a: 1, 1, 2
    # d: 6, 5, 4, 3, 4, 3
]


@pytest.mark.parametrize("rt_lua", [False, True])
def test_rlist(rt_lua: bool) -> None:
    """
    Tests redis lists.

    Args:
        rt_lua (bool): Whether to use the redis or memory runtime.
    """
    rt = get_setup("test_rlist", rt_lua, lua_script=LUA_SCRIPT)

    ctx = FnContext()
    cmp = ctx.add_arg("cmp")
    inp = RedisList(ctx.add_key("inp"))
    left = RedisList(ctx.add_key("left"))
    right = RedisList(ctx.add_key("right"))
    cur = ctx.add_local(inp.lpop())
    loop = ctx.while_(cur.ne_(None))
    b_then, b_else = loop.if_(ToNum(cur).lt_(cmp))
    b_then.add(left.lpush(cur))
    b_else.add(right.rpush(cur))
    loop.add(cur.assign(inp.lpop()))
    r_then, _ = ctx.if_(left.llen().gt_(right.llen()))
    r_then.add(right.lpush(left.rpop()))
    ctx.set_return_value(None)

    rt.rpush("a", "5", "2", "3", "7", "9", "8", "8", "1", "4")
    assert rt.lrange("a", -1, -1) == ["4"]
    rt.lpush("d", "9", "8", "7", "6", "5", "4", "3", "2", "1")

    def tester(
            runner: ExecFunction,
            vals: tuple[str, str, str, int]) -> tuple[int, int]:
        k_inp, k_left, k_right, a_cmp = vals
        res = runner(
            keys={
                "inp": k_inp,
                "left": k_left,
                "right": k_right,
            },
            args={
                "cmp": a_cmp,
            })
        assert res is None
        assert rt.llen(k_inp) == 0
        return (rt.llen(k_left), rt.llen(k_right))

    run_code(rt, ctx, tests=RUN_TESTS, tester=tester)
    # a: 1, 1, 2
    # b:
    # c: 5, 7, 9, 8, 8
    # d: 6, 5, 4, 3, 4, 3
    # e: 2, 7, 8, 9
    assert rt.rpop("a", 3) == ["2", "1", "1"]
    assert rt.rpop("b", 3) is None
    assert rt.lpop("b", 3) is None
    assert rt.lpop("c", 3) == ["5", "7", "9"]
    assert rt.lpop("c", 3) == ["8", "8"]
    assert rt.rpop("d", 7) == ["3", "4", "3", "4", "5", "6"]
    assert rt.lpop("e", 4) == ["2", "7", "8", "9"]
