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
"""Tests sorted sets."""
from test.util import get_setup, run_code

import pytest

from redipy.backend.backend import ExecFunction
from redipy.symbolic.fun import FindFn
from redipy.symbolic.rzset import RedisSortedSet
from redipy.symbolic.seq import FnContext
from redipy.util import lua_fmt


EMPTY_OBJ = r"{}"
PLS = "redipy.pairlist_scores"


LUA_SCRIPT = lua_fmt(f"""
-- HELPERS START --
local redipy = {EMPTY_OBJ}
function redipy.nil_or_index (val)
    if val ~= nil then
        val = val - 1
    end
    return val
end
function redipy.pairlist_scores (arr)
    local res = {EMPTY_OBJ}
    local key = nil
    for ix, value in ipairs(arr) do
        if ix % 2 == 1 then
            key = value
        else
            res[#res + 1] = {{key, tonumber(value)}}
        end
    end
    return res
end
-- HELPERS END --
--[[ KEYV
zset
]]
--[[ ARGV
prefix
]]
local arg_0 = cjson.decode(ARGV[1])  -- prefix
local key_0 = (KEYS[1])  -- zset
local var_0 = cjson.decode("[]")
for ix_0, val_0 in ipairs({PLS}(redis.call("zpopmin", key_0, 5))) do
    if (redipy.nil_or_index(string.find(val_0[0 + 1], arg_0)) == 0) then
        var_0[#var_0 + 1] = val_0[0 + 1]
    end
end
return cjson.encode(var_0)
""")


ASET: dict[str, float] = {
    "a_x_0": 0.0,
    "a_p_1": 1.0,
    "a_s_2": 2.0,
    "a_d_3": 3.0,
    "a_f_4": 4.0,
    "b_v_0": 0.5,
    "b_t_1": 1.0,
    "b_q_2": 1.5,
    "b_u_3": 2.0,
    "c_e_0": 2.0,
    "c_a_1": 2.1,
    "c_i_2": 2.2,
    "c_k_3": 2.3,
    "c_l_4": 2.4,
    "c_b_5": 2.5,
}


BSET: dict[str, float] = {
    "a_z_0": 0,
    "a_y_1": 1,
    "a_x_2": 2,
    "a_w_3": 3,
    "a_v_4": 4,
    "b_u_0": 5,
    "b_t_1": 6,
    "b_s_2": 7,
    "b_r_3": 8,
    "c_q_0": 9,
    "c_p_1": 10,
    "c_o_2": 11,
    "c_n_3": 12,
    "c_m_4": 13,
    "c_l_5": 14,
    "c_k_6": 15,
}


RUN_TESTS: list[tuple[tuple[str, str], list[str] | None]] = [
    # -- a --
    # a_x_0: 0.0
    # b_v_0: 0.5
    # a_p_1: 1.0
    # b_t_1: 1.0
    # b_q_2: 1.5
    (("a", "a_"), ["a_x_0", "a_p_1"]),
    # -- b --
    # a_z_0: 0
    # a_y_1: 1
    # a_x_2: 2
    # a_w_3: 3
    # a_v_4: 4
    (("b", "a_"), ["a_z_0", "a_y_1", "a_x_2", "a_w_3", "a_v_4"]),
    # -- a --
    # a_s_2: 2.0
    # b_u_3: 2.0
    # c_e_0: 2.0
    # c_a_1: 2.1
    # c_i_2: 2.2
    (("a", "b_"), ["b_u_3"]),
    # -- b --
    # b_u_0: 5
    # b_t_1: 6
    # b_s_2: 7
    # b_r_3: 8
    # c_q_0: 9
    (("b", "b_"), ["b_u_0", "b_t_1", "b_s_2", "b_r_3"]),
    # -- a --
    # c_k_3: 2.3
    # c_l_4: 2.4
    # c_b_5: 2.5
    # a_d_3: 3.0
    # a_f_4: 4.0
    (("a", "c_"), ["c_k_3", "c_l_4", "c_b_5"]),
    # -- b --
    # c_p_1: 10
    # c_o_2: 11
    # c_n_3: 12
    # c_m_4: 13
    # c_l_5: 14
    (("b", "c_"), ["c_p_1", "c_o_2", "c_n_3", "c_m_4", "c_l_5"]),
    # -- b --
    # c_k_6: 15
    (("b", "a_"), None),
]


@pytest.mark.parametrize("rt_lua", [False, True])
def test_rzset(rt_lua: bool) -> None:
    """
    Tests sorted sets.

    Args:
        rt_lua (bool): Whether to use the redis or memory runtime.
    """
    rt = get_setup("test_rzset", rt_lua, lua_script=LUA_SCRIPT)

    ctx = FnContext()
    prefix = ctx.add_arg("prefix")
    zset = RedisSortedSet(ctx.add_key("zset"))
    res = ctx.add_local([])
    loop, _, cur = ctx.for_(zset.pop_min(5))
    b_then, _ = loop.if_(FindFn(cur[0], prefix).eq_(0))
    b_then.add(res.set_at(res.len_(), cur[0]))
    ctx.set_return_value(res)

    rt.zadd("a", ASET)
    rt.zadd("b", BSET)
    rt.zadd("c", {"a": 0, "b": 1})

    def tester(
            runner: ExecFunction,
            vals: tuple[str, str]) -> list[str] | None:
        res = runner(keys={"zset": vals[0]}, args={"prefix": vals[1]})
        if res:
            assert isinstance(res, list)
        else:
            assert res is None
        return res

    run_code(rt, ctx, tests=RUN_TESTS, tester=tester)
    assert rt.zcard("a") == 0
    assert rt.zcard("b") == 0
    assert rt.zpop_max("c") == [("b", 1)]
    assert rt.zcard("c") == 1
