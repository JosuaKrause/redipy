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
from redipy.main import Redis

# from redipy.symbolic.expr import Strs
# from redipy.symbolic.fun import LogFn, ToJSON
from redipy.symbolic.rset import RedisSet
from redipy.symbolic.seq import FnContext
from redipy.util import lua_fmt


EMPTY_OBJ = r"{}"


LUA_SCRIPT = lua_fmt(f"""
--[[ KEYV
left_set
right_set
]]
--[[ ARGV
swap
]]
local arg_0 = cjson.decode(ARGV[1])  -- swap
local key_0 = (KEYS[1])  -- left_set
local key_1 = (KEYS[2])  -- right_set
local var_0 = cjson.decode("{EMPTY_OBJ}")
for ix_0, val_0 in ipairs(arg_0) do
    var_0[val_0] = true
end
local var_1 = cjson.decode("{EMPTY_OBJ}")
for ix_1, val_1 in ipairs(redis.call("smembers", key_0)) do
    if var_0[val_1] then
        if (not (redis.call("sismember", key_1, val_1) ~= 0)) then
            redis.call("sadd", key_1, val_1)
            redis.call("srem", key_0, val_1)
            var_1[val_1] = true
        end
    end
end
for ix_2, val_2 in ipairs(redis.call("smembers", key_1)) do
    if var_0[val_2] then
        if (not (redis.call("sismember", key_0, val_2) ~= 0)) then
            redis.call("sadd", key_0, val_2)
            redis.call("srem", key_1, val_2)
            var_1[val_2] = true
        end
    end
end
return cjson.encode(var_1)
""")


ASET: set[str] = {
    "a",
    "b",
    "c",
    "d",
}


BSET: set[str] = {
    "b",
    "d",
    "e",
    "f",
}


RUN_TESTS: list[tuple[list[str], set[str] | None]] = [
    # -- a --
    # a
    # b
    # c
    # d
    # -- b --
    # b
    # d
    # e
    # f
    (["a", "b", "c"], {"a", "c"}),
    # -- a --
    # b
    # d
    # -- b --
    # a
    # b
    # c
    # d
    # e
    # f
    (["a", "d", "e", "f"], {"a", "e", "f"}),
    # -- a --
    # a
    # b
    # d
    # e
    # f
    # -- b --
    # b
    # c
    # d
    (["c"], {"c"}),
    # -- a --
    # a
    # b
    # c
    # d
    # e
    # f
    # -- b --
    # b
    # d
    (["d"], None),
]


@pytest.mark.parametrize("rt_lua", [False, True])
def test_rset(rt_lua: bool) -> None:
    """
    Tests sets.

    Args:
        rt_lua (bool): Whether to use the redis or memory runtime.
    """
    redis = Redis(rt=get_setup("test_rset", rt_lua, lua_script=LUA_SCRIPT))

    ctx = FnContext()
    swap = ctx.add_arg("swap")

    left_set = RedisSet(ctx.add_key("left_set"))
    right_set = RedisSet(ctx.add_key("right_set"))

    # ctx.add(LogFn("warning", Strs("swap ", ToJSON(swap))))

    lookup = ctx.add_local({})
    rloop, _, rcur = ctx.for_(swap)
    # rloop.add(LogFn("warning", Strs("rcur ", ToJSON(rcur))))
    rloop.add(lookup.set_key(rcur, True))

    res = ctx.add_local({})
    # ctx.add(LogFn("warning", Strs("left_set ", ToJSON(left_set.members()))))
    # ctx.add(
    #     LogFn("warning", Strs("right_set ", ToJSON(right_set.members()))))
    # ctx.add(LogFn("warning", Strs("lookup ", ToJSON(lookup))))
    left_loop, _, cur_left = ctx.for_(left_set.members())
    # left_loop.add(LogFn("warning", Strs("cur_left ", cur_left)))
    # left_loop.add(
    #     LogFn("warning", Strs("get_key ", ToJSON(lookup.get_key(cur_left)))))
    # left_loop.add(
    #     LogFn("warning", Strs("has ", ToJSON(right_set.has(cur_left)))))
    left_then, _ = left_loop.if_(lookup.get_key(cur_left))
    right_empty, _ = left_then.if_(right_set.has(cur_left).not_())
    right_empty.add(right_set.add(cur_left))
    right_empty.add(left_set.remove(cur_left))
    right_empty.add(res.set_key(cur_left, True))
    # right_empty.add(LogFn("warning", Strs("res ", ToJSON(res))))

    right_loop, _, cur_right = ctx.for_(right_set.members())
    # right_loop.add(LogFn("warning", Strs("cur_right ", cur_right)))
    # right_loop.add(LogFn(
    #     "warning",
    #     Strs("get_key ", ToJSON(lookup.get_key(cur_right)))))
    # right_loop.add(
    #     LogFn("warning", Strs("has ", ToJSON(right_set.has(cur_right)))))
    right_then, _ = right_loop.if_(lookup.get_key(cur_right))
    left_empty, _ = right_then.if_(left_set.has(cur_right).not_())
    left_empty.add(left_set.add(cur_right))
    left_empty.add(right_set.remove(cur_right))
    left_empty.add(res.set_key(cur_right, True))
    # left_empty.add(LogFn("warning", Strs("res ", ToJSON(res))))

    ctx.set_return_value(res)

    assert redis.sadd("a", *ASET) == len(ASET)
    assert redis.smembers("a") == ASET
    assert redis.sadd("b", *BSET) == len(BSET)
    assert redis.smembers("b") == BSET
    cset = {"a", "b"}
    assert redis.sadd("c", *cset) == len(cset)
    assert redis.smembers("c") == cset

    def tester(
            runner: ExecFunction,
            vals: list[str]) -> set[str] | None:
        print(f"execute {vals}")
        res = runner(
            keys={"left_set": "a", "right_set": "b"},
            args={"swap": vals})
        if res:
            assert isinstance(res, dict)
            return set(res.keys())
        assert res is None
        return None

    run_code(redis, ctx, tests=RUN_TESTS, tester=tester)
    assert redis.scard("a") == 6
    assert redis.smembers("a") == {"a", "b", "c", "d", "e", "f"}
    assert redis.scard("b") == 2
    assert redis.smembers("b") == {"b", "d"}
    assert redis.sismember("c", "a")
    assert redis.sismember("c", "b")
    assert not redis.sismember("c", "c")
    assert redis.scard("c") == 2

    assert redis.srem("a", "a", "b", "c", "g") == 3
    with redis.pipeline() as pipe:
        pipe.scard("a")
        pipe.smembers("a")
        pipe.srem("a", "d", "e", "f")
        pipe.exists("a")
        pipe.sismember("b", "a")
        pipe.sismember("b", "b")
        pipe.srem("b", "b")
        pipe.scard("b")
        pipe.srem("b", "d")
        pipe.scard("b")
        pipe_res = pipe.execute()
    assert pipe_res[0] == 3  # scard
    assert pipe_res[1] == {"d", "e", "f"}  # smembers
    assert pipe_res[2] == 3  # srem
    assert not pipe_res[3]  # exists
    assert not pipe_res[4]  # sismember
    assert pipe_res[5]  # sismember
    assert pipe_res[6] == 1  # srem
    assert pipe_res[7] == 1  # scard
    assert pipe_res[8] == 1  # srem
    assert pipe_res[9] == 0  # scard
