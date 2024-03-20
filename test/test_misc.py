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
"""Tests miscellaneous redis functionality."""
import io
from contextlib import redirect_stdout
from test.util import get_setup, get_test_config

import pytest

from redipy.graph.expr import (
    ExprObj,
    find_literal,
    get_literal,
    is_none_literal,
)
from redipy.main import Redis
from redipy.redis.conn import RedisConnection
from redipy.symbolic.fun import FromJSON, LogFn, ToJSON, ToStr, TypeStr
from redipy.symbolic.rzset import RedisSortedSet
from redipy.symbolic.seq import FnContext
from redipy.util import lua_fmt


MSG = (
    "[TEST] this message is emitted by test/test_misc.py#test_misc. "
    "the output is not verified for the redis backend but if "
    "you see this message in the redis logs it means the test works."
)


CARD_CALL = "redis.call(\"zcard\", key_0)"


LUA_SCRIPT = lua_fmt(f"""
--[[ KEYV
in
]]
--[[ ARGV
]]
redis.log(redis.LOG_WARNING, "{MSG}")
local var_0 = tostring(5)
local var_1 = cjson.encode(var_0)
var_1 = cjson.decode(var_1)
local var_2 = true
if (not (type(var_1) == "string")) then
    var_2 = false
else
    if (var_0 ~= var_1) then
        var_2 = false
    end
end
local key_0 = (KEYS[1])  -- in
redis.call("zadd", key_0, 1, "a")
redis.call("zadd", key_0, 2, "b")
redis.call("zpopmax", key_0, 1)
if (not (({CARD_CALL} > 0) and ({CARD_CALL} < 2))) then
    var_2 = false
end
if (tostring(nil) ~= "nil") then
    var_2 = false
end
if (redis.call("type", key_0)["ok"] ~= "zset") then
    var_2 = false
end
return cjson.encode(var_2)
""")


@pytest.mark.parametrize("rt_lua", [False, True])
def test_misc(rt_lua: bool) -> None:
    """
    Tests miscellaneous redis functionality.

    Args:
        rt_lua (bool): Whether to use the redis or memory runtime.
    """
    rt = get_setup(
        "test_misc", rt_lua, lua_script=LUA_SCRIPT, no_compile_hook=True)

    ctx = FnContext()
    ctx.add(LogFn("warning", MSG))
    lcl_str = ctx.add_local(ToStr(5))
    lcl_json = ctx.add_local(ToJSON(lcl_str))
    ctx.add(lcl_json.assign(FromJSON(lcl_json)))
    lcl_res = ctx.add_local(True)

    b_then, b_else = ctx.if_(TypeStr(lcl_json).eq_("string").not_())
    b_then.add(lcl_res.assign(False))
    s_then, _ = b_else.if_(lcl_str.ne_(lcl_json))
    s_then.add(lcl_res.assign(False))

    zset = RedisSortedSet(ctx.add_key("in"))
    ctx.add(zset.add(1, "a"))
    ctx.add(zset.add(2, "b"))
    ctx.add(zset.pop_max(1))
    z_then, _ = ctx.if_(zset.card().gt_(0).and_(zset.card().lt_(2)).not_())
    z_then.add(lcl_res.assign(False))

    n_then, _ = ctx.if_(ToStr(None).ne_("nil"))
    n_then.add(lcl_res.assign(False))

    t_then, _ = ctx.if_(zset.key_type().ne_("zset"))
    t_then.add(lcl_res.assign(False))

    ctx.set_return_value(lcl_res)

    exec_fun = rt.register_script(ctx)
    out = io.StringIO()
    with redirect_stdout(out):
        res = exec_fun(keys={"in": "foo"}, args={})
        assert res
    cmp = "" if rt_lua else f"WARNING: {MSG}\n"
    assert cmp in out.getvalue()

    with rt.pipeline() as pipe:
        pipe.lpush("bar", "a", "b", "c")
        assert pipe.execute() == [3]
        pipe.llen("bar")
        assert pipe.execute() == [3]

    def check_redis(redis: Redis) -> None:
        assert redis.set_value("test_rt", "yes")
        assert redis.get_value("test_rt") == "yes"
        assert redis.delete("test_rt") == 1

    cfg = get_test_config()
    check_redis(Redis(
        "infer",
        redis_module="test_misc",
        host=cfg["host"],
        port=cfg["port"],
        passwd=cfg["passwd"],
        path=cfg["path"],
        prefix=cfg["prefix"]))
    check_redis(Redis(
        "redis",
        redis_module="test_misc",
        host=cfg["host"],
        port=cfg["port"],
        passwd=cfg["passwd"],
        path=cfg["path"],
        prefix=cfg["prefix"]))
    check_redis(Redis(
        "infer",
        redis_module="test_misc",
        cfg=cfg))
    check_redis(Redis(
        "infer",
        rt=RedisConnection("test_misc", cfg=cfg)))


def test_literals() -> None:
    """Tests literal expression helper functions."""
    assert get_literal(
        {
            "kind": "load_json_arg",
            "index": 0,
        },
        "int") is None
    assert get_literal(
        {
            "kind": "val",
            "type": "int",
            "value": 5,
        },
        "int") == 5
    assert get_literal(
        {
            "kind": "val",
            "type": "int",
            "value": 5,
        },
        "bool") is None

    assert not is_none_literal(
        {
            "kind": "load_json_arg",
            "index": 0,
        })
    assert not is_none_literal(
        {
            "kind": "val",
            "type": "int",
            "value": 5,
        })
    assert is_none_literal(
        {
            "kind": "val",
            "type": "none",
            "value": None,
        })

    exprs: list[ExprObj] = [
        {
            "kind": "load_json_arg",
            "index": 0,
        },
        {
            "kind": "val",
            "type": "int",
            "value": 5,
        },
        {
            "kind": "val",
            "type": "str",
            "value": "push",
        },
        {
            "kind": "val",
            "type": "none",
            "value": None,
        },
    ]

    assert find_literal(exprs, 6) is None
    assert find_literal(exprs, 5) == (1, 5)
    assert find_literal(exprs, 5, vtype="int") == (1, 5)
    assert find_literal(exprs, 5, vtype="float") is None
    assert find_literal(exprs, "push", vtype="str") == (2, "push")
    assert find_literal(exprs, "PUSH", vtype="str") is None
    assert find_literal(exprs, "PUSH", no_case=True) is None  # needs vtype=str
    assert find_literal(
        exprs, "PUSH", vtype="str", no_case=True) == (2, "push")
    assert find_literal(exprs, None) == (3, None)
    assert find_literal(exprs, None, vtype="none") == (3, None)
