import json
from test.util import get_test_config

from redipy.memory.local import LocalBackend
from redipy.memory.rt import LocalRuntime
from redipy.redis.conn import RedisConnection
from redipy.redis.lua import LuaBackend
from redipy.symbolic.expr import JSONType
from redipy.symbolic.fun import ToNum
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
    ctx = FnContext()
    a = ctx.add_arg("a")
    k = RedisVar(ctx.add_key("k"))
    b_then, _ = ctx.if_(ToNum(k.get(no_adjust=True).or_(0)).le_(a))
    b_then.add(k.set(a))
    res = ctx.add_local(k.get())
    r_then, _ = ctx.if_(res.ne_(None))
    r_then.add(res.assign(ToNum(res)))
    ctx.set_return_value(res)

    compiled = ctx.compile()
    print(json.dumps(compiled, indent=2, sort_keys=True))

    lcl = LocalBackend()
    lcl_code = lcl.translate(compiled)
    lrt = LocalRuntime()
    run_lcl = lcl.create_executable(lcl_code, lrt)

    lrt.set("pos", "5")
    for (k_in, a_in, expect_out) in RUN_TESTS:
        is_out = run_lcl({"k": k_in}, {"a": a_in})
        assert is_out == expect_out
    assert lrt.get("foo") == "9"
    assert lrt.get("bar") == "7"
    assert lrt.get("neg") == "1"
    assert lrt.get("pos") == "6"

    lua = LuaBackend()
    lua_code = lua.translate(compiled)
    assert code_fmt(lua_code) == LUA_SCRIPT

    rrt = RedisConnection("test_rvar", cfg=get_test_config())
    run_redis = lua.create_executable(lua_code, rrt)

    rrt.set("pos", "5")
    for (k_in, a_in, expect_out) in RUN_TESTS:
        is_out = run_redis({"k": k_in}, {"a": a_in})
        assert is_out == expect_out
    assert rrt.get("foo") == "9"
    assert rrt.get("bar") == "7"
    assert rrt.get("neg") == "1"
    assert rrt.get("pos") == "6"
