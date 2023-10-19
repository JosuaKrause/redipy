import io
from contextlib import redirect_stdout
from test.util import get_setup

import pytest

from redipy.symbolic.fun import FromJSON, LogFn, ToJSON, ToStr, TypeStr
from redipy.symbolic.seq import FnContext
from redipy.util import lua_fmt


MSG = (
    "this message is emitted by test/test_misc.py#test_misc. "
    "the output is not verified for the redis backend but if "
    "you see this message in the redis logs it means the test works."
)


LUA_SCRIPT = lua_fmt(f"""
--[[ KEYV
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
return cjson.encode(var_2)
""")


@pytest.mark.parametrize("rt_lua", [False, True])
def test_misc(rt_lua: bool) -> None:
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
    ctx.set_return_value(lcl_res)

    exec_fun = rt.register_script(ctx)
    out = io.StringIO()
    with redirect_stdout(out):
        res = exec_fun(keys={}, args={})
        assert res
    cmp = "" if rt_lua else f"WARNING: {MSG}\n"
    assert out.getvalue() == cmp
