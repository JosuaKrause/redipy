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
"""Tests a complex example class using redipy scripts."""
from collections.abc import Callable
from test.util import get_test_config
from typing import cast

import pytest

from redipy.api import RedisClientAPI
from redipy.backend.backend import ExecFunction
from redipy.graph.expr import JSONType
from redipy.main import Redis
from redipy.symbolic.expr import Strs
from redipy.symbolic.fun import ToIntStr, ToNum
from redipy.symbolic.rhash import RedisHash
from redipy.symbolic.rvar import RedisVar
from redipy.symbolic.seq import FnContext
from redipy.util import code_fmt, lua_fmt


GET_KEY_0 = "(redis.call(\"get\", key_0) or 0)"
RET = "return cjson.encode"
RC = "redis.call"
RP = "redipy.asintstr"
PLD = "redipy.pairlist_dict"
KEY_1_P = "(key_1) .. (\":\")"
EMPTY_OBJ = r"{}"


LUA_SET_VALUE = f"""
-- HELPERS START --
local redipy = {EMPTY_OBJ}
function redipy.asintstr (val)
    return math.floor(val)
end
-- HELPERS END --
--[[ KEYV
size
frame
]]
--[[ ARGV
field
value
]]
local key_0 = (KEYS[1])  -- size
local key_1 = (KEYS[2])  -- frame
local arg_0 = cjson.decode(ARGV[1])  -- field
local arg_1 = cjson.decode(ARGV[2])  -- value
{RC}("hset", (key_1) .. (":") .. ({RP}({GET_KEY_0})), arg_0, arg_1)
"""


LUA_GET_VALUE = f"""
-- HELPERS START --
local redipy = {EMPTY_OBJ}
function redipy.asintstr (val)
    return math.floor(val)
end
-- HELPERS END --
--[[ KEYV
size
frame
]]
--[[ ARGV
field
]]
local key_0 = (KEYS[1])  -- size
local key_1 = (KEYS[2])  -- frame
local arg_0 = cjson.decode(ARGV[1])  -- field
{RET}(({RC}("hget", (key_1) .. (":") .. ({RP}({GET_KEY_0})), arg_0) or nil))
"""


LUA_POP_FRAME = f"""
-- HELPERS START --
local redipy = {EMPTY_OBJ}
function redipy.asintstr (val)
    return math.floor(val)
end
function redipy.pairlist_dict (arr)
    local res = {EMPTY_OBJ}
    local key = nil
    for _, value in ipairs(arr) do
        if key ~= nil then
            res[key] = value
            key = nil
        else
            key = value
        end
    end
    return res
end
-- HELPERS END --
--[[ KEYV
size
frame
]]
--[[ ARGV
]]
local key_0 = (KEYS[1])  -- size
local key_1 = (KEYS[2])  -- frame
local var_0 = {PLD}({RC}("hgetall", {KEY_1_P} .. ({RP}({GET_KEY_0}))))
redis.call("del", {KEY_1_P} .. ({RP}({GET_KEY_0})))
if (tonumber({GET_KEY_0}) > 0) then
    redis.call("incrbyfloat", key_0, -1)
else
    redis.call("del", key_0)
end
return cjson.encode(var_0)
"""


LUA_GET_CASCADING = f"""
-- HELPERS START --
local redipy = {EMPTY_OBJ}
function redipy.asintstr (val)
    return math.floor(val)
end
-- HELPERS END --
--[[ KEYV
size
frame
]]
--[[ ARGV
field
]]
local key_0 = (KEYS[1])  -- size
local key_1 = (KEYS[2])  -- frame
local var_0 = key_1
local arg_0 = cjson.decode(ARGV[1])  -- field
local var_1 = tonumber({GET_KEY_0})
local var_2 = nil
local var_3 = nil
while ((var_2 == nil) and (var_1 >= 0)) do
    var_3 = (var_0) .. (":") .. (redipy.asintstr(var_1))
    var_2 = (redis.call("hget", var_3, arg_0) or nil)
    var_1 = (var_1 - 1)
end
return cjson.encode(var_2)
"""


class RStack:
    """An example class that simulates a key value stack."""
    def __init__(
            self,
            rt: RedisClientAPI,
            set_lua_script: Callable[[str | None, str | None], None]) -> None:
        self._rt = rt

        set_lua_script("set_value", LUA_SET_VALUE)
        self._set_value = self._set_value_script()
        set_lua_script("get_value", LUA_GET_VALUE)
        self._get_value = self._get_value_script()
        set_lua_script("pop_frame", LUA_POP_FRAME)
        self._pop_frame = self._pop_frame_script()
        set_lua_script("get_cascading", LUA_GET_CASCADING)
        self._get_cascading = self._get_cascading_script()
        set_lua_script(None, None)

    def key(self, base: str, name: str) -> str:
        """
        Compute the key.

        Args:
            base (str): The base key.

            name (str): The name.

        Returns:
            str: The key associated with the name.
        """
        return f"{base}:{name}"

    def push_frame(self, base: str) -> None:
        """
        Pushes a new stack frame.

        Args:
            base (str): The base key.
        """
        self._rt.incrby(self.key(base, "size"), 1)

    def pop_frame(self, base: str) -> dict[str, str]:
        """
        Pops the current stack frame and returns its values.

        Args:
            base (str): The base key.

        Returns:
            dict[str, str] | None: The content of the stack frame.
        """
        res = self._pop_frame(
            keys={
                "size": self.key(base, "size"),
                "frame": self.key(base, "frame"),
            },
            args={})
        if res is None:
            return {}
        return cast(dict, res)

    def set_value(self, base: str, field: str, value: str) -> None:
        """
        Set a value in the current stack frame.

        Args:
            base (str): The base key.

            field (str): The field.

            value (str): The value.
        """
        self._set_value(
            keys={
                "size": self.key(base, "size"),
                "frame": self.key(base, "frame"),
            },
            args={"field": field, "value": value})

    def get_value(self, base: str, field: str) -> JSONType:
        """
        Returns a value from the current stack frame.

        Args:
            base (str): The base key.

            field (str): The field.

        Returns:
            JSONType: The value.
        """
        return self._get_value(
            keys={
                "size": self.key(base, "size"),
                "frame": self.key(base, "frame"),
            },
            args={"field": field})

    def get_cascading(self, base: str, field: str) -> JSONType:
        """
        Returns a value from the stack. If the value is not in the current
        stack frame the value is recursively retrieved from the previous
        stack frames.

        Args:
            base (str): The base key.

            field (str): The field.

        Returns:
            JSONType: The value.
        """
        return self._get_cascading(
            keys={
                "size": self.key(base, "size"),
                "frame": self.key(base, "frame"),
            },
            args={"field": field})

    def _set_value_script(self) -> ExecFunction:
        ctx = FnContext()
        rsize = RedisVar(ctx.add_key("size"))
        rframe = RedisHash(Strs(
            ctx.add_key("frame"),
            ":",
            ToIntStr(rsize.get_value(default=0))))
        field = ctx.add_arg("field")
        value = ctx.add_arg("value")
        ctx.add(rframe.hset({
            field: value,
        }))
        ctx.set_return_value(None)
        return self._rt.register_script(ctx)

    def _get_value_script(self) -> ExecFunction:
        ctx = FnContext()
        rsize = RedisVar(ctx.add_key("size"))
        rframe = RedisHash(Strs(
            ctx.add_key("frame"),
            ":",
            ToIntStr(rsize.get_value(default=0))))
        field = ctx.add_arg("field")
        ctx.set_return_value(rframe.hget(field))
        return self._rt.register_script(ctx)

    def _pop_frame_script(self) -> ExecFunction:
        ctx = FnContext()
        rsize = RedisVar(ctx.add_key("size"))
        rframe = RedisHash(Strs(
            ctx.add_key("frame"), ":", ToIntStr(rsize.get_value(default=0))))
        lcl = ctx.add_local(rframe.hgetall())
        ctx.add(rframe.delete())

        b_then, b_else = ctx.if_(ToNum(rsize.get_value(default=0)).gt_(0))
        b_then.add(rsize.incrby(-1))
        b_else.add(rsize.delete())

        ctx.set_return_value(lcl)
        return self._rt.register_script(ctx)

    def _get_cascading_script(self) -> ExecFunction:
        ctx = FnContext()
        rsize = RedisVar(ctx.add_key("size"))
        base = ctx.add_local(ctx.add_key("frame"))
        field = ctx.add_arg("field")
        pos = ctx.add_local(ToNum(rsize.get_value(default=0)))
        res = ctx.add_local(None)
        cur = ctx.add_local(None)
        rframe = RedisHash(cur)

        loop = ctx.while_(res.eq_(None).and_(pos.ge_(0)))
        loop.add(cur.assign(Strs(base, ":", ToIntStr(pos))))
        loop.add(res.assign(rframe.hget(field)))
        loop.add(pos.assign(pos - 1))

        ctx.set_return_value(res)
        return self._rt.register_script(ctx)


@pytest.mark.parametrize("rt_lua", [False, True])
def test_stack(rt_lua: bool) -> None:
    """
    Tests a complex example class using redipy scripts.

    Args:
        rt_lua (bool): Whether to use the redis or memory runtime.
    """
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
    stack = RStack(redis, set_lua_script)

    stack.set_value("foo", "a", "hi")
    assert stack.get_value("bar", "a") is None
    assert stack.get_value("foo", "a") == "hi"

    stack.set_value("foo", "b", "foo")
    stack.set_value("bar", "a", "bye")
    stack.set_value("bar", "b", "bar")

    assert stack.get_value("foo", "a") == "hi"
    assert stack.get_value("foo", "b") == "foo"
    assert stack.get_value("foo", "c") is None
    assert stack.get_value("foo", "d") is None

    assert stack.get_value("bar", "a") == "bye"
    assert stack.get_value("bar", "b") == "bar"
    assert stack.get_value("bar", "c") is None
    assert stack.get_value("bar", "d") is None

    stack.push_frame("foo")
    stack.set_value("foo", "b", "bb")
    stack.set_value("foo", "c", "cc")
    stack.set_value("foo", "d", "dd")

    stack.push_frame("bar")
    stack.set_value("bar", "a", "2a")
    stack.set_value("bar", "b", "2b")

    assert stack.get_value("foo", "a") is None
    assert stack.get_value("foo", "b") == "bb"
    assert stack.get_value("foo", "c") == "cc"
    assert stack.get_value("foo", "d") == "dd"

    assert stack.get_value("bar", "a") == "2a"
    assert stack.get_value("bar", "b") == "2b"
    assert stack.get_value("bar", "c") is None
    assert stack.get_value("bar", "d") is None

    assert stack.get_cascading("foo", "a") == "hi"
    assert stack.get_cascading("foo", "b") == "bb"
    assert stack.get_cascading("foo", "c") == "cc"
    assert stack.get_cascading("foo", "d") == "dd"

    assert stack.get_cascading("bar", "a") == "2a"
    assert stack.get_cascading("bar", "b") == "2b"
    assert stack.get_cascading("bar", "c") is None
    assert stack.get_cascading("bar", "d") is None

    stack.push_frame("foo")
    stack.set_value("foo", "b", "bbb")
    stack.set_value("foo", "c", "ccc")

    stack.push_frame("bar")
    stack.set_value("bar", "b", "3b")
    stack.set_value("bar", "c", "3c")

    assert stack.get_value("foo", "a") is None
    assert stack.get_value("foo", "b") == "bbb"
    assert stack.get_value("foo", "c") == "ccc"
    assert stack.get_value("foo", "d") is None

    assert stack.get_value("bar", "a") is None
    assert stack.get_value("bar", "b") == "3b"
    assert stack.get_value("bar", "c") == "3c"
    assert stack.get_value("bar", "d") is None

    assert stack.get_cascading("foo", "a") == "hi"
    assert stack.get_cascading("foo", "b") == "bbb"
    assert stack.get_cascading("foo", "c") == "ccc"
    assert stack.get_cascading("foo", "d") == "dd"

    assert stack.get_cascading("bar", "a") == "2a"
    assert stack.get_cascading("bar", "b") == "3b"
    assert stack.get_cascading("bar", "c") == "3c"
    assert stack.get_cascading("bar", "d") is None

    assert stack.pop_frame("foo") == {
        "b": "bbb",
        "c": "ccc",
    }
    assert stack.pop_frame("bar") == {
        "b": "3b",
        "c": "3c",
    }

    assert stack.get_value("foo", "a") is None
    assert stack.get_value("foo", "b") == "bb"
    assert stack.get_value("foo", "c") == "cc"
    assert stack.get_value("foo", "d") == "dd"

    assert stack.get_value("bar", "a") == "2a"
    assert stack.get_value("bar", "b") == "2b"
    assert stack.get_value("bar", "c") is None
    assert stack.get_value("bar", "d") is None

    assert stack.get_cascading("foo", "a") == "hi"
    assert stack.get_cascading("foo", "b") == "bb"
    assert stack.get_cascading("foo", "c") == "cc"
    assert stack.get_cascading("foo", "d") == "dd"

    assert stack.get_cascading("bar", "a") == "2a"
    assert stack.get_cascading("bar", "b") == "2b"
    assert stack.get_cascading("bar", "c") is None
    assert stack.get_cascading("bar", "d") is None

    assert stack.pop_frame("foo") == {
        "b": "bb",
        "c": "cc",
        "d": "dd",
    }
    assert stack.pop_frame("bar") == {
        "a": "2a",
        "b": "2b",
    }

    assert stack.get_value("foo", "a") == "hi"
    assert stack.get_value("foo", "b") == "foo"
    assert stack.get_value("foo", "c") is None
    assert stack.get_value("foo", "d") is None

    assert stack.get_value("bar", "a") == "bye"
    assert stack.get_value("bar", "b") == "bar"
    assert stack.get_value("bar", "c") is None
    assert stack.get_value("bar", "d") is None

    assert stack.get_cascading("foo", "a") == "hi"
    assert stack.get_cascading("foo", "b") == "foo"
    assert stack.get_cascading("foo", "c") is None
    assert stack.get_cascading("foo", "d") is None

    assert stack.get_cascading("bar", "a") == "bye"
    assert stack.get_cascading("bar", "b") == "bar"
    assert stack.get_cascading("bar", "c") is None
    assert stack.get_cascading("bar", "d") is None

    assert redis.exists("foo:size", "foo:frame:0") == 2
    assert redis.exists("foo:frame:1") == 0
    assert redis.exists("bar:size", "bar:frame:0") == 2
    assert redis.exists("bar:frame:1") == 0

    assert stack.pop_frame("foo") == {
        "a": "hi",
        "b": "foo",
    }
    assert stack.pop_frame("bar") == {
        "a": "bye",
        "b": "bar",
    }

    assert stack.get_cascading("foo", "a") is None
    assert stack.get_cascading("foo", "b") is None
    assert stack.get_cascading("foo", "c") is None
    assert stack.get_cascading("foo", "d") is None

    assert stack.get_cascading("bar", "a") is None
    assert stack.get_cascading("bar", "b") is None
    assert stack.get_cascading("bar", "c") is None
    assert stack.get_cascading("bar", "d") is None

    assert stack.pop_frame("foo") == {}
    assert stack.pop_frame("bar") == {}

    assert stack.get_cascading("foo", "a") is None
    assert stack.get_cascading("foo", "b") is None
    assert stack.get_cascading("foo", "c") is None
    assert stack.get_cascading("foo", "d") is None

    assert stack.get_cascading("bar", "a") is None
    assert stack.get_cascading("bar", "b") is None
    assert stack.get_cascading("bar", "c") is None
    assert stack.get_cascading("bar", "d") is None

    assert redis.exists("bar:size", "bar:frame:0") == 0
    assert redis.exists("bar:frame:1") == 0
