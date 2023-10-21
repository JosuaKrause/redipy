from collections.abc import Callable
from test.util import get_test_config

import pytest

from redipy.api import RedisClientAPI
from redipy.backend.backend import ExecFunction
from redipy.graph.expr import JSONType
from redipy.main import Redis
from redipy.symbolic.expr import Strs
from redipy.symbolic.rhash import RedisHash
from redipy.symbolic.rvar import RedisVar
from redipy.symbolic.seq import FnContext
from redipy.util import code_fmt, lua_fmt


LUA_SET_VALUE = """
"""


LUA_GET_VALUE = """
"""


LUA_POP_FRAME = """
"""


LUA_GET_CASCADING = """
"""


class RStack:
    def __init__(
            self,
            rt: RedisClientAPI,
            base: str,
            set_lua_script: Callable[[str | None], None]) -> None:
        self._rt = rt
        self._base = base

        set_lua_script(LUA_SET_VALUE)
        self._set_value = self._set_value_script()
        set_lua_script(LUA_GET_VALUE)
        self._get_value = self._get_value_script()
        set_lua_script(LUA_POP_FRAME)
        self._pop_frame = self._pop_frame_script()
        set_lua_script(LUA_GET_CASCADING)
        self._get_cascading = self._get_cascading_script()
        set_lua_script(None)

    def key(self, name: str) -> str:
        return f"{self._base}:{name}"

    def init(self) -> None:
        self._rt.set(self.key("size"), "0")

    def push_frame(self) -> None:
        self._rt.incrby(self.key("size"), 1)

    def pop_frame(self) -> None:
        self._pop_frame(
            keys={"size": self.key("size"), "frame": self.key("frame")},
            args={})

    def set_value(self, field: str, value: str) -> None:
        self._set_value(
            keys={"size": self.key("size"), "frame": self.key("frame")},
            args={"field": field, "value": value})

    def get_value(self, field: str) -> JSONType:
        return self._get_value(
            keys={"size": self.key("size"), "frame": self.key("frame")},
            args={"field": field})

    def get_cascading(self, field: str) -> JSONType:
        return self._get_cascading(
            keys={"size": self.key("size"), "frame": self.key("frame")},
            args={"field": field})

    def _set_value_script(self) -> ExecFunction:
        ctx = FnContext()
        rsize = RedisVar(ctx.add_key("size"))
        rframe = RedisHash(Strs(ctx.add_key("frame"), ":", rsize.get()))
        field = ctx.add_arg("field")
        value = ctx.add_arg("value")
        rframe.hset({
            field: value,
        })
        ctx.set_return_value(None)
        return self._rt.register_script(ctx)

    def _get_value_script(self) -> ExecFunction:
        ctx = FnContext()
        rsize = RedisVar(ctx.add_key("size"))
        rframe = RedisHash(Strs(ctx.add_key("frame"), ":", rsize.get()))
        field = ctx.add_arg("field")
        ctx.set_return_value(rframe.hget(field))
        return self._rt.register_script(ctx)

    def _pop_frame_script(self) -> ExecFunction:
        ctx = FnContext()
        rsize = RedisVar(ctx.add_key("size"))
        rframe = RedisHash(Strs(ctx.add_key("frame"), ":", rsize.get()))
        rsize.incrby(-1)
        rframe.delete()
        ctx.set_return_value(None)
        return self._rt.register_script(ctx)

    def _get_cascading_script(self) -> ExecFunction:
        ctx = FnContext()
        rsize = RedisVar(ctx.add_key("size"))
        base = ctx.add_local(ctx.add_key("frame"))
        field = ctx.add_arg("field")
        pos = ctx.add_local(rsize.get())
        res = ctx.add_local(None)
        cur = ctx.add_local(None)
        rframe = RedisHash(cur)

        loop = ctx.while_(res.eq_(None).and_(pos.le_(0)))
        loop.add(cur.assign(Strs(base, ":", pos)))
        loop.add(res.assign(rframe.hget(field)))
        loop.add(pos.assign(pos - 1))

        ctx.set_return_value(res)
        return self._rt.register_script(ctx)


@pytest.mark.parametrize("rt_lua", [False, True])
def test_stack(rt_lua: bool) -> None:
    lua_script = None

    def set_lua_script(lscript: str | None) -> None:
        nonlocal lua_script

        lua_script = lscript

    def code_hook(code: list[str]) -> None:
        nonlocal lua_script

        if lua_script is None:
            return
        code_str = code_fmt(code)
        lscript = lua_fmt(lua_script)
        assert code_str == lscript

    redis = Redis(
        "redis" if rt_lua else "memory",
        cfg=get_test_config() if rt_lua else None,
        lua_code_hook=code_hook)
    stack_foo = RStack(redis, "foo", set_lua_script)
    stack_bar = RStack(redis, "bar", set_lua_script)

    stack_foo.init()
    stack_bar.init()

    stack_foo.set_value("a", "hi")
    assert stack_bar.get_value("a") is None
    assert stack_foo.get_value("a") == "hi"

    stack_foo.set_value("b", "foo")
    stack_bar.set_value("a", "bye")
    stack_bar.set_value("b", "bar")

    assert stack_foo.get_value("a") == "hi"
    assert stack_foo.get_value("b") == "foo"
    assert stack_foo.get_value("c") is None
    assert stack_foo.get_value("d") is None

    assert stack_bar.get_value("a") == "bye"
    assert stack_bar.get_value("b") == "bar"
    assert stack_bar.get_value("c") is None
    assert stack_bar.get_value("d") is None

    stack_foo.push_frame()
    stack_foo.set_value("b", "bb")
    stack_foo.set_value("c", "cc")
    stack_foo.set_value("d", "dd")

    stack_bar.push_frame()
    stack_bar.set_value("a", "2a")
    stack_bar.set_value("b", "2b")

    assert stack_foo.get_value("a") is None
    assert stack_foo.get_value("b") == "bb"
    assert stack_foo.get_value("c") == "cc"
    assert stack_foo.get_value("d") == "dd"

    assert stack_bar.get_value("a") == "2a"
    assert stack_bar.get_value("b") == "2b"
    assert stack_bar.get_value("c") is None
    assert stack_bar.get_value("d") is None

    assert stack_foo.get_cascading("a") == "hi"
    assert stack_foo.get_cascading("b") == "bb"
    assert stack_foo.get_cascading("c") == "cc"
    assert stack_foo.get_cascading("d") == "dd"

    assert stack_bar.get_cascading("a") == "2a"
    assert stack_bar.get_cascading("b") == "2b"
    assert stack_bar.get_cascading("c") is None
    assert stack_bar.get_cascading("d") is None

    stack_foo.push_frame()
    stack_foo.set_value("b", "bbb")
    stack_foo.set_value("c", "ccc")

    stack_bar.push_frame()
    stack_bar.set_value("b", "3b")
    stack_bar.set_value("c", "3c")

    assert stack_foo.get_value("a") is None
    assert stack_foo.get_value("b") == "bbb"
    assert stack_foo.get_value("c") == "ccc"
    assert stack_foo.get_value("d") is None

    assert stack_bar.get_value("a") is None
    assert stack_bar.get_value("b") == "3b"
    assert stack_bar.get_value("c") == "3c"
    assert stack_bar.get_value("d") is None

    assert stack_foo.get_cascading("a") == "hi"
    assert stack_foo.get_cascading("b") == "bbb"
    assert stack_foo.get_cascading("c") == "ccc"
    assert stack_foo.get_cascading("d") == "dd"

    assert stack_bar.get_cascading("a") == "2a"
    assert stack_bar.get_cascading("b") == "3b"
    assert stack_bar.get_cascading("c") == "3c"
    assert stack_bar.get_cascading("d") is None

    stack_foo.pop_frame()
    stack_bar.pop_frame()

    assert stack_foo.get_value("a") is None
    assert stack_foo.get_value("b") == "bb"
    assert stack_foo.get_value("c") == "cc"
    assert stack_foo.get_value("d") == "dd"

    assert stack_bar.get_value("a") == "2a"
    assert stack_bar.get_value("b") == "2b"
    assert stack_bar.get_value("c") is None
    assert stack_bar.get_value("d") is None

    assert stack_foo.get_cascading("a") == "hi"
    assert stack_foo.get_cascading("b") == "bb"
    assert stack_foo.get_cascading("c") == "cc"
    assert stack_foo.get_cascading("d") == "dd"

    assert stack_bar.get_cascading("a") == "2a"
    assert stack_bar.get_cascading("b") == "2b"
    assert stack_bar.get_cascading("c") is None
    assert stack_bar.get_cascading("d") is None

    stack_foo.pop_frame()
    stack_bar.pop_frame()

    assert stack_foo.get_value("a") == "hi"
    assert stack_foo.get_value("b") == "foo"
    assert stack_foo.get_value("c") is None
    assert stack_foo.get_value("d") is None

    assert stack_bar.get_value("a") == "bye"
    assert stack_bar.get_value("b") == "bar"
    assert stack_bar.get_value("c") is None
    assert stack_bar.get_value("d") is None

    assert stack_foo.get_cascading("a") == "hi"
    assert stack_foo.get_cascading("b") == "foo"
    assert stack_foo.get_cascading("c") is None
    assert stack_foo.get_cascading("d") is None

    assert stack_bar.get_cascading("a") == "bye"
    assert stack_bar.get_cascading("b") == "bar"
    assert stack_bar.get_cascading("c") is None
    assert stack_bar.get_cascading("d") is None
