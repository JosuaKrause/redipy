from test.util import get_setup

import pytest


@pytest.mark.parametrize("rt_lua", [False, True])
def test_pipe(rt_lua: bool) -> None:
    rt = get_setup("test_pipe", rt_lua, lua_script="")

    rt.rpush("foo", "a", "b", "c", "d")
    rt.rpush("bar", "e", "f", "g")
    with rt.pipeline() as pipe:
        left = pipe.lpop("foo")
        assert left is not None
        right = pipe.lpop("bar")
        assert right is not None
        assert pipe.rpush("baz", left, right) == 2
        lpop_foo, lpop_bar, rpush_baz = pipe.execute()
    assert lpop_foo == "a"
    assert lpop_bar == "e"
    assert rpush_baz == 2
