from test.util import get_setup

import pytest


@pytest.mark.parametrize("rt_lua", [False, True])
def test_pipe(rt_lua: bool) -> None:
    rt = get_setup("test_pipe", rt_lua, lua_script="")

    rt.rpush("foo", "a", "b", "c", "d")
    rt.rpush("bar", "e", "f", "g")
    with rt.pipeline() as pipe:
        pipe.lpop("foo", 3)
        assert rt.llen("foo") == 4
        rt.lpush("foo", "-")
        assert rt.llen("foo") == 5
        pipe.rpop("bar")
        assert rt.llen("bar") == 3
        pipe.rpush("baz", "h")
        assert rt.llen("baz") == 0
        lpop_foo, lpop_bar, rpush_baz = pipe.execute()
    assert lpop_foo == ["-", "a", "b"]
    assert lpop_bar == "g"
    assert rpush_baz == 1
    assert rt.lpop("foo", 2) == ["c", "d"]
    assert rt.rpop("bar") == "f"
    assert rt.rpush("baz", "i") == 2
