# pylint: disable=singleton-comparison
"""Tests pipeline functionality."""
from test.util import get_setup

import pytest


@pytest.mark.parametrize("rt_lua", [False, True])
def test_pipe(rt_lua: bool) -> None:
    """
    Tests pipeline functionality.

    Args:
        rt_lua (bool): Whether to use the redis or memory runtime.
    """
    rt = get_setup("test_pipe", rt_lua)

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

    with rt.pipeline() as pipe:
        assert rt.zadd("zset", {
            "a": 5,
            "b": 6,
            "c": 4,
        }) == 3
        pipe.zadd("zset", {
            "a": 0,
            "b": -1,
            "c": 1,
        })
        assert rt.zpop_min("zset") == [("c", 4)]
        pipe.zpop_min("zset")
        assert rt.zpop_max("zset") == [("b", 6)]
        pipe.zpop_max("zset")
        assert rt.zcard("zset") == 1
        pipe.zcard("zset")
        zadd_zset, zmin_zset, zmax_zset, zcard_zset = pipe.execute()
    assert zadd_zset == 2
    assert zmin_zset == [("b", -1)]
    assert zmax_zset == [("c", 1)]
    assert zcard_zset == 1

    rt.set("value", "5")
    rt.set("third", "3")
    with rt.pipeline() as pipe:
        pipe.delete("value")
        pipe.exists("value")
        pipe.set("value", "10")
        pipe.exists("value")
        v_0, v_1, v_2, v_3 = pipe.execute()
        pipe.set("other", "a")
        pipe.delete("third")
        v_4, v_5 = pipe.execute()
    assert v_0 == True  # noqa
    assert v_1 == False  # noqa
    assert v_2 == True  # noqa
    assert v_3 == True  # noqa
    assert v_4 == False  # noqa
    assert v_5 == True  # noqa
    assert rt.exists("value") == True  # noqa
    assert rt.exists("other") == True  # noqa
    assert rt.exists("third") == False  # noqa
    assert rt.get("value") == "10"
    assert rt.get("other") == "a"
    assert rt.get("third") is None

    # FIXME test deleting during a pipe and filling fresh (for all key types)
    # FIXME test deleting and creating a different key during a pipe
    # FIXME check if value "exists" after deleting in pipe
