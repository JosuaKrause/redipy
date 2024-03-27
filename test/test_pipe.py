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

    rt.set_value("value", "5")
    rt.set_value("third", "3")
    rt.lpush("lval", "1")
    rt.hset("hval", {"b": "b"})
    rt.zadd("zval", {"c": 1.0})
    rt.lpush("cval", "abc")
    assert rt.exists("value", "third", "lval", "hval", "zval") == 5
    with rt.pipeline() as pipe:
        pipe.delete("value")
        pipe.exists("value")
        pipe.set_value("value", "10")
        pipe.exists("value")
        v_0, v_1, v_2, v_3 = pipe.execute()
        pipe.set_value("other", "a")
        pipe.delete("third")
        v_4, v_5 = pipe.execute()
        pipe.delete("lval")
        pipe.exists("lval")
        pipe.lpush("lval", "2")
        v_6, v_7, v_8 = pipe.execute()
        pipe.delete("hval")
        pipe.exists("hval")
        pipe.hset("hval", {"a": "a"})
        v_9, v_10, v_11 = pipe.execute()
        pipe.delete("zval")
        pipe.exists("zval")
        pipe.zadd("zval", {"a": 0.5, "b": 1.5})
        v_12, v_13, v_14 = pipe.execute()
        pipe.delete("cval")
        pipe.exists("cval")
        pipe.hset("cval", {"a": "0", "b": "1", "c": "2"})
        v_15, v_16, v_17 = pipe.execute()
        pipe.set_value("late_val", "a")
        pipe.delete("late_val")
        pipe.set_value("late_val", "b")
        pipe.delete("late_val")
        pipe.set_value("late_val", "c")
        v_18, v_19, v_20, v_21, v_22 = pipe.execute()
    assert v_0 == True  # noqa
    assert v_1 == 0
    assert v_2 == True  # noqa
    assert v_3 == 1
    assert v_4 == True  # noqa
    assert v_5 == True  # noqa
    assert v_6 == True  # noqa
    assert v_7 == 0
    assert v_8 == 1
    assert v_9 == True  # noqa
    assert v_10 == 0
    assert v_11 == 1
    assert v_12 == True  # noqa
    assert v_13 == 0
    assert v_14 == 2
    assert v_15 == True  # noqa
    assert v_16 == 0
    assert v_17 == 3
    assert v_18 == True  # noqa
    assert v_19 == True  # noqa
    assert v_20 == True  # noqa
    assert v_21 == True  # noqa
    assert v_22 == True  # noqa
    assert rt.exists("value") == 1
    assert rt.exists("other") == 1
    assert rt.exists("third") == 0
    assert rt.get_value("value") == "10"
    assert rt.get_value("other") == "a"
    assert rt.get_value("third") is None
    assert rt.lpop("lval") == "2"
    assert rt.hgetall("hval") == {"a": "a"}
    assert rt.zpop_max("zval", 3) == [("b", 1.5), ("a", 0.5)]
    assert rt.hgetall("cval") == {"a": "0", "b": "1", "c": "2"}
    assert rt.hkeys("cval") == ["a", "b", "c"]
    assert rt.hvals("cval") == ["0", "1", "2"]
    with pytest.raises(TypeError, match=r"key.*(ha|i)s a"):
        assert rt.lpop("cval")
    assert rt.get_value("late_val") == "c"
