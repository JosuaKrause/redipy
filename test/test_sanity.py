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
"""Tests to verify unintuitive redis or lua behavior."""
from test.util import get_test_config

import pytest
import redis as redis_lib

from redipy.redis.conn import RedisConnection


def test_sanity() -> None:
    """Test to verify unintuitive redis or lua behavior."""
    redis = RedisConnection("test_sanity", cfg=get_test_config())

    def check_expression(
            raw: str,
            out: str,
            *,
            keys: list[str] | None = None,
            args: list[str] | None = None) -> None:
        if keys is None:
            keys = []
        if args is None:
            args = []
        code = f"return tostring({raw})"
        run = redis.get_dynamic_script(code)
        with redis.get_connection() as conn:
            res = run(
                keys=[redis.with_prefix(key) for key in keys],
                args=args,
                client=conn)
        assert res.decode("utf-8") == out

    # get
    check_expression("redis.call('get', KEYS[1])", "false", keys=["foo"])
    check_expression(
        "type(redis.call('get', KEYS[1]))", "boolean", keys=["foo"])
    assert redis.get_value("foo") is None

    # set
    check_expression(
        "cjson.encode(redis.call('set', KEYS[1], 'd'))",
        r'{"ok":"OK"}',
        keys=["bar"])
    check_expression(
        "cjson.encode(redis.call('set', KEYS[1], 'a', 'NX'))",
        "false",
        keys=["bar"])
    check_expression(
        "cjson.encode(redis.call('set', KEYS[1], 'a', 'XX'))",
        r'{"ok":"OK"}',
        keys=["bar"])
    check_expression("redis.call('get', KEYS[1])", "a", keys=["bar"])
    check_expression(
        "type(redis.call('get', KEYS[1]))", "string", keys=["bar"])
    check_expression(
        "cjson.encode(redis.call('set', KEYS[1], 'b'))",
        r'{"ok":"OK"}',
        keys=["bar"])
    check_expression("redis.call('get', KEYS[1])", "b", keys=["bar"])
    check_expression(
        "type(redis.call('get', KEYS[1]))", "string", keys=["bar"])
    assert redis.get_value("bar") == "b"
    assert redis.set_value("baz", "c") is True
    assert redis.get_value("baz") == "c"

    # type
    check_expression(
        "redis.call('type', KEYS[1])['ok']", "string", keys=["bar"])
    check_expression(
        "type(redis.call('type', KEYS[1])['ok'])", "string", keys=["bar"])

    # lpop
    check_expression("redis.call('lpop', KEYS[1])", "false", keys=["foo"])
    check_expression(
        "type(redis.call('lpop', KEYS[1]))", "boolean", keys=["foo"])
    assert redis.lpop("foo") is None

    # rpop
    check_expression("redis.call('rpop', KEYS[1])", "false", keys=["foo"])
    check_expression(
        "type(redis.call('rpop', KEYS[1]))", "boolean", keys=["foo"])
    assert redis.rpop("foo") is None

    # llen
    check_expression("redis.call('llen', KEYS[1])", "0", keys=["foo"])
    check_expression(
        "type(redis.call('llen', KEYS[1]))", "number", keys=["foo"])
    assert redis.llen("foo") == 0

    # zpopmax
    check_expression(
        "cjson.encode(redis.call('zpopmax', KEYS[1]))", r"{}", keys=["foo"])
    assert redis.zpop_max("foo") == []  # pylint: disable=C1803
    check_expression("redis.call('zadd', KEYS[1], 2, 'a')", "1", keys=["zbar"])
    check_expression(
        "cjson.encode(redis.call('zpopmax', KEYS[1]))",
        "[\"a\",\"2\"]",
        keys=["zbar"])
    check_expression("redis.call('zadd', KEYS[1], 2, 'a')", "1", keys=["zbar"])
    check_expression(
        "cjson.encode(redis.call('zpopmax', KEYS[1], 1))",
        "[\"a\",\"2\"]",
        keys=["zbar"])
    check_expression("redis.call('zadd', KEYS[1], 2, 'a')", "1", keys=["zbar"])
    check_expression("redis.call('zadd', KEYS[1], 3, 'b')", "1", keys=["zbar"])
    check_expression(
        "cjson.encode(redis.call('zpopmax', KEYS[1], 2))",
        "[\"b\",\"3\",\"a\",\"2\"]",
        keys=["zbar"])
    check_expression("redis.call('zadd', KEYS[1], 2, 'a')", "1", keys=["zbar"])
    assert redis.zpop_max("zbar", 2) == [("a", 2)]
    check_expression("redis.call('zadd', KEYS[1], 2, 'a')", "1", keys=["zbar"])
    check_expression("redis.call('zadd', KEYS[1], 3, 'b')", "1", keys=["zbar"])
    assert redis.zpop_max("zbar", 2) == [("b", 3), ("a", 2)]

    # zpopmin
    check_expression(
        "cjson.encode(redis.call('zpopmin', KEYS[1]))", r"{}", keys=["foo"])
    assert redis.zpop_min("foo") == []  # pylint: disable=C1803
    check_expression("redis.call('zadd', KEYS[1], 2, 'a')", "1", keys=["zbar"])
    check_expression(
        "cjson.encode(redis.call('zpopmin', KEYS[1]))",
        "[\"a\",\"2\"]",
        keys=["zbar"])
    check_expression("redis.call('zadd', KEYS[1], 2, 'a')", "1", keys=["zbar"])
    check_expression(
        "cjson.encode(redis.call('zpopmin', KEYS[1], 1))",
        "[\"a\",\"2\"]",
        keys=["zbar"])
    check_expression("redis.call('zadd', KEYS[1], 2, 'a')", "1", keys=["zbar"])
    check_expression("redis.call('zadd', KEYS[1], 3, 'b')", "1", keys=["zbar"])
    check_expression(
        "cjson.encode(redis.call('zpopmin', KEYS[1], 2))",
        "[\"a\",\"2\",\"b\",\"3\"]",
        keys=["zbar"])
    check_expression("redis.call('zadd', KEYS[1], 2, 'a')", "1", keys=["zbar"])
    assert redis.zpop_min("zbar", 2) == [("a", 2)]
    check_expression("redis.call('zadd', KEYS[1], 2, 'a')", "1", keys=["zbar"])
    check_expression("redis.call('zadd', KEYS[1], 3, 'b')", "1", keys=["zbar"])
    assert redis.zpop_min("zbar", 2) == [("a", 2), ("b", 3)]

    # scard
    check_expression("redis.call('scard', KEYS[1])", "0", keys=["rset"])
    check_expression(
        "type(redis.call('scard', KEYS[1]))", "number", keys=["rset"])
    assert redis.sadd("rset", "a", "b", "c")
    check_expression("redis.call('scard', KEYS[1])", "3", keys=["rset"])
    check_expression(
        "type(redis.call('scard', KEYS[1]))", "number", keys=["rset"])
    assert redis.scard("rset") == 3


def test_ensure_name_available() -> None:
    """Verifies that new top level functions introduced in redipy do not exist
    already in redis or lua and would cause a name clash."""
    redis = RedisConnection(
        "test_ensure_name_available", cfg=get_test_config())

    def check_name(
            name: str,
            args: str,
            exc_type: type[BaseException],
            error_msg: str,
            error_chain: list[tuple[type[BaseException], str]]) -> None:
        code = f"return {name}({args})"
        run = redis.get_dynamic_script(code)
        with pytest.raises(exc_type, match=error_msg) as exc_info:
            with redis.get_connection() as conn:
                run(
                    keys=[],
                    args=[],
                    client=conn)
        cur_exc = exc_info.value
        for chain_type, chain_message in error_chain:
            next_exc = cur_exc.__cause__
            assert next_exc is not None
            assert isinstance(next_exc, chain_type)
            assert chain_message in f"{next_exc}"
            cur_exc = next_exc

    check_name(
        "asintstr",
        "3.2",
        ValueError,
        r"Error while executing script:.*user_script:[^:]*:[^\n]*asintstr"
        r"[^\n]*\nCode:\nreturn asintstr\(3\.2\)\n\n"
        r"Context:\n  \n  \n  \n> return asintstr\(3\.2\)",
        [
            (
                redis_lib.exceptions.ResponseError,
                r"user_script:1: Script attempted to access nonexistent "
                r"global variable 'asintstr'",
            ),
        ])
