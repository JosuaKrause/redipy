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
    assert redis.get("foo") is None

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
    assert redis.get("bar") == "b"
    assert redis.set("baz", "c") is True
    assert redis.get("baz") == "c"

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


def test_ensure_name_available() -> None:
    """Verifies that new top level functions introduced in redipy do not exist
    already in redis or lua and would cause a name clash."""
    redis = RedisConnection(
        "test_ensure_name_available", cfg=get_test_config())

    def check_name(
            name: str,
            args: str,
            error_msg: str,
            error_notes: list[str]) -> None:
        code = f"return {name}({args})"
        run = redis.get_dynamic_script(code)
        with pytest.raises(
                ValueError,
                match=error_msg) as exc_info:
            with redis.get_connection() as conn:
                run(
                    keys=[],
                    args=[],
                    client=conn)
        assert exc_info.value.__notes__ == error_notes

    check_name(
        "asintstr",
        "3.2",
        r"user_script:1: Script attempted to access nonexistent global "
        r"variable 'asintstr'",
        [
            "Code:\nreturn asintstr(3.2)\n\nContext:\n  \n  \n  \n"
            "> return asintstr(3.2)",
        ])
