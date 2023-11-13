"""Utilities for the test module."""
import json
import os
from collections.abc import Callable
from typing import TypeVar

import pytest

from redipy.backend.backend import ExecFunction
from redipy.backend.runtime import Runtime
from redipy.graph.seq import SequenceObj
from redipy.memory.rt import LocalRuntime
from redipy.redis.conn import RedisConfig, RedisConnection
from redipy.symbolic.seq import FnContext
from redipy.util import code_fmt, get_test_salt


def get_test_config() -> RedisConfig:
    """
    Returns the test configuration for redis.

    Returns:
        RedisConfig: The test redis connection information.
    """
    return {
        "host": "localhost",
        "port": 6380,
        "passwd": "",
        "prefix": f"test:{get_test_salt()}",
        "path": "userdata/test/",
    }


T = TypeVar('T')
BT = TypeVar('BT')
BR = TypeVar('BR')


def get_setup(
        test_name: str,
        rt_lua: bool,
        *,
        lua_script: str | None = None,
        no_compile_hook: bool = False,
        ) -> Runtime:
    """
    Creates a redipy runtime.

    Args:
        test_name (str): The name of the test.

        rt_lua (bool): Whether the runtime should be redis (True) or memory
        (False).

        lua_script (str | None, optional): If set the compiled lua code must
        match exactly. Defaults to None.

        no_compile_hook (bool, optional): If set no compilation info will be
        printed to stdout. Defaults to False.

    Returns:
        Runtime: The runtime.
    """
    if rt_lua:
        res: Runtime = RedisConnection(test_name, cfg=get_test_config())
        if lua_script is not None:

            def code_hook(code: list[str]) -> None:
                code_str = code_fmt(code)
                assert code_str == lua_script

            res.set_code_hook(code_hook)
    else:
        res = LocalRuntime()

    if not no_compile_hook:

        def compile_hook(compiled: SequenceObj) -> None:
            print(json.dumps(compiled, indent=2, sort_keys=True))

        res.set_compile_hook(compile_hook)
    return res


def run_code(
        rt: Runtime,
        ctx: FnContext,
        *,
        tests: list[tuple[BT, BR]],
        tester: Callable[[ExecFunction, BT], BR]) -> None:
    """
    Executes tests on the given script and verifies their outputs.

    Args:
        rt (Runtime): The runtime.

        ctx (FnContext): The script.

        tests (list[tuple[BT, BR]]): A list of input values and expected
        values.

        tester (Callable[[ExecFunction, BT], BR]): A function that takes the
        script callback and an input variable and outputs the results of the
        script.
    """
    runner = rt.register_script(ctx)

    for (t_values, t_expect) in tests:
        t_out = tester(runner, t_values)
        assert t_out == t_expect


def to_bool(text: str | None) -> bool:
    """
    Converts a value into boolean.

    Args:
        text (str | None): The value.

    Returns:
        bool: The boolean.
    """
    if text is None:
        return False
    try:
        return int(text) > 0
    except ValueError:
        pass
    return f"{text}".lower() == "true"


IS_GH_ACTION: bool | None = None


def is_github_action() -> bool:
    """
    Whether the code is running in a GitHub Action.

    Returns:
        bool: Whether the code is running in a GitHub Action.
    """
    global IS_GH_ACTION

    if IS_GH_ACTION is None:
        IS_GH_ACTION = to_bool(os.getenv("GITHUB_ACTIONS"))
    return IS_GH_ACTION


def skip_on_gha_if(condition: bool, reason: str) -> None:
    """
    Skip a test on GitHub Actions if a given condition is met.

    Args:
        condition (bool): The condition.

        reason (str): The reason for skipping.
    """
    if is_github_action() and condition:
        pytest.skip(reason)
