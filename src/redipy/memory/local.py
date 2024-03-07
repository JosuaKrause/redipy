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
"""Defines the script backend for the memory runtime."""
import json
from collections.abc import Callable
from typing import Any, cast, Literal, TYPE_CHECKING, TypedDict

from redipy.api import PipelineAPI, RedisAPI
from redipy.backend.backend import Backend, ExecFunction
from redipy.graph.cmd import CommandObj
from redipy.graph.expr import BinOps, ExprObj, JSONType
from redipy.graph.seq import SequenceObj
from redipy.util import json_compact


if TYPE_CHECKING:
    from redipy.memory.rt import LocalRuntime
    from redipy.memory.state import Machine


ExecState = tuple[
    list[str],  # keyv
    list[JSONType],  # argv
    list[dict[str, JSONType]],  # arg names
    list[dict[str, str]],  # key names
    list[list[JSONType]],  # stack
    'LocalRuntime',  # fns
    'Machine',  # redis state
    list[JSONType],  # return value
    dict[str, str],  # keys
    dict[str, JSONType],  # args
]
"""The execution state of the script. For fast access this is a tuple instead
of a dictionary. Use the `STATE_*` constants to access the individual fields
by copying the value into a local variable first (local variables are index
lookups in python while global variables are dictionary lookups)."""


STATE_KEYV: Literal[0] = 0
"""The list of key arguments to the script."""
STATE_ARGV: Literal[1] = 1
"""The list of value arguments to the script."""
STATE_ARG_NAMES: Literal[2] = 2
"""Mapping of script argument names to their values."""
STATE_KEY_NAMES: Literal[3] = 3
"""Mapping of script key argument names to their values."""
STATE_STACK: Literal[4] = 4
"""The local variable stack of the script. Each stack frame is a list for fast
variable access via index. The last stack frame in the list is the current one.
"""
STATE_FNS: Literal[5] = 5
"""An interface for function calls."""
STATE_SM: Literal[6] = 6
"""An interface for raw memory access (redis-like memory)."""
STATE_RETURN: Literal[7] = 7
"""The return value stack. The rightmost value is the current return value of
the script or function."""
STATE_KEYS: Literal[8] = 8
"""The actual key arguments to the script. This value is filled before the
script starts executing."""
STATE_ARGS: Literal[9] = 9
"""The actual arguments to the script. This value is filled before the
script starts executing."""


Cmd = Callable[[ExecState], None]
"""A command callback executing a statement."""
ExprCmd = Callable[[ExecState], JSONType]
"""A command callback executing an expression."""


CmdContext = TypedDict('CmdContext', {
    "local_count": int,
    "local_names": dict[str, int],
})
"""Command context to keep track of local variables."""


class _Uninit():
    """An uninitialized variable. When a stack frame is created during
    execution values are initialized with this singleton value."""
    def __str__(self) -> str:
        return "UNINIT"  # pragma: no cover

    def __repr__(self) -> str:
        return self.__str__()  # pragma: no cover


UNINIT = _Uninit()
"""Marks an uninitialized variable."""


class LocalBackend(
        Backend[Cmd, ExprCmd, CmdContext, CmdContext, 'LocalRuntime']):
    """The script backend for the memory runtime."""
    def create_command_context(self) -> CmdContext:
        return {
            "local_count": 0,
            "local_names": {},
        }

    def finish(self, ctx: CmdContext, script: Cmd) -> Cmd:
        # state_keyv = STATE_KEYV
        # state_argv = STATE_ARGV
        state_arg_names = STATE_ARG_NAMES
        state_key_names = STATE_KEY_NAMES
        state_stack = STATE_STACK
        # state_fns = STATE_FNS
        # state_sm = STATE_SM
        # state_return = STATE_RETURN
        # state_keys = STATE_KEYS
        # state_args = STATE_ARGS

        frame_size = ctx["local_count"]

        def exec_script(state: ExecState) -> None:
            state[state_stack].append([cast(JSONType, UNINIT)] * frame_size)
            state[state_arg_names].append({})
            state[state_key_names].append({})
            script(state)

        return exec_script

    def compile_sequence(
            self, ctx: CmdContext, seq: SequenceObj) -> Cmd:
        state_keyv = STATE_KEYV
        state_argv = STATE_ARGV
        # state_arg_names = STATE_ARG_NAMES
        # state_key_names = STATE_KEY_NAMES
        # state_stack = STATE_STACK
        # state_fns = STATE_FNS
        # state_sm = STATE_SM
        # state_return = STATE_RETURN
        state_keys = STATE_KEYS
        state_args = STATE_ARGS

        cmds = [
            self.compile_command(ctx, cmd)
            for cmd in seq["cmds"]
        ]
        if seq["kind"] == "seq":

            def exec_cmds(state: ExecState) -> None:
                for cmd in cmds:
                    cmd(state)

            return exec_cmds

        if seq["kind"] == "function":
            raise ValueError("not implemented yet")

        key_order = seq["keyv"]
        arg_order = seq["argv"]

        def exec_frame(state: ExecState) -> None:
            keys = state[state_keys]
            assert not state[state_keyv]
            state[state_keyv].extend(keys[key] for key in key_order)
            args = state[state_args]
            assert not state[state_argv]
            state[state_argv].extend(args[arg] for arg in arg_order)
            for cmd in cmds:
                cmd(state)

        return exec_frame

    def compile_command(
            self, ctx: CmdContext, cmd: CommandObj) -> Cmd:
        # state_keyv = STATE_KEYV
        # state_argv = STATE_ARGV
        state_arg_names = STATE_ARG_NAMES
        state_key_names = STATE_KEY_NAMES
        state_stack = STATE_STACK
        # state_fns = STATE_FNS
        # state_sm = STATE_SM
        state_return = STATE_RETURN
        # state_keys = STATE_KEYS
        # state_args = STATE_ARGS

        def declare_var(name: str) -> int:
            ix = ctx["local_count"]
            ctx["local_count"] += 1
            ctx["local_names"][name] = ix
            return ix

        if cmd["kind"] == "assign" or cmd["kind"] == "declare":
            var_name = cmd["assign"]["name"]
            is_declare = cmd["kind"] == "declare"
            rhs = self.compile_expr(ctx, cmd["value"])

            if cmd["assign"]["kind"] in ("var", "index"):
                if is_declare:
                    cur_ix = declare_var(var_name)
                else:
                    cur_ix = ctx["local_names"][var_name]

                def exec_var_assign(state: ExecState) -> None:
                    state[state_stack][-1][cur_ix] = rhs(state)

                return exec_var_assign

            if cmd["assign"]["kind"] == "arg":

                def exec_arg_assign(state: ExecState) -> None:
                    state[state_arg_names][-1][var_name] = rhs(state)

                return exec_arg_assign

            if cmd["assign"]["kind"] == "key":

                def exec_key_assign(state: ExecState) -> None:
                    key = rhs(state)
                    state[state_key_names][-1][var_name] = f"{key}"

                return exec_key_assign

            raise ValueError(f"unknown kind {cmd['assign']['kind']}")
        if cmd["kind"] == "assign_at":
            var_name = cmd["assign"]["name"]
            arr_index = self.compile_expr(ctx, cmd["index"])
            rhs = self.compile_expr(ctx, cmd["value"])

            if cmd["assign"]["kind"] == "var":
                cur_ix = ctx["local_names"][var_name]

                def exec_var_assign_at(state: ExecState) -> None:
                    arr = cast(list, state[state_stack][-1][cur_ix])
                    ix = int(cast(int, arr_index(state)))
                    elem = rhs(state)
                    if ix == len(arr):
                        arr.append(elem)
                    else:
                        arr[ix] = elem

                return exec_var_assign_at

            raise ValueError(
                f"cannot assign to position of {cmd['assign']['kind']}")
        if cmd["kind"] == "assign_key":
            var_name = cmd["assign"]["name"]
            obj_key = self.compile_expr(ctx, cmd["key"])
            rhs = self.compile_expr(ctx, cmd["value"])

            if cmd["assign"]["kind"] == "var":
                cur_ix = ctx["local_names"][var_name]

                def exec_var_assign_key(state: ExecState) -> None:
                    obj = cast(dict, state[state_stack][-1][cur_ix])
                    key = obj_key(state)
                    elem = rhs(state)
                    obj[key] = elem

                return exec_var_assign_key

            raise ValueError(
                f"cannot assign to key of {cmd['assign']['kind']}")
        if cmd["kind"] == "stmt":
            stmt = self.compile_expr(ctx, cmd["expr"])

            def exec_stmt(state: ExecState) -> None:
                stmt(state)

            return exec_stmt
        if cmd["kind"] == "branch":
            condition = self.compile_expr(ctx, cmd['condition'])
            exec_then = self.compile_sequence(ctx, cmd["then"])
            exec_else = self.compile_sequence(ctx, cmd["else"])

            def exec_if(state: ExecState) -> None:
                if condition(state):
                    exec_then(state)
                else:
                    exec_else(state)

            return exec_if
        if cmd["kind"] == "for":
            assert cmd["index"]["kind"] == "index"
            assert cmd["value"]["kind"] == "var"
            index_ix = declare_var(cmd["index"]["name"])
            value_ix = declare_var(cmd["value"]["name"])
            array_val = self.compile_expr(ctx, cmd["array"])
            loop_body = self.compile_sequence(ctx, cmd["body"])

            def exec_for(state: ExecState) -> None:
                arr = array_val(state)
                for ix, val in enumerate(cast(list, arr)):
                    state[state_stack][-1][index_ix] = ix
                    state[state_stack][-1][value_ix] = val
                    loop_body(state)

            return exec_for
        if cmd["kind"] == "while":
            condition = self.compile_expr(ctx, cmd['condition'])
            loop_body = self.compile_sequence(ctx, cmd["body"])

            def exec_while(state: ExecState) -> None:
                while condition(state):
                    loop_body(state)

            return exec_while
        if cmd["kind"] == "return":
            if cmd["value"] is None:

                def exec_return_none(state: ExecState) -> None:
                    state[state_return].append(None)

                return exec_return_none
            ret_val = self.compile_expr(ctx, cmd["value"])

            def exec_return_val(state: ExecState) -> None:
                state[state_return].append(ret_val(state))

            return exec_return_val
        raise ValueError(f"unknown kind {cmd['kind']} for command {cmd}")

    def compile_expr(self, ctx: CmdContext, expr: ExprObj) -> ExprCmd:
        state_keyv = STATE_KEYV
        state_argv = STATE_ARGV
        state_arg_names = STATE_ARG_NAMES
        state_key_names = STATE_KEY_NAMES
        state_stack = STATE_STACK
        state_fns = STATE_FNS
        state_sm = STATE_SM
        # state_return = STATE_RETURN
        # state_keys = STATE_KEYS
        # state_args = STATE_ARGS

        if expr["kind"] == "var":
            var_name = expr["name"]
            var_ix = ctx["local_names"][var_name]

            def get_var(state: ExecState) -> JSONType:
                res = state[state_stack][-1][var_ix]
                if res is UNINIT:
                    raise ValueError(f"{var_name} is uninitialized")
                return res

            return get_var
        if expr["kind"] == "index":
            ix_name = expr["name"]
            ix_ix = ctx["local_names"][ix_name]

            def get_index(state: ExecState) -> JSONType:
                res = state[state_stack][-1][ix_ix]
                if res is UNINIT:
                    raise ValueError(f"{ix_name} is uninitialized")
                return int(cast(int, res))

            return get_index
        if expr["kind"] == "arg":
            arg_name = expr["name"]
            arg_readable = expr["readable"]

            def get_arg(state: ExecState) -> JSONType:
                try:
                    return state[state_arg_names][-1][arg_name]
                except IndexError as ie:
                    raise KeyError(f"unknown argument {arg_readable}") from ie

            return get_arg
        if expr["kind"] == "key":
            key_name = expr["name"]
            key_readable = expr["readable"]

            def get_key(state: ExecState) -> JSONType:
                try:
                    return state[state_key_names][-1][key_name]
                except IndexError as ie:
                    raise KeyError(f"unknown argument {key_readable}") from ie

            return get_key
        if expr["kind"] == "load_json_arg":
            arg_ix = expr["index"]
            return lambda state: state[state_argv][arg_ix]
        if expr["kind"] == "load_key_arg":
            key_ix = expr["index"]
            return lambda state: state[state_keyv][key_ix]
        if expr["kind"] == "val":
            val_type = expr["type"]
            value = expr["value"]
            if val_type == "list":
                res_json = json_compact(value)
                return lambda state: json.loads(res_json)
            if val_type == "dict":
                res_json = json_compact(value)
                return lambda state: json.loads(res_json)
            if val_type == "bool":
                res: JSONType = bool(value)
            elif val_type == "int":
                res = int(cast(str, value))
            elif val_type == "float":
                res = float(cast(str, value))
            elif val_type == "str":
                res = f"{value}"
            elif val_type == "none":
                res = None
            else:
                raise ValueError(f"unknown value type {val_type} for {expr}")
            return lambda state: res
        if expr["kind"] == "unary":
            unary_op = self.compile_expr(ctx, expr["arg"])
            if expr["op"] == "not":
                return lambda state: not unary_op(state)
            raise ValueError(f"unknown op {expr['op']} for {expr}")
        if expr["kind"] == "binary":
            lhs = cast(
                Callable[[ExecState], Any],
                self.compile_expr(ctx, expr["left"]))
            rhs = cast(
                Callable[[ExecState], Any],
                self.compile_expr(ctx, expr["right"]))
            ops: dict[BinOps, ExprCmd] = {
                "and": lambda state: lhs(state) and rhs(state),
                "or": lambda state: lhs(state) or rhs(state),
                "add": lambda state: lhs(state) + rhs(state),
                "sub": lambda state: lhs(state) - rhs(state),
                "lt": lambda state: lhs(state) < rhs(state),
                "le": lambda state: lhs(state) <= rhs(state),
                "gt": lambda state: lhs(state) > rhs(state),
                "ge": lambda state: lhs(state) >= rhs(state),
                "eq": lambda state: lhs(state) == rhs(state),
                "ne": lambda state: lhs(state) != rhs(state),
            }
            op = ops.get(expr["op"])
            if op is not None:
                return op
            raise ValueError(f"unknown op {expr['op']} for {expr}")
        if expr["kind"] == "constant":
            raw = expr["raw"]

            def exec_const(state: ExecState) -> JSONType:
                return state[state_fns].get_constant(raw)

            return exec_const
        if expr["kind"] == "array_at":
            arr_ref = self.compile_expr(ctx, expr["arr"])
            arr_ix = self.compile_expr(ctx, expr["index"])

            def exec_arr_at(state: ExecState) -> JSONType:
                return cast(list, arr_ref(state))[cast(int, arr_ix(state))]

            return exec_arr_at
        if expr["kind"] == "dict_key":
            obj_ref = self.compile_expr(ctx, expr["obj"])
            obj_key = self.compile_expr(ctx, expr["key"])

            def exec_dict_key(state: ExecState) -> JSONType:
                return cast(dict, obj_ref(state)).get(obj_key(state))

            return exec_dict_key
        if expr["kind"] == "array_len":
            # NOTE: can also be used for dict
            arr_ref = self.compile_expr(ctx, expr["var"])
            return lambda state: len(cast(list, arr_ref(state)))
        if expr["kind"] == "concat":
            exec_strs = [
                self.compile_expr(ctx, strobj)
                for strobj in expr["strings"]
            ]

            def exec_concat(state: ExecState) -> JSONType:
                strs = [f"{expr_str(state)}" for expr_str in exec_strs]
                return "".join(strs)

            return exec_concat
        if expr["kind"] == "call":
            exec_args = [
                self.compile_expr(ctx, arg_fn)
                for arg_fn in expr["args"]
            ]
            fn_name = expr["name"]

            def exec_call(state: ExecState) -> JSONType:
                args = [expr_arg(state) for expr_arg in exec_args]
                return state[state_fns].call_fn(state[state_sm], fn_name, args)

            return exec_call
        raise ValueError(f"unknown kind {expr['kind']} for expression {expr}")

    def create_executable(
            self,
            code: Cmd,
            runtime: 'LocalRuntime',
            ) -> ExecFunction:
        state_return = STATE_RETURN

        def exec_code_fn(
                *,
                keys: dict[str, str],
                args: dict[str, JSONType],
                active_rt: 'LocalRuntime',
                sm: 'Machine') -> JSONType:
            with active_rt.lock():
                success = False
                try:
                    state: ExecState = (
                        [],  # keyv
                        [],  # argv
                        [],  # arg names
                        [],  # key names
                        [],  # stack
                        active_rt,  # fns
                        sm,  # redis state
                        [],  # return value
                        keys,  # keys
                        args,  # args
                    )
                    code(state)
                    # print(f"state: {state}")
                    try:
                        res = state[state_return].pop()
                    except IndexError as e:
                        msg = "did you forget to call 'set_return_value'?"
                        raise ValueError(msg) from e
                    if res in ({}, []):
                        # NOTE: we turn empty lists or objects into None
                        # to have a consistent behavior with lua
                        res = None
                    success = True
                finally:
                    if not success:
                        print(f"state: {state}")
                return res

        def exec_code(
                *,
                keys: dict[str, str],
                args: dict[str, JSONType],
                client: RedisAPI | PipelineAPI | None = None) -> JSONType:
            active_rt = runtime
            if client is not None:
                from redipy.memory.rt import LocalPipeline, LocalRuntime

                if isinstance(client, LocalRuntime):
                    active_rt = client
                elif isinstance(client, LocalPipeline):
                    pipe = cast(LocalPipeline, client)
                    p_rt, p_sm = pipe.get_runtime_tuple()
                    pipe.add_cmd(
                        lambda: exec_code_fn(
                            keys=keys,
                            args=args,
                            active_rt=p_rt,
                            sm=p_sm))
                    return None
                else:
                    # FIXME: we could handle any runtime if we keep the
                    # intermediate representation around
                    get_rt = getattr(client, "get_memory_runtime")
                    if get_rt is None:
                        raise ValueError(f"incompatible runtime: {client}")
                    active_rt = get_rt()
            sm = active_rt.get_machine()
            return exec_code_fn(
                keys=keys, args=args, active_rt=active_rt, sm=sm)

        return exec_code
