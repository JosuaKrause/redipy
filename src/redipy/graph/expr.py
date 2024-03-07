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
"""Defines all execution graph node types of expressions. An expression cannot
be executed alone and usually has no side-effects."""
from typing import Literal, TypedDict


LiteralValueType = str | int | float | bool | list | dict | None
"""A literal value."""
JSONType = str | int | float | list | dict | None
"""A literal value that can be converted to JSON."""


ValueType = Literal["str", "int", "float", "bool", "list", "dict", "none"]
"""Named literal value types."""


BinOps = Literal[
    "add",
    "sub",
    "and",
    "or",
    "eq",
    "ne",
    "lt",
    "gt",
    "le",
    "ge",
]
"""Binary operations."""


ArgObj = TypedDict('ArgObj', {
    "kind": Literal["arg"],
    "name": str,
    "readable": str,
})
"""Reads a script argument."""
KeyObj = TypedDict('KeyObj', {
    "kind": Literal["key"],
    "name": str,
    "readable": str,
})
"""Reads a script key argument."""
VarObj = TypedDict('VarObj', {
    "kind": Literal["var"],
    "name": str,
})
"""Reads a local variable."""
IndexObj = TypedDict('IndexObj', {
    "kind": Literal["index"],
    "name": str,
})
"""Reads an index variable."""
RefIdObj = ArgObj | KeyObj | VarObj | IndexObj
"""References some variable."""


LoadArg = TypedDict('LoadArg', {
    "kind": Literal["load_json_arg", "load_key_arg"],
    "index": int,
})
"""Loads a (key or value) argument. Usually used to assign to an argument
variable."""
LiteralValObj = TypedDict('LiteralValObj', {
    "kind": Literal["val"],
    "value": LiteralValueType,
    "type": ValueType,
})
"""Loads a literal value."""
ConstantObj = TypedDict('ConstantObj', {
    "kind": Literal["constant"],
    "raw": str,
})
"""Loads a named constant."""
UnaryOpObj = TypedDict('UnaryOpObj', {
    "kind": Literal["unary"],
    "op": Literal["not"],
    "arg": 'ExprObj',
})
"""Performs a unary operation."""
BinaryOpObj = TypedDict('BinaryOpObj', {
    "kind": Literal["binary"],
    "op": BinOps,
    "left": 'ExprObj',
    "right": 'ExprObj',
})
"""Performs a binary operation."""
ArrayAtObj = TypedDict('ArrayAtObj', {
    "kind": Literal["array_at"],
    "arr": 'ExprObj',
    "index": 'ExprObj',
})
"""Reads an index from an array."""
DictKeyObj = TypedDict('DictKeyObj', {
    "kind": Literal["dict_key"],
    "obj": 'ExprObj',
    "key": 'ExprObj',
})
"""Reads a key from a dictionary."""
ArrayLengthObj = TypedDict('ArrayLengthObj', {
    "kind": Literal["array_len"],
    "var": RefIdObj,
})
"""Reads the length of an array or dictionary."""
ConcatObj = TypedDict('ConcatObj', {
    "kind": Literal["concat"],
    "strings": 'list[ExprObj]',
})
"""String concatenates a sequence of expressions. The expressions should be
strings."""
CallObj = TypedDict('CallObj', {
    "kind": Literal["call"],
    "name": str,
    "args": 'list[ExprObj]',
    "no_adjust": bool,
})
"""Calls a function."""
ExprObj = (
    RefIdObj
    | LoadArg
    | LiteralValObj
    | ConstantObj
    | UnaryOpObj
    | BinaryOpObj
    | ArrayAtObj
    | DictKeyObj
    | ArrayLengthObj
    | ConcatObj
    | CallObj
)
"""An expression evaluates to a value."""


def get_literal(obj: ExprObj, vtype: ValueType | None = None) -> JSONType:
    """
    Returns the literal value of a literal expression object.

    Args:
        obj (ExprObj): The literal expression.

        vtype (ValueType | None, optional): The expected type.
        Defaults to None.

    Returns:
        JSONType: The literal value if the expression is a literal and the
        value has the correct type.
    """
    if obj["kind"] != "val":
        return None
    if vtype is not None and obj["type"] != vtype:
        return None
    return obj["value"]


def is_none_literal(obj: ExprObj) -> bool:
    """
    Whether a literal expression is None.

    Args:
        obj (ExprObj): The literal expression.

    Returns:
        bool: Whether it contains None as literal.
    """
    if obj["kind"] != "val":
        return False
    if obj["type"] != "none":
        return False
    return True


def find_literal(
        objs: list[ExprObj],
        value: JSONType,
        *,
        vtype: ValueType | None = None,
        no_case: bool = False) -> tuple[int, JSONType] | None:
    """
    Searches for a specified literal in a list of expressions.

    Args:
        objs (list[ExprObj]): The list of expressions.

        value (JSONType): The value to look for.

        vtype (ValueType | None, optional): The type of the value to look for.
            Defaults to None.

        no_case (bool, optional): Whether the value should be search without
            considering case (only for strings). Defaults to False.

    Returns:
        tuple[int, JSONType] | None: If found, the index and the literal.
        Otherwise None.
    """
    if vtype != "none" and value is not None:

        def value_check(obj: ExprObj) -> tuple[bool, JSONType]:
            res = get_literal(obj, vtype)
            if no_case and vtype == "str":
                return (f"{res}".upper() == f"{value}".upper(), res)
            return (res == value, res)

        check = value_check
    else:

        def none_check(obj: ExprObj) -> tuple[bool, JSONType]:
            is_none = is_none_literal(obj)
            return (is_none, None)

        check = none_check

    for ix, obj in enumerate(objs):
        is_hit, val = check(obj)
        if is_hit:
            return (ix, val)
    return None
