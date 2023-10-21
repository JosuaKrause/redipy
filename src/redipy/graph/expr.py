from typing import Literal, TypedDict


LiteralValueType = str | int | float | bool | list | None
JSONType = str | int | float | list | dict | None


ValueType = Literal["str", "int", "float", "bool", "list", "none"]


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


ArgObj = TypedDict('ArgObj', {
    "kind": Literal["arg"],
    "name": str,
    "readable": str,
})
KeyObj = TypedDict('KeyObj', {
    "kind": Literal["key"],
    "name": str,
    "readable": str,
})
VarObj = TypedDict('VarObj', {
    "kind": Literal["var"],
    "name": str,
})
IndexObj = TypedDict('IndexObj', {
    "kind": Literal["index"],
    "name": str,
})
RefIdObj = ArgObj | KeyObj | VarObj | IndexObj


LoadArg = TypedDict('LoadArg', {
    "kind": Literal["load_json_arg", "load_key_arg"],
    "index": int,
})
LiteralValObj = TypedDict('LiteralValObj', {
    "kind": Literal["val"],
    "value": LiteralValueType,
    "type": ValueType,
})
ConstantObj = TypedDict('ConstantObj', {
    "kind": Literal["constant"],
    "raw": str,
})
UnaryOpObj = TypedDict('UnaryOpObj', {
    "kind": Literal["unary"],
    "op": Literal["not"],
    "arg": 'ExprObj',
})
BinaryOpObj = TypedDict('BinaryOpObj', {
    "kind": Literal["binary"],
    "op": BinOps,
    "left": 'ExprObj',
    "right": 'ExprObj',
})
ArrayAtObj = TypedDict('ArrayAtObj', {
    "kind": Literal["array_at"],
    "var": RefIdObj,
    "index": 'ExprObj',
})
ArrayLengthObj = TypedDict('ArrayLengthObj', {
    "kind": Literal["array_len"],
    "var": RefIdObj,
})
ConcatObj = TypedDict('ConcatObj', {
    "kind": Literal["concat"],
    "strings": 'list[ExprObj]',
})
CallObj = TypedDict('CallObj', {
    "kind": Literal["call"],
    "name": str,
    "args": 'list[ExprObj]',
    "no_adjust": bool,
})
ExprObj = (
    RefIdObj
    | LoadArg
    | LiteralValObj
    | ConstantObj
    | UnaryOpObj
    | BinaryOpObj
    | ArrayAtObj
    | ArrayLengthObj
    | ConcatObj
    | CallObj
)


def get_literal(obj: ExprObj, vtype: ValueType | None = None) -> JSONType:
    if obj["kind"] != "val":
        return None
    if vtype is not None and obj["type"] != vtype:
        return None
    return obj["value"]


def is_none_literal(obj: ExprObj) -> bool:
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
