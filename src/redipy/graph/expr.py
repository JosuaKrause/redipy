from typing import Literal, TypedDict


LiteralValueType = str | int | float | bool | list | None


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
    | CallObj
)
