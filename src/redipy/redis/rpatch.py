from redipy.graph.expr import (
    CallObj,
    ExprObj,
    find_literal,
    get_literal,
    LiteralValObj,
)
from redipy.plugin import LuaRedisPatch


class RSetPatch(LuaRedisPatch):
    @staticmethod
    def names() -> set[str]:
        return {"set"}

    def patch(
            self,
            expr: CallObj,
            args: list[ExprObj],
            *,
            is_expr_stmt: bool) -> ExprObj:
        if is_expr_stmt:
            return expr
        if find_literal(
                args[1:], "GET", vtype="str", no_case=True) is not None:
            return expr
        return {
            "kind": "binary",
            "op": "ne",
            "left": expr,
            "right": {
                "kind": "val",
                "type": "bool",
                "value": False,
            },
        }


class RGetPatch(LuaRedisPatch):
    @staticmethod
    def names() -> set[str]:
        return {"get", "lpop", "rpop", "hget"}

    def patch(
            self,
            expr: CallObj,
            args: list[ExprObj],
            *,
            is_expr_stmt: bool) -> ExprObj:
        if is_expr_stmt:
            return expr
        return {
            "kind": "binary",
            "op": "or",
            "left": expr,
            "right": {
                "kind": "val",
                "type": "none",
                "value": None,
            },
        }


class RSortedPopPatch(LuaRedisPatch):
    @staticmethod
    def names() -> set[str]:
        return {"zpopmax", "zpopmin"}

    def patch(
            self,
            expr: CallObj,
            args: list[ExprObj],
            *,
            is_expr_stmt: bool) -> ExprObj:
        if is_expr_stmt:
            return expr
        return {
            "kind": "call",
            "name": f"{self.helper_pkg()}.pairlist_scores",
            "args": [expr],
            "no_adjust": False,
        }


class RIncrByPatch(LuaRedisPatch):
    @staticmethod
    def names() -> set[str]:
        return {"incrby", "hincrby"}

    def patch(
            self,
            expr: CallObj,
            args: list[ExprObj],
            *,
            is_expr_stmt: bool) -> ExprObj:
        name = f"{get_literal(expr['args'][0], 'str')}float"
        literal: LiteralValObj = {
            "kind": "val",
            "type": "str",
            "value": name,
        }
        expr["args"][0] = literal
        return expr


class RHashGetAllPatch(LuaRedisPatch):
    @staticmethod
    def names() -> set[str]:
        return {"hgetall"}

    def patch(
            self,
            expr: CallObj,
            args: list[ExprObj],
            *,
            is_expr_stmt: bool) -> ExprObj:
        if is_expr_stmt:
            return expr
        return {
            "kind": "call",
            "name": f"{self.helper_pkg()}.pairlist_dict",
            "args": [expr],
            "no_adjust": False,
        }
