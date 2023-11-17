"""Module for patching lua redis function calls."""
from redipy.graph.expr import CallObj, ExprObj, find_literal, LiteralValObj
from redipy.plugin import LuaRedisPatch


class RSetPatch(LuaRedisPatch):
    """Converts the output of SET into a proper boolean value."""
    @staticmethod
    def names() -> set[str]:
        return {"set"}

    def patch(
            self,
            name: str,
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
    """Ensures GET-like operations return None (nil) if the key or value is
    missing."""
    @staticmethod
    def names() -> set[str]:
        return {"get", "lpop", "rpop", "hget"}

    def patch(
            self,
            name: str,
            expr: CallObj,
            args: list[ExprObj],
            *,
            is_expr_stmt: bool) -> ExprObj:
        if is_expr_stmt:
            return expr
        # check if 2nd argument (count) exists for lpop or rpop
        if len(args) > 1 and name in ["lpop", "rpop"]:
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
    """Converts the output of sorted set list functions into pair lists."""
    @staticmethod
    def names() -> set[str]:
        return {"zpopmax", "zpopmin"}

    def patch(
            self,
            name: str,
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
    """Uses the float variant for INCRBY-like functions and convert the result
    to a number."""
    @staticmethod
    def names() -> set[str]:
        return {"incrby", "hincrby"}

    def patch(
            self,
            name: str,
            expr: CallObj,
            args: list[ExprObj],
            *,
            is_expr_stmt: bool) -> ExprObj:
        name = f"{name}float"
        literal: LiteralValObj = {
            "kind": "val",
            "type": "str",
            "value": name,
        }
        expr["args"][0] = literal
        if is_expr_stmt:
            return expr
        return {
            "kind": "call",
            "name": "tonumber",
            "args": [expr],
            "no_adjust": False,
        }


class RHashGetSomePatch(LuaRedisPatch):
    """Uses keys and values to build a dictionary."""
    @staticmethod
    def names() -> set[str]:
        return {"hmget"}

    def patch(
            self,
            name: str,
            expr: CallObj,
            args: list[ExprObj],
            *,
            is_expr_stmt: bool) -> ExprObj:
        if is_expr_stmt:
            return expr
        return {
            "kind": "call",
            "name": f"{self.helper_pkg()}.keyval_dict",
            "args": [expr, *args[1:]],
            "no_adjust": False,
        }


class RHashGetAllPatch(LuaRedisPatch):
    """Converts an alternating key value list into a dictionary."""
    @staticmethod
    def names() -> set[str]:
        return {"hgetall"}

    def patch(
            self,
            name: str,
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
