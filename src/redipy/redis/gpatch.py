from redipy.graph.expr import CallObj, ExprObj
from redipy.plugin import LuaGeneralPatch


class GStringFindPatch(LuaGeneralPatch):
    @staticmethod
    def names() -> set[str]:
        return {"string.find"}

    def patch(
            self,
            expr: CallObj,
            *,
            is_expr_stmt: bool) -> ExprObj:
        if is_expr_stmt:
            return expr
        return {
            "kind": "call",
            "name": f"{self.helper_pkg()}.nil_or_index",
            "args": [expr],
            "no_adjust": False,
        }


class GAsIntStrPatch(LuaGeneralPatch):
    @staticmethod
    def names() -> set[str]:
        return {"asintstr"}

    def patch(
            self,
            expr: CallObj,
            *,
            is_expr_stmt: bool) -> ExprObj:
        return {
            "kind": "call",
            "name": f"{self.helper_pkg()}.asintstr",
            "args": expr["args"],
            "no_adjust": False,
        }
