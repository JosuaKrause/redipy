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
"""Patching general lua functions."""
from redipy.graph.expr import CallObj, ExprObj
from redipy.plugin import LuaGeneralPatch


class GStringFindPatch(LuaGeneralPatch):
    """Ensuring the index returned by the function is 0-based (python)."""
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
    """Adds a function to convert a number into an integer string."""
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
