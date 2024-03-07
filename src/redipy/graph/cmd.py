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
"""Defines all execution graph node types of statements. A statement can be
executed alone and usually has side-effects."""
from typing import Literal, TYPE_CHECKING, TypedDict

from redipy.graph.expr import ExprObj, IndexObj, RefIdObj, VarObj


if TYPE_CHECKING:
    from redipy.graph.seq import SequenceObj


AssignmentObj = TypedDict('AssignmentObj', {
    "kind": Literal["assign", "declare"],
    "assign": RefIdObj,
    "value": ExprObj,
})
"""Assigns an expression to a reference."""
AssignAtObj = TypedDict('AssignAtObj', {
    "kind": Literal["assign_at"],
    "assign": RefIdObj,
    "index": ExprObj,
    "value": ExprObj,
})
"""Assigns an expression to an index of an array reference."""
AssignKeyObj = TypedDict('AssignKeyObj', {
    "kind": Literal["assign_key"],
    "assign": RefIdObj,
    "key": ExprObj,
    "value": ExprObj,
})
"""Assigns an expression to a key of a dictionary reference."""
StmtObj = TypedDict('StmtObj', {
    "kind": Literal["stmt"],
    "expr": ExprObj,
})
"""Executes an expression as statement."""
BranchObj = TypedDict('BranchObj', {
    "kind": Literal["branch"],
    "condition": ExprObj,
    "then": 'SequenceObj',
    "else": 'SequenceObj',
})
"""A branch with subsequences whether an expression was truthy or falsey."""
ForLoopObj = TypedDict('ForLoopObj', {
    "kind": Literal["for"],
    "array": ExprObj,
    "index": IndexObj,
    "value": VarObj,
    "body": 'SequenceObj',
})
"""A loop iterating through an array."""
WhileObj = TypedDict('WhileObj', {
    "kind": Literal["while"],
    "condition": ExprObj,
    "body": 'SequenceObj',
})
"""A loop executing until the condition fails."""
ReturnObj = TypedDict('ReturnObj', {
    "kind": Literal["return"],
    "value": ExprObj | None,
})
"""Sets the return value of the current stack frame."""
CommandObj = (
    AssignmentObj
    | AssignAtObj
    | AssignKeyObj
    | StmtObj
    | BranchObj
    | ForLoopObj
    | WhileObj
    | ReturnObj
)
"""Defines all statements."""
