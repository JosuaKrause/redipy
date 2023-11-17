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
"""Assigns an expression to an index of a reference."""
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
    | StmtObj
    | BranchObj
    | ForLoopObj
    | WhileObj
    | ReturnObj
)
"""Defines all statements."""
