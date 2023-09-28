from typing import Literal, TYPE_CHECKING, TypedDict

from redipy.graph.expr import ExprObj, IndexObj, RefIdObj, VarObj


if TYPE_CHECKING:
    from redipy.graph.seq import SequenceObj


AssignmentObj = TypedDict('AssignmentObj', {
    "kind": Literal["assign", "declare"],
    "assign": RefIdObj,
    "value": ExprObj,
})
AssignAtObj = TypedDict('AssignAtObj', {
    "kind": Literal["assign_at"],
    "assign": RefIdObj,
    "index": ExprObj,
    "value": ExprObj,
})
StmtObj = TypedDict('StmtObj', {
    "kind": Literal["stmt"],
    "expr": ExprObj,
})
BranchObj = TypedDict('BranchObj', {
    "kind": Literal["branch"],
    "condition": ExprObj,
    "then": 'SequenceObj',
    "else": 'SequenceObj',
})
ForLoopObj = TypedDict('ForLoopObj', {
    "kind": Literal["for"],
    "array": ExprObj,
    "index": IndexObj,
    "value": VarObj,
    "body": 'SequenceObj',
})
WhileObj = TypedDict('WhileObj', {
    "kind": Literal["while"],
    "condition": ExprObj,
    "body": 'SequenceObj',
})
ReturnObj = TypedDict('ReturnObj', {
    "kind": Literal["return"],
    "value": ExprObj | None,
})
CommandObj = (
    AssignmentObj
    | AssignAtObj
    | StmtObj
    | BranchObj
    | ForLoopObj
    | WhileObj
    | ReturnObj
)
