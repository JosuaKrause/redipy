"""Defines all execution graph node types of sequences. A sequence groups
together statements."""
from typing import Literal, TypedDict

from redipy.graph.cmd import CommandObj


SeqObj = TypedDict('SeqObj', {
    "kind": Literal["seq"],
    "cmds": list[CommandObj],
})
"""A normal sequence that does not add a new stack frame."""
ScriptObj = TypedDict('ScriptObj', {
    "kind": Literal["script"],
    "cmds": list[CommandObj],
    "argv": list[str],
    "keyv": list[str],
})
"""The base sequence of the script."""
FunctionObj = TypedDict('FunctionObj', {
    "kind": Literal["function"],
    "cmds": list[CommandObj],
    "argc": int,
})
"""A sequence for defining custom local functions."""


SequenceObj = SeqObj | ScriptObj | FunctionObj
"""A sequence grouping together statements."""
