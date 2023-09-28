from typing import Literal, TypedDict

from redipy.graph.cmd import CommandObj


SeqObj = TypedDict('SeqObj', {
    "kind": Literal["seq"],
    "cmds": list[CommandObj],
})
ScriptObj = TypedDict('ScriptObj', {
    "kind": Literal["script"],
    "cmds": list[CommandObj],
    "argv": list[str],
    "keyv": list[str],
})
FunctionObj = TypedDict('FunctionObj', {
    "kind": Literal["function"],
    "cmds": list[CommandObj],
    "argc": int,
})


SequenceObj = SeqObj | ScriptObj | FunctionObj
