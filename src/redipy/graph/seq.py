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
