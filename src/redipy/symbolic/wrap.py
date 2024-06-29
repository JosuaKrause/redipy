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
"""A wrapper for easier redis function definition."""
from typing import Protocol

from redipy.backend.backend import ExecFunction
from redipy.backend.runtime import Runtime
from redipy.symbolic.expr import MixedType
from redipy.symbolic.seq import FnContext


class FunctionBuilder(Protocol):  # pylint: disable=too-few-public-methods
    """
    A redis script builder. Calling this function builds the script.
    """
    def __call__(self, ctx: FnContext) -> MixedType:
        """
        Builds a redis script.

        Args:
            ctx (FnContext): The function context.

        Returns:
            MixedType: The symbolic result value of the script.
        """


def redis_fn(builder: FunctionBuilder, rt: Runtime) -> ExecFunction:
    """
    Convenience wrapper to define redis scripts.

    Args:
        builder (FunctionBuilder): Function to build the redis script.
        rt (Runtime): The runtime to register the script with.

    Returns:
        ExecFunction: The callable redis script.
    """
    ctx = FnContext()
    return_value = builder(ctx)
    ctx.set_return_value(return_value)
    return rt.register_script(ctx)
