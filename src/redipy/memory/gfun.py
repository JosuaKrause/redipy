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
"""Implements general global functions for the memory backend scripts."""
import json
from typing import cast

from redipy.graph.expr import JSONType
from redipy.plugin import ArgcSpec, LocalGeneralFunction


class GStringFindFn(LocalGeneralFunction):
    """Implements finding a substring in a string."""
    @staticmethod
    def name() -> str:
        return "string.find"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 2,
            "at_most": 3,
        }

    @staticmethod
    def call(args: list[JSONType]) -> JSONType:
        found_ix = f"{args[0]}".find(
            f"{args[1]}",
            int(cast(int, args[2])) if len(args) > 2 else None)
        return None if found_ix < 0 else found_ix


class GCJSONDecodeFn(LocalGeneralFunction):
    """Implements decoding a JSON."""
    @staticmethod
    def name() -> str:
        return "cjson.decode"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
        }

    @staticmethod
    def call(args: list[JSONType]) -> JSONType:
        return json.loads(f"{args[0]}")


class GCJSONEncodeFn(LocalGeneralFunction):
    """Implements encoding a JSON."""
    @staticmethod
    def name() -> str:
        return "cjson.encode"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
        }

    @staticmethod
    def call(args: list[JSONType]) -> JSONType:
        return json.dumps(
            f"{args[0]}",
            sort_keys=True,
            indent=None,
            separators=(",", ":"))


class GToNumberFn(LocalGeneralFunction):
    """Implements converting strings into numbers."""
    @staticmethod
    def name() -> str:
        return "tonumber"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
        }

    @staticmethod
    def call(args: list[JSONType]) -> JSONType:
        val = cast(str, args[0])
        try:
            return int(val)
        except (ValueError, TypeError):
            return float(val)


class GAsIntStrFn(LocalGeneralFunction):
    """Implements converting numbers into integer strings."""
    @staticmethod
    def name() -> str:
        return "asintstr"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
        }

    @staticmethod
    def call(args: list[JSONType]) -> JSONType:
        val = cast(str, args[0])
        try:
            return int(val)
        except (ValueError, TypeError):
            return int(float(val))


class GToStringFn(LocalGeneralFunction):
    """Implements converting a value into a string."""
    @staticmethod
    def name() -> str:
        return "tostring"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
        }

    @staticmethod
    def call(args: list[JSONType]) -> JSONType:
        oval = args[0]
        if isinstance(oval, bool):
            return f"{oval}".lower()
        # TODO dict, list
        if oval is None:
            return "nil"
        return f"{oval}"


class GTypeFn(LocalGeneralFunction):
    """Implements extracting the type of a value."""
    @staticmethod
    def name() -> str:
        return "type"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 1,
        }

    @staticmethod
    def call(args: list[JSONType]) -> JSONType:
        tmap: dict[type, str] = {
            bool: "boolean",
            dict: "table",
            float: "number",
            int: "number",
            list: "table",
            str: "string",
            type(None): "nil",
        }
        return tmap[type(args[0])]


class GRedisLogFn(LocalGeneralFunction):
    """Implements logging."""
    @staticmethod
    def name() -> str:
        return "redis.log"

    @staticmethod
    def argc() -> ArgcSpec:
        return {
            "count": 2,
        }

    @staticmethod
    def call(  # pylint: disable=useless-return
            args: list[JSONType]) -> JSONType:
        print(f"{args[0]}: {args[1]}")
        return None
