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
"""Tests utility functions."""
from typing import Any

from redipy.util import (
    convert_pattern,
    escape,
    indent,
    lua_fmt,
    retain_some,
    unescape,
)


def test_retain_some() -> None:
    """Tests the function `retain_some`."""

    def test_rs(
            input_arr: list[int],
            count: int,
            output_arr: list[int],
            delete_arr: set[int],
            **kwargs: Any) -> None:
        res, to_delete = retain_some(
            input_arr, count, key=lambda v: v, **kwargs)
        assert res == output_arr
        assert set(to_delete) == delete_arr

    test_rs(
        [5, 3, 2, 1, 9, 3, 0, 6],
        4,
        [3, 3, 5, 6, 9],
        {0, 1, 2})
    test_rs(
        [5, 3, 2, 1, 9, 3, 0, 6],
        4,
        [3, 5, 6, 9],
        {0, 1, 2, 3},
        keep_last=False)
    test_rs(
        [5, 3, 2, 1, 9, 3, 0, 6],
        4,
        [6, 3, 2, 1, 0],
        {3, 5, 9},
        reverse=True)
    test_rs(
        [5, 3, 2, 1, 9, 3, 0, 6],
        4,
        [3, 2, 1, 0],
        {3, 5, 6, 9},
        reverse=True,
        keep_last=False)
    test_rs(
        [5, 3, 2, 1, 9, 3, 0, 6],
        5,
        [2, 3, 3, 5, 6, 9],
        {0, 1})
    test_rs(
        [5, 3, 2, 1, 9, 3, 0, 6],
        5,
        [3, 3, 5, 6, 9],
        {0, 1, 2},
        keep_last=False)
    test_rs(
        [5, 3, 2, 1, 9, 3, 0, 6],
        5,
        [6, 3, 3, 2, 1, 0],
        {5, 9},
        reverse=True)
    test_rs(
        [5, 3, 2, 1, 9, 3, 0, 6],
        5,
        [3, 3, 2, 1, 0],
        {5, 6, 9},
        reverse=True,
        keep_last=False)
    test_rs(
        [5, 3, 2, 1, 9, 3],
        5,
        [1, 2, 3, 3, 5, 9],
        set())
    test_rs(
        [5, 3, 2, 1, 9, 3],
        6,
        [1, 2, 3, 3, 5, 9],
        set(),
        keep_last=False)
    test_rs(
        [5, 3, 2],
        6,
        [2, 3, 5],
        set())
    test_rs(
        [5, 3, 2],
        6,
        [2, 3, 5],
        set(),
        keep_last=False)


def test_escape() -> None:
    """Tests the functions `escape` and `unescape`."""

    def test(text: str, subs: dict[str, str]) -> None:
        rsubs = {
            repl: key
            for key, repl in subs.items()
        }
        assert text == unescape(escape(text, subs), rsubs)

    test("abc", {"\n": "n"})
    test("abc\0\n", {"\n": "n"})
    test("\\n\n", {"\n": "n"})
    test("\\n0\\0\0\n", {"\n": "n"})

    test("abc", {"\0": "0"})
    test("abc\0\n", {"\0": "0"})
    test("\\n\n", {"\0": "0"})
    test("\\n0\\0\0\n", {"\0": "0"})

    test("abc", {"\n": "n", "\0": "0"})
    test("abc\0\n", {"\n": "n", "\0": "0"})
    test("\\n\n", {"\n": "n", "\0": "0"})
    test("\\n0\\0\0\n", {"\n": "n", "\0": "0"})


def test_indent() -> None:
    """Test indentation functions."""
    fmt_str = lua_fmt(r"""
        local res = {}
        local key = nil
        for ix, value in ipairs(arr) do
            if ix % 2 == 1 then
                key = value
            else
                res[#res + 1] = {key, value}
            end
        end
        return res
    """)
    ref = (
        r"local res = {}"
        "\n"
        r"local key = nil"
        "\n"
        r"for ix, value in ipairs(arr) do"
        "\n"
        r"  if ix % 2 == 1 then"
        "\n"
        r"    key = value"
        "\n"
        r"  else"
        "\n"
        r"    res[#res + 1] = {key, value}"
        "\n"
        r"  end"
        "\n"
        r"end"
        "\n"
        r"return res"
        "\n"
    )
    assert fmt_str == ref
    ref_indent = [
        r"  local res = {}",
        r"  local key = nil",
        r"  for ix, value in ipairs(arr) do",
        r"    if ix % 2 == 1 then",
        r"      key = value",
        r"    else",
        r"      res[#res + 1] = {key, value}",
        r"    end",
        r"  end",
        r"  return res",
    ]
    fmt_ind = indent(fmt_str, 2)
    assert fmt_ind == ref_indent


def test_patterns() -> None:
    """Test redis pattern conversions."""

    def check(
            pattern: str,
            expect_prefix: str,
            expect_regex: str,
            match: list[str],
            reject: list[str]) -> None:
        prefix, pat = convert_pattern(pattern)
        print(f"checking: {pattern} '{prefix}' /{pat.pattern}/")
        print("prefix")
        assert prefix == expect_prefix
        print("regex")
        assert pat.pattern == f"^{expect_regex}$"
        for candidate in match:
            print(f"match {candidate}")
            assert pat.match(candidate) is not None, \
                f"{pattern} should match {candidate}"
        for candidate in reject:
            print(f"reject {candidate}")
            assert pat.match(candidate) is None, \
                f"{pattern} should reject {candidate}"

    check("abc", "abc", "abc", ["abc"], ["def", "a", ""])
    check(
        "abc*def",
        "abc",
        "abc.*def",
        ["abcdef", "abchidef", "abcdedef", "abcdefdef"],
        ["abc", "def", "", "abcdeabcf"])
    check(
        "abc?def",
        "abc",
        "abc.def",
        ["abcidef", "abcddef", "abccdef"],
        ["abc", "def", "", "abcdeabcf", "abcdef"])
    check("h?llo", "h", "h.llo", ["hello", "hallo", "hxllo"], ["abc", "hllo"])
    check("h*llo", "h", "h.*llo", ["hllo", "heeeello"], ["abc", "helo"])
    check("h[ae]llo", "h", "h[ae]llo", ["hello", "hallo"], ["hillo", "hllo"])
    check("h[^e]llo", "h", "h[^e]llo", ["hallo", "hbllo"], ["hello", "hllo"])
    check("h[a-b]llo", "h", "h[a-b]llo", ["hallo", "hbllo"], ["hcllo", "hllo"])
    check(
        r"abc\*def",
        "abc*def",
        r"abc\*def",
        ["abc*def"],
        ["abc", "def", "", "abcdeabcf", "abcdef", "abchidef"])
    check(
        r"abc\?def",
        "abc?def",
        r"abc\?def",
        ["abc?def"],
        ["abc", "def", "", "abcdeabcf", "abcdef", "abchidef"])
    check(
        r"abc\[de]f",
        r"abc[de]f",
        r"abc\[de]f",
        ["abc[de]f"],
        ["abc", "abcdf", "abcef", "", "abcdeabcf", "abcdef", "abchidef"])
    check(
        r"abc\[de\]f",
        r"abc[de]f",
        r"abc\[de\]f",
        ["abc[de]f"],
        ["abc", "abcdf", "abcef", "", "abcdeabcf", "abcdef", "abchidef"])
    check(
        r"abc\\*def",
        "abc\\",
        r"abc\\.*def",
        [r"abc\def", r"abc\\def", r"abc\dddef", r"abc\efdef"],
        ["abc", "def", "", "abcdeabcf", "abcdef", "abchidef"])
    check(
        r"abc\\\*def",
        r"abc\*def",
        r"abc\\\*def",
        [r"abc\*def"],
        [r"abc\\\*def", r"abc\\*def", r"abc\\\abcdef", r"abc\\\def"])
    check(
        r"h[a\-c]llo",
        "h",
        r"h[a\-c]llo",
        ["hallo", "hcllo", "h-llo"], ["hello", "hbllo", "hllo"])
    check(
        r"h[\^a]llo",
        "h",
        r"h[\^a]llo",
        ["hallo", "h^llo"], ["hello", "hbllo", "hllo"])
    check(
        r"h[*a]llo",
        "h",
        r"h[*a]llo",
        ["hallo", "h*llo"], ["hello", "hbllo", "haallo", "hllo", "heeello"])
    check(
        r"h[a?]llo",
        "h",
        r"h[a?]llo",
        ["hallo", "h?llo"], ["hello", "hbllo", "haallo", "hllo", "heeello"])
    check(
        r"h[\]a]llo",
        "h",
        r"h[\]a]llo",
        ["hallo", "h]llo"], ["hello", "hbllo", "h[]allo", "hllo", "heeello"])
    check(
        r"h[\\]llo",
        "h",
        r"h[\\]llo",
        [r"h\llo"], ["hello", "hbllo", r"h[\]allo", "hllo", r"h\\llo"])
    check(
        r"k???",
        "k",
        r"k...",
        ["k100", "k999"], ["ak100", "k1000", "k10"])
