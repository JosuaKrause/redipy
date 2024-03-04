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
"""
This module contains various useful functions. The functions in this module are
not necessarily be considered part of the package API and might change or
disappear in the future. Use with caution outside of the package internals.
"""
import datetime
import hashlib
import inspect
import json
import os
import re
import string
import threading
import uuid
from collections.abc import Callable, Iterable
from typing import Any, IO, NoReturn, overload, TypeVar

import pytz


ET = TypeVar('ET')
CT = TypeVar('CT')
RT = TypeVar('RT')
VT = TypeVar('VT')


NL = "\n"


TEST_SALT_LOCK = threading.RLock()
TEST_SALT: dict[str, str] = {}


def is_test() -> bool:
    """
    Whether we are currently running a test with pytest.

    Returns:
        bool: Whether the current execution environment is within a pytest.
    """
    test_id = os.getenv("PYTEST_CURRENT_TEST")
    return test_id is not None


def get_test_salt() -> str | None:
    """
    Creates a unique salt to be used as redis key prefix for unit tests.

    Returns:
        str | None: A unique prefix for the current test. None if we are not
        running inside a pytest environment.
    """
    test_id = os.getenv("PYTEST_CURRENT_TEST")
    if test_id is None:
        return None
    res = TEST_SALT.get(test_id)
    if res is None:
        with TEST_SALT_LOCK:
            res = TEST_SALT.get(test_id)
            if res is None:
                res = f"salt:{uuid.uuid4().hex}"
                TEST_SALT[test_id] = res
    return res


def indent(text: str, amount: int = 0) -> list[str]:
    """
    Breaks a string into lines and indents each line by a set amount.

    Args:
        text (str): The text to break into lines.

        amount (int, optional): The indentation amount. Defaults to 0.

    Returns:
        list[str]: The indented lines.
    """
    istr = " " * amount
    return [
        f"{istr}{line}" for line in text.splitlines()
    ]


def deindent(text: str) -> str:
    """
    Removes a fixed amount of whitespace in front of each line. The amount is
    the maximum length of whitespace that is present on every line. The first
    line and empty lines are treated specially to result in a pleasing output.

    Args:
        text (str): The text to deindent.

    Returns:
        str: The deindented text.
    """
    min_indent = None
    lines = text.rstrip().splitlines()
    if lines and not lines[0]:
        lines.pop(0)
    for line in lines:
        if not line.strip():
            continue
        cur_indent = len(line) - len(line.lstrip())
        if min_indent is None or min_indent > cur_indent:
            min_indent = cur_indent
            if min_indent == 0:
                break
    return "".join((f"{line[min_indent:]}\n" for line in lines))


def lua_fmt(text: str) -> str:
    """
    Formats a multi-line string by deindenting and changing the indent size
    from 4 to 2.

    Args:
        text (str): The multi-line string.

    Returns:
        str: A deindented 2-space indented string.
    """
    return deindent(text).replace("    ", "  ")


def code_fmt(lines: list[str]) -> str:
    """
    Joins a list of lines.

    Args:
        lines (list[str]): The list of lines.

    Returns:
        str: The output as single string.
    """
    return "".join(f"{line.rstrip()}\n" for line in lines)


def get_text_hash(text: str) -> str:
    """
    Computes the blake2b hash of the given string.

    Args:
        text (str): The string.

    Returns:
        str: The hash of length text_hash_size.
    """
    blake = hashlib.blake2b(digest_size=32)
    blake.update(text.encode("utf-8"))
    return blake.hexdigest()


def text_hash_size() -> int:
    """
    The length of the hash produced by get_text_hash.

    Returns:
        int: The length of the hash.
    """
    return 64


def get_short_hash(text: str) -> str:
    """
    Computes a short blake2b hash of the given string.

    Args:
        text (str): The string.

    Returns:
        str: The hash of length short_hash_size.
    """
    blake = hashlib.blake2b(digest_size=4)
    blake.update(text.encode("utf-8"))
    return blake.hexdigest()


def short_hash_size() -> int:
    """
    The length of the hash produced by get_short_hash.

    Returns:
        int: The length of the hash.
    """
    return 8


BUFF_SIZE = 65536  # 64KiB
"""
The buffer size used to compute file hashes.
"""


def get_file_hash(fname: str) -> str:
    """
    Computes the blake2b hash of the given file's content.

    Args:
        fname (str): The filename.

    Returns:
        str: The hash of length file_hash_size.
    """
    blake = hashlib.blake2b(digest_size=32)
    with open(fname, "rb") as fin:
        while True:
            buff = fin.read(BUFF_SIZE)
            if not buff:
                break
            blake.update(buff)
    return blake.hexdigest()


def file_hash_size() -> int:
    """
    The length of the hash produced by get_file_hash.

    Returns:
        int: The length of the hash.
    """
    return 64


def is_hex(text: str) -> bool:
    """
    Whether the string represents a hexadecimal value.

    Args:
        text (str): The string to inspect.

    Returns:
        bool: Whether the string represents a hex value.
    """
    hex_digits = set(string.hexdigits)
    return all(char in hex_digits for char in text)


def only(arr: list[RT]) -> RT:
    """
    Extracts the only item of a single item list.

    Args:
        arr (list[RT]): The single item list.

    Raises:
        ValueError: If the length of the list is not 1.

    Returns:
        RT: The single item.
    """
    if len(arr) != 1:
        raise ValueError(f"array must have exactly one element: {arr}")
    return arr[0]


# time units for logging request durations
ELAPSED_UNITS: list[tuple[int, str]] = [
    (1, "s"),
    (60, "m"),
    (60*60, "h"),
    (60*60*24, "d"),
]
"""Unit conversions for time durations."""


def elapsed_time_string(elapsed: float) -> str:
    """Convert elapsed time into a readable string."""
    cur = ""
    for (conv, unit) in ELAPSED_UNITS:
        if elapsed / conv >= 1 or not cur:
            cur = f"{elapsed / conv:8.3f}{unit}"
        else:
            break
    return cur


def now() -> datetime.datetime:
    """
    Returns a timestamp representing now.

    Returns:
        datetime.datetime: The timestamp representing now.
    """
    return datetime.datetime.now(datetime.timezone.utc).astimezone()


def fmt_time(when: datetime.datetime) -> str:
    """
    Format a timestamp.

    Args:
        when (datetime.datetime): The timestamp.

    Returns:
        str: The formatted time.
    """
    return when.isoformat()


def get_time_str() -> str:
    """
    Formatted now.

    Returns:
        str: Formatted now.
    """
    return fmt_time(now())


def parse_time_str(time_str: str) -> datetime.datetime:
    """
    Parses a timestamp formatted via fmt_time.

    Args:
        time_str (str): The time as string.

    Returns:
        datetime.datetime: A timestamp.
    """
    return datetime.datetime.fromisoformat(time_str)


def time_diff(
        from_time: datetime.datetime, to_time: datetime.datetime) -> float:
    """
    Computes the time difference between time points in seconds.

    Args:
        from_time (datetime.datetime): The earlier time point.
        to_time (datetime.datetime): The later time point.

    Returns:
        float: The time difference in seconds. If `from_time` occurs after
        `to_time` the value will be negative.
    """
    return (to_time - from_time).total_seconds()


def to_bool(value: bool | float | int | str) -> bool:
    """
    Tries converting a given value to a boolean.

    Args:
        value (bool | float | int | str): The value.

    Raises:
        ValueError: If the value cannot be interpreted as boolean.

    Returns:
        bool: The value interpreted as boolean. A string can be 'true' or
        'false' and numbers are interpreted as True if they're non-zero.
    """
    value = f"{value}".lower()
    if value == "true":
        return True
    if value == "false":
        return False
    try:
        return bool(int(float(value)))
    except ValueError:
        pass
    raise ValueError(f"{value} cannot be interpreted as bool")


def to_list(value: Any) -> list[Any]:
    """
    Ensures a given value is a list.

    Args:
        value (Any): The value.

    Raises:
        ValueError: If it is not a list.

    Returns:
        list[Any]: The value as list.
    """
    if not isinstance(value, list):
        raise ValueError(f"{value} is not a list")
    return value


def is_int(value: Any) -> bool:
    """
    Determines whether a value can be interpreted as integer.

    Args:
        value (Any): The value.

    Returns:
        bool: Whether the value can be interpreted as integer.
    """
    try:
        int(value)
        return True
    except ValueError:
        return False


def is_float(value: Any) -> bool:
    """
    Determines whether a value can be interpreted as float.

    Args:
        value (Any): The value.

    Returns:
        bool: Whether the value can be interpreted as float.
    """
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def to_number_str(value: int | float) -> str:
    """
    Converts a number into a string. If the number appears as int it will be
    printed without decimal places even if its type is float.

    Args:
        value (int | float): The number.

    Returns:
        str: The string representation.
    """
    if int(value) == value:
        return f"{int(value)}"
    return f"{value}"


def is_json(value: str) -> bool:
    """
    Determines whether a value can be decoded as JSON.

    Args:
        value (Any): The value.

    Returns:
        bool: Whether the value can be decoded as JSON.
    """
    try:
        json.loads(value)
    except json.JSONDecodeError:
        return False
    return True


def report_json_error(err: json.JSONDecodeError) -> NoReturn:
    """
    Provides information about JSON decode errors.

    Args:
        err (json.JSONDecodeError): The JSON decode error.

    Raises:
        ValueError: The error raised again with more information about the
        issue.
    """
    raise ValueError(
        f"JSON parse error ({err.lineno}:{err.colno}): "
        f"{repr(err.doc)}") from err


def json_maybe_read(data: str) -> Any | None:
    """
    Maybe read a JSON.

    Args:
        data (str): The data that might contain a JSON.

    Returns:
        Any | None: The JSON or None if it is not a JSON decodable input.
    """
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        return None


def json_load(fin: IO[str]) -> Any:
    """
    Load a JSON document from a file-like object.

    Args:
        fin (IO[str]): The file-like input.

    Raises:
        e (ValueError): If the document cannot be parsed as JSON.

    Returns:
        Any: The JSON.
    """
    try:
        return json.load(fin)
    except json.JSONDecodeError as e:
        report_json_error(e)


def json_dump(obj: Any, fout: IO[str]) -> None:
    """
    Writes the given object as JSON to the given file-like object.

    Args:
        obj (Any): The object to dump as JSON.
        fout (IO[str]): The file-like output.
    """
    print(json_pretty(obj), file=fout)


def json_pretty(obj: Any) -> str:
    """
    Dumps the given object as pretty JSON string.

    Args:
        obj (Any): The object to dump.

    Returns:
        str: The pretty JSON string.
    """
    return json.dumps(obj, sort_keys=True, indent=2)


def json_compact(obj: Any) -> bytes:
    """
    Dumps the given object as compact JSON bytes.

    Args:
        obj (Any): The object to dump.

    Returns:
        bytes: The compact JSON bytes.
    """
    return json.dumps(
        obj,
        sort_keys=True,
        indent=None,
        separators=(",", ":")).encode("utf-8")


def json_read(data: bytes) -> Any:
    """
    Parses the given bytes as JSON.

    Args:
        data (bytes): The byte input.

    Raises:
        e (ValueError): If the document cannot be parsed as JSON.

    Returns:
        Any: The JSON.
    """
    try:
        return json.loads(data.decode("utf-8"))
    except json.JSONDecodeError as e:
        report_json_error(e)


def read_jsonl(fin: IO[str]) -> Iterable[Any]:
    """
    Reads a JSONL from a file-like object. Each line in the file is interpreted
    as JSON. Each JSON cannot contain a newline character.

    Args:
        fin (IO[str]): The file-like input.

    Raises:
        e (ValueError): If any line cannot be parsed as JSON.

    Yields:
        Any: One JSON object for each line in the input.
    """
    for line in fin:
        line = line.rstrip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError as e:
            report_json_error(e)


def from_timestamp(timestamp: float) -> datetime.datetime:
    """
    Converts a float POSIX-timestamp to a datetime object.

    Args:
        timestamp (float): The float POSIX-timestamp.

    Returns:
        datetime.datetime: The datetime object.
    """
    return datetime.datetime.fromtimestamp(timestamp, pytz.utc)


def to_timestamp(time: datetime.datetime) -> float:
    """
    Converts a datetime object into a float POSIX-timestamp.

    Args:
        time (datetime.datetime): The datetime object.

    Returns:
        float: The float POSIX-timestamp.
    """
    return time.timestamp()


def now_ts() -> datetime.datetime:
    """
    Returns a datetime object representing the current time.

    Returns:
        datetime.datetime: The datetime object.
    """
    return datetime.datetime.now(pytz.utc)


def get_function_info(*, clazz: type) -> tuple[str, int, str]:
    """
    Computes where in the current execution stack the instruction pointer was
    in the given class.

    Args:
        clazz (type): The class of interest.

    Returns:
        tuple[str, int, str]: At which file, line, and function the execution
        is when walking down the stack.
    """
    stack = inspect.stack()

    def get_method(cur_clazz: type) -> tuple[str, int, str] | None:
        class_filename = inspect.getfile(cur_clazz)
        for level in stack:
            if os.path.samefile(level.filename, class_filename):
                return level.filename, level.lineno, level.function
        return None

    queue = [clazz]
    while queue:
        cur = queue.pop(0)
        res = get_method(cur)
        if res is not None:
            return res
        queue.extend(cur.__bases__)
    frame = stack[1]
    return frame.filename, frame.lineno, frame.function


def get_relative_function_info(
        depth: int) -> tuple[str, int, str, dict[str, Any]]:
    """
    Computes the execution position of the stack frame `depth` levels down.

    Args:
        depth (int): The depth of the stack to inspect.

    Returns:
        tuple[str, int, str, dict[str, Any]]: The filename, line number,
        function name, and local variables.
    """
    depth += 1
    stack = inspect.stack()
    if depth >= len(stack):
        return "unknown", -1, "unknown", {}
    frame = stack[depth]
    return frame.filename, frame.lineno, frame.function, frame.frame.f_locals


def identity(obj: RT) -> RT:
    """
    A generic identity function.

    Args:
        obj (RT): The object.

    Returns:
        RT: The same object.
    """
    return obj


NUMBER_PATTERN = re.compile(r"\d+")
"""Regex to extract numbers."""


def extract_list(
        arr: Iterable[str],
        prefix: str | None = None,
        postfix: str | None = None) -> Iterable[tuple[str, str]]:
    """
    Extract substrings from a stream of strings.

    Args:
        arr (Iterable[str]): The stream of input strings.

        prefix (str | None, optional): The prefix to match. Defaults to None.

        postfix (str | None, optional): The postfix to match. Defaults to None.

    Yields:
        tuple[str, str]: Returns each matching string.
        The first element of the tuple is the full string. The second element
        is only the substring after removing the matching prefix and postfix.
    """
    for elem in arr:
        text = elem
        if prefix is not None:
            if not text.startswith(prefix):
                continue
            text = text[len(prefix):]
        if postfix is not None:
            if not text.endswith(postfix):
                continue
            text = text[:-len(postfix)]
        yield (elem, text)


def extract_number(
        arr: Iterable[str],
        prefix: str | None = None,
        postfix: str | None = None) -> Iterable[tuple[str, int]]:
    """
    Extract numbers from a stream of strings. The numbers are extracted from
    the substring excluding the prefix and postfix (if set). Additional
    non-numeric characters are allowed.

    Args:
        arr (Iterable[str]): The stream of strings.

        prefix (str | None, optional): The prefix must match. Defaults to None.

        postfix (str | None, optional): The postfix must match.
        Defaults to None.

    Yields:
        tuple[str, int]: The matching strings and the extracted
        numbers. The first element of the tuple is the full string. The second
        element is the extracted number.
    """

    def get_num(text: str) -> int | None:
        match = re.search(NUMBER_PATTERN, text)
        if match is None:
            return None
        try:
            return int(match.group())
        except ValueError:
            return None

    for elem, text in extract_list(arr, prefix=prefix, postfix=postfix):
        num = get_num(text)
        if num is None:
            continue
        yield elem, num


def highest_number(
        arr: Iterable[str],
        prefix: str | None = None,
        postfix: str | None = None) -> tuple[str, int] | None:
    """
    Retrieves the highest number extracted from a stream of strings. The
    numbers are extracted from the substring excluding the prefix and postfix
    (if set). Additional non-numeric characters are allowed. Only the match
    with the highest number is returned.

    Args:
        arr (Iterable[str]): The stream of strings.

        prefix (str | None, optional): The prefix to match. Defaults to None.

        postfix (str | None, optional): The postfix to match. Defaults to None.

    Returns:
        tuple[str, int] | None: The matching element with the highest number.
        The first element of the tuple is the matching string. The second
        element is the extracted number.
    """
    res = None
    res_num = 0
    for elem, num in extract_number(arr, prefix=prefix, postfix=postfix):
        if res is None or num > res_num:
            res = elem
            res_num = num
    return None if res is None else (res, res_num)


def retain_some(
        arr: Iterable[VT],
        count: int,
        *,
        key: Callable[[VT], Any],
        reverse: bool = False,
        keep_last: bool = True) -> tuple[list[VT], list[VT]]:
    """
    Retains up to `count` elements from `arr`.

    Args:
        arr (Iterable[VT]): The stream of elements.

        count (int): The number of elements to retain.

        key (Callable[[VT], Any]): The key for elements to define an order.

        reverse (bool, optional): Whether to reverse the order. Defaults to
        False.

        keep_last (bool, optional): Whether to retain the last
        elements. Defaults to True.

    Returns:
        tuple[list[VT], list[VT]]: The first element of the tuple is the list
        of elements to retain. The second element is the list of elements to
        remove.
    """
    res: list[VT] = []
    to_delete: list[VT] = []
    if keep_last:
        for elem in arr:
            if len(res) <= count:
                res.append(elem)
                continue
            res.sort(key=key, reverse=reverse)
            to_delete.extend(res[:-count])
            res = res[-count:]
            res.append(elem)
    else:
        for elem in arr:
            res.append(elem)
            if len(res) < count:
                continue
            res.sort(key=key, reverse=reverse)
            to_delete.extend(res[:-count])
            res = res[-count:]
    res.sort(key=key, reverse=reverse)
    return res, to_delete


def python_module() -> str:
    """
    Computes the caller's python module.

    Raises:
        ValueError: If the module cannot be found.

    Returns:
        str: The full name of the module.
    """
    stack = inspect.stack()
    module = inspect.getmodule(stack[1][0])
    if module is None:
        raise ValueError("module not found")
    res = module.__name__
    if res != "__main__":
        return res
    package = module.__package__
    if package is None:
        package = ""
    mfname = module.__file__
    if mfname is None:
        return package
    fname = os.path.basename(mfname)
    fname = fname.removesuffix(".py")
    if fname in ("__init__", "__main__"):
        return package
    return f"{package}.{fname}"


def parent_python_module(p_module: str) -> str:
    """
    Computes the parent module of a given module.

    Args:
        p_module (str): The module.

    Returns:
        str: The parent module or `""` if the module was already a top level
        module.
    """
    dot_ix = p_module.rfind(".")
    if dot_ix < 0:
        return ""
    return p_module[:dot_ix]


def check_pid_exists(pid: int) -> bool:
    """
    Checks whether a given pid exists.

    Args:
        pid (int): The process id.

    Returns:
        bool: Whether a process with the given id exists.
    """
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def ideal_thread_count() -> int:
    """
    Computes the ideal thread count for the given machine.

    Returns:
        int: The ideal number of threads.
    """
    res = os.cpu_count()
    if res is None:
        return 4
    return res


def escape(text: str, subs: dict[str, str]) -> str:
    """
    Escapes characters in the text according to the substitution dictionary.

    Args:
        text (str): The text.

        subs (dict[str, str]): The substitution dictionary. For example,
        `{"\\n", "n"}` would replace all newlines with `\\n` in the text.

    Returns:
        str: The escaped text.
    """
    text = text.replace("\\", "\\\\")
    for key, repl in subs.items():
        text = text.replace(key, f"\\{repl}")
    return text


def unescape(text: str, subs: dict[str, str]) -> str:
    """
    Unescapes characters in the text according to the substitution dictionary.

    Args:
        text (str): The text.

        subs (dict[str, str]): The substitution dictionary. For example,
        `{"n": "\\n"}` would replace all `\\n` sequences in the text with
        newlines.

    Returns:
        str: The unescaped text.
    """
    res: list[str] = []
    in_escape = False
    for c in text:
        if in_escape:
            in_escape = False
            if c == "\\":
                res.append("\\")
                continue
            done = False
            for key, repl in subs.items():
                if c == key:
                    res.append(repl)
                    done = True
                    break
            if done:
                continue
        if c == "\\":
            in_escape = True
            continue
        res.append(c)
    return "".join(res)


@overload
def to_maybe_str(res: bytes) -> str:
    ...


@overload
def to_maybe_str(res: None) -> None:
    ...


def to_maybe_str(res: bytes | None) -> str | None:
    """
    Converts bytes to a string. If the input is None then only None will be
    returned.

    Args:
        res (bytes | None): The bytes.

    Returns:
        str | None: The string from the bytes or None if the input was None.
    """
    if res is None:
        return res
    return res.decode("utf-8")


@overload
def to_list_str(
        res: Iterable[bytes],
        transform: Callable[[str], str] | None = None) -> list[str]:
    ...


@overload
def to_list_str(
        res: None,
        transform: Callable[[str], str] | None = None) -> None:
    ...


def to_list_str(
        res: Iterable[bytes] | None,
        transform: Callable[[str], str] | None = None) -> list[str] | None:
    """
    Converts a list of bytes into a list of strings. If the input is None
    then only None is returned.

    Args:
        res (Iterable[bytes] | None): The list of bytes.
        transform (Callable[[str], str] | None, optional): An optional mapping
            function to be applied to each element.

    Returns:
        list[str] | None: The list of strings. If the input was None then None
        is returned.
    """
    if res is None:
        return res
    if transform is not None:
        return [transform(val.decode("utf-8")) for val in res]
    return [val.decode("utf-8") for val in res]


def normalize_values(res: Any) -> Any:
    """
    Converts all bytes into string in the data structure.

    Args:
        res (Any): The data structure. Can be plain bytes, lists, tuples,
        dictionaries, and other literal values.

    Returns:
        Any: Returns the data structure with all bytes converted to strings.
    """
    if res is None:
        return None
    if isinstance(res, bytes):
        return res.decode("utf-8")
    if isinstance(res, list):
        return [normalize_values(val) for val in res]
    if isinstance(res, tuple):
        return tuple(normalize_values(val) for val in res)
    if isinstance(res, dict):
        return {
            normalize_values(key): normalize_values(value)
            for key, value in res.items()
        }
    return res


def convert_pattern(pattern: str) -> tuple[str, re.Pattern]:
    """
    Convert a redis pattern into a prefix and a regular expression.

    Args:
        pattern (str): The redis pattern. A redis pattern can contain the
            wildcards `*` (variable match) and `?` (single character match) and
            character groups `[...]` which can be negated via `^` and allow
            `-` to specify character ranges. `\\` can be used to use the
            literal special characters.

    Returns:
        tuple[str, re.Pattern]: A tuple of the longest prefix without special
            character and an equivalent regular expression.
    """
    bs = "\\"
    star = "*"
    one = "?"
    sqo = "["
    sqc = "]"
    setop = "^-"
    special = f"{star}{one}{sqo}"
    ix = 0
    is_bs = False
    is_set = False
    is_prefix = True
    prefix = ""
    pat = ""
    while ix < len(pattern):
        cur = pattern[ix]
        exclude_prefix = False
        is_escape = True
        reset_bs = True
        if cur == bs:
            is_escape = False
            if not is_bs:
                is_bs = True
                exclude_prefix = True
                reset_bs = False
        if not is_bs:
            if cur == sqc:
                is_escape = False
                is_set = False
            elif cur in special:
                is_escape = False
                if cur == sqo:
                    is_set = True
                elif not is_set:
                    if cur == star:
                        cur = ".*"
                    elif cur == one:
                        cur = "."
                is_prefix = False
            elif cur in setop:
                is_escape = False
        if is_prefix and not exclude_prefix:
            prefix += cur
        if is_escape and not is_bs:
            pat += re.escape(cur)
        else:
            pat += cur
        if reset_bs:
            is_bs = False
        ix += 1
    return prefix, re.compile(f"^{pat}$")
