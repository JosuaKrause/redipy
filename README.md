# redipy

`redipy` is a Python library that provides a uniform interface to Redis-like
storage systems. It allows you to use the same Redis API with different backends
that implement the same functionality, such as:

- `redipy.memory`: A backend that runs inside the current process and stores
  data in memory using Python data structures.
- `redipy.redis`: A backend that connects to an actual Redis instance and
  delegates all operations to it.

[![redipy logo][logo-small]][logo]

### Warning

This library is still early in development and [not all redis functions are
available yet][implemented]!
If you need certain functionality or found a bug, have a look at the
[contributing](#contributing) section.
It is easy to add redis functions to the API.

## Quick Access

1. [Installation](#installation)
2. [Usage](#usage)
3. [Features](#features)
4. [Custom Scripts](#custom-scripts)
  * [Simple Example](#simple-example)
  * [Advanced Example](#advanced-example)
5. [Limitations](#limitations)
6. [Contributing](#contributing)
  * [If You Find a Bug](#if-you-find-a-bug)
  * [Missing Redis or Lua Functions](#missing-redis-or-lua-functions)
  * [Implementing New Redis Functions](#implementing-new-redis-functions)
7. [Changelog](#changelog)
8. [License](#license)
9. [Feedback](#feedback)

## Installation<a id="installation"></a>
You can install `redipy` using pip:

```sh
pip install redipy
```

## Usage<a id="usage"></a>
To use `redipy`, you need to import the library and create a `redipy` client
object with the desired backend. For example:

```python
# Import the redipy library
import redipy

# Create a redipy client using the memory backend
r = redipy.Redis()

# Create a redipy client using the redis backend
r = redipy.Redis(host="localhost", port=6379)

# Or preferred
r = redipy.Redis(
    cfg={
        "host": "localhost",
        "port": 6379,
        "passwd": "",
        "prefix": "",
    })

# You can specify the backend explicitly to ensure that the correct parameters
# are passed to the constructor
r = redipy.Redis(
    backend="redis",
    cfg={
        "host": "localhost",
        "port": 6379,
        "passwd": "",
        "prefix": "",
    })
```

The `redipy` client object supports similar methods and attributes to the
official [redis][redis] Python client library.
You can use them as you would normally do with `redis`. For example:

```python
# Set some values
r.set("foo", "bar")
r.set("baz", "qux")

# Get some values
r.get("foo")  # "bar"
r.get("baz")  # "qux"

# Push some values
r.lpush("mylist", "a", "b", "c")
r.rpush("mylist", "d")

# Pop values
r.lpop("mylist")  # "c"
r.rpop("mylist", 3)  # ["d", "a", "b"]
```

## Features<a id="features"></a>
The main features of `redipy` are:

- Flexibility: You can choose from different backends that suit your needs and
  preferences, without changing your code or learning new APIs.

- Adaptability: You can start your project small with the memory backend and
  only switch to a full redis server once the application grows.

- Scripting: You can create backend independent redis scripts without using lua.
  Scripts are written using a symbolic API in python.

- Compatibility: You can use any Redis client or tool with any backend.

- Mockability: You can use redipy in tests that require redis with the memory
  backend to easily mock the functionality without actually having to run a
  redis server in the background. Also, this avoids issues that might occur
  when running tests in parallel with an actual redis server.

- Performance: You can leverage the high performance of Redis or other backends
  that offer fast and scalable data storage and retrieval.

- Migration: You can easily migrate data between different backends, or use
  multiple backends simultaneously.

## Custom Scripts<a id="custom-scripts"></a>

Redis scripts can be defined via a symbolic API in python and can be executed
by any backend.

### Simple Example<a id="simple-example"></a>

Here, we are writing a filter function that drains a redis list
and puts items into a "left" and a "right" list by comparing each items
numerical value with a given `cmp` value:

```python
import redipy

# set up script
ctx = redipy.script.FnContext()
# add argument
cmp = ctx.add_arg("cmp")
# add key arguments
inp = redipy.script.RedisList(ctx.add_key("inp"))
left = redipy.script.RedisList(ctx.add_key("left"))
right = redipy.script.RedisList(ctx.add_key("right"))

# add local variable which contains the current value pop'ed from the list
cur = ctx.add_local(inp.lpop())
# we consume "inp" until it is empty
loop = ctx.while_(cur.ne_(None))
# push the value to the list depending on whether it is smaller than `cmp`
b_then, b_else = loop.if_(redipy.script.ToNum(cur).lt_(cmp))
b_then.add(left.rpush(cur))
b_else.add(right.rpush(cur))
# pop next value and store in local variable
loop.add(cur.assign(inp.lpop()))
# the script doesn't return a value
ctx.set_return_value(None)

# make sure to build the script only once and reuse the filter_list function
filter_list = r.register_script(ctx)

r.rpush("mylist", "1", "3", "2", "4")
filter_list(
    keys={
        "inp": "mylist",
        "left": "small",
        "right": "big",
    },
    args={
        "cmp": 3,
    })

r.lpop("mylist", 4)  # []
r.lpop("small", 4)  # ["1", "2"]
r.lpop("big", 4)  # ["3", "4"]
```

### Advanced Example<a id="advanced-example"></a>

Here, we are implementing and object stack with fall-through lookup. Each frame
in the stack has its own fields. If the user tries to access a field that
doesn't exist in the current stack frame (and they are using `get_cascading`)
the accessor will recursively go down the stack until a value for the given
field is found (or the end of the stack is reached).

```python
from typing import cast
from redipy import RedisClientAPI
from redipy.script import (
    ExecFunction,
    FnContext,
    JSONType,
    RedisHash,
    RedisVar,
    Strs,
    ToIntStr,
    ToNum,
)


class RStack:
    """An example class that simulates a key value stack."""
    def __init__(self, rt: RedisClientAPI) -> None:
        self._rt = rt

        self._set_value = self._set_value_script()
        self._get_value = self._get_value_script()
        self._pop_frame = self._pop_frame_script()
        self._get_cascading = self._get_cascading_script()

    def key(self, base: str, name: str) -> str:
        """
        Compute the key.

        Args:
            base (str): The base key.

            name (str): The name.

        Returns:
            str: The key associated with the name.
        """
        return f"{base}:{name}"

    def push_frame(self, base: str) -> None:
        """
        Pushes a new stack frame.

        Args:
            base (str): The base key.
        """
        self._rt.incrby(self.key(base, "size"), 1)

    def pop_frame(self, base: str) -> dict[str, str]:
        """
        Pops the current stack frame and returns its values.

        Args:
            base (str): The base key.

        Returns:
            dict[str, str] | None: The content of the stack frame.
        """
        res = self._pop_frame(
            keys={
                "size": self.key(base, "size"),
                "frame": self.key(base, "frame"),
            },
            args={})
        if res is None:
            return {}
        return cast(dict, res)

    def set_value(self, base: str, field: str, value: str) -> None:
        """
        Set a value in the current stack frame.

        Args:
            base (str): The base key.

            field (str): The field.

            value (str): The value.
        """
        self._set_value(
            keys={
                "size": self.key(base, "size"),
                "frame": self.key(base, "frame"),
            },
            args={"field": field, "value": value})

    def get_value(self, base: str, field: str) -> JSONType:
        """
        Returns a value from the current stack frame.

        Args:
            base (str): The base key.

            field (str): The field.

        Returns:
            JSONType: The value.
        """
        return self._get_value(
            keys={
                "size": self.key(base, "size"),
                "frame": self.key(base, "frame"),
            },
            args={"field": field})

    def get_cascading(self, base: str, field: str) -> JSONType:
        """
        Returns a value from the stack. If the value is not in the current
        stack frame the value is recursively retrieved from the previous
        stack frames.

        Args:
            base (str): The base key.

            field (str): The field.

        Returns:
            JSONType: The value.
        """
        return self._get_cascading(
            keys={
                "size": self.key(base, "size"),
                "frame": self.key(base, "frame"),
            },
            args={"field": field})

    def _set_value_script(self) -> ExecFunction:
        ctx = FnContext()
        rsize = RedisVar(ctx.add_key("size"))
        rframe = RedisHash(Strs(
            ctx.add_key("frame"),
            ":",
            ToIntStr(rsize.get(default=0))))
        field = ctx.add_arg("field")
        value = ctx.add_arg("value")
        ctx.add(rframe.hset({
            field: value,
        }))
        ctx.set_return_value(None)
        return self._rt.register_script(ctx)

    def _get_value_script(self) -> ExecFunction:
        ctx = FnContext()
        rsize = RedisVar(ctx.add_key("size"))
        rframe = RedisHash(Strs(
            ctx.add_key("frame"),
            ":",
            ToIntStr(rsize.get(default=0))))
        field = ctx.add_arg("field")
        ctx.set_return_value(rframe.hget(field))
        return self._rt.register_script(ctx)

    def _pop_frame_script(self) -> ExecFunction:
        ctx = FnContext()
        rsize = RedisVar(ctx.add_key("size"))
        rframe = RedisHash(
            Strs(ctx.add_key("frame"), ":", ToIntStr(rsize.get(default=0))))
        lcl = ctx.add_local(rframe.hgetall())
        ctx.add(rframe.delete())

        b_then, b_else = ctx.if_(ToNum(rsize.get(default=0)).gt_(0))
        b_then.add(rsize.incrby(-1))
        b_else.add(rsize.delete())

        ctx.set_return_value(lcl)
        return self._rt.register_script(ctx)

    def _get_cascading_script(self) -> ExecFunction:
        ctx = FnContext()
        rsize = RedisVar(ctx.add_key("size"))
        base = ctx.add_local(ctx.add_key("frame"))
        field = ctx.add_arg("field")
        pos = ctx.add_local(ToNum(rsize.get(default=0)))
        res = ctx.add_local(None)
        cur = ctx.add_local(None)
        rframe = RedisHash(cur)

        loop = ctx.while_(res.eq_(None).and_(pos.ge_(0)))
        loop.add(cur.assign(Strs(base, ":", ToIntStr(pos))))
        loop.add(res.assign(rframe.hget(field)))
        loop.add(pos.assign(pos - 1))

        ctx.set_return_value(res)
        return self._rt.register_script(ctx)
```

## Limitations<a id="limitations"></a>
The current limitations of `redipy` are:

- Not all Redis commands are supported yet: This will eventually be resolved.
- The API differs slightly: Most notably stored values are always strings
  (i.e., the bytes returned by redis are decoded as utf-8).
- The semantic of redis functions inside scripts has been altered to feel more
  natural coming from python: Redis functions inside lua scripts often differ
  greatly from the documented behavior. For example, `LPOP` returns `false` for
  an empty list inside lua (instead of `nil` or `cjson.null`). While `LPOP`
  returns `None` in the python API. The script API of `redipy` has been altered
  to match the python API more closely. As the user doesn't code in lua directly
  the benefit of having a more consistent API outweighs the more complicated lua
  code that needs to be generated in the backend.
- Scripts aim to use python semantics as best as possible: In lua array indices
  start at 1. The script API uses a 0 based indexing system and transparently
  adjusts indices in the lua backend. Other, similar changes are performed
  as well.
- Scripts use JSON to pass arguments and return values: The arguments to the
  script are passed as JSON bytes for the lua backend. Keys are passed as is.
  The return value of the script is also converted into JSON when moving from
  lua to python. Note, that the empty dictionary (`{}`) and the empty list
  (`[]`) are indistinguishable in lua so `None` is returned instead of setting
  the return value to either of these.

## Contributing<a id="contributing"></a>

Any contribution, even if it is just creating an issue for a bug,
is much appreciated.

### If You Find a Bug<a id="if-you-find-a-bug"></a>

If you encounter a bug, please open an issue to draw attention to it or give
a thumbsup if the issue already exists. This helps with prioritizing
implementation efforts. Even if you cannot solve the bug yourself,
investigating why it happens or creating a PR to add test cases helps a lot.
If you have a fix for a bug don't hesistate to open a PR.

### Missing Redis or Lua Functions<a id="missing-redis-or-lua-functions"></a>
If you encounter a missing redis or lua function please consider adding it
yourself (see the [implementing](#implementing-new-redis-functions) section).
Here also opening an issue or giving a thumbsup to existing issues helps
with prioritization.

However, if you need it only in your local setup
without API support or support for multiple backends, pipelines, etc. you can
use the raw underlying redis connection via
`redipy.main.Redis.get_redis_runtime` and
`redipy.redis.conn.RedisConnection.get_connection` or make use of
the plug-in mechanism.

For the memory backend you can use
`redipy.memory.rt.LocalRuntime.add_redis_function_plugin` or
`redipy.memory.rt.LocalRuntime.add_general_function_plugin`. The methods need
a module that contains subclasses of `redipy.plugin.LocalRedisFunction` and
`redipy.plugin.LocalGeneralFunction` respectively. Once the new functions are
defined via loading the plugin they can be used in a `redipy.script.FnContext`
via `redipy.script.RedisFn` or `redipy.script.CallFn` respectively.

Note, that `redipy.script.RedisFn` and `redipy.script.CallFn` can always be
used in redis backend scripts. However, calling functions this way will have
the native lua behavior which can lead to surprising results. To patch those
up as well you can use `redipy.redis.lua.LuaBackend.add_redis_patch_plugin`,
`redipy.redis.lua.LuaBackend.add_general_patch_plugin`, and
`redipy.redis.lua.LuaBackend.add_helper_function_plugin` to add the subclasses
of `redipy.plugin.LuaRedisPatch`, `redipy.plugin.LuaRedisPatch`, and
`redipy.plugin.HelperFunction` respectively. Those functions then can also be
used with the `redipy.script.RedisFn` and `redipy.script.CallFn` commands.

Adding functions as described above is discouraged as it may lead to
inconsistent support of different backends and inconsistent behavior across
different backends.

### Implementing New Redis Functions<a id="implementing-new-redis-functions"></a>

The easiest way to contribute to `redipy` is to pick some redis API functions
that have not (or not completely) been [implemented][implemented] in `redipy`
yet. It is also much appreciated if you just add test cases or the stubs in a
PR. For a full implementation follow these steps:

1. Add the signature of the function to `redipy.api.RedisAPI`. Adjust as
  necessary from the redis spec to get a pythonic feel. Also, add the signature
  to `redipy.api.PipelineAPI` but with `None` as return value. Additionally,
  add the redirect to the backend in `redipy.main.Redis`.
2. Implement the function in `redipy.redis.conn.RedisConnection` and
  `redipy.redis.conn.PipelineConnection`. This should
  be straightforward as there are not too many changes expected. Don't forget
  to convert bytes into strings via `...decode("utf-8")` (there are various
  helper functions for this in `redipy.util`).
3. Add tests to `test/test_sanity.py` to determine the function's behavior in
  lua (especially its edge cases).
4. If the lua behavior needs to be changed to provide a better feel you can add
  a monkeypatch for the function call by either creating a class in
  `redipy.redis.rpatch` to directly change the returned expr for the execution
  graph or using a lua helper function via adding a class to
  `redipy.redis.helpers` (you need to use a patch to use the helper in the
  right location).
5. Next, add and implement the functionality in
  `redipy.memory.state.Machine` and add the appropriate redirects in
  `redipy.memory.rt.LocalRuntime` and `redipy.memory.rt.LocalPipeline`.
6. To make the new function accessible in scripts from the memory backend add
  a class in `redipy.memory.rfun`.
7. Add the approriate class or method in the right `redipy.symbolic.r...py`
  file. If it is a new class / file add an import to `redipy.script`.
8. Add a new test in `test/test_api.py` to verify the new function works inside
  a script for all backends. You can run `make pytest FILE=test/test_api.py`
  to execute the test and `make coverage-report` to verify that the new code
  is executed.
9. Make sure `make lint-all` passes, as well as, all tests (`make pytest`)
  run without issue.

You can submit your patch as pull request [here][pulls].

## Changelog<a id="changelog"></a>
The changelog can be found [here][changelog].

## License<a id="license"></a>
`redipy` is licensed under the [Apache License (Version 2.0)][license].

## Feedback<a id="feedback"></a>
If you have any questions, suggestions, or issues with `redipy`, please feel
free to [open an issue][issues] on GitHub. I would love to hear your feedback
and improve `redipy`. Thank you!

[changelog]: https://github.com/JosuaKrause/redipy/blob/main/CHANGELOG.md
[implemented]: https://github.com/JosuaKrause/redipy/issues/8
[issues]: https://github.com/JosuaKrause/redipy/issues
[license]: https://github.com/JosuaKrause/redipy/blob/v0.4.2/LICENSE
[logo-small]: https://raw.githubusercontent.com/JosuaKrause/redipy/v0.4.2/img/redipy_logo_small.png
[logo]: https://raw.githubusercontent.com/JosuaKrause/redipy/v0.4.2/img/redipy_logo.png
[pulls]: https://github.com/JosuaKrause/redipy/pulls
[redis]: https://pypi.org/project/redis/
