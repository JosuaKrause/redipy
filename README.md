# redipy

`redipy` is a Python library that provides a uniform interface to Redis-like
storage systems. It allows you to use the same Redis API with different backends
that implement the same functionality, such as:

- `redipy.memory`: A backend that runs inside the current process and stores
  data in memory using Python data structures.
- `redipy.redis`: A backend that connects to an actual Redis instance and
  delegates all operations to it.

### Warning

This library is still early in development and not all redis functions are
available yet!
If you need certain functionality have a look at the
[contributing](#Contributing) section.
It is easy to add redis functions to the API.

## Installation
You can install `redipy` using pip:

```sh
pip install redipy
```

## Usage
To use `redipy`, you need to import the library and create a `redipy` client
object with the desired backend. For example:

```python
# Import the redipy library
import redipy

# Create a redipy client using the memory backend
r = redipy.Redis(backend="memory")

# Create a redipy client using the redis backend
r = redipy.Redis(backend="redis", host="localhost", port=6379)

# Or
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
official [redis](https://pypi.org/project/redis/) Python client library. You
can use them as you would normally do with `redis`. For example:

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

## Features
The main features of `redipy` are:

- Flexibility: You can choose from different backends that suit your needs and
  preferences, without changing your code or learning new APIs.

- Performance: You can leverage the high performance of Redis or other backends
  that offer fast and scalable data storage and retrieval.

- Compatibility: You can use any Redis client or tool with any backend.

- Migration: You can easily migrate data between different backends, or use
  multiple backends simultaneously.

- Scripting: You can create backend independent redis scripts without using lua.
  Scripts are written using a symbolic API in python.

## Scripts

Redis scripts can be defined via a symbolic API in python and can be executed
by any backend:

```python
ctx = redipy.script.FnContext()
cmp = ctx.add_arg("cmp")
inp = redipy.script.RedisList(ctx.add_key("inp"))
left = redipy.script.RedisList(ctx.add_key("left"))
right = redipy.script.RedisList(ctx.add_key("right"))

cur = ctx.add_local(inp.lpop())
# we consume "inp" until it is empty
loop = ctx.while_(cur.ne_(None))
b_then, b_else = loop.if_(redipy.script.ToNum(cur).lt_(cmp))
b_then.add(left.rpush(cur))
b_else.add(right.rpush(cur))
loop.add(cur.assign(inp.lpop()))

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

## Limitations
The current limitations of `redipy` are:

- Not all Redis commands are supported yet: This will eventually be resolved.
- The API differs slightly: Most notably stored values are always strings
  (i.e., the bytes returned by redis are decoded as utf-8).
- The semantic of redis functions inside scripts has been altered to feel more
  natural coming from python: Redis functions inside lua scripts often differ
  greatly from the documented behavior. For example, `LPOP` returns `false` for
  an empty list inside lua (instead of `nil`). While `LPOP` returns `None` in
  the python API. The script API of `redipy` has been altered to match the
  python API more closely. As the user doesn't code in lua directly the benefit
  of having a more consistent API outweighs the more complicated lua code that
  needs to be generated in the backend.
- Scripts aim to use python semantics as best as possible: In lua array indices
  start at 1. The script API uses a 0 based indexing system and transparently
  adjusts indices in the lua backend. Other, similar changes are performed
  as well.
- Scripts use JSON to pass arguments and return values: The arguments to the
  script are passed as JSON bytes for the lua backend. Keys are passed as is.
  The return value of the script is also converted into JSON when moving from
  lua to python. Note, that the empty dictionary (`{}`) and the empty list
  (`[]`) are indistinguishable in lua so `None` is returned instead if setting
  the return value to either of these.

## TODOs

- implement more redis functions
- `redipy.sql`: A backend that provides the Redis functionality via SQL on
  traditional database systems, such as SQLite, PostgreSQL, or MySQL.
- switch_backend: dynamically switch backends at runtime which (potentially)
  migrates data to the new backend
- documentation

## License
`redipy` is licensed under the [Apache License (Version 2.0)](LICENSE).

## Missing Redis or Lua Functions
If you encounter a missing redis or lua function please consider adding it
yourself (see #Contributing). However, if you need it only in your local setup
without API support or support for multiple backends, pipelines, etc. you can
use the plug-in mechanism.

For the memory backend you can use
`redipy.memory.rt.LocalRuntime.add_redis_function_plugin` or
`redipy.memory.rt.LocalRuntime.add_general_function_plugin`. The methods need
a module that contains subclasses of `redipy.plugin.LocalRedisFunction` and
`redipy.plugin.LocalGeneralFunction` respectively. Once the new functions are
defined via loading the plugin they can be used in a `redipy.script.FnContext`
via `redipy.script.RedisFn` or `redipy.script.CallFn` respectively.

Note, that `redipy.script.RedisFn` and `redipy.script.CallFn` can always be
used in redis backend scripts. However, calling functions this way will have
the native lua behavior which can lead to surprising results.

Adding functions as described above is discouraged as it may lead to
inconsistent support of different backends and inconsistent behavior across
different backends.

## Contributing
The easiest way to contribute to `redipy` is to pick some redis API functions
that have not (or not completely) been implemented in `redipy` yet. For this
follow these steps:

1. Add the signature of the function to `redipy.api.RedisAPI`. Adjust as
  necessary from the redis spec to get a pythonic feel. Also, add the signature
  to `redipy.api.PipelineAPI` but with `None` as return value.
2. Implement the function in `redipy.redis.conn.RedisConnection` and
  `redipy.redis.conn.PipelineConnection`. This should
  be straightforward as there are not too many changes expected. Don't forget
  to convert bytes into strings via `...decode("utf-8")` (there are various
  helper functions for this in `redipy.util`).
3. Add tests to `test/test_sanity.py` to determine the function's behavior in
  lua (especially its edge cases).
4. If the lua behavior needs to be changed to provide a better feel you can
  monkeypatch the function call in `redipy.redis.lua.LuaFnHook.adjust_redis_fn`
  by either directly changing the returned expr for the execution graph or using
  a lua helper function via `redipy.redis.lua.HELPER_FNS`.
5. Next, add and implement the functionality in
  `redipy.memory.state.Machine` and add the appropriate redirects in
  `redipy.memory.rt.LocalRuntime` and `redipy.memory.rt.LocalPipeline`.
6. To make the new function accessible in scripts from the memory backend add
  a class in `redipy.memory.rfun.py`.
7. Add the approriate class or method in the right `redipy.symbolic.r...py`
  file. If it is a new class / file add an import to `redipy.script`.
8. Add a new test to verify the new function works inside a script for all
  backends. You can run `make pytest FILE=test/...py` to execute the test and
  `make coverage-report` to verify that the new code is executed.
9. Make sure `make lint-all` passes, as well as, all tests (`make pytest`)
  run without issue.

You can submit your patch as pull request
[here](https://github.com/JosuaKrause/redipy/pulls).

## Feedback
If you have any questions, suggestions, or issues with `redipy`, please feel
free to [open an issue](https://github.com/JosuaKrause/redipy/issues) on
GitHub. I would love to hear your feedback and improve `redipy`. Thank you!
