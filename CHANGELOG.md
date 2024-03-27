# Changelog

## [0.6.0] - 2024-03-27

### Breaking

- Removed `set` and `get` ([#17])

### Added

- Redis function `EXPIRE` (`P*` versions and `PERSIST` as well) ([#17])
- Redis function `TTL` ([#17])
- Redis function `FLUSHALL` with implementation defined synchrony ([#17])

## [0.5.0] - 2024-03-06

### Changed

- Started renaming `set` to `set_value` and `get` to `get_value` ([#15])

### Added

- Add script functionality for dictionaries ([#15])
- Redis keys functions: `TYPE`, `SCAN`, `KEYS` ([#15])
- Redis set functions: `SADD`, `SREM`, `SISMEMBER`, `SCARD`, `SMEMBERS` ([#15])

## [0.4.2] - 2023-11-19

### Bug-Fixes

- Fix more pypi links.

## [0.4.1] - 2023-11-19

### Bug-Fixes

- Fix readme links.

## [0.4.0] - 2023-11-17

### Breaking

- Scripts now need explicitly named arguments. ([#3])
- Script errors are now `ValueError` instead of
  `redis.exceptions.ResponseError`. ([#3])
- Add name argument to `redipy.plugin.LuaRedisPatch`. ([#7])

### Added

- Added `ZRANGE` (partially) and `LRANGE`. ([#7])
- Inferring backend in Redis constructor. ([#3])
- Allow access to raw runtime and redis connections. ([#3])
- Executing scripts from a pipeline. ([#3])
- Documentation. ([#3])

### Bug-Fixes

- Ignored assignment after deleting a key in a pipeline. ([#3])
- Error when missing execute call in pipeline. ([#3])
- `HGETALL` returning the wrong result in lua. ([#3])
- `HSET` not working in the redis backend. ([#3])

### Changed

- More info about contributing in the readme. ([#3])

### Notable Internal Changes

- Removed some usage of `nil` in lua scripts (in favor of `cjson.null`). ([#3])

[#3]: https://github.com/JosuaKrause/redipy/pull/3
[#7]: https://github.com/JosuaKrause/redipy/pull/7
[#15]: https://github.com/JosuaKrause/redipy/pull/15
[#17]: https://github.com/JosuaKrause/redipy/pull/17
