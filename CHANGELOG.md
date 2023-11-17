# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Breaking

- Scripts now need explicitly named arguments. ([#3][1])
- Script errors are now `ValueError` instead of `redis.exceptions.ResponseError`. ([#3][1])

### Added

- Inferring backend in Redis constructor. ([#3][1])
- Allow access to raw runtime and redis connections. ([#3][1])
- Executing scripts from a pipeline. ([#3][1])
- Documentation. ([#3][1])

### Bug-Fixes

- Ignored assignment after deleting a key in a pipeline. ([#3][1])
- Error when missing execute call in pipeline. ([#3][1])
- `HGETALL` returning the wrong result in lua. ([#3][1])
- `HSET` not working in the redis backend. ([#3][1])

### Changed

- More info about contributing in the readme. ([#3][1])

### Notable Internal Changes

- Remove usage of `nil` in lua scripts (in favor of `cjson.null`).

[1]: https://github.com/JosuaKrause/redipy/pull/3
