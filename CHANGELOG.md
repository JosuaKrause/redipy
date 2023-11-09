# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.4.0] - 2023-11-07

### Added

- New redis APIs.
- Inferring backend in Redis constructor.
- Allow access to raw runtime and redis connections.
- Executing scripts from a pipeline.
- Documentation.

### Fixed

- Ignored set after deleting a key in a pipeline.
- Error when missing execute call in pipeline.
- HGETALL returning the wrong result in lua.
- HSET not working in the redis backend.

### Changed

- More info about contributing in the readme.
- Scripts now need explicit named argument.

### Removed

- TODO
