# Changelog

## [0.5.2] - 2026-06-19

### Added

- Multiple broker support: Added support for different BGP brokers including BGPKIT (default), BGPStream, and BGPFinder
- New broker abstraction layer `BGPBroker` with implementations `BGPKITBroker` and `BGPStreamBroker`
- Added a new `broker` parameter to `BGPStreamConfig`, with supported values: `bgpkit`, `bgpstream`, and `bgpfinder`
- Strict typing throughout the codebase

### Changed

- Refactored parsers with improved type annotations and bug fixes
- Improved code structure with dedicated broker modules
- Better crash handling for archives downloader

### Fixed

- Various bug fixes in parser implementations
- Type annotation issues throughout the codebase

## [0.5.1] - 2026-06-08

### Added

- Remote parsing support for the `pybgpstream` parser backend. When `remote_parse=True` (default) and `pybgpstream` is selected, MRT files are parsed directly from the remote URL without downloading to disk or RAM.

### Fixed

- Prevent cache miss data race: concurrent processes writing the same cache file are now coordinated with a PID-based lock file, preventing partial reads.
- Background prefetch thread is now properly shut down after the stream is exhausted or abandoned.

### Removed

- Removed `chunk_time` parameter and chunked fetch/parse cycles (replaced by always-on async prefetch queues).

## [0.5.0] - 2026-05-14

### Changed

- Project renamed from `pybgpkitstream` to `pybgpflux`.
- `BGPKITStream` class renamed to `BGPStream`.
- `PyBGPKITStreamConfig` and `BGPStreamConfig` merged into a single flat `BGPStreamConfig`. Implementation parameters (`parser`, `cache_dir`, `ram_fetch`, `chunk_time`, `max_concurrent_downloads`) are now optional fields on `BGPStreamConfig` directly — no more nested config.
- CLI entry point renamed from `pybgpkitstream` to `pybgpflux`.

### Removed

- `PyBGPKITStreamConfig` class and its `bgpstream_config` nested field.
- `nest_bgpstream_params` model validator (no longer needed with flat config).
