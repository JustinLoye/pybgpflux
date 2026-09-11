# Changelog

## [Unreleased]

### Added

- `rib_period` option on `BGPStreamConfig` and `BGPStream`, equivalent to `bgpreader -p`:
  sets the minimum archive-time interval between two RIBs of the same collector.
  `None` (default) keeps every RIB, a zero or negative interval keeps only the first
  RIB of each collector, and a positive interval keeps the first RIB then skips the
  following ones until that much archive time has elapsed. Updates are never affected.
  Also exposed on the CLI as `--rib-period <seconds>`.
- `BGPBroker` is now a base class with a template `query()` method, so broker-level
  query options such as `rib_period` apply to every broker, including when a broker
  is used standalone. Backends implement `_query()`.
- `BGPBroker.query()` guarantees results sorted by ascending archive start time.

### Changed

- `BGPStreamBroker` now stamps `BrokerItem.ts_start`/`ts_end` with the same ISO-8601
  format as the BGPKIT broker instead of raw epoch seconds.

### Fixed

- **`bgpstream` and `bgpfinder` brokers silently truncated long windows.** The
  BGPStream v2 API caps every response — by archive time on CAIDA (under two
  hours), by file count on BGPFinder (around five hundred) — so any longer stream
  was built from a prefix of the available files: a two-day window returned 32 of
  801 archives. The page size is now measured from the first response and the rest
  of the interval is tiled with chunks of that size, queried concurrently. All
  three brokers return the same file list for the same window.
- `BGPStream.from_config` now forwards the `broker` setting from `BGPStreamConfig`
  (it previously always used the default BGPKIT broker).

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
