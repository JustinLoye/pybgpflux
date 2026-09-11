# AGENTS.md

Guidance for AI agents working in the `pybgpflux` repository.

## What this project is

`pybgpflux` is a pure-Python, drop-in replacement for
[PyBGPStream](https://bgpstream.caida.org/). It queries a *broker* for MRT
archive URLs, downloads/parses them with a pluggable *parser* backend, and
yields time-ordered `BGPElement` records that are byte-for-byte comparable with
`pybgpstream.BGPElem`.

**PyBGPStream parity is the specification.** Most tests assert that a
`pybgpflux` stream returns exactly the same elements (or at least the same
count) as `pybgpstream` for the same config. When behaviour is ambiguous, match
PyBGPStream.

## Environment & commands

The project uses [uv](https://docs.astral.sh/uv/). There is a `.venv` in the
repo; always go through `uv run` (or `.venv/bin/python`) rather than a bare
`python`, which is not on `PATH`.

```sh
uv sync --all-extras --group dev   # install everything (test + docs groups)
uv run pytest tests                # full suite
uv run pytest tests/test_filters.py -k pybgpkit   # narrow run
uv run pybgpflux --help            # CLI entry point (src/pybgpflux/cli.py)
uv run mkdocs serve                # docs preview
uv build                           # wheel + sdist
```

Formatting/linting follows `ruff format` defaults (`ruff` is on `PATH` but there
is no config in `pyproject.toml`, so don't reformat files you aren't touching).
Type annotations are strict and pyright-clean — new code should keep the
`# pyright: ignore[...]` discipline used for untyped third-party imports
(`bgpkit`, `pybgpstream`).

### Tests hit the real network

Every test except `smoke_test.py` queries live brokers (BGPKIT, CAIDA) and
downloads real MRT files from RouteViews/RIS archives. Consequences:

- The suite is slow (minutes) and can fail from upstream outages rather than
  from your change. Distinguish the two before "fixing" anything.
- `tests/test_live.py` opens a RIS Live websocket and runs for ~30 s.
- `tests/test_stream.py` and `tests/test_filters.py` write into `./cache/`, which
  is gitignored and already populated — reuse it, and prefer running a single
  test file while iterating.
- Parser-specific tests self-skip when the backend binary/library is missing
  (`bgpdump`, `bgpkit-parser`, `pybgpstream`). All three happen to be installed
  in this environment, so skips signal a real problem.

## Architecture

```
BGPStreamConfig (pydantic)  →  BGPStream  →  Iterator[BGPElement]
                                  │
                                  ├── broker.query(config) → BrokerItem urls
                                  ├── downloader.safe_download_all()  (async, background thread)
                                  └── RCStream per (data_type, collector) → parser_cls
```

- `bgpstreamconfig.py` — `BGPStreamConfig`, `FilterOptions`, `LiveStreamConfig`.
  Pydantic models; validators normalise datetimes to UTC and check that the
  selected parser is actually installed. This is the public, documented surface.
- `bgpstream.py` — `BGPStream`, the orchestrator. `_iter_archive()` starts an
  asyncio loop on a daemon thread, kicks off downloads, then `heapq.merge`s one
  `RCStream` per (data_type, collector) on `BGPElement.time`. `_iter_live()`
  delegates to RIS Live. `from_config()` is the intended entry point.
- `downloader.py` — async downloads with a global semaphore, retry/backoff,
  cross-process cache locking, and bounded prefetch queues (`PREFETCH_SIZE`)
  that provide backpressure. `RCStream` is the sync bridge: it pulls filepaths
  from the async queue via `run_coroutine_threadsafe` and feeds them to a parser.
- `parsers/` — one class per backend implementing the `BGPParser` protocol.
- `brokers/` — one class per archive index implementing the `BGPBroker` protocol.
- `bgpelement.py` — `BGPElement`, a `NamedTuple` mirroring `pybgpstream.BGPElem`
  (including its `__str__` pipe format). Keep it allocation-cheap; it is created
  millions of times per run.
- `rislive.py` — RIS Live websocket client plus a heap-based jitter buffer that
  reorders messages arriving out of order.

## Performance is a feature

This project exists to be faster than PyBGPStream, and `README.md` advertises
concrete speedups — a throughput regression is a product regression.

**`BGPStream.__iter__` and everything it drives is a hot loop.** A single run
yields millions of `BGPElement`s, so anything on the per-element path costs real
time: `BGPElement` construction, parser `__iter__` bodies, filter predicates, the
`heapq.merge` comparison key, and `RCStream`'s inner loop. On that path:

- No per-element allocations you can avoid, no pydantic validation, no logging
  calls, no repeated attribute lookups that could be bound once outside the loop.
- Hoist setup out: translating `FilterOptions` into a backend's native filter
  syntax, building subprocess arguments, and anything config-derived belongs in
  `__init__` or at module scope. The existing parsers all do this.
- Push filtering down to the backend (bgpkit/bgpdump/pybgpstream native filters)
  and only fall back to Python-side predicates for what the backend cannot do.
- Keep it lazy: generators end to end, never a `list(...)` over elements.
- Per-file and per-collector work (broker queries, download scheduling, RIB
  period filtering) happens a handful of times per run and is *not* hot — favour
  clarity there.

When a change could plausibly affect throughput, measure it. `perf.md` documents
the benchmark methodology and results.

### Invariants worth protecting

- **Elements are yielded in non-decreasing `time` order.** Several tests assert
  this with `pairwise`. Anything that reorders or parallelises merging must
  preserve it.
- **Lazy, bounded memory.** Streams are generators end to end; a RIB run must
  never materialise a full file. Don't introduce `list(...)` over elements.
- `data_types` are plural (`"ribs"`, `"updates"`) everywhere in this codebase.
  The BGPKIT broker API uses the singular form; `BGPKITBroker.query` translates
  in both directions. Don't leak the singular form outward.
- **Broker results are sorted by ascending start time**, guaranteed by
  `BGPBroker.query()`, and `BGPStream._set_urls` relies on it.
- `downloader.generate_cache_filename()` produces
  `cache-{rib|updates}.{YYYYMMDD}.{HHMM}.{crc32}.{gz|bz2}`. This is deliberately
  **compatible with the BGPKIT parser's own cache**, so a shared `cache_dir`
  works across tools. Changing the scheme is a breaking change.
- Cache writes are coordinated by a PID-bearing `<file>.lock` and a
  `<file>.<pid>.tmp` staging file so concurrent processes never read a partial
  archive (`tests/test_threadsafe.py` guards this).
- Temporary downloads go to `/dev/shm` when `ram_fetch=True`; the file is deleted
  right after parsing unless caching or remote parsing is on.

## Adding a parser backend

1. Implement the `BGPParser` protocol (`parsers/bgpparser.py`): the
   `(filepath, is_rib, collector, filters)` constructor, `__iter__` yielding
   `BGPElement`, and the `supports_remote_parsing` class attribute.
2. Translate `FilterOptions` into the backend's native filtering where possible,
   and fall back to Python-side filtering for the rest — but the *observable*
   result must be identical across backends (`tests/test_filters.py`
   parametrises every filter over every parser).
3. RIB elements get `type="R"`; updates get `"A"`/`"W"`. STATE messages are
   dropped.
4. Register it in four places: `name2parser` in `bgpstream.py`,
   `parsers/__init__.py`, the `parser` `Literal` in `BGPStreamConfig` (plus its
   availability check in `check_parser_available`), and `--parser` in `cli.py`.
5. Add it to `PARSERS_TO_TEST` in `tests/test_filters.py` with a skip marker.

## Adding a broker

Subclass `BGPBroker` and implement `_query(config) -> list[BrokerItem]`, raising
`BrokerQueryError` on failure or an empty result and normalising `data_type` to
the plural form. Do **not** override the public `query()`: it is a template
method that sorts results and applies broker-level query options (currently
`rib_period`) so every backend behaves the same, including when a broker is used
standalone without a stream. Then register the backend in the `match broker:`
block of `BGPStream.__init__` and in the `broker` `Literal` of
`BGPStreamConfig`.

`tests/test_brokers.py` cross-checks that brokers return the same archive set for
the same window.

**The BGPStream v2 API (`bgpstream`, `bgpfinder`) silently caps every response**,
and the cap differs per deployment — CAIDA by archive time (under two hours),
BGPFinder by file count (around five hundred) — so a single query returns a short
prefix of a longer window with nothing to signal it. `BGPStreamBroker` therefore
measures the page size from the first response and tiles the remaining interval
with chunks of that size, queried concurrently. Its `minInitialTime` cursor is
*not* a usable alternative: for sparse data types it can return the same file
again or skip one outright, depending on the deployment. Any test that exercises
these brokers must span more than two hours, or it will pass on truncated data —
this is exactly how the bug got in.

## Docs and release

- Docs are MkDocs Material under `docs/`, published to GitHub Pages.
  `docs/guide/*.md` embeds files from `examples/` via `--8<--` snippets, so every
  example must stay runnable — if you change an API, update the example, not just
  the prose.
- API reference pages are generated by mkdocstrings from Google-style
  docstrings; keep docstrings on public classes complete.
- Releasing: bump `version` in `pyproject.toml`, add a `CHANGELOG.md` entry, then
  push a `v*` tag — `.github/workflows/publish.yml` builds, smoke-tests the wheel
  and sdist, and publishes to PyPI.

## Local files

Two gitignored directories keep local work out of the package and out of commits:

- `scratch/` — debugging scripts, playgrounds, one-off benchmarks. Put throwaway
  code here rather than at the repository root, where it is easy to commit by
  accident and where a bare `pytest` would collect it (`norecursedirs` also
  excludes it).
- `data/` — MRT archives used by hand and by the standalone examples, which
  expect paths like `data/updates.20100901.0000.bz2`. These files are large and
  must never be committed.

Anything machine-specific that does not belong in the shared `.gitignore` goes in
`.git/info/exclude`, which is per-clone and not committed.
