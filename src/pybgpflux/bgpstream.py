import asyncio
import threading
from typing import Iterator, Literal
from collections import defaultdict
from heapq import merge
from operator import attrgetter
import logging
from tempfile import TemporaryDirectory

import bgpkit
from bgpkit.bgpkit_broker import BrokerItem

from pybgpflux.bgpstreamconfig import (
    BGPStreamConfig,
    FilterOptions,
    LiveStreamConfig,
)
from pybgpflux.bgpelement import BGPElement
from pybgpflux.bgpparser import (
    BGPParser,
    PyBGPKITParser,
    BGPKITParser,
    PyBGPStreamParser,
    BGPdumpParser,
)
from pybgpflux.rislive import RISLiveStream, jitter_buffer_stream
from pybgpflux.utils import Directory, get_shared_memory
from pybgpflux.downloader import (
    PREFETCH_SIZE,
    RCStream,
    download_all,
)

name2parser = {
    "pybgpkit": PyBGPKITParser,
    "bgpkit": BGPKITParser,
    "pybgpstream": PyBGPStreamParser,
    "bgpdump": BGPdumpParser,
}


logger = logging.getLogger(__name__)


class BGPStream:
    """Stream and process BGP messages from multiple collectors.

    BGPStream is a high-performance alternative to PyBGPStream that parses BGP
    MRT files using BGPKIT. It can stream both historical and live BGP data with
    support for advanced filtering, multiple parser backends, and memory-efficient
    lazy loading.

    Attributes:
        collectors (list[str]): List of collector names to fetch data from.
        data_type (list[Literal["update", "rib"]]): Data types to stream ("update" or "rib").
        ts_start (float | None): Start timestamp (Unix epoch). None for live mode.
        ts_end (float | None): End timestamp (Unix epoch). None for live mode.
        filters (FilterOptions): Filtering options for BGP elements.
        cache_dir (Directory | TemporaryDirectory): Cache directory for downloaded files.
        parser_name (str): Backend parser to use ("pybgpkit", "bgpkit", "bgpdump", "pybgpstream").
        max_concurrent_downloads (int): Maximum concurrent file downloads.
        ram_fetch (bool): Use RAM disk (/dev/shm, /Volumes/RAMDisk) if available.
        jitter_buffer_delay (float): Delay (seconds) for jitter buffer in live mode.

    Examples:
        Stream historical BGP data:

        ```python
        config = BGPStreamConfig(
            start_time=datetime.datetime(2010, 9, 1, 0, 0),
            end_time=datetime.datetime(2010, 9, 1, 2, 0),
            collectors=["route-views.wide"],
        )
        stream = BGPStream.from_config(config)
        for elem in stream:
            print(elem)
        ```

        Direct instantiation with filters:

        ```python
        stream = BGPStream(
            collectors=["route-views.wide"],
            data_type=["update"],
            ts_start=1283203200,
            ts_end=1283289600,
            filters=FilterOptions(origin_asn=64512),
            parser_name="bgpkit",
        )
        for elem in stream:
            print(f"{elem.prefix}: {elem.fields['as-path']}")
        ```

        Live streaming from RIS Live:

        ```python
        config = BGPStreamConfig(
            collectors=["rrc00"],
            data_types=["updates"],
        )
        stream = BGPStream.from_config(config)
        for elem in stream:
            print(f"Live: {elem.type} {elem.prefix}")
        ```
    """

    def __init__(
        self,
        collectors: list[str],
        data_type: list[Literal["update", "rib"]],
        ts_start: float = None,
        ts_end: float = None,
        filters: FilterOptions | None = None,
        cache_dir: str | None = None,
        max_concurrent_downloads: int | None = 10,
        ram_fetch: bool | None = True,
        parser_name: str | None = "pybgpkit",
        jitter_buffer_delay: float | None = 10.0,
    ):
        """Initialize a BGP stream.

        Args:
            collectors: List of collector names (e.g., ["route-views.wide", "rrc04"]).
            data_type: List of data types to stream ("update", "rib", or both).
            ts_start: Start timestamp (Unix epoch) for historical data. None for live mode.
            ts_end: End timestamp (Unix epoch) for historical data. None for live mode.
            filters: Optional FilterOptions to filter BGP elements. Defaults to no filtering.
            cache_dir: Directory to cache downloaded MRT files. If None, uses temporary directory.
            max_concurrent_downloads: Maximum concurrent downloads. Default is 10.
            ram_fetch: Use RAM disk for temporary files if available. Default is True.
            parser_name: Parser backend ("pybgpkit", "bgpkit", "bgpdump", "pybgpstream").
                Default is "pybgpkit" (no system dependencies).
            jitter_buffer_delay: Delay (seconds) for jitter buffer in live mode. Default is 10.0.

        Raises:
            ValueError: If parser_name is invalid.

        Note:
            For live mode, set both ts_start and ts_end to None.
            For historical data, both ts_start and ts_end must be provided.
        """
        # Stream config
        self.ts_start = ts_start
        self.ts_end = ts_end
        self.collectors = collectors
        self.data_type = data_type
        if not filters:
            filters = FilterOptions()
        self.filters = filters

        # Implementation config
        self.max_concurrent_downloads = max_concurrent_downloads
        self.ram_fetch = ram_fetch
        self.cache_dir = cache_dir
        if not parser_name:
            self.parser_name = "pybgpkit"
        else:
            self.parser_name = parser_name

        self.broker = bgpkit.Broker()
        self.parser_cls: BGPParser = name2parser[parser_name]

        # Live config
        self.jitter_buffer_delay = jitter_buffer_delay

    def _set_urls(self):
        """Set archive files URL with bgpkit broker and setup prefetch queues"""
        self.urls = {
            "rib": defaultdict(lambda: ([], asyncio.Queue(maxsize=PREFETCH_SIZE))),
            "update": defaultdict(lambda: ([], asyncio.Queue(maxsize=PREFETCH_SIZE))),
        }
        for data_type in self.data_type:
            items: list[BrokerItem] = self.broker.query(
                ts_start=int(self.ts_start - 60),
                ts_end=int(self.ts_end),
                collector_id=",".join(self.collectors),
                data_type=data_type,
            )
            for item in items:
                self.urls[data_type][item.collector_id][0].append(item.url)

    def __iter__(self):
        if self.ts_start is None and self.ts_end is None:
            return self._iter_live()
        return self._iter_archive()

    @staticmethod
    def _run_loop(loop: asyncio.AbstractEventLoop, ready: threading.Event):
        """Background thread: run the event loop forever until stopped."""
        asyncio.set_event_loop(loop)
        loop.call_soon(ready.set)
        loop.run_forever()

    def _iter_archive(self) -> Iterator[BGPElement]:
        """__iter__ for data types [ribs, updates] or [updates]"""

        if cache_dir := self.cache_dir:
            is_caching = True
            cache_dir = Directory(self.cache_dir)
        else:
            is_caching = False
            if self.ram_fetch:
                cache_dir = TemporaryDirectory(dir=get_shared_memory())
            else:
                cache_dir = TemporaryDirectory()

        with cache_dir:
            self._set_urls()

            loop = asyncio.new_event_loop()

            # Single background thread runs the event loop
            ready = threading.Event()
            bg_thread = threading.Thread(
                target=self._run_loop, args=(loop, ready), daemon=True
            )
            bg_thread.start()
            ready.wait()

            # Kick off all download tasks in the background thread
            asyncio.run_coroutine_threadsafe(
                download_all(self.urls, cache_dir.name, self.max_concurrent_downloads),
                loop,
            )

            streams = [
                RCStream(
                    self.parser_cls,
                    data_type,
                    collector,
                    self.filters,
                    is_caching,
                    async_q,
                    loop,
                )
                for data_type in self.urls
                for collector, (_, async_q) in self.urls[data_type].items()
            ]

            for bgpelem in merge(*streams, key=attrgetter("time")):
                if self.ts_start <= bgpelem.time <= self.ts_end:
                    yield bgpelem

    def _iter_live(self) -> Iterator[BGPElement]:
        ris_collectors = [
            collector for collector in self.collectors if collector[:3] == "rrc"
        ]

        stream = RISLiveStream(collectors=ris_collectors, filters=self.filters)

        if self.jitter_buffer_delay is not None and self.jitter_buffer_delay > 0:
            stream = jitter_buffer_stream(stream, buffer_delay=self.jitter_buffer_delay)

        for elem in stream:
            yield elem

    @classmethod
    def from_config(cls, config: BGPStreamConfig | LiveStreamConfig) -> "BGPStream":
        """Create a BGPStream from a configuration object.

        Factory method to create a stream from various configuration types,
        automatically handling conversions and parameter mappings.

        Args:
            config: Configuration object, one of:
                - BGPStreamConfig: Unified configuration with query and implementation options.
                - LiveStreamConfig: Configuration for live RIS Live streaming.

        Returns:
            BGPStream: Initialized stream ready for iteration.

        Examples:
            ```python
            from pybgpflux import BGPStreamConfig, BGPStream
            import datetime

            config = BGPStreamConfig(
                start_time=datetime.datetime(2010, 9, 1, 0, 0),
                end_time=datetime.datetime(2010, 9, 1, 2, 0),
                collectors=["route-views.wide"],
            )
            stream = BGPStream.from_config(config)
            for elem in stream:
                print(elem)
            ```
        """
        if isinstance(config, BGPStreamConfig):
            if not config.is_live():
                return cls(
                    ts_start=config.start_time.timestamp(),
                    ts_end=config.end_time.timestamp(),
                    collectors=config.collectors,
                    data_type=[dtype[:-1] for dtype in config.data_types],
                    filters=config.filters if config.filters else FilterOptions(),
                    cache_dir=str(config.cache_dir) if config.cache_dir else None,
                    max_concurrent_downloads=config.max_concurrent_downloads
                    if config.max_concurrent_downloads
                    else 10,
                    ram_fetch=config.ram_fetch if config.ram_fetch else None,
                    parser_name=config.parser if config.parser else "pybgpkit",
                )
            else:
                return cls(
                    collectors=config.collectors,
                    data_type=["update"],
                    filters=config.filters if config.filters else FilterOptions(),
                    jitter_buffer_delay=10,
                )

        elif isinstance(config, LiveStreamConfig):
            return cls(
                collectors=config.collectors,
                data_type=["update"],
                filters=config.filters if config.filters else FilterOptions(),
                jitter_buffer_delay=config.jitter_buffer_delay,
            )

        else:
            raise ValueError("Unsupported config type")
