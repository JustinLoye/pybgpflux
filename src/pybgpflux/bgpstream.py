import asyncio
import threading
from typing import Iterator, Literal
from collections import defaultdict
from heapq import merge
from operator import attrgetter
import logging
from tempfile import TemporaryDirectory

import bgpkit # pyright: ignore[reportMissingTypeStubs]
from bgpkit.bgpkit_broker import BrokerItem # pyright: ignore[reportMissingTypeStubs]

from pybgpflux.bgpstreamconfig import (
    BGPStreamConfig,
    FilterOptions,
    LiveStreamConfig,
)
from pybgpflux.bgpelement import BGPElement
from pybgpflux.parsers.bgpdump import BGPdumpParser
from pybgpflux.parsers.pybgpkit import PyBGPKITParser
from pybgpflux.parsers.pybgpstream import PyBGPStreamParser
from pybgpflux.parsers.bgpkit import BGPKITParser
from pybgpflux.parsers.bgpparser import (
    BGPParser,
)
from pybgpflux.rislive import RISLiveStream, jitter_buffer_stream
from pybgpflux.utils import Directory, get_shared_memory
from pybgpflux.downloader import (
    PREFETCH_SIZE,
    RCStream,
    cancel_all_tasks,
    safe_download_all,
    RCUrlsWithQueues
)

name2parser: dict[str, type[BGPParser]] = {
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
        data_type (list[Literal["ribs", "updates"]]): Data types to stream ("ribs" or "updates").
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
            data_type=["updates"],
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
        data_type: list[Literal["ribs", "updates"]],
        ts_start: float | None = None,
        ts_end: float | None = None,
        filters: FilterOptions | None = None,
        cache_dir: str | None = None,
        max_concurrent_downloads: int | None = 10,
        ram_fetch: bool | None = True,
        parser_name: str | None = "pybgpkit",
        remote_parse: bool | None = True,
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
        if max_concurrent_downloads:
            self.max_concurrent_downloads = max_concurrent_downloads
        else:
            self.max_concurrent_downloads = 10
        self.ram_fetch = ram_fetch
        self.cache_dir = cache_dir
        if not parser_name:
            self.parser_name = "pybgpkit"
        else:
            self.parser_name = parser_name
        if not remote_parse:
            self.remote_parse = True
        else:
            self.remote_parse = remote_parse
        if cache_dir:
            self.remote_parse = False

        self.broker = bgpkit.Broker()
        self.parser_cls: type[BGPParser] = name2parser[self.parser_name]

        # Live config
        self.jitter_buffer_delay = jitter_buffer_delay

    def _set_urls(self):
        """Set archive files URL with bgpkit broker and setup prefetch queues"""
        self.urls: RCUrlsWithQueues = {
            "ribs": defaultdict(lambda: ([], asyncio.Queue(maxsize=PREFETCH_SIZE))),
            "updates": defaultdict(lambda: ([], asyncio.Queue(maxsize=PREFETCH_SIZE))),
        }
        for data_type in self.data_type:
            items: list[BrokerItem] = self.broker.query( # type: ignore
                ts_start=str(int(self.ts_start - 60)), # type: ignore
                ts_end=str(int(self.ts_end)),  # type: ignore
                collector_id=",".join(self.collectors),
                data_type=data_type[:-1], # removes plural form
            )
            for item in items: # type: ignore
                self.urls[data_type][item.collector_id][0].append(item.url) # type: ignore

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
            # Note that if the parser supports remote parsing, cache_dir will not be populated
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

            is_remote_parsing = not is_caching and self.parser_cls.supports_remote_parsing and self.remote_parse
            
            # Kick off all download tasks in the background thread
            asyncio.run_coroutine_threadsafe(
                safe_download_all(
                    self.urls, cache_dir.name, self.max_concurrent_downloads, is_remote_parsing
                ),
                loop,
            )

            streams = [
                RCStream(
                    self.parser_cls,
                    data_type,
                    collector,
                    self.filters,
                    is_caching,
                    is_remote_parsing,
                    async_q,
                    loop,
                )
                for data_type in self.urls
                for collector, (_, async_q) in self.urls[data_type].items()
            ]

            try:
                for bgpelem in merge(*streams, key=attrgetter("time")):
                    if self.ts_start <= bgpelem.time <= self.ts_end: # type: ignore
                        yield bgpelem
            finally:
                # Cancel in-flight download tasks first so workers blocked on a
                # full prefetch queue are released, then stop and close the loop.
                try:
                    asyncio.run_coroutine_threadsafe(
                        cancel_all_tasks(), loop
                    ).result(timeout=5.0)
                except Exception as e:
                    logger.debug(f"Task cancellation during teardown failed: {e}")
                loop.call_soon_threadsafe(loop.stop)
                bg_thread.join(timeout=5.0)
                loop.close()

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
        match config:
            case BGPStreamConfig():
                if not config.is_live():
                    return cls(
                        ts_start=config.start_time.timestamp(), # type: ignore
                        ts_end=config.end_time.timestamp(), # type: ignore
                        collectors=config.collectors,
                        data_type=config.data_types,
                        filters=config.filters if config.filters else FilterOptions(),
                        cache_dir=str(config.cache_dir) if config.cache_dir else None,
                        max_concurrent_downloads=config.max_concurrent_downloads
                        if config.max_concurrent_downloads
                        else 10,
                        ram_fetch=config.ram_fetch if config.ram_fetch else None,
                        parser_name=config.parser if config.parser else "pybgpkit",
                        remote_parse=config.remote_parse if config.remote_parse else True
                    )
                else:
                    return cls(
                        collectors=config.collectors,
                        data_type=["updates"],
                        filters=config.filters if config.filters else FilterOptions(),
                        jitter_buffer_delay=10,
                )
            case LiveStreamConfig():
                
                return cls(
                    collectors=config.collectors,
                    data_type=["updates"],
                    filters=config.filters if config.filters else FilterOptions(),
                    jitter_buffer_delay=config.jitter_buffer_delay,
                )

            case _:
                raise ValueError("Unsupported config type")
