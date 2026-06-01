import asyncio
import os
import re
import datetime
import threading
from typing import Iterator, Literal
from collections import defaultdict
from itertools import chain
from heapq import merge
from operator import attrgetter, itemgetter
import binascii
import logging
from tempfile import TemporaryDirectory

import aiofiles
import httpx
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
from pybgpflux.utils import dt_from_filepath

name2parser = {
    "pybgpkit": PyBGPKITParser,
    "bgpkit": BGPKITParser,
    "pybgpstream": PyBGPStreamParser,
    "bgpdump": BGPdumpParser,
}


logger = logging.getLogger(__name__)

# Download retry constants
MAX_RETRIES = 5
INITIAL_BACKOFF = 0.2  # seconds
REQUEST_DELAY = 0.05  # 50ms
PREFETCH_SIZE = 1


def crc32(input_str: str):
    input_bytes = input_str.encode("utf-8")
    crc = binascii.crc32(input_bytes) & 0xFFFFFFFF
    return f"{crc:08x}"


class Directory:
    """Permanent directory that mimics TemporaryDirectory interface."""

    def __init__(self, path):
        self.name = str(path)

    def cleanup(self):
        """No-op cleanup for permanent directories."""
        pass


def get_shared_memory():
    """Get a RAM-based temp path if available, otherwise fall back to default."""
    if os.path.exists("/dev/shm"):  # Linux tmpfs
        return "/dev/shm"
    elif os.path.exists("/Volumes/RAMDisk"):  # macOS (if mounted)
        return "/Volumes/RAMDisk"
    return None  # Fall back to default temp directory

class RCStream:
    """Synchronous iterator over BGP elements for one (data_type, collector), fed by the shared download thread."""

    def __init__(self, parser_cls: BGPParser, data_type: str, collector: str, filters: FilterOptions, is_caching: bool, async_q: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        self.parser_cls = parser_cls
        self.data_type = data_type
        self.collector = collector
        self.filters = filters
        self.is_caching = is_caching
        self._q = async_q
        self._loop = loop

    def __iter__(self) -> Iterator[BGPElement]:
        is_rib = self.data_type == 'rib'
        while True:
            filepath = asyncio.run_coroutine_threadsafe(self._q.get(), self._loop).result()
            if filepath is None:
                break

            logging.info(f"🧠 [{self.collector}] Parsing started for {filepath}")
            parser = self.parser_cls(filepath=filepath, is_rib=is_rib, collector=self.collector, filters=self.filters)
            yield from parser

            if not self.is_caching:
                try:
                    os.remove(filepath)
                    logging.info(f"🗑️ [{self.collector}] Cleaned up {filepath}")
                except OSError as e:
                    logging.error(f"Failed to delete {filepath}: {e}")


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
        chunk_time (float): Time window (seconds) for processing chunks. Default is 2 hours.
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
        chunk_time: float | None = datetime.timedelta(hours=2).seconds,
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
            chunk_time: Time window (seconds) for streaming chunks. Default is 2 hours (7200s).
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
        self.chunk_time = chunk_time
        self.ram_fetch = ram_fetch
        if cache_dir:
            self.is_caching = True
            self.cache_dir = Directory(cache_dir)
            
        else:
            self.is_caching = False
            if ram_fetch:
                self.cache_dir = TemporaryDirectory(dir=get_shared_memory())
            else:
                self.cache_dir = TemporaryDirectory()
        if not parser_name:
            self.parser_name = "pybgpkit"
        else:
            self.parser_name = parser_name

        self.broker = bgpkit.Broker()
        self.parser_cls: BGPParser = name2parser[parser_name]

        # Live config
        self.jitter_buffer_delay = jitter_buffer_delay

    @staticmethod
    def _generate_cache_filename(url):
        """Generate a cache filename compatible with BGPKIT parser."""

        hash_suffix = crc32(url)

        if "updates." in url:
            data_type = "updates"
        elif "rib" in url or "view" in url:
            data_type = "rib"
        else:
            raise ValueError("Could not understand data type from url")

        # Look for patterns like rib.20100901.0200 or updates.20100831.2345
        timestamp_match = re.search(r"(\d{8})\.(\d{4})", url)
        if timestamp_match:
            timestamp = f"{timestamp_match.group(1)}.{timestamp_match.group(2)}"
        else:
            raise ValueError("Could not parse timestamp from url")

        if url.endswith(".bz2"):
            compression_ext = "bz2"
        elif url.endswith(".gz"):
            compression_ext = "gz"
        else:
            raise ValueError("Could not parse extension from url")

        return f"cache-{data_type}.{timestamp}.{hash_suffix}.{compression_ext}"

    def _set_urls(self):
        """Set archive files URL with bgpkit broker and setup prefetch queues"""
        # Set the urls with bgpkit broker
        self.urls = {
            "rib": defaultdict(lambda: ([], asyncio.Queue(maxsize=PREFETCH_SIZE))),
            "update": defaultdict(
                lambda: ([], asyncio.Queue(maxsize=PREFETCH_SIZE))
            ),
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

    async def _download_file(
        self,
        url: str,
        filepath: str,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
    ) -> str:
        
        async with semaphore:
            if os.path.exists(filepath):
                logging.debug(f"{filepath} is a cache hit")
                return filepath
            for attempt in range(MAX_RETRIES + 1):
                try:
                    await asyncio.sleep(REQUEST_DELAY)
                    async with client.stream("GET", url) as resp:
                        resp.raise_for_status()
                        temp_filepath = f"{filepath}.tmp"
                        async with aiofiles.open(temp_filepath, mode="wb") as fd:
                            async for chunk in resp.aiter_bytes(chunk_size=32768):
                                await fd.write(chunk)
                        os.rename(temp_filepath, filepath)
                        return filepath
                except (httpx.HTTPError, asyncio.TimeoutError) as e:
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(INITIAL_BACKOFF * (2**attempt))
                    else:
                        if os.path.exists(f"{filepath}.tmp"):
                            os.remove(f"{filepath}.tmp")
                        raise RuntimeError(f"Cannot download {url}") from e

    async def _download_all(self):
        """Single async entry point: one client, one global semaphore, one task per data_type,collector."""
        dl_semaphore = asyncio.Semaphore(self.max_concurrent_downloads)

        async with httpx.AsyncClient(timeout=httpx.Timeout(None, read=300.0)) as client:

            async def worker(collector: str, urls: list[str], async_q: asyncio.Queue):
                for url in urls:
                    filename = self._generate_cache_filename(url)
                    filepath = os.path.join(self.cache_dir.name, filename)
                    try:
                        path = await self._download_file(
                            url, filepath, client, dl_semaphore
                        )
                        await async_q.put(
                            path
                        )  # blocks when queue is full (backpressure)
                    except Exception as e:
                        logging.error(f"[{collector}] Failed to download {url}: {e}")
                await async_q.put(None)  # sentinel

            async with asyncio.TaskGroup() as tg:
                for data_type in self.urls:
                    for collector, (urls, async_q) in self.urls[data_type].items():
                        tg.create_task(worker(collector, urls, async_q))


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
        # __iter__ for data types [ribs, updates] or [updates]
        
        self._set_urls()

        loop = asyncio.new_event_loop()

        # Single background thread runs the event loop
        ready = threading.Event()
        bg_thread = threading.Thread(
            target=self._run_loop, args=(loop, ready), daemon=True
        )
        bg_thread.start()
        ready.wait()

        # Kick off all download tasks
        asyncio.run_coroutine_threadsafe(self._download_all(), loop)

        # Build stream iterators
        streams = [
            RCStream(self.parser_cls, data_type, collector, self.filters, self.is_caching, async_q, loop) for data_type in self.urls for collector, (_, async_q) in self.urls[data_type].items() 
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
                    chunk_time=config.chunk_time.seconds if config.chunk_time else None,
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
