import asyncio
import os
import re
import logging
from typing import Iterator

import aiofiles
import httpx

from pybgpflux.bgpparser import BGPParser
from pybgpflux.bgpstreamconfig import FilterOptions
from pybgpflux.bgpelement import BGPElement
from pybgpflux.utils import crc32

# Download retry constants
MAX_RETRIES = 10
INITIAL_BACKOFF = 0.2  # seconds
REQUEST_DELAY = 0.05  # 50ms
PREFETCH_SIZE = 1


RCUrlsWithQueues = dict[str, dict[str, tuple[list, asyncio.Queue]]]
"""Data type -> route collector -> (urls, prefetch queue)"""


def generate_cache_filename(url: str) -> str:
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


async def download_file(
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


async def download_all(urls: RCUrlsWithQueues, cache_dir: str, max_concurrent: int):
    """Single async entry point: one client, one global semaphore, one task per data_type,collector."""
    dl_semaphore = asyncio.Semaphore(max_concurrent)

    async with httpx.AsyncClient(timeout=httpx.Timeout(None, read=300.0)) as client:

        async def worker(collector: str, url_list: list[str], async_q: asyncio.Queue):
            for url in url_list:
                filename = generate_cache_filename(url)
                filepath = os.path.join(cache_dir, filename)
                try:
                    path = await download_file(url, filepath, client, dl_semaphore)
                    await async_q.put(path)  # blocks when queue is full (backpressure)
                except Exception as e:
                    logging.error(f"[{collector}] Failed to download {url}: {e}")
            await async_q.put(None)  # sentinel

        async with asyncio.TaskGroup() as tg:
            for data_type in urls:
                for collector, (url_list, async_q) in urls[data_type].items():
                    tg.create_task(worker(collector, url_list, async_q))


class RCStream:
    """Synchronous iterator over BGP elements for one (data_type, collector), fed by the shared download thread."""

    def __init__(
        self,
        parser_cls: BGPParser,
        data_type: str,
        collector: str,
        filters: FilterOptions,
        is_caching: bool,
        async_q: asyncio.Queue,
        loop: asyncio.AbstractEventLoop,
    ):
        self.parser_cls = parser_cls
        self.data_type = data_type
        self.collector = collector
        self.filters = filters
        self.is_caching = is_caching
        self._q = async_q
        self._loop = loop

    def __iter__(self) -> Iterator[BGPElement]:
        is_rib = self.data_type == "rib"
        while True:
            # Query the background thread for prefetched files
            filepath = asyncio.run_coroutine_threadsafe(
                self._q.get(), self._loop
            ).result()
            if filepath is None:
                break

            logging.info(f"🧠 [{self.collector}] Parsing started for {filepath}")
            parser = self.parser_cls(
                filepath=filepath,
                is_rib=is_rib,
                collector=self.collector,
                filters=self.filters,
            )
            yield from parser

            if not self.is_caching:
                try:
                    os.remove(filepath)
                    logging.info(f"🗑️ [{self.collector}] Cleaned up {filepath}")
                except OSError as e:
                    logging.error(f"Failed to delete {filepath}: {e}")
