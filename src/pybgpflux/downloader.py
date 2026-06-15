import asyncio
import os
import re
import glob
import time
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
LOCK_TIMEOUT = 1800  # seconds – fallback if PID check fails
LOCK_POLL_INTERVAL = 1.0  # seconds


RCUrlsWithQueues = dict[str, dict[str, tuple[list[str], asyncio.Queue[str | None]]]]
"""Data type -> route collector -> (urls, prefetch queue)"""


def _pid_alive(pid: int) -> bool:
    """Check whether a process is still running."""
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, we just can't signal it


def _try_acquire_lock(filepath: str) -> bool:
    """Atomically create a lock file and write our PID. Returns True if acquired."""
    lock_path = f"{filepath}.lock"
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        return True
    except FileExistsError:
        return False


def _release_lock(filepath: str) -> None:
    """Remove the lock file, ignoring if already gone."""
    try:
        os.remove(f"{filepath}.lock")
    except FileNotFoundError:
        pass


async def _wait_for_peer(filepath: str, timeout: float = LOCK_TIMEOUT) -> bool:
    """Poll until the file appears, the lock disappears, or the lock holder dies.

    Returns True if the final file exists, False if the caller should retry.
    """
    lock_path = f"{filepath}.lock"
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if os.path.exists(filepath):
            return True
        if not os.path.exists(lock_path):
            return False  # peer released lock but file missing → peer failed, retry
        # Check if lock holder is still alive
        try:
            with open(lock_path) as f:
                pid = int(f.read().strip())
            if not _pid_alive(pid):
                logging.warning(
                    f"Lock holder (PID {pid}) is dead. Breaking lock for {filepath}"
                )
                try:
                    os.remove(lock_path)
                except FileNotFoundError:
                    pass
                return False
        except (ValueError, FileNotFoundError, OSError):
            pass  # lock file vanished or unreadable, retry next iteration
        await asyncio.sleep(LOCK_POLL_INTERVAL)

    # Timeout – break stale lock
    logging.warning(f"Lock timeout ({timeout}s) for {filepath}. Breaking lock.")
    try:
        os.remove(lock_path)
    except FileNotFoundError:
        pass
    return False


def _sweep_stale(cache_dir: str) -> None:
    """Remove lock/temp files left behind by a previously crashed process.

    A lock is stale if its recorded PID is no longer running. A temp file is
    stale if the PID embedded in its name (``<file>.<pid>.tmp``) is dead.
    """
    for lock_path in glob.glob(os.path.join(cache_dir, "*.lock")):
        try:
            with open(lock_path) as f:
                pid = int(f.read().strip())
        except (ValueError, OSError):
            pid = None
        if pid is None or not _pid_alive(pid):
            try:
                os.remove(lock_path)
            except FileNotFoundError:
                pass

    for tmp_path in glob.glob(os.path.join(cache_dir, "*.tmp")):
        match = re.search(r"\.(\d+)\.tmp$", tmp_path)
        if match and not _pid_alive(int(match.group(1))):
            try:
                os.remove(tmp_path)
            except FileNotFoundError:
                pass


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
    while True:
        # Fast path: already fully downloaded
        if os.path.exists(filepath):
            logging.debug(f"{filepath} is a cache hit")
            return filepath

        if _try_acquire_lock(filepath):
            # We are the downloader
            try:
                # Double-check after acquiring lock
                if os.path.exists(filepath):
                    return filepath

                temp_filepath = f"{filepath}.{os.getpid()}.tmp"
                try:
                    async with semaphore:
                        for attempt in range(MAX_RETRIES + 1):
                            try:
                                await asyncio.sleep(REQUEST_DELAY)
                                async with client.stream("GET", url) as resp:
                                    resp.raise_for_status()
                                    async with aiofiles.open(
                                        temp_filepath, mode="wb"
                                    ) as fd:
                                        async for chunk in resp.aiter_bytes(
                                            chunk_size=32768
                                        ):
                                            await fd.write(chunk)
                                os.rename(temp_filepath, filepath)
                                return filepath
                            except (httpx.HTTPError, asyncio.TimeoutError) as e:
                                if attempt < MAX_RETRIES:
                                    await asyncio.sleep(INITIAL_BACKOFF * (2**attempt))
                                else:
                                    raise RuntimeError(f"Cannot download {url}") from e
                finally:
                    # Remove any partial temp file on error or cancellation.
                    if os.path.exists(temp_filepath):
                        os.remove(temp_filepath)
            finally:
                _release_lock(filepath)
        else:
            # Another process/thread is downloading this file – wait for it
            if await _wait_for_peer(filepath):
                return filepath  # peer succeeded
            # peer failed or lock was stale → loop back and retry


async def download_all(urls: RCUrlsWithQueues, cache_dir: str, max_concurrent: int, is_remote_parsing: bool):
    """Single async entry point: one client, one global semaphore, one task per data_type,collector."""
    dl_semaphore = asyncio.Semaphore(max_concurrent)

    if not is_remote_parsing:
        _sweep_stale(cache_dir)

    async with httpx.AsyncClient(timeout=httpx.Timeout(None, read=300.0)) as client:

        async def worker(collector: str, url_list: list[str], async_q: asyncio.Queue[str | None]):
            # For remote parsing, don't need to prefetch. So simply queue url.
            if is_remote_parsing:
                for url in url_list:
                    await async_q.put(url)
                
            # For local parsing we need to download and communicate to consumer by putting the filepath in the queue
            else:
                for url in url_list:
                    filename = generate_cache_filename(url)
                    filepath = os.path.join(cache_dir, filename)
                    try:
                        path = await download_file(url, filepath, client, dl_semaphore)
                        await async_q.put(path)  # blocks when queue is full (backpressure)
                    except Exception as e:
                        logging.error(f"[{collector}] Failed to download {url}: {e}")
                
            await async_q.put(None)  # sentinel

        await asyncio.gather(
            *(
                worker(collector, url_list, async_q)
                for data_type in urls
                for collector, (url_list, async_q) in urls[data_type].items()
            )
        )


async def safe_download_all(
    urls: RCUrlsWithQueues, cache_dir: str, max_concurrent: int, is_remote_parsing: bool
):
    """Run download_all and guarantee sentinels are pushed on a genuine crash.

    Note: ``CancelledError`` is intentionally *not* caught. During teardown the
    consumer cancels this task, and trying to push sentinels onto a loop that is
    shutting down would fail noisily.
    """
    try:
        await download_all(urls, cache_dir, max_concurrent, is_remote_parsing)
    except Exception as e:
        logging.error(f"Download orchestrator crashed: {e}")
        for data_type in urls:
            for _, (_, async_q) in urls[data_type].items():
                await async_q.put(None)


async def cancel_all_tasks() -> None:
    """Cancel every other task on this loop and wait for them to unwind.

    Called during teardown so that download workers blocked on a full prefetch
    queue (backpressure) are released cleanly instead of being abandoned.
    """
    current = asyncio.current_task()
    pending = [t for t in asyncio.all_tasks() if t is not current]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)



class RCStream:
    """Synchronous iterator over BGP elements for one (data_type, collector), fed by the shared download thread."""

    def __init__(
        self,
        parser_cls: type[BGPParser],
        data_type: str,
        collector: str,
        filters: FilterOptions,
        is_caching: bool,
        is_remote_parsing: bool,
        async_q: asyncio.Queue[str | None],
        loop: asyncio.AbstractEventLoop,
    ):
        self.parser_cls = parser_cls
        self.data_type = data_type
        self.collector = collector
        self.filters = filters
        self.is_caching = is_caching
        self.is_remote_parsing = is_remote_parsing
        self._q = async_q
        self._loop = loop

    def __iter__(self) -> Iterator[BGPElement]:
        is_rib = self.data_type == "ribs"
        while True:
            # Query the background thread for prefetched files
            filepath = asyncio.run_coroutine_threadsafe(
                self._q.get(), self._loop
            ).result()
            if filepath is None:
                break

            logging.debug(f"🧠 [{self.collector}] Parsing started for {filepath}")
            parser = self.parser_cls(
                filepath=filepath,
                is_rib=is_rib,
                collector=self.collector,
                filters=self.filters,
            )
            yield from parser

            if not (self.is_remote_parsing or self.is_caching):
                try:
                    os.remove(filepath)
                    logging.debug(f"🗑️ [{self.collector}] Cleaned up {filepath}")
                except OSError as e:
                    logging.error(f"Failed to delete {filepath}: {e}")
