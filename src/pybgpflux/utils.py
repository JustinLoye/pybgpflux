import binascii
import datetime
import os
import re


def dt_from_filepath(filepath: str, pattern=r"(\d{8}\.\d{4})") -> datetime.datetime:
    match = re.search(pattern, filepath)
    if not match:
        raise RuntimeError("Could not determine time from filepath")
    timestamp_str = match.group(1)
    dt = datetime.datetime.strptime(timestamp_str, "%Y%m%d.%H%M")
    dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt


def crc32(input_str: str):
    input_bytes = input_str.encode("utf-8")
    crc = binascii.crc32(input_bytes) & 0xFFFFFFFF
    return f"{crc:08x}"


class Directory:
    """Permanent directory that mimics the TemporaryDirectory interface."""

    def __init__(self, path: str):
        self.name = str(path)

        os.makedirs(self.name, exist_ok=True)

    def cleanup(self):
        """No-op cleanup for permanent directories."""
        pass

    def __enter__(self):
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        """No-op on exit."""
        pass


def get_shared_memory():
    """Get a RAM-based temp path if available, otherwise fall back to default."""
    if os.path.exists("/dev/shm"):  # Linux tmpfs
        return "/dev/shm"
    elif os.path.exists("/Volumes/RAMDisk"):  # macOS (if mounted)
        return "/Volumes/RAMDisk"
    return None  # Fall back to default temp directory
