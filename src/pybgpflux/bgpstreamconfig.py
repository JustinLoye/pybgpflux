import datetime
import importlib
import shutil
from pydantic import BaseModel, Field, DirectoryPath, field_validator, model_validator
from typing import Literal
from ipaddress import IPv4Address, IPv6Address


class FilterOptions(BaseModel):
    """A unified model for the available filter options."""

    origin_asn: int | None = Field(
        default=None, description="Filter by the origin AS number."
    )
    prefix: str | None = Field(
        default=None, description="Filter by an exact prefix match."
    )
    prefix_super: str | None = Field(
        default=None,
        description="Filter by the exact prefix and its more general super-prefixes.",
    )
    prefix_sub: str | None = Field(
        default=None,
        description="Filter by the exact prefix and its more specific sub-prefixes.",
    )
    prefix_super_sub: str | None = Field(
        default=None,
        description="Filter by the exact prefix and both its super- and sub-prefixes.",
    )
    peer_ip: str | IPv4Address | IPv6Address | None = Field(
        default=None, description="Filter by the IP address of a single BGP peer."
    )
    peer_ips: list[str | IPv4Address | IPv6Address] | None = Field(
        default=None, description="Filter by a list of BGP peer IP addresses."
    )
    peer_asn: int | None = Field(
        default=None, description="Filter by the AS number of the BGP peer."
    )
    update_type: Literal["withdraw", "announce"] | None = Field(
        default=None, description="Filter by the BGP update message type."
    )
    as_path: str | None = Field(
        default=None, description="Filter by a regular expression matching the AS path."
    )
    ip_version: Literal[4, 6] | None = Field(
        default=None, description="Filter by ip version."
    )


class BGPStreamConfig(BaseModel):
    """Unified BGPStream config"""

    # --- Query parameters (required) ---
    start_time: datetime.datetime | None = Field(
        default=None, description="Start of the stream"
    )
    end_time: datetime.datetime | None = Field(
        default=None, description="End of the stream"
    )
    collectors: list[str] = Field(description="List of collectors to get data from")
    data_types: list[Literal["ribs", "updates"]] | None = Field(
        default=["updates"],
        description="List of archives files to consider (`ribs` or `updates`)",
    )
    filters: FilterOptions | None = Field(default=None, description="Optional filters")

    # --- Implementation parameters (optional) ---
    max_concurrent_downloads: int | None = Field(
        default=10, description="Maximum concurrent downloads of archive files."
    )
    cache_dir: DirectoryPath | None = Field(
        default=None,
        description="Specifies the directory for caching downloaded files.",
    )
    ram_fetch: bool | None = Field(
        default=False,
        description=(
            "If caching is disabled, fetch temp files in shared RAM memory (/dev/shm) or normal disc temp dir (/tmp)."
            "Default (False) to reduce RAM usage."
        ),
    )
    parser: Literal["pybgpkit", "bgpkit", "pybgpstream", "bgpdump"] = Field(
        default="pybgpkit",
        description=(
            "MRT files parser. Default `pybgpkit` is installed but slow, the others are system dependencies."
        ),
    )
    remote_parse: bool | None = Field(
        default=True,
        description=(
            "Parse directly from a remote source without saving archives locally or in RAM. "
            "Currently supported only by the pybgpstream parser."
        ),
    )

    @field_validator("start_time", "end_time", mode="before")
    @classmethod
    def normalize_to_utc(cls, dt: datetime.datetime) -> datetime.datetime:
        if dt is None:
            return None
        # if naive datetime (not timezone-aware) assume it's UTC
        if dt.tzinfo is None:
            return dt.replace(tzinfo=datetime.timezone.utc)
        # if timezone-aware, convert to utc
        else:
            return dt.astimezone(datetime.timezone.utc)

    @field_validator("parser")
    @classmethod
    def check_parser_available(cls, parser: str) -> str:
        if parser == "pybgpkit":
            if importlib.util.find_spec("bgpkit") is None:
                raise ValueError(
                    "pybgpkit is not installed. Install with: pip install pybgpkit"
                )

        elif parser == "pybgpstream":
            if importlib.util.find_spec("pybgpstream") is None:
                raise ValueError(
                    "pybgpstream is not installed. "
                    "Install with: pip install pybgpstream (ensure system dependencies are met)"
                )

        elif parser == "bgpdump":
            if shutil.which("bgpdump") is None:
                raise ValueError(
                    "bgpdump binary not found in PATH. "
                    "Install with: sudo apt-get install bgpdump"
                )

        elif parser == "bgpkit":
            if shutil.which("bgpkit-parser") is None:
                raise ValueError(
                    "bgpkit binary not found in PATH. "
                    "Install from: https://github.com/bgpkit/bgpkit-parser "
                    "or use cargo: cargo install bgpkit-parser --features cli"
                )

        return parser

    @model_validator(mode="after")
    def validate(self) -> "BGPStreamConfig":
        
        if (self.start_time is None) ^ (self.end_time is None):
            raise ValueError(
                "Provide both start and end times, or leave both as None for live mode."
            )
            
        
        if not self.is_live():
            assert self.start_time < self.end_time
        # Force data_type to update for live mode
        else:
            if self.data_types is None:
                self.data_types = ["updates"]
                       
        # Disable remote parsing when caching
        if self.cache_dir:
            self.remote_parse = False

        return self

    def is_live(self) -> bool:
        return self.start_time is None and self.end_time is None


class LiveStreamConfig(BaseModel):
    """Config for live mode"""

    collectors: list[str] = Field(
        description="List of collectors to get data from (for now only RIS live collectors)"
    )
    filters: FilterOptions | None = Field(default=None, description="Optional filters")
    jitter_buffer_delay: float | None = Field(
        default=10.0,
        description="Jitter buffer time in seconds to make sure RIS live updates are time-sorted. Introduce a slight delay. Set to None or 0 to disable",
    )
