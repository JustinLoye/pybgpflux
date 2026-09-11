# Broker query implementation for BGPStream V2 and bgpfinder broker
from concurrent.futures import ThreadPoolExecutor
import datetime
from typing import Any, Literal

from bgpkit.bgpkit_broker import BrokerItem  # pyright: ignore[reportMissingTypeStubs]
import httpx
from pydantic import AnyUrl, BaseModel
from urllib import parse  # weird import to make pyright happy

from pybgpflux.bgpstreamconfig import BGPStreamConfig  # pyright: ignore[reportMissingTypeStubs]
from pybgpflux.brokers.bgpbroker import BGPBroker, BrokerQueryError


EpochTime = int | datetime.datetime
IntervalType = tuple[EpochTime, EpochTime]
BgpDataType = Literal["ribs", "updates"]
BgpResourceType = Literal["stream", "batch"]

# Floor on the window a single broker request is assumed to cover. Responses
# are capped server-side, by time on some deployments and by file count on
# others, so the real page size is measured per query and this is only the
# fallback for a response that shows nothing.
MIN_CHUNK_SECONDS = 3600
MAX_BROKER_WORKERS = 8


class BGPStreamBrokerQuery(BaseModel):
    """
    Query for BGPStream broker v2 data API
    https://bgpstream.caida.org/docs/api/broker

    Example:
    https://broker.bgpstream.caida.org/v2/data?human&intervals[]=1438819200,1438819200&collectors[]=route-views2&collectors[]=rrc03&types[]=updates
    """

    # Required parameter: Accepts a single tuple or a list of tuples
    intervals: IntervalType | list[IntervalType]

    # Dual-mode inputs: Pass a single string/value or a list of them
    collectors: str | list[str] | None = None
    projects: str | list[str] | None = None
    types: BgpDataType | list[BgpDataType] | None = None
    resource_types: BgpResourceType | list[BgpResourceType] | None = None

    # Strict Array-only fields from the API documentation
    routers: str | list[str] | None = None
    peer_asns: int | list[int] | None = None

    # Epoch window modifiers
    min_initial_time: EpochTime | None = None
    data_added_since: EpochTime | None = None
    human: bool = False

    def to_api_params(self) -> dict[str, Any]:
        """
        Serialize the model to a dict easy to use for queries.
        """
        params: dict[str, Any] = {}

        def _to_epoch(v: EpochTime) -> int:
            return int(v.timestamp()) if isinstance(v, datetime.datetime) else v

        # Handle dual-mode parameters (Maps to 'key' if single value, 'keys[]' if list)
        dual_params = {
            "collectors": ("collector", "collectors"),
            "projects": ("project", "projects"),
            "types": ("type", "types"),
            "resource_types": ("resourceType", "resourceTypes"),
        }
        for attr, (singular_key, plural_key) in dual_params.items():
            val = getattr(self, attr)
            if val is not None:
                if isinstance(val, list):
                    params[f"{plural_key}[]"] = val
                else:
                    params[singular_key] = val

        # Handle strict array-only fields (Always appends [])
        if self.routers is not None:
            params["routers[]"] = (
                self.routers if isinstance(self.routers, list) else [self.routers]
            )
        if self.peer_asns is not None:
            params["peer_asns[]"] = (
                self.peer_asns if isinstance(self.peer_asns, list) else [self.peer_asns]
            )

        # Process intervals (Converts datetimes/ints into "start,end" strings)
        iv_list = (
            self.intervals if isinstance(self.intervals, list) else [self.intervals]
        )
        params["intervals[]"] = [
            f"{_to_epoch(start)},{_to_epoch(end)}" for start, end in iv_list
        ]

        # Process individual scalars
        if self.min_initial_time is not None:
            params["minInitialTime"] = _to_epoch(self.min_initial_time)
        if self.data_added_since is not None:
            params["dataAddedSince"] = _to_epoch(self.data_added_since)
        if self.human:
            params["human"] = "true"

        return params

    def to_query_string(self) -> str:
        """Generates an escaped raw URL query string."""
        return parse.urlencode(self.to_api_params(), doseq=True)

    @classmethod
    def from_config(cls, config: BGPStreamConfig) -> "BGPStreamBrokerQuery":
        """Maps the universal config to the CAIDA-specific query format."""
        assert config.start_time
        assert config.end_time
        return cls(
            intervals=[(config.start_time, config.end_time)],
            collectors=config.collectors,
            types=config.data_types,
            human=False,
        )


class BGPStreamBrokerItem(BaseModel):
    """
    Represents a single resource file item.

    Example:
    {
        "url": "http://data.ris.ripe.net/rrc06/2010.08/updates.20100831.2355.gz",
        "format": "mrt",
        "transport": "file",
        "project": "ris",
        "collector": "rrc06",
        "type": "updates",
        "initialTime": 1283298900,
        "duration": 300,
        "attr": []
    }
    """

    url: AnyUrl
    format: Literal["mrt", "parquet"]
    transport: Literal["file"]
    project: Literal["ris", "routeviews"]
    collector: str
    type: Literal["updates", "ribs"]
    initialTime: int
    duration: int
    attr: list[Any] = []

    @staticmethod
    def _to_bgpkit_ts(epoch: int) -> str:
        """Format an epoch as the naive UTC ISO-8601 string bgpkit uses."""
        return (
            datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc)
            .replace(tzinfo=None)
            .isoformat()
        )

    def to_bgpkit_item(self) -> BrokerItem:
        """Converts CAIDA format to bgpkit BrokerItem."""
        # Assuming BrokerItem structure based on bgpkit requirements
        return BrokerItem(
            ts_start=self._to_bgpkit_ts(self.initialTime),
            ts_end=self._to_bgpkit_ts(self.initialTime + self.duration),
            collector_id=self.collector,
            data_type=self.type,
            url=str(self.url),
            rough_size=0,
            exact_size=0,
        )


class BGPStreamDataPayload(BaseModel):
    """Matches the 'data' block in the API payload."""

    resources: list[BGPStreamBrokerItem]


class BGPStreamResponseEnvelope(BaseModel):
    """Matches the absolute top level of the CAIDA broker response."""

    version: str
    time: int
    type: str
    error: str | None
    data: BGPStreamDataPayload


BGPStreamBrokerUrls = Literal[
    "https://bgpfinder.inetintel.cc.gatech.edu", "https://broker.bgpstream.caida.org/v2"
]


class BGPStreamBroker(BGPBroker):
    url = "https://bgpfinder.inetintel.cc.gatech.edu"

    def __init__(
        self, url: BGPStreamBrokerUrls = "https://bgpfinder.inetintel.cc.gatech.edu"
    ) -> None:
        self.url = url
        super().__init__()

    def _fetch_page(self, query: BGPStreamBrokerQuery) -> list[BGPStreamBrokerItem]:
        """Run one broker request and return its resources."""
        try:
            response = httpx.get(f"{self.url}/data?{query.to_query_string()}")
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise BrokerQueryError(
                f"Network request to BGPStream failed: {exc}"
            ) from exc

        try:
            envelope = BGPStreamResponseEnvelope.model_validate(response.json())
        except (ValueError, KeyError) as exc:
            raise BrokerQueryError(
                f"Failed to validate response schema: {exc}"
            ) from exc

        # Check if the API returned an explicit internal error message
        if envelope.error:
            raise BrokerQueryError(f"BGPStream API returned error: {envelope.error}")

        return envelope.data.resources

    def _chunk_query(
        self, config: BGPStreamConfig, interval: IntervalType
    ) -> BGPStreamBrokerQuery:
        query = BGPStreamBrokerQuery.from_config(config)
        query.intervals = [interval]
        return query

    def _page_span(
        self, config: BGPStreamConfig, page: list[BGPStreamBrokerItem], start: int
    ) -> int:
        """How far past `start` this response reached, for its least covered group.

        A response is a prefix of the matching files, so the last file of a
        (collector, data type) marks how much of the window that group was
        given. The smallest of those spans is how far the whole response can be
        trusted; a group with nothing to show contributes the floor, since all
        that proves is an empty window.
        """
        group_span = {
            (collector, data_type): MIN_CHUNK_SECONDS
            for collector in config.collectors
            for data_type in config.data_types
        }
        for resource in page:
            if resource.initialTime < start:
                continue  # the archive preceding the window, not coverage of it
            key = (resource.collector, resource.type)
            group_span[key] = max(
                group_span.get(key, MIN_CHUNK_SECONDS), resource.initialTime - start
            )
        return max(min(group_span.values()), MIN_CHUNK_SECONDS)

    def _query(self, config: BGPStreamConfig) -> list[BrokerItem]:
        """Query the interval, in as few requests as the deployment allows.

        A BGPStream v2 response is capped, silently, and the cap differs per
        deployment: CAIDA returns less than two hours of archive data whatever
        interval is asked for, while bgpfinder returns around five hundred
        files. Either way a single request silently returns a prefix of a long
        window. The `minInitialTime` cursor is not a way out: for sparse data
        types it can return the same file again, making no progress, or skip
        one entirely.

        So the page size is measured instead of assumed. The first request asks
        for the whole interval and reveals how much of it the server is willing
        to serve at once; the remainder is then tiled with chunks of that size,
        which are independent and run concurrently. On a generous deployment
        this is two requests; on a stingy one it degrades to one request per
        `MIN_CHUNK_SECONDS` of stream.

        Chunks overlap by one file, since the broker also returns the archive
        preceding a window, hence the deduplication by URL.
        """
        assert config.start_time
        assert config.end_time
        start = int(config.start_time.timestamp())
        end = int(config.end_time.timestamp())

        pages = [self._fetch_page(self._chunk_query(config, (start, end)))]

        chunk = self._page_span(config, pages[0], start)
        if start + chunk < end:
            queries = [
                self._chunk_query(config, (chunk_start, min(chunk_start + chunk, end)))
                for chunk_start in range(start + chunk + 1, end + 1, chunk + 1)
            ]
            with ThreadPoolExecutor(
                max_workers=min(len(queries), MAX_BROKER_WORKERS)
            ) as pool:
                pages.extend(pool.map(self._fetch_page, queries))

        items: list[BrokerItem] = []
        seen: set[str] = set()
        for page in pages:
            for resource in page:
                url = str(resource.url)
                if url not in seen:
                    seen.add(url)
                    items.append(resource.to_bgpkit_item())
        return items
