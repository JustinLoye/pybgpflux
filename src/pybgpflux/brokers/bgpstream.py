# Broker query implementation for BGPStream V2 and bgpfinder broker
import datetime
from typing import Any, Literal

from bgpkit.bgpkit_broker import BrokerItem # pyright: ignore[reportMissingTypeStubs]
import httpx
from pydantic import AnyUrl, BaseModel
from urllib import parse  # weird import to make pyright happy

from pybgpflux.bgpstreamconfig import BGPStreamConfig  # pyright: ignore[reportMissingTypeStubs]
from pybgpflux.brokers.bgpbroker import BGPBroker, BrokerQueryError


EpochTime = int | datetime.datetime
IntervalType = tuple[EpochTime, EpochTime]
BgpDataType = Literal["ribs", "updates"]
BgpResourceType = Literal["stream", "batch"]


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

    def to_bgpkit_item(self) -> BrokerItem:
        """Converts CAIDA format to bgpkit BrokerItem."""
        # Assuming BrokerItem structure based on bgpkit requirements
        return BrokerItem(
            ts_start=str(self.initialTime),
            ts_end=str(self.initialTime + self.duration),
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

    def query(self, config: BGPStreamConfig) -> list[BrokerItem]:
        bgpstream_query = BGPStreamBrokerQuery.from_config(config)

        try:
            response = httpx.get(f"{self.url}/data?{bgpstream_query.to_query_string()}")
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

        return [item.to_bgpkit_item() for item in envelope.data.resources]
