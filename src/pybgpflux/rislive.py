from typing import Iterator
import json
import heapq
import websocket
from typing import Any

from pybgpflux.bgpelement import BGPElement
from pybgpflux.bgpstreamconfig import FilterOptions


def ris_message2bgpelem(ris_message: dict[str, Any]) -> Iterator[BGPElement]:

    timestamp = float(ris_message["timestamp"])
    collector = ris_message["host"].split(".")[0]
    peer_asn = int(ris_message["peer_asn"])
    peer_address = ris_message["peer"]
    path = ris_message["path"]
    communities = ris_message["community"]
    if communities:
        communities = [f"{asn}:{community}" for asn, community in communities]

    for pfx in ris_message["withdrawals"]:
        yield BGPElement(
            type="W",
            collector=collector,
            time=timestamp,
            peer_asn=peer_asn,
            peer_address=peer_address,
            fields={
                "as-path": path,
                "communities": communities,
                "prefix": pfx,
            },
        )

    for announcement in ris_message["announcements"]:
        for pfx in announcement["prefixes"]:
            yield BGPElement(
                type="A",
                collector=collector,
                time=timestamp,
                peer_asn=peer_asn,
                peer_address=peer_address,
                fields={
                    "next-hop": announcement["next_hop"].split(",")[0],
                    "as-path": path,
                    "communities": communities,
                    "prefix": pfx,
                },
            )


class RISLiveStream:
    def __init__(
        self,
        collectors: list[str],
        client: str="pybgpflux",
        filters: FilterOptions = None,
    ):
        self.collectors = collectors
        self.client = client
        self.filters = self._convert_filter_options(filters)

    @staticmethod
    def _convert_filter_options(f: FilterOptions) -> dict[str, Any]:
        """Convert FilterOptions to RIS live filters"""
        if f is None:
            return {}

        if not f.model_dump(exclude_unset=True):
            return {}

        res = {}
        if f.update_type == "withdraw":
            res["require"] = "withdrawals"
        elif f.update_type == "announce":
            res["require"] = "announcements"
        if f.peer_ip:
            res["peer"] = f.peer_ip
        path_elements: list[str] = []
        if f.peer_asn:
            path_elements.append(f"^{f.peer_asn}")
        if f.origin_asn:
            path_elements.append(f"{f.origin_asn}$")
        res["path"] = ",".join(path_elements)

        if f.prefix:
            res["prefix"] = f.prefix
            # default is True which I think is not consistent with BGPKIT/BGPStream
            res["moreSpecific"] = False
        if f.prefix_sub:
            res["prefix"] = f.prefix_sub
            res["moreSpecific"] = True
        if f.prefix_super:
            res["prefix"] = f.prefix_super
            res["lessSpecific"] = True
        if f.prefix_super_sub:
            res["prefix"] = f.prefix_super_sub
            res["moreSpecific"] = True
            res["lessSpecific"] = True

        return res

    def __iter__(self) -> Iterator[BGPElement]:
        ws = websocket.WebSocket()
        ws.connect(f"wss://ris-live.ripe.net/v1/ws/?client={self.client}") # pyright: ignore[reportUnknownMemberType]

        # Subscribe to each collector on the same connection
        for collector in self.collectors:
            params = {"host": collector, "type": "UPDATE"}
            params = params | self.filters
            ws.send(json.dumps({"type": "ris_subscribe", "data": params}))

        for data in ws:
            parsed = json.loads(data)["data"]
            yield from ris_message2bgpelem(parsed)


def jitter_buffer_stream(stream: RISLiveStream, buffer_delay: float=10.0) -> Iterator[BGPElement]:
    """
    Produces an ordered stream by buffering elements for `buffer_delay` seconds.
    """
    heap: list[BGPElement] = []
    max_ts_seen = float("-inf")

    for elem in stream:
        # Track the latest timestamp seen in the jittery stream
        if elem.time > max_ts_seen:
            max_ts_seen = elem.time

        heapq.heappush(heap, elem)

        # Flush from buffer if timestamp is old enough
        while heap and (max_ts_seen - heap[0].time) > buffer_delay:
            yield heapq.heappop(heap)

    # Clean up when stream ends (never hopefully)
    while heap:
        yield heapq.heappop(heap)
