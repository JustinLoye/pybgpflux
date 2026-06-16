from typing import Any, Iterator

import bgpkit  # pyright: ignore[reportMissingTypeStubs]

from pybgpflux.bgpelement import BGPElement
from pybgpflux.bgpstreamconfig import FilterOptions
from pybgpflux.parsers.bgpparser import BGPParser


class PyBGPKITParser(BGPParser):
    """Use BGPKIT Python bindings (default parser). Slower than other alternatives but easier to ship (no system dependencies)."""

    # In theory bgpkit supports remote parsing, but in practice I get unreliable results
    # due to connection drop when the kmerge is slow (big RIBs)
    supports_remote_parsing = False

    def __init__(
        self,
        filepath: str,
        is_rib: bool,
        collector: str,
        filters: FilterOptions = FilterOptions(),
    ):
        self.filepath = filepath
        self.parser = None  # placeholder for lazy instantiation
        self.is_rib = is_rib
        self.collector = collector
        self.filters = filters

        self.bgpkit_filters: dict[str, Any] = filters.model_dump(
            exclude_unset=True, exclude_none=True
        )
        # cast int ipv to pybgpkit ipv4 or ipv6 string
        if "ip_version" in self.bgpkit_filters:
            ipv_int = self.bgpkit_filters["ip_version"]
            if ipv_int:
                self.bgpkit_filters["ip_version"] = f"ipv{ipv_int}"
        if self.bgpkit_filters.get("peer_asn"):
            self.bgpkit_filters["peer_asn"] = str(self.bgpkit_filters["peer_asn"])
        if self.bgpkit_filters.get("origin_asn"):
            self.bgpkit_filters["origin_asn"] = str(self.bgpkit_filters["origin_asn"])
        if self.bgpkit_filters.get("update_type"):
            val = self.bgpkit_filters.pop("update_type")
            self.bgpkit_filters["type"] = val
        if self.bgpkit_filters.get("peer_ips"):
            self.bgpkit_filters["peer_ips"] = ", ".join(self.bgpkit_filters["peer_ips"])

    def _convert(self, element: Any) -> BGPElement:
        return BGPElement(
            type="R" if self.is_rib else element.elem_type,
            collector=self.collector,
            time=element.timestamp,
            peer_asn=element.peer_asn,
            peer_address=element.peer_ip,
            fields={
                "next-hop": element.next_hop,
                "as-path": element.as_path,
                "communities": [] if not element.communities else element.communities,
                "prefix": element.prefix,
            },
        )

    def __iter__(self) -> Iterator[BGPElement]:
        parser = bgpkit.Parser(self.filepath, filters=self.bgpkit_filters)  # type: ignore
        for elem in parser:  # type: ignore
            yield self._convert(elem)
