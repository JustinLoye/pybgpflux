import logging

from pybgpflux.bgpstreamconfig import FilterOptions
from pybgpflux.parsers.bgpparser import BGPParser

try:
    import pybgpstream # pyright: ignore[reportMissingTypeStubs]
except ImportError:
    pass


def generate_bgpstream_filters(f: FilterOptions) -> str | None:
    """Generates a filter string compatible with PyBGPStream from a FilterOptions object."""
    if not f:
        return None
    if not f.model_dump(exclude_unset=True):
        return None

    parts: list[str] = []

    if f.peer_asn:
        parts.append(f"peer {f.peer_asn}")

    if f.as_path:
        # Quote the value to handle potential spaces in the regex
        parts.append(f'aspath "{f.as_path}"')

    if f.origin_asn:
        # Filtering by origin ASN is typically done via an AS path regex
        parts.append(f'aspath "_{f.origin_asn}$"')

    if f.update_type:
        # The parser expects 'announcements' or 'withdrawals'
        value = "announcements" if f.update_type == "announce" else "withdrawals"
        parts.append(f"elemtype {value}")

    # Handle all prefix variations
    if f.prefix:
        parts.append(f"prefix exact {f.prefix}")
    if f.prefix_super:
        parts.append(f"prefix less {f.prefix_super}")
    if f.prefix_sub:
        parts.append(f"prefix more {f.prefix_sub}")
    if f.prefix_super_sub:
        parts.append(f"prefix any {f.prefix_super_sub}")

    if f.ip_version:
        parts.append(f"ipversion {f.ip_version}")

    # Warn about unsupported fields
    if f.peer_ip or f.peer_ips:
        logging.debug(
            "Filtering by peer_ip is not supported natively by pybgpstream (falling back to python-side filtering)"
        )

    # Join all parts with 'and' as required by the parser
    return " and ".join(parts)


class PyBGPStreamParser(BGPParser):
    """
    Use pybgpstream as a MRT parser with the `singlefile` data interface

    Yields pybgpstream.BGPElem instead instead of pybgpflux.BGPElement for better performance (save casting, and the two are almost idential anyway))
    """

    supports_remote_parsing = True

    def __init__(
        self,
        filepath: str,
        is_rib: bool,
        collector: str,
        filters: FilterOptions,
    ):
        self.filepath = filepath
        self.collector = collector
        self.filters = filters
        self.is_rib = is_rib

    def _iter_normal(self):
        """when there is no filter or filters are supported by pybgpstream"""
        stream = pybgpstream.BGPStream(  # type: ignore
            data_interface="singlefile",
            filter=generate_bgpstream_filters(self.filters) if self.filters else None,
        )
        stream.set_data_interface_option(
            "singlefile", "rib-file" if self.is_rib else "upd-file", self.filepath
        )

        for elem in stream:
            elem.collector = self.collector  # type: ignore
            yield elem

    def _iter_python_filter(self):
        """when filters are not supported by pybgpstream, filter from the python side"""
        bgpstream_filter = generate_bgpstream_filters(self.filters)
        stream = pybgpstream.BGPStream(  # type: ignore
            data_interface="singlefile",
            filter=bgpstream_filter if bgpstream_filter else None,
        )
        stream.set_data_interface_option(
            "singlefile", "rib-file" if self.is_rib else "upd-file", self.filepath
        )
        assert (
            self.filters.peer_ips is not None
        )  # guaranteed by __iter__ but make typing happy
        peer_ips = set(self.filters.peer_ips)

        for elem in stream:
            if elem.peer_address not in peer_ips:
                continue
            elem.collector = self.collector  # type: ignore
            yield elem

    def __iter__(self):  # type: ignore
        if not self.filters.peer_ip and not self.filters.peer_ips:
            return self._iter_normal()
        else:
            if self.filters.peer_ip:
                self.filters.peer_ips = [self.filters.peer_ip]
            return self._iter_python_filter()