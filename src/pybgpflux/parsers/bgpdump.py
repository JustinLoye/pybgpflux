import ipaddress
import re
import subprocess as sp
from typing import Iterator

from pybgpflux.bgpelement import BGPElement
from pybgpflux.bgpstreamconfig import FilterOptions
from pybgpflux.parsers.bgpparser import BGPParser


class BGPdumpParser(BGPParser):
    """Run bgpdump as a subprocess."""

    supports_remote_parsing = False

    def __init__(
        self, filepath: str, is_rib: bool, collector: str, filters: FilterOptions
    ):
        self.filepath = filepath
        self.is_rib = is_rib
        self.collector = collector
        self.filters = filters

        self._init_filters(filters)
        self.parser: sp.Popen[str] | None = None  # placeholder for lazy instantiation

    def __iter__(self) -> Iterator[BGPElement]:
        self.parser = sp.Popen(
            ["bgpdump", "-m", "-v", self.filepath], stdout=sp.PIPE, text=True, bufsize=1
        )
        assert self.parser.stdout is not None
        try:
            raw_stream = (self._convert(line) for line in self.parser.stdout)
            # Filter STATE message
            clean_stream = (e for e in raw_stream if e is not None)

            if self._filter_func:
                yield from filter(self._filter_func, clean_stream)
            else:
                yield from clean_stream
        finally:
            # Cleanup happens whether exhausted or abandoned
            self.parser.stdout.close()
            self.parser.terminate()
            self.parser.wait()  # Reap the zombie process

    def _convert(self, line: str) -> BGPElement | None:
        # Extract type once to avoid repeated list lookups
        element = line.rstrip().split("|")
        elem_type = element[2]
        if elem_type == "STATE":
            return

        # 1. Handle Withdrawals (Fastest path, fewer fields)
        if elem_type == "W":
            return BGPElement(
                float(element[1]),
                "W",
                self.collector,
                int(element[4]),
                element[3],
                {"prefix": element[5]},  # Dict literal is faster than assignment
            )

        # 2. Handle RIB (TABLE_DUMP2) and Announcements (A)
        # Common vars
        rec_comm = element[11]

        # Logic: if TABLE_DUMP2, type is R, else A
        # Construct fields dict in one shot (BUILD_MAP opcode)
        return BGPElement(
            float(element[1]),
            "R" if elem_type == "B" else "A",
            self.collector,
            int(element[4]),
            element[3],
            {
                "prefix": element[5],
                "as-path": element[6],
                "next-hop": element[8],
                # Check for empty string before splitting (avoids creating [''])
                "communities": rec_comm.split(" ") if rec_comm else [],
            },
        )

    def _init_filters(self, f: FilterOptions) -> None:
        if not f.model_dump(exclude_unset=True):
            self._filter_func = None
            return

        self.filter_peer_asn: int | None = f.peer_asn

        self.filter_peer_ips: set[str] | None = None
        if f.peer_ip:
            self.filter_peer_ips = {str(f.peer_ip)}
        elif f.peer_ips:
            self.filter_peer_ips = {str(ip) for ip in f.peer_ips}

        # Regex handles AS_SETs like "{1234,5678}" properly
        self.filter_origin_asn_re: re.Pattern[str] | None = (
            re.compile(rf"\b{f.origin_asn}\b") if f.origin_asn else None
        )

        # Map human-readable update_type to wire-level codes
        self.filter_update_types: frozenset[str] | None = None
        if f.update_type == "announce":
            self.filter_update_types = frozenset({"A", "R"})
        elif f.update_type == "withdraw":
            self.filter_update_types = frozenset({"W"})

        self.filter_ip_version: int | None = f.ip_version
        self.filter_as_path_re: re.Pattern[str] | None = (
            re.compile(f.as_path) if f.as_path else None
        )

        # strict=False prevents ValueError on dirty BGP data
        self.filter_exact_net: ipaddress.IPv4Network | ipaddress.IPv6Network | None = (
            ipaddress.ip_network(f.prefix, strict=False) if f.prefix else None
        )
        self.filter_sub_net: ipaddress.IPv4Network | ipaddress.IPv6Network | None = (
            ipaddress.ip_network(f.prefix_sub, strict=False) if f.prefix_sub else None
        )
        self.filter_super_net: ipaddress.IPv4Network | ipaddress.IPv6Network | None = (
            ipaddress.ip_network(f.prefix_super, strict=False)
            if f.prefix_super
            else None
        )
        self.filter_ss_net: ipaddress.IPv4Network | ipaddress.IPv6Network | None = (
            ipaddress.ip_network(f.prefix_super_sub, strict=False)
            if f.prefix_super_sub
            else None
        )

        self._filter_func = self._compile_filter()

    def _compile_filter(self):
        # Localize variables to the closure to bypass 'self' lookups
        filter_peer_asn = self.filter_peer_asn
        filter_peer_ips = self.filter_peer_ips
        filter_origin_asn_re = self.filter_origin_asn_re
        filter_update_types = self.filter_update_types
        filter_ip_version = self.filter_ip_version
        filter_as_path_re = self.filter_as_path_re

        filter_exact_net = self.filter_exact_net
        filter_sub_net = self.filter_sub_net
        filter_super_net = self.filter_super_net
        filter_ss_net = self.filter_ss_net

        # Cache boolean check outside the loop
        any_prefix_filter = any(
            [filter_exact_net, filter_sub_net, filter_super_net, filter_ss_net]
        )

        # Cache whether we need to touch AS Path fields at all
        needs_path_parsing = (
            filter_as_path_re is not None or filter_origin_asn_re is not None
        )

        # Cache whether we need to touch Prefix fields at all
        needs_prefix_parsing = any_prefix_filter or filter_ip_version is not None

        def filter_logic(e: BGPElement) -> bool:
            # 1. Quickest checks first: Scalars and native attributes
            if filter_peer_asn is not None and e.peer_asn != filter_peer_asn:
                return False
            if filter_peer_ips is not None and e.peer_address not in filter_peer_ips:
                return False
            if filter_update_types is not None and e.type not in filter_update_types:
                return False

            # 2. Dictionary/String checks: Only executed if path filters are active
            if needs_path_parsing:
                # Withdrawals mathematically cannot match AS path filters.
                if e.type == "W":
                    return False

                as_path: str = e.fields.get("as-path", "")

                if filter_as_path_re is not None and not filter_as_path_re.search(
                    as_path
                ):
                    return False

                if filter_origin_asn_re is not None:
                    if not as_path:
                        return False
                    last_segment = as_path.rsplit(" ", 1)[-1]
                    if not filter_origin_asn_re.search(last_segment):
                        return False

            # 3. Heaviest checks (CIDR / IP Version): Only executed if prefix filters are active
            if needs_prefix_parsing:
                prefix_str: str | None = e.fields.get("prefix")

                if not prefix_str:
                    return False  # Active filter but no prefix data

                # Check version cheaply without instantiating ipaddress objects
                is_ipv6 = ":" in prefix_str
                elem_ip_version = 6 if is_ipv6 else 4

                if (
                    filter_ip_version is not None
                    and filter_ip_version != elem_ip_version
                ):
                    return False

                if any_prefix_filter:
                    try:
                        elem_net = ipaddress.ip_network(prefix_str, strict=False)
                    except ValueError:
                        return False

                    # Prevent TypeError by asserting IP versions match before subnet calculations
                    if filter_exact_net:
                        if (
                            elem_ip_version != filter_exact_net.version
                            or elem_net != filter_exact_net
                        ):
                            return False

                    if filter_sub_net:
                        if (
                            elem_ip_version != filter_sub_net.version
                            or not elem_net.subnet_of(filter_sub_net)  # pyright: ignore[reportArgumentType]
                        ):
                            return False

                    if filter_super_net:
                        if (
                            elem_ip_version != filter_super_net.version
                            or not elem_net.supernet_of(filter_super_net)  # pyright: ignore[reportArgumentType]
                        ):  # pyright: ignore[reportArgumentType]
                            return False

                    if filter_ss_net:
                        if elem_ip_version != filter_ss_net.version:
                            return False
                        if not (
                            elem_net.subnet_of(filter_ss_net)  # pyright: ignore[reportArgumentType]
                            or elem_net.supernet_of(filter_ss_net)  # pyright: ignore[reportArgumentType]
                        ):  # pyright: ignore[reportArgumentType]
                            return False

            return True

        return filter_logic
