import bgpkit  # pyright: ignore[reportMissingTypeStubs]
from pybgpflux.bgpstreamconfig import FilterOptions
from pybgpflux.bgpelement import BGPElement
from typing import Any, Iterator, Protocol
import re
import ipaddress
import subprocess as sp
from pybgpflux.utils import dt_from_filepath
import logging

try:
    import pybgpstream  # pyright: ignore[reportMissingTypeStubs]
except ImportError:
    pass


class BGPParser(Protocol):
    filepath: str
    is_rib: bool
    collector: str
    filters: FilterOptions
    supports_remote_parsing: bool

    def __init__(
        self,
        filepath: str,
        is_rib: bool,
        collector: str,
        filters: FilterOptions,
    ) -> None: ...

    def __iter__(self) -> Iterator[BGPElement]: ...


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


class BGPKITParser(BGPParser):
    """Run BGPKIT's CLI `bgpkit-parser` as a subprocess."""

    # In theory bgpkit supports remote parsing, but in practice I get unreliable results
    # due to connection drop when the kmerge is slow (big RIBs)
    supports_remote_parsing = False

    def __init__(
        self,
        filepath: str,
        is_rib: bool,
        collector: str,
        filters: FilterOptions,
    ):
        self.filepath = filepath
        self.parser: sp.Popen[str] | None = None  # placeholder for lazy instantiation
        self.is_rib = is_rib
        self.collector = collector
        self.filters = filters

        # Set timestamp for the same behavior as bgpdump default (timestamp match rib time, not last change)
        self.time = int(dt_from_filepath(self.filepath).timestamp())

    def _convert_rib(self, line: str) -> BGPElement:
        element = line.rstrip().split("|")
        rec_type = element[0]

        if rec_type == "W":
            return BGPElement(
                time=self.time,
                type="W",
                collector=self.collector,
                peer_asn=int(element[3]),
                peer_address=element[2],
                fields={"prefix": element[4]},
            )

        rec_comm = element[10]
        return BGPElement(
            self.time,
            "R",
            self.collector,
            int(element[3]),
            element[2],
            {
                "prefix": element[4],
                "as-path": element[5],
                "next-hop": element[7],
                "communities": rec_comm.split() if rec_comm else [],
            },
        )

    def _convert_upd(self, line: str) -> BGPElement:
        element = line.rstrip().split("|")
        rec_type = element[0]

        if rec_type == "W":
            return BGPElement(
                time=float(element[1]),
                type="W",
                collector=self.collector,
                peer_asn=int(element[3]),
                peer_address=element[2],
                fields={"prefix": element[4]},
            )

        rec_comm = element[10]
        return BGPElement(
            float(element[1]),
            "A",
            self.collector,
            int(element[3]),
            element[2],
            {
                "prefix": element[4],
                "as-path": element[5],
                "next-hop": element[7],
                "communities": rec_comm.split() if rec_comm else [],
            },
        )

    def __iter__(self) -> Iterator[BGPElement]:
        convert = self._convert_rib if self.is_rib else self._convert_upd
        cmd = build_bgpkit_cmd(self.filepath, self.filters)
        self.parser = sp.Popen(cmd, stdout=sp.PIPE, text=True, bufsize=1)
        assert self.parser.stdout is not None

        stream = (convert(line) for line in self.parser.stdout)

        try:
            yield from stream
        finally:
            self.parser.stdout.close()
            self.parser.terminate()
            self.parser.wait()


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


class BGPdumpParser(BGPParser):
    """Run bgpdump as a subprocess."""

    supports_remote_parsing = False

    def __init__(self,
                 filepath: str,
                 is_rib: bool,
                 collector: str, 
                 filters: FilterOptions):
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
                ipaddress.ip_network(f.prefix_super, strict=False) if f.prefix_super else None
            )
            self.filter_ss_net: ipaddress.IPv4Network | ipaddress.IPv6Network | None = (
                ipaddress.ip_network(f.prefix_super_sub, strict=False) if f.prefix_super_sub else None
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
        any_prefix_filter = any([
            filter_exact_net, 
            filter_sub_net, 
            filter_super_net, 
            filter_ss_net
        ])

        # Cache whether we need to touch AS Path fields at all
        needs_path_parsing = filter_as_path_re is not None or filter_origin_asn_re is not None
        
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
                
                if filter_as_path_re is not None and not filter_as_path_re.search(as_path):
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
                    return False # Active filter but no prefix data
                
                # Check version cheaply without instantiating ipaddress objects
                is_ipv6 = ":" in prefix_str
                elem_ip_version = 6 if is_ipv6 else 4
                
                if filter_ip_version is not None and filter_ip_version != elem_ip_version:
                    return False

                if any_prefix_filter:
                    try:
                        elem_net = ipaddress.ip_network(prefix_str, strict=False)
                    except ValueError:
                        return False

                    # Prevent TypeError by asserting IP versions match before subnet calculations
                    if filter_exact_net:
                        if elem_ip_version != filter_exact_net.version or elem_net != filter_exact_net:
                            return False
                            
                    if filter_sub_net:
                        if elem_ip_version != filter_sub_net.version or not elem_net.subnet_of(filter_sub_net): # pyright: ignore[reportArgumentType]
                            return False
                            
                    if filter_super_net:
                        if elem_ip_version != filter_super_net.version or not elem_net.supernet_of(filter_super_net): # pyright: ignore[reportArgumentType]
                            return False
                            
                    if filter_ss_net:
                        if elem_ip_version != filter_ss_net.version:
                            return False
                        if not (elem_net.subnet_of(filter_ss_net) or elem_net.supernet_of(filter_ss_net)): # pyright: ignore[reportArgumentType]
                            return False

            return True

        return filter_logic


def generate_bgpstream_filters(f: FilterOptions) -> str | None:
    """Generates a filter string compatible with BGPStream's C parser from a BGPStreamConfig object."""
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


def build_bgpkit_cmd(filepath: str, filters: FilterOptions) -> list[str]:
    # Start with the base command and file path
    cmd = ["bgpkit-parser", filepath]

    # 1. Simple Integer/String Mappings
    if filters.origin_asn:
        cmd.extend(["--origin-asn", str(filters.origin_asn)])

    if filters.peer_ip:
        cmd.extend(["--peer-ip", str(filters.peer_ip)])

    if filters.peer_asn:
        cmd.extend(["--peer-asn", str(filters.peer_asn)])

    if filters.as_path:
        cmd.extend(["--as-path", filters.as_path])

    # 2. Prefix Logic (Handling super/sub flags)
    # We prioritize the most specific prefix field provided
    prefix_val = None
    if filters.prefix:
        prefix_val = filters.prefix
    elif filters.prefix_super:
        prefix_val = filters.prefix_super
        cmd.append("--include-super")
    elif filters.prefix_sub:
        prefix_val = filters.prefix_sub
        cmd.append("--include-sub")
    elif filters.prefix_super_sub:
        prefix_val = filters.prefix_super_sub
        cmd.extend(["--include-super", "--include-sub"])

    if prefix_val:
        cmd.extend(["--prefix", prefix_val])

    # 3. List-based filters (using the --filter "key=value" format)
    if filters.peer_ips:
        # If it's a list, we add a generic filter for the comma-separated string
        ips_str = ",".join(str(ip) for ip in filters.peer_ips)
        cmd.extend(["--filter", f"peer_ips={ips_str}"])

    # 4. Enums and Literals
    if filters.update_type:
        # CLI accepts 'a' for announce and 'w' for withdraw
        val = "a" if filters.update_type == "announce" else "w"
        cmd.extend(["--elem-type", val])

    if filters.ip_version:
        if filters.ip_version == 4:
            cmd.append("--ipv4-only")
        elif filters.ip_version == 6:
            cmd.append("--ipv6-only")

    return cmd
