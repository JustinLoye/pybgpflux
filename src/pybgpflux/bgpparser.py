import bgpkit
from pybgpflux.bgpstreamconfig import FilterOptions
from pybgpflux.bgpelement import BGPElement
from typing import Iterator, Protocol
import re
import ipaddress
import subprocess as sp
from pybgpflux.utils import dt_from_filepath
import logging

try:
    import pybgpstream
except ImportError:
    pass


class BGPParser(Protocol):
    filepath: str
    is_rib: bool
    collector: str
    filters: FilterOptions
    supports_remote_parsing: bool

    def __iter__(self) -> Iterator[BGPElement]: ...


class PyBGPKITParser(BGPParser):
    """Use BGPKIT Python bindings (default parser). Slower than other alternatives but easier to ship (no system dependencies)."""
    
    # In theory bgpkit supports remote parsing, but in practice I get unreliable results
    # due to connection drop when the kmerge is slow (big RIBs)
    supports_remote_parsing=False

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
        self.filters: dict = filters.model_dump(exclude_unset=True, exclude_none=True)
        # cast int ipv to pybgpkit ipv4 or ipv6 string
        if "ip_version" in self.filters:
            ipv_int = self.filters["ip_version"]
            if ipv_int:
                self.filters["ip_version"] = f"ipv{ipv_int}"
        if self.filters.get("peer_asn"):
            self.filters["peer_asn"] = str(self.filters["peer_asn"])
        if self.filters.get("origin_asn"):
            self.filters["origin_asn"] = str(self.filters["origin_asn"])
        if self.filters.get("update_type"):
            val = self.filters.pop("update_type")
            self.filters["type"] = val
        if self.filters.get("peer_ips"):
            self.filters["peer_ips"] = ", ".join(self.filters["peer_ips"])

    def _convert(self, element) -> BGPElement:
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
        parser = bgpkit.Parser(self.filepath, filters=self.filters)
        for elem in parser:
            yield self._convert(elem)


class BGPKITParser(BGPParser):
    """Run BGPKIT's CLI `bgpkit-parser` as a subprocess."""
    
    # In theory bgpkit supports remote parsing, but in practice I get unreliable results
    # due to connection drop when the kmerge is slow (big RIBs)
    supports_remote_parsing=False

    def __init__(
        self,
        filepath: str,
        is_rib: bool,
        collector: str,
        filters: FilterOptions | str | None = None,
    ):
        self.filepath = filepath
        self.parser = None  # placeholder for lazy instantiation
        self.is_rib = is_rib
        self.collector = collector
        self.filters = filters

        # Set timestamp for the same behavior as bgpdump default (timestamp match rib time, not last change)
        self.time = int(dt_from_filepath(self.filepath).timestamp())

    def __iter__(self):
        cmd = build_bgpkit_cmd(self.filepath, self.filters)
        self.parser = sp.Popen(cmd, stdout=sp.PIPE, text=True, bufsize=1)

        stream = (self._convert(line) for line in self.parser.stdout)

        try:
            yield from stream
        finally:
            # Cleanup happens whether exhausted or abandoned
            self.parser.stdout.close()
            self.parser.terminate()
            self.parser.wait()  # Reap the zombie process

    def _convert(self, element: str):
        element = element.rstrip().split("|")
        rec_type = element[0]

        # 1. Handle Withdrawals (W)
        # Structure: Type|Time|PeerIP|PeerAS|Prefix
        if rec_type == "W":
            return BGPElement(
                time=self.time,  # force RIB filename timestamp instead of last changed
                type="W",
                collector=self.collector,
                peer_asn=int(element[3]),
                peer_address=element[2],
                fields={"prefix": element[4]},
            )

        # 2. Handle Announcements (A)
        # Structure: Type|Time|PeerIP|PeerAS|Prefix|ASPath|Origin|NextHop|...|Communities|...
        # bgpkit-parser index mapping:
        # 0: Type, 1: Time, 2: PeerIP, 3: PeerAS, 4: Prefix,
        # 5: ASPath, 7: NextHop, 10: Communities

        rec_comm = element[10]

        return BGPElement(
            # bgpkit outputs 'A' for both Updates and RIB entries.
            self.time,
            "R" if self.is_rib else rec_type,
            self.collector,
            # float(element[1]),
            int(element[3]),
            element[2],
            {
                "prefix": element[4],
                "as-path": element[5],
                "next-hop": element[7],
                # Fast check for empty communities
                "communities": rec_comm.split() if rec_comm else [],
            },
        )


class PyBGPStreamParser(BGPParser):
    """Use pybgpstream as a MRT parser with the `singlefile` data interface"""
    
    supports_remote_parsing=True

    def __init__(
        self,
        filepath: str,
        is_rib: bool,
        collector: str,
        filters: FilterOptions,
        *args,
        **kwargs,
    ):
        self.filepath = filepath
        self.collector = collector
        self.filters = filters

    def _iter_normal(self):
        """when there is no filter or filters are supported by pybgpstream"""
        stream = pybgpstream.BGPStream(
            data_interface="singlefile",
            filter=generate_bgpstream_filters(self.filters) if self.filters else None,
        )
        stream.set_data_interface_option("singlefile", "rib-file", self.filepath)

        for elem in stream:
            elem.collector = self.collector
            yield elem

    def _iter_python_filter(self):
        """when filters are not supported by pybgpstream, filter from the python side"""
        bgpstream_filter = generate_bgpstream_filters(self.filters)
        stream = pybgpstream.BGPStream(
            data_interface="singlefile",
            filter=bgpstream_filter if bgpstream_filter else None,
        )
        stream.set_data_interface_option("singlefile", "rib-file", self.filepath)
        peer_ips = set(self.filters.peer_ips)

        for elem in stream:
            if elem.peer_address not in peer_ips:
                continue
            elem.collector = self.collector
            yield elem

    def __iter__(self):
        if not self.filters.peer_ip and not self.filters.peer_ips:
            return self._iter_normal()
        else:
            if self.filters.peer_ip:
                self.filters.peer_ips = [self.filters.peer_ip]
            return self._iter_python_filter()


class BGPdumpParser(BGPParser):
    """Run bgpdump as a subprocess."""
    
    supports_remote_parsing = False

    def __init__(self, filepath, is_rib, collector, filters):
        self.filepath = filepath
        self.collector = collector

        self._init_filters(filters)

    def __iter__(self):
        self.parser = sp.Popen(
            ["bgpdump", "-m", "-v", self.filepath], stdout=sp.PIPE, text=True, bufsize=1
        )

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

    def _convert(self, element: str):
        # Extract type once to avoid repeated list lookups
        element = element.rstrip().split("|")
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

    def _init_filters(self, f: FilterOptions):
        # 1. Pre-process sets for O(1) lookups and compile Regex
        # self.peer_asns = set([f.peer_asn]) if f.peer_asn else (set(f.peer_ips) if f.peer_ips else None)
        if not f.model_dump(exclude_unset=True):
            self._filter_func = None

        self.peer_asn = f.peer_asn

        # Peer IPs (handles both single and list)
        self.peer_ips = None
        if f.peer_ip:
            self.peer_ips = {str(f.peer_ip)}
        elif f.peer_ips:
            self.peer_ips = {str(ip) for ip in f.peer_ips}

        self.origin_asn = str(f.origin_asn) if f.origin_asn else None
        self.update_type = (
            f.update_type[0].upper() if f.update_type else None
        )  # 'A' or 'W'
        self.ip_version = f.ip_version

        # Regex and CIDR objects
        self.as_path_re = re.compile(f.as_path) if f.as_path else None
        self.exact_net = ipaddress.ip_network(f.prefix) if f.prefix else None
        self.sub_net = ipaddress.ip_network(f.prefix_sub) if f.prefix_sub else None
        self.super_net = (
            ipaddress.ip_network(f.prefix_super) if f.prefix_super else None
        )
        self.ss_net = (
            ipaddress.ip_network(f.prefix_super_sub) if f.prefix_super_sub else None
        )

        # 2. Build the optimized filter function
        self._filter_func = self._compile_filter()

    def _compile_filter(self):
        # Localize variables to the closure to avoid 'self' lookups in the loop
        p_asn = self.peer_asn
        p_ips = self.peer_ips
        o_asn = self.origin_asn
        u_type = self.update_type
        version = self.ip_version
        path_re = self.as_path_re

        e_net = self.exact_net
        sub_n = self.sub_net
        sup_n = self.super_net
        ss_n = self.ss_net

        def filter_logic(e: BGPElement) -> bool:
            # 1. Cheap checks first (Integers and Strings)
            if p_asn is not None and int(e.peer_asn) != p_asn:
                return False
            if p_ips is not None and e.peer_address not in p_ips:
                return False
            if u_type is not None and e.type != u_type:
                return False

            # 2. String processing (Origin ASN and AS Path)
            # Use .get() or direct access depending on your confidence in 'fields' content
            as_path = e.fields.get("as-path", "")
            if o_asn is not None:
                if not as_path or as_path.rsplit(" ", 1)[-1] != o_asn:
                    return False
            if path_re is not None and not path_re.search(as_path):
                return False

            # 3. CIDR / IP Logic (Expensive)
            prefix_str = e.fields.get("prefix")
            if version is not None:
                # Fast check for IP version without parsing
                is_v6 = ":" in prefix_str if prefix_str else False
                if (version == 6 and not is_v6) or (version == 4 and is_v6):
                    return False

            if e_net or sub_n or sup_n or ss_n:
                if not prefix_str:
                    return False
                net = ipaddress.ip_network(prefix_str)
                if e_net and net != e_net:
                    return False
                if sub_n and not net.subnet_of(sub_n):
                    return False
                if sup_n and not net.supernet_of(sup_n):
                    return False
                if ss_n and not (net.subnet_of(ss_n) or net.supernet_of(ss_n)):
                    return False

            return True

        return filter_logic


def generate_bgpstream_filters(f: FilterOptions) -> str | None:
    """Generates a filter string compatible with BGPStream's C parser from a BGPStreamConfig object."""
    if not f:
        return None
    if not f.model_dump(exclude_unset=True):
        return None

    parts = []

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
        logging.info(
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
