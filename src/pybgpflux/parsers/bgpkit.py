import subprocess as sp
from typing import Iterator

from pybgpflux.bgpelement import BGPElement
from pybgpflux.bgpstreamconfig import FilterOptions
from pybgpflux.parsers.bgpparser import BGPParser
from pybgpflux.utils import dt_from_filepath


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
        self.time = dt_from_filepath(self.filepath).timestamp()

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
