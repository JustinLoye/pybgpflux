from pybgpflux.bgpstreamconfig import FilterOptions
from pybgpflux.parsers import BGPdumpParser, BGPKITParser

filters = FilterOptions(peer_asn=2497)

bgpdump_parser = BGPdumpParser(
    collector="route-views.wide",
    is_rib=False,
    filepath="updates.20100901.0000.bz2",
    filters=filters,
)

bgpkit_parser = BGPKITParser(
    collector="route-views.wide",
    is_rib=False,
    filepath="updates.20100901.0000.bz2",
    filters=filters,
)

assert sum(1 for _ in bgpdump_parser) == sum(1 for _ in bgpkit_parser)
    