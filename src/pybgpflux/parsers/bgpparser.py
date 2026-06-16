from typing import Iterator, Protocol

from pybgpflux.bgpstreamconfig import FilterOptions
from pybgpflux.bgpelement import BGPElement

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
