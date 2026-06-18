from typing import Protocol

from bgpkit.bgpkit_broker import BrokerItem  # pyright: ignore[reportMissingTypeStubs]

from pybgpflux.bgpstreamconfig import BGPStreamConfig


class BrokerError(Exception):
    """Base exception for all BGP Broker abstraction errors."""

    pass


class BrokerQueryError(BrokerError):
    """Raised when a broker query fails due to network, API, or data errors."""

    pass


class BGPBroker(Protocol):
    def query(self, config: BGPStreamConfig) -> list[BrokerItem]: ...
