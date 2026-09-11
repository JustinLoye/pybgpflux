from abc import ABC, abstractmethod
import datetime

from bgpkit.bgpkit_broker import BrokerItem  # pyright: ignore[reportMissingTypeStubs]

from pybgpflux.bgpstreamconfig import BGPStreamConfig
from pybgpflux.utils import dt_from_filepath


class BrokerError(Exception):
    """Base exception for all BGP Broker abstraction errors."""

    pass


class BrokerQueryError(BrokerError):
    """Raised when a broker query fails due to network, API, or data errors."""

    pass


def item_datetime(item: BrokerItem) -> datetime.datetime:
    """Timezone-aware UTC start time of a broker item.

    `BrokerItem.ts_start` is an unparsed string whose format depends on the
    broker (ISO-8601 for BGPKIT, epoch seconds for others), so both are accepted
    and the timestamp embedded in the archive filename is used as a fallback.
    """
    ts_start = item.ts_start
    try:
        if ts_start.isdigit():
            dt = datetime.datetime.fromtimestamp(
                int(ts_start), tz=datetime.timezone.utc
            )
        else:
            dt = datetime.datetime.fromisoformat(ts_start)
    except (AttributeError, TypeError, ValueError):
        return dt_from_filepath(item.url)

    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


def filter_rib_period(
    items: list[BrokerItem], rib_period: datetime.timedelta | None
) -> list[BrokerItem]:
    """Enforce a minimum archive-time interval between the RIBs of a collector.

    Mirrors the `bgpreader -p` / PyBGPStream `rib-period` filter. Updates are
    never dropped, and each collector is treated independently.

    Args:
        items: Broker results, **sorted by ascending start time**.
        rib_period: `None` keeps every RIB; a zero or negative interval keeps
            only the first RIB of each collector; a positive interval keeps the
            first RIB, then skips the following ones until that much archive
            time has elapsed since the last kept RIB.

    Returns:
        The kept items, in the order they were given.
    """
    if rib_period is None:
        return items

    only_first = rib_period <= datetime.timedelta(0)

    kept: list[BrokerItem] = []
    last_kept: dict[str, datetime.datetime] = {}
    for item in items:
        if item.data_type != "ribs":
            kept.append(item)
            continue

        ts = item_datetime(item)
        previous = last_kept.get(item.collector_id)
        # Skip RIBs that are still within the period of the last kept one
        if previous is not None and (only_first or ts - previous < rib_period):
            continue

        last_kept[item.collector_id] = ts
        kept.append(item)

    return kept


class BGPBroker(ABC):
    """Common behaviour shared by every BGP archive broker.

    Subclasses implement `_query`; the public `query` wraps it so that
    broker-level query options work the same way for every backend, whether the
    broker is driven by `BGPStream` or used standalone.
    """

    def query(self, config: BGPStreamConfig) -> list[BrokerItem]:
        """Return the archive files matching `config`, sorted by start time.

        Raises:
            BrokerQueryError: If the query fails or matches no archive.
        """
        items = self._query(config)
        items.sort(key=item_datetime)
        return filter_rib_period(items, config.rib_period)

    @abstractmethod
    def _query(self, config: BGPStreamConfig) -> list[BrokerItem]:
        """Backend-specific query. `data_type` must be normalized to the plural
        form (`ribs`, `updates`) before returning."""
        ...
