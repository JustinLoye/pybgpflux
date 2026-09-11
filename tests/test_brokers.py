from collections import defaultdict
import datetime
from itertools import pairwise
from typing import Literal
import pytest
from urllib.parse import urlsplit

from pybgpflux.bgpstreamconfig import BGPStreamConfig
from pybgpflux.brokers import (
    BGPBroker,
    BGPKITBroker,
    BGPStreamBroker,
    BrokerItem,
    item_datetime,
)


def normalize_url(url: str) -> str:
    """Remove scheme from url"""
    parts = urlsplit(url)
    return parts._replace(scheme="").geturl()


@pytest.mark.parametrize(
    "collectors",
    [
        ["rrc06"],
        ["route-views.wide"],
        ["rrc06", "route-views.wide"],
    ],
)
@pytest.mark.parametrize(
    "data_types",
    [
        ["ribs"],
        ["updates"],
        ["ribs", "updates"],
    ],
)
def test_broker_consistency(
    collectors: list[str], data_types: list[Literal["ribs", "updates"]]
):
    """
    Test that BGPKITBroker and BGPStreamBroker return identical datasets
    for given combinations of collectors and data types.
    """
    
    # The window must be longer than the ~2h of data a BGPStream broker fits in
    # one response, otherwise a truncated result would pass unnoticed.
    config = BGPStreamConfig(
        start_time=datetime.datetime(2010, 9, 1, 0, 0),
        end_time=datetime.datetime(2010, 9, 1, 5, 59),
        collectors=collectors,
        data_types=data_types,
    )

    bgpkit_broker = BGPKITBroker()
    bgpstream_broker = BGPStreamBroker(url="https://broker.bgpstream.caida.org/v2")

    bgpkit_items = bgpkit_broker.query(config)
    bgpstream_items = bgpstream_broker.query(config)

    # Assert matching dataset counts
    assert len(bgpkit_items) == len(bgpstream_items), (
        f"Item count mismatch for collectors={collectors}, types={data_types}. "
        f"BGPKIT: {len(bgpkit_items)}, BGPStream: {len(bgpstream_items)}"
    )

    # Compare element for element
    bgpkit_items.sort(key=lambda item: item.url)
    bgpstream_items.sort(key=lambda item: item.url)
    for i, (kit_item, stream_item) in enumerate(zip(bgpkit_items, bgpstream_items)):
        # url normalization is needed because bgpstream returns http, bgpkit returns https
        assert normalize_url(kit_item.url) == normalize_url(stream_item.url), (
            f"URL mismatch at index {i}"
        )
        assert kit_item.collector_id == stream_item.collector_id, (
            f"Collector mismatch for {kit_item.url}"
        )
        assert kit_item.data_type == stream_item.data_type, (
            f"Data type mismatch for {kit_item.url}"
        )


COLLECTORS = ["rrc06", "route-views.wide"]

BROKERS = [
    pytest.param(BGPKITBroker(), id="broker:bgpkit"),
    pytest.param(
        BGPStreamBroker(url="https://broker.bgpstream.caida.org/v2"),
        id="broker:bgpstream",
    ),
]


def group_by_collector(
    items: list[BrokerItem], data_type: str
) -> dict[str, list[BrokerItem]]:
    grouped: dict[str, list[BrokerItem]] = defaultdict(list)
    for item in items:
        if item.data_type == data_type:
            grouped[item.collector_id].append(item)
    return grouped


def rib_period_config(rib_period: datetime.timedelta | None) -> BGPStreamConfig:
    # A two-day window, wide enough that both a collector dumping RIBs every two
    # hours (route-views.wide) and every eight hours (rrc06) get several of them.
    return BGPStreamConfig(
        start_time=datetime.datetime(2010, 9, 1, 0, 0),
        end_time=datetime.datetime(2010, 9, 2, 23, 59),
        collectors=COLLECTORS,
        data_types=["ribs", "updates"],
        rib_period=rib_period,
    )


@pytest.mark.parametrize("broker", BROKERS)
def test_broker_results_are_time_sorted(broker: BGPBroker):
    """query() guarantees ascending start times, which BGPStream relies on."""
    items = broker.query(rib_period_config(None))
    times = [item_datetime(item) for item in items]
    assert times == sorted(times)


@pytest.mark.parametrize("broker", BROKERS)
@pytest.mark.parametrize(
    "rib_period",
    [datetime.timedelta(0), datetime.timedelta(hours=-2)],
    ids=["zero", "negative"],
)
def test_rib_period_non_positive(broker: BGPBroker, rib_period: datetime.timedelta):
    """A zero or negative period only produces the first RIB of each collector"""
    reference = broker.query(rib_period_config(None))
    items = broker.query(rib_period_config(rib_period))

    ribs = group_by_collector(items, "ribs")
    reference_ribs = group_by_collector(reference, "ribs")
    for rc in COLLECTORS:
        assert len(reference_ribs[rc]) > 1, "window too small to exercise the filter"
        assert len(ribs[rc]) == 1
        # The RIB kept is the earliest one available
        assert ribs[rc][0].url == reference_ribs[rc][0].url

    # Updates are untouched
    assert group_by_collector(items, "updates") == group_by_collector(
        reference, "updates"
    )


@pytest.mark.parametrize("broker", BROKERS)
def test_rib_period_positive(broker: BGPBroker):
    """A positive period spaces the RIBs of a collector by at least that much"""
    rib_period = datetime.timedelta(hours=12)
    reference = broker.query(rib_period_config(None))
    items = broker.query(rib_period_config(rib_period))

    ribs = group_by_collector(items, "ribs")
    reference_ribs = group_by_collector(reference, "ribs")
    for rc in COLLECTORS:
        assert ribs[rc], f"no RIB left for {rc}"
        assert len(ribs[rc]) < len(reference_ribs[rc]), (
            "window too small to exercise the filter"
        )
        # The first RIB is always kept, the following ones are spaced out
        assert ribs[rc][0].url == reference_ribs[rc][0].url
        times = [item_datetime(item) for item in ribs[rc]]
        assert all(b - a >= rib_period for a, b in pairwise(times))

    assert group_by_collector(items, "updates") == group_by_collector(
        reference, "updates"
    )
