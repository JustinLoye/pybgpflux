import datetime
from typing import Literal
import pytest
from urllib.parse import urlsplit

from pybgpflux.bgpstreamconfig import BGPStreamConfig
from pybgpflux.brokers import (
    BGPKITBroker,
    BGPStreamBroker,
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
    
    config = BGPStreamConfig(
        start_time=datetime.datetime(2010, 9, 1, 0, 0),
        end_time=datetime.datetime(2010, 9, 1, 0, 59),
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
