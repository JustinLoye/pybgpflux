"""Offline unit tests for the broker-level RIB period filter."""

import datetime

import pytest
from bgpkit.bgpkit_broker import BrokerItem  # pyright: ignore[reportMissingTypeStubs]

from pybgpflux.brokers.bgpbroker import filter_rib_period, item_datetime


BASE = datetime.datetime(2010, 9, 1, tzinfo=datetime.timezone.utc)


def make_item(
    collector: str, data_type: str, offset: datetime.timedelta, iso_ts: bool = True
) -> BrokerItem:
    """Build a synthetic broker item `offset` after 2010-09-01T00:00:00Z."""
    ts = BASE + offset
    name = "rib" if data_type == "ribs" else "updates"
    return BrokerItem(
        ts_start=ts.replace(tzinfo=None).isoformat()
        if iso_ts
        else str(int(ts.timestamp())),
        ts_end=ts.replace(tzinfo=None).isoformat(),
        collector_id=collector,
        data_type=data_type,
        url=f"http://example.org/{collector}/{name}.{ts:%Y%m%d.%H%M}.bz2",
        rough_size=0,
        exact_size=0,
    )


def make_items(hours: list[int], data_type: str = "ribs") -> list[BrokerItem]:
    return [
        make_item(collector, data_type, datetime.timedelta(hours=h))
        for h in hours
        for collector in ("rrc06", "route-views.wide")
    ]


def kept_hours(items: list[BrokerItem], collector: str) -> list[float]:
    return [
        (item_datetime(item) - BASE).total_seconds() / 3600
        for item in items
        if item.collector_id == collector
    ]


@pytest.mark.parametrize("iso_ts", [True, False])
def test_item_datetime_accepts_both_broker_formats(iso_ts: bool):
    """BGPKIT stamps items with an ISO string, other brokers with an epoch."""
    item = make_item("rrc06", "ribs", datetime.timedelta(hours=7), iso_ts=iso_ts)
    assert item_datetime(item) == BASE + datetime.timedelta(hours=7)


def test_item_datetime_falls_back_to_filename():
    item = make_item("rrc06", "ribs", datetime.timedelta(hours=7))
    item.ts_start = "not a timestamp"
    assert item_datetime(item) == BASE + datetime.timedelta(hours=7)


def test_none_period_keeps_everything():
    items = make_items([0, 2, 4, 6])
    assert filter_rib_period(items, None) == items


@pytest.mark.parametrize(
    "period", [datetime.timedelta(0), datetime.timedelta(hours=-2)]
)
def test_non_positive_period_keeps_first_rib_only(period: datetime.timedelta):
    kept = filter_rib_period(make_items([0, 2, 4, 6]), period)
    assert kept_hours(kept, "rrc06") == [0]
    assert kept_hours(kept, "route-views.wide") == [0]


def test_positive_period_skips_ribs_inside_the_period():
    # RIBs every 2 hours, keep at most one every 12 hours
    kept = filter_rib_period(
        make_items(list(range(0, 48, 2))), datetime.timedelta(hours=12)
    )
    assert kept_hours(kept, "rrc06") == [0, 12, 24, 36]
    assert kept_hours(kept, "route-views.wide") == [0, 12, 24, 36]


def test_period_is_measured_from_the_last_kept_rib():
    """An irregular RIB schedule must not drift the window forward."""
    kept = filter_rib_period(
        make_items([0, 11, 13, 23, 25]), datetime.timedelta(hours=12)
    )
    assert kept_hours(kept, "rrc06") == [0, 13, 25]


def test_collectors_are_independent():
    items = [
        make_item("rrc06", "ribs", datetime.timedelta(hours=h)) for h in (0, 6, 12)
    ] + [
        make_item("route-views.wide", "ribs", datetime.timedelta(hours=h))
        for h in (3, 9)
    ]
    items.sort(key=item_datetime)

    kept = filter_rib_period(items, datetime.timedelta(hours=6))
    assert kept_hours(kept, "rrc06") == [0, 6, 12]
    assert kept_hours(kept, "route-views.wide") == [3, 9]


def test_updates_are_never_dropped():
    items = make_items([0, 2, 4], data_type="ribs") + make_items(
        [0, 2, 4], data_type="updates"
    )
    items.sort(key=item_datetime)

    kept = filter_rib_period(items, datetime.timedelta(hours=12))
    assert sum(1 for item in kept if item.data_type == "ribs") == 2  # one per collector
    assert sum(1 for item in kept if item.data_type == "updates") == 6


def test_order_is_preserved():
    items = make_items(list(range(0, 24, 2)), data_type="ribs")
    items.sort(key=item_datetime)

    kept = filter_rib_period(items, datetime.timedelta(hours=5))
    assert kept == sorted(kept, key=item_datetime)


def test_empty_input():
    assert filter_rib_period([], datetime.timedelta(hours=12)) == []
