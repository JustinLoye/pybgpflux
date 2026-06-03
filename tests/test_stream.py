import pytest
import datetime
from itertools import pairwise
from pybgpflux import BGPStream, BGPStreamConfig
import pybgpstream
from tests.pybgpstream_utils import make_bgpstream
import os

cache_dir = "cache"
os.makedirs(cache_dir, exist_ok=True)


@pytest.fixture
def config():
    return BGPStreamConfig(
        start_time=datetime.datetime(2010, 9, 1, 0, 0),
        end_time=datetime.datetime(2010, 9, 1, 1, 59),
        collectors=["route-views.sydney", "route-views.wide"],
        data_types=["updates"],
    )


@pytest.fixture
def config_with_rib():
    """Configuration with RIBs from both projects."""
    rib_config = BGPStreamConfig(
        start_time=datetime.datetime(2010, 9, 1, 0, 0),
        end_time=datetime.datetime(2010, 9, 1, 9, 0),
        collectors=["route-views.wide", "rrc06"],
        data_types=["ribs"],
    )
    yield rib_config


@pytest.fixture
def config_with_cache():
    """Configuration with a temporary cache directory."""
    cache_config = BGPStreamConfig(
        start_time=datetime.datetime(2010, 9, 1, 0, 0),
        end_time=datetime.datetime(2010, 9, 1, 1, 59),
        collectors=["route-views.sydney", "route-views.wide"],
        data_types=["updates"],
        cache_dir=cache_dir,
        max_concurrent_downloads=8,
    )
    yield cache_config


@pytest.fixture
def pybgpflux_stream(config):
    """A fixture that returns a BGPStream object."""
    return BGPStream.from_config(config)


@pytest.fixture
def pybgpstream_stream(config):
    """A fixture that returns a pybgpstream.BGPStream object."""
    return make_bgpstream(config)


@pytest.fixture
def pybgpflux_stream_with_rib(config_with_rib):
    """A BGPStream object using the config with ribs."""
    return BGPStream.from_config(config_with_rib)


@pytest.fixture
def pybgpstream_stream_with_rib(config_with_rib):
    """A pybgpstream.BGPStream object using the config with ribs."""
    return make_bgpstream(config_with_rib)


@pytest.fixture
def pybgpflux_stream_with_cache(config_with_cache):
    """A BGPStream object using the config with cache."""
    return BGPStream.from_config(config_with_cache)


@pytest.fixture
def pybgpstream_stream_with_cache(config_with_cache):
    """A pybgpstream.BGPStream object using the config with cache."""
    return make_bgpstream(config_with_cache)


def test_pybgpflux(pybgpflux_stream, pybgpstream_stream, config):
    """Test if the streamw are consistent and if they return the same number of elements"""
    assert validate_stream(pybgpflux_stream, config) == validate_stream(
        pybgpstream_stream, config
    )


def test_pybgpflux_with_rib(
    pybgpflux_stream_with_rib, pybgpstream_stream_with_rib, config_with_rib
):
    """Test if the streams are consistent and if they return the same number of elements (WITH CACHE)"""
    assert validate_stream(
        pybgpflux_stream_with_rib, config_with_rib
    ) == validate_stream(pybgpstream_stream_with_rib, config_with_rib)


def test_pybgpflux_with_cache(
    pybgpflux_stream_with_cache, pybgpstream_stream_with_cache, config_with_cache
):
    """Test if the streams are consistent and if they return the same number of elements (WITH CACHE)"""
    assert validate_stream(
        pybgpflux_stream_with_cache, config_with_cache
    ) == validate_stream(
        pybgpstream_stream_with_cache, config_with_cache
    )


def test_pybgpflux_with_tz(pybgpflux_stream, config):
    """Test if the stream is consistent when changing the timezone"""
    import os
    import time

    os.environ["TZ"] = "UTC"
    time.tzset()
    utc_elems = validate_stream(pybgpflux_stream, config)

    os.environ["TZ"] = "Asia/Tokyo"
    time.tzset()
    tokyo_elems = validate_stream(pybgpflux_stream, config)

    assert utc_elems == tokyo_elems


def validate_stream(
    stream: BGPStream | pybgpstream.BGPStream, config: BGPStreamConfig
):
    """Test if the output of `stream` is consistent with `config`"""

    types = set()
    collectors = set()
    peer_asns = set()
    times = []

    for i, elem in enumerate(stream):
        types.add(elem.type)
        collectors.add(elem.collector)
        peer_asns.add(elem.peer_asn)
        times.append(elem.time)

    assert i > 0

    if "ribs" in config.data_types:
        assert "R" in types
    if "updates" in config.data_types:
        assert "W" in types and "A" in types

    assert collectors == set(config.collectors)

    if config.filters and config.filters.peer_asn:
        assert peer_asns == set(config.filters.peer_asn)

    assert all([a <= b for a, b in pairwise(times)])
        
    assert times[0] >= config.start_time.timestamp()
    assert times[-1] <= config.end_time.timestamp()

    return i
