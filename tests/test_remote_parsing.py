import importlib

import pytest
from pybgpflux import BGPStreamConfig, BGPStream
import datetime
from tests.pybgpstream_utils import make_bgpstream


def has_pybgpstream():
    return importlib.util.find_spec("pybgpstream") is not None


PARSERS_TO_TEST = [
    pytest.param(
        "pybgpstream",
        id="parser:pybgpstream",
        marks=pytest.mark.skipif(
            not has_pybgpstream(), reason="pybgpstream lib not found"
        ),
    ),
]


@pytest.mark.parametrize("parser", PARSERS_TO_TEST)
def test_remote_parsing(parser: str):
    """Test if the streams are consistent and if they return the same number of elements"""
    
    # Configuration with RIBs from both projects, helpful to spot dropped http connections (helped for pybgpkit)
    config = BGPStreamConfig(
        start_time=datetime.datetime(2010, 9, 1, 0, 0),
        end_time=datetime.datetime(2010, 9, 1, 1, 59),
        collectors=["route-views.wide", "rrc06"],
        data_types=["ribs"],
        parser=parser
    )
    
    pybgpflux_stream = BGPStream.from_config(config)
    assert pybgpflux_stream.parser_cls.supports_remote_parsing, "Parser does not support remote parsing"
    
    pybgpstream_stream = make_bgpstream(config)

    assert sum((1 for _ in pybgpflux_stream)) == sum((1 for _ in pybgpstream_stream))